"""实现 WorkspaceManagerBase，管理中立沙箱的缓存、并发、续期与回收。

核心能力
- get_workspace：核对执行身份，逐绑定串行构建；不同实例可并行初始化。
- renew_workspace：维护执行使用计数和周期续期，失败终止本次执行而不重放工具。
- close_workspaces / destroy_workspaces：协调缓存连接与 Core 生命周期。
- close_all：包括正在构建的实例，关闭所有当前跟踪的环境，可再次使用 Manager。
- 原生上下文接口启动/停止空闲回收任务，不依赖任何厂商 Manager 私有状态。
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import Callable

from agentscope.app.message_bus import MessageBus, MessageBusKeys
from agentscope.app.workspace_manager import IsolationPolicy, WorkspaceManagerBase

from egis_agentkit.client._tracing import annotate_runtime, trace_phase
from egis_agentkit.client.mcp._gateway_mcp_client_override import (
    AuthorizedGatewayMCPClient,
)
from egis_agentkit.client.nacos._settings import (
    SandboxWorkspaceConfig,
    WorkspaceLockConfig,
    WorkspaceProbeConfig,
)
from egis_agentkit.client.sandbox import SandboxClient, SandboxInstanceNotFound
from egis_agentkit.core.workspace._egis_sandbox_workspace import EgisSandboxWorkspace
from egis_agentkit.core.workspace._identity import sandbox_binding_for, workspace_id_for
from egis_agentkit.errors import Conflict, Forbidden, NotFound, WorkspaceProbeError
from egis_agentkit.storage.project._files import ProjectFileStorage

logger = logging.getLogger(__name__)


class EgisSandboxManager(WorkspaceManagerBase):
    """管理工作区实例，不创建厂商连接配置或持有 SDK 对象。"""

    def __init__(
        self,
        workspace_root: str | Path,
        settings: SandboxWorkspaceConfig,
        *,
        client_factory: Callable[[], SandboxClient],
        read_guard_factory: Callable,
        bus: MessageBus,
        probe: WorkspaceProbeConfig,
        lock: WorkspaceLockConfig,
    ):
        super().__init__(isolation=IsolationPolicy.PER_USER)
        self.files = ProjectFileStorage(Path(workspace_root))
        self.settings = settings
        self.bus = bus
        self.probe_settings = probe
        self.lock_settings = lock
        self._failed_workspaces = {}
        self._read_guard_factory = read_guard_factory
        self._client_factory = client_factory
        self._cache: dict[str, tuple[EgisSandboxWorkspace, float]] = {}
        self._active_workspaces: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._workspace_locks: dict[
            str | tuple[str, ...], tuple[asyncio.Lock, int]
        ] = {}
        self._building: dict[str, tuple[str, str]] = {}
        self._closing_workspaces: set[str] = set()
        self._destroying_projects: set[tuple[str, str]] = set()
        self._closing_all = False
        self._ttl = settings.ttl_seconds
        self._sweep_task = None

    async def assign_workspace_id(
        self, *, user_id: str, agent_id: str, session_id: str
    ) -> str:
        """原生创建流程和企业会话始终采用同一用户逻辑身份。"""
        return workspace_id_for(user_id)

    @asynccontextmanager
    async def _workspace_lock(self, identifier):
        """按执行绑定或关闭作用域互斥，计数等待者，最后一位离开时删除锁记录。"""
        async with self._lock:
            lock, count = self._workspace_locks.get(identifier, (asyncio.Lock(), 0))
            self._workspace_locks[identifier] = (lock, count + 1)
        try:
            async with lock:
                yield
        finally:
            async with self._lock:
                _, count = self._workspace_locks[identifier]
                if count == 1:
                    self._workspace_locks.pop(identifier)
                else:
                    self._workspace_locks[identifier] = (lock, count - 1)

    @staticmethod
    def _instance_key(identifier):
        return "ak:workspace:" + identifier

    @asynccontextmanager
    async def _distributed_lock(self, identifier):
        """复用集群 PG 租约；预算只约束等待，加锁后续期与失锁取消由总线保证。"""
        async with AsyncExitStack() as stack:
            try:
                async with asyncio.timeout(self.lock_settings.wait_timeout_seconds):
                    await stack.enter_async_context(
                        self.bus.acquire_lock(
                            self._instance_key(identifier),
                            ttl_secs=self.lock_settings.lease_seconds,
                        )
                    )
            except TimeoutError:
                raise WorkspaceProbeError("lease", "busy", retryable=True) from None
            yield

    @asynccontextmanager
    async def _instance_lock(self, identifier):
        """进程内锁只保护缓存；同绑定的远程生命周期操作另外取得分布式锁。"""
        async with self._workspace_lock(identifier):
            async with self._distributed_lock(identifier):
                yield

    async def _session_in_use(self, workspace):
        """覆盖 get_workspace 到回复结束之间的整个原生执行阶段，包括工具装配。"""
        return bool(workspace.egis_session_id) and await self.bus.is_locked(
            MessageBusKeys.session_lock(workspace.egis_session_id)
        )

    def _require_open(self, user_id, project_id):
        if (
            self._closing_all
            or workspace_id_for(user_id) in self._closing_workspaces
            or (user_id, project_id) in self._destroying_projects
        ):
            raise Conflict("工作区正在关闭，请在当前操作结束后重试")

    @asynccontextmanager
    async def renew_workspace(self, workspace):
        """从回复开始到结束持有使用计数，续期失败时终止执行而不重放命令。"""
        identifier = workspace.runtime_binding_id
        async with self._distributed_lock(identifier):
            # 1. 先在回收锁内登记使用者，不能租用已经被其他请求替换的句柄。
            async with self._lock:
                entry = self._cache.get(identifier)
                if entry is None or entry[0] is not workspace:
                    raise Conflict("工作区实例已关闭，请重试当前消息")
                self._active_workspaces[identifier] = (
                    self._active_workspaces.get(identifier, 0) + 1
                )
            try:
                await workspace.check_ready()
                await workspace.renew_workspace()
                # 2. 子任务失败会取消当前执行；正常结束或用户取消时停止续期任务。
                try:
                    async with asyncio.TaskGroup() as group:
                        renewal = group.create_task(
                            self._renew_active_workspace(workspace)
                        )
                        try:
                            yield
                        finally:
                            renewal.cancel()
                except ExceptionGroup as exc:
                    # 单一错误保持原类型，授权拒绝等业务异常不能被并发容器改写。
                    if len(exc.exceptions) == 1:
                        raise exc.exceptions[0] from None
                    raise
            finally:
                # 3. 从本次执行结束起重新计算空闲时间，给原生上下文保存留出时间。
                async with self._lock:
                    remaining = self._active_workspaces[identifier] - 1
                    if remaining:
                        self._active_workspaces[identifier] = remaining
                    else:
                        self._active_workspaces.pop(identifier)
                    entry = self._cache.get(identifier)
                    if entry is not None and entry[0] is workspace:
                        self._cache[identifier] = (workspace, time.monotonic())

    async def get_workspace(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
        workspace_id: str | None = None,
        *,
        project_id: str | None = None,
    ) -> EgisSandboxWorkspace:
        """取得当前项目的沙箱，缓存命中后仍检查实例归属。"""
        from egis_agentkit.core.session._context import (
            session_execution_context,
            session_setup_error,
        )

        # 1. 检查当前会话上下文，确定唯一项目绑定。
        error = session_setup_error.get()
        if error is not None:
            raise error
        scope = session_execution_context.get()
        if scope is not None and scope.user_id != user_id:
            raise Forbidden("工作区执行上下文与用户不一致")
        if (
            scope is not None
            and project_id is not None
            and project_id != scope.project_id
        ):
            raise Forbidden("项目参数与当前会话不一致")
        project_id = scope.project_id if scope is not None else project_id
        logical_id = workspace_id_for(user_id)
        if workspace_id is not None and workspace_id != logical_id:
            raise Forbidden("会话工作区与用户归属不一致")
        if not project_id:
            raise NotFound("会话未绑定项目")
        annotate_runtime(name="chat.run")
        # 项目目录由 ProjectFileManager 管理，此处只选择用户工作区的执行环境。
        runtime_id = sandbox_binding_for(user_id, project_id, agent_id, session_id)
        async with self._instance_lock(runtime_id):
            async with self._lock:
                self._require_open(user_id, project_id)
            with trace_phase("workspace.cache_check"):
                await self._refresh_cached_workspace(runtime_id)
            entry = self._cache.get(runtime_id)
            if entry is not None:
                workspace = entry[0]
                logger.debug(
                    "workspace.cache_hit",
                    extra={
                        "context": {
                            "session_id": session_id,
                            "runtime_binding_id": runtime_id,
                        }
                    },
                )
            else:
                async with self._lock:
                    self._require_open(user_id, project_id)
                    failure = self._failed_workspaces.get(runtime_id)
                    if failure is not None and time.monotonic() < failure[2]:
                        raise WorkspaceProbeError(
                            failure[0], failure[1], retryable=False
                        )
                    self._failed_workspaces.pop(runtime_id, None)
                    self._building[runtime_id] = (user_id, project_id)
                try:
                    workspace = await self._build_workspace(
                        runtime_id, user_id, project_id, agent_id, session_id
                    )
                    async with self._lock:
                        try:
                            self._require_open(user_id, project_id)
                        except Conflict:
                            rejected = True
                        else:
                            self._cache[runtime_id] = (workspace, time.monotonic())
                            rejected = False
                    if rejected:
                        await self._close_workspace_safely(workspace)
                        raise Conflict("构建期间工作区已开始关闭，请重试")
                except WorkspaceProbeError as exc:
                    if not exc.retryable:
                        self._failed_workspaces[runtime_id] = (
                            exc.stage,
                            exc.reason,
                            time.monotonic()
                            + self.probe_settings.failure_cooldown_seconds,
                        )
                    raise
                finally:
                    async with self._lock:
                        self._building.pop(runtime_id, None)
            # 网络续期期间可能启动关闭；返回前再次确认生命周期。
            async with self._lock:
                self._require_open(user_id, project_id)
            if (
                workspace.egis_user_id != user_id
                or workspace.egis_project_id != project_id
            ):
                raise Forbidden("工作区实例与用户或项目不一致")
            return workspace

    async def _close_cached_workspace(self, identifier, *, check_session=True):
        async with self._workspace_lock(identifier):
            entry = self._cache.get(identifier)
            if entry is None:
                return
            workspace = entry[0]
            # 其他节点持有 Session 时，只断开本地连接；不能注销其 MCP 或暂停实例。
            if check_session and await self._session_in_use(workspace):
                self._cache.pop(identifier, None)
                await workspace.disconnect_workspace()
                return
            async with self._distributed_lock(identifier):
                self._cache.pop(identifier, None)
                if check_session and await self._session_in_use(workspace):
                    await workspace.disconnect_workspace()
                else:
                    await workspace.close_workspace()

    async def close_workspaces(self, workspace_id):
        """关闭用户逻辑工作区下的缓存和构建中实例，保留持久文件。"""
        async with self._workspace_lock(("close_user", workspace_id)):
            async with self._lock:
                self._closing_workspaces.add(workspace_id)
                identifiers = {
                    key
                    for key, (workspace, _) in self._cache.items()
                    if workspace.workspace_id == workspace_id
                }
                identifiers.update(
                    key
                    for key, (uid, _) in self._building.items()
                    if workspace_id_for(uid) == workspace_id
                )
            try:
                results = await asyncio.gather(
                    *(self._close_cached_workspace(key) for key in identifiers),
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, BaseException):
                        raise result
            finally:
                async with self._lock:
                    self._closing_workspaces.discard(workspace_id)

    async def destroy_workspaces(self, user_id, project_id):
        """先关闭该项目的缓存/构建中连接，再由 Core 校验并销毁提供方实例。"""
        async with self._workspace_lock(("destroy_project", user_id, project_id)):
            async with self._lock:
                self._destroying_projects.add((user_id, project_id))
                identifiers = {
                    key
                    for key, (workspace, _) in self._cache.items()
                    if (workspace.egis_user_id, workspace.egis_project_id)
                    == (user_id, project_id)
                }
                identifiers.update(
                    key
                    for key, owner in self._building.items()
                    if owner == (user_id, project_id)
                )
            try:
                for identifier in identifiers:
                    await self._close_cached_workspace(identifier, check_session=False)
                await EgisSandboxWorkspace.destroy_sandbox_instances(
                    user_id,
                    project_id,
                    client=self._client_factory(),
                    guard_instance=self._distributed_lock,
                )
            finally:
                async with self._lock:
                    self._destroying_projects.discard((user_id, project_id))

    async def _refresh_cached_workspace(self, runtime_id):
        """调用者持有绑定锁；网络请求不持有全局缓存锁，只有明确失效才移除实例。"""
        entry = self._cache.get(runtime_id)
        if entry is None:
            return
        workspace, _ = entry
        try:
            await workspace.check_ready()
            await workspace.renew_workspace()
        except (SandboxInstanceNotFound, WorkspaceProbeError) as exc:
            if isinstance(exc, WorkspaceProbeError) and (exc.stage, exc.reason) != (
                "instance",
                "not_running",
            ):
                raise
            async with self._lock:
                if self._active_workspaces.get(runtime_id):
                    raise Conflict("沙箱实例已失效，请等待当前执行结束后重试") from None
                self._cache.pop(runtime_id, None)
            await workspace.disconnect_workspace()
        else:
            async with self._lock:
                self._cache[runtime_id] = (workspace, time.monotonic())

    async def _renew_active_workspace(self, workspace):
        while True:
            await asyncio.sleep(self.settings.renew_interval_seconds)
            await workspace.renew_workspace()

    @staticmethod
    async def _close_workspace_safely(workspace):
        try:
            await workspace.close_workspace()
        except Exception as exc:
            logger.warning("工作区连接关闭失败：%s", type(exc).__name__)

    async def _recycle_idle_workspaces(self):
        """先确认本地空闲，再在分布式实例锁内检查远端 Session，避免跨节点误暂停。"""
        now = time.monotonic()
        self._failed_workspaces = {
            key: value
            for key, value in self._failed_workspaces.items()
            if now < value[2]
        }
        async with self._lock:
            identifiers = [
                key
                for key, (_, accessed) in self._cache.items()
                if not self._active_workspaces.get(key) and now - accessed > self._ttl
            ]
        for identifier in identifiers:
            try:
                # 先做非阻塞判断；真正暂停前仍必须在分布式锁内复核。
                if await self.bus.is_locked(self._instance_key(identifier)):
                    continue
                async with self._instance_lock(identifier):
                    entry = self._cache.get(identifier)
                    if (
                        entry is None
                        or self._active_workspaces.get(identifier)
                        or time.monotonic() - entry[1] <= self._ttl
                    ):
                        continue
                    if await self._session_in_use(entry[0]):
                        continue
                    self._cache.pop(identifier)
                    await self._close_workspace_safely(entry[0])
            except Exception as exc:
                logger.warning("工作区回收未完成：%s", type(exc).__name__)

    async def _recycle_loop(self):
        while True:
            await asyncio.sleep(self.settings.sweep_interval_seconds)
            await self._recycle_idle_workspaces()

    async def _build_workspace(
        self, runtime_id, user_id, project_id, agent_id, session_id
    ):
        """构建 Core Workspace，具体 Client 由 Bootstrap 注入。"""
        started = time.monotonic()
        context = {
            "user_id": user_id,
            "project_id": project_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "runtime_binding_id": runtime_id,
        }
        logger.info("workspace.preparing", extra={"context": context})
        client = self._client_factory()
        workspace = None
        try:
            workspace = EgisSandboxWorkspace(
                user_id=user_id,
                agent_id=agent_id,
                project_id=project_id,
                session_id=session_id,
                files=self.files,
                client=client,
                settings=self.settings,
                probe=self.probe_settings,
                read_guard_factory=self._read_guard_factory,
                mcp_client_factory=AuthorizedGatewayMCPClient,
                workspace_id=workspace_id_for(user_id),
                runtime_binding_id=runtime_id,
            )
            try:
                with trace_phase("sandbox.initialize"):
                    await workspace.initialize()
            except RuntimeError:
                raise WorkspaceProbeError(
                    "runtime", "initialization_failed", retryable=False
                ) from None
            with trace_phase("sandbox.probe"):
                await workspace.renew_workspace()
                await workspace.check_ready(full=True)
            logger.info(
                "workspace.ready",
                extra={
                    "context": {
                        **context,
                        "sandbox_id": workspace.sandbox_id,
                        "duration_ms": round((time.monotonic() - started) * 1000),
                    }
                },
            )
            return workspace
        except BaseException as exc:
            logger.warning(
                "workspace.prepare_failed",
                extra={
                    "context": {
                        **context,
                        "error_type": type(exc).__name__,
                        "duration_ms": round((time.monotonic() - started) * 1000),
                    }
                },
            )
            if workspace is not None:
                # 取消可能来自租约丢失；此时只释放句柄，禁止暂停新持有者的实例。
                try:
                    await workspace.disconnect_workspace()
                except Exception:
                    logger.warning("工作区本地连接释放失败")
            else:
                await client.close()
            raise

    async def close(self, workspace_id):
        """原生关闭接口使用用户逻辑工作区身份。"""
        await self.close_workspaces(workspace_id)

    async def close_all(self):
        """包括构建中的实例；完整等待关闭结果，不把某个失败变成后台遗留任务。"""
        async with self._workspace_lock(("close_all",)):
            async with self._lock:
                self._closing_all = True
                identifiers = set(self._cache) | set(self._building)
            try:
                results = await asyncio.gather(
                    *(self._close_cached_workspace(key) for key in identifiers),
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, BaseException):
                        raise result
            finally:
                async with self._lock:
                    self._closing_all = False

    async def __aenter__(self):
        if self._sweep_task is None:
            self._sweep_task = asyncio.create_task(self._recycle_loop())
        return self

    async def __aexit__(self, *exc):
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            await asyncio.gather(self._sweep_task, return_exceptions=True)
            self._sweep_task = None
        await self.close_all()
