"""继承原生 OpenSandbox Manager，补充企业项目绑定和实例构造。

核心能力
- get_workspace 校验绑定并续期；失效缓存按原项目挂载重新构建。
- on_reply / keep_alive 在执行期间续期，并阻止空闲回收关闭正在使用的实例。
- _build_and_start 构建带项目挂载的企业 Workspace，失败时释放连接。
- 空闲回收保留原生暂停语义；实例失效时不自动重放工具命令。
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from agentscope.app.workspace_manager import OpenSandboxWorkspaceManager

from egis_agentkit.client.nacos._settings import OpenSandboxConfig
from egis_agentkit.core.workspace._identity import sandbox_binding_for, workspace_id_for
from egis_agentkit.core.workspace._opensandbox_workspace import EgisOpenSandboxWorkspace
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.extension._opensandbox_mcp_client_override import (
    AuthorizedGatewayMCPClient,
)
from egis_agentkit.extension._workspace_read_guard_middleware_impl import (
    ReadToolGuardMiddleware,
)
from egis_agentkit.storage.project._files import ProjectFileStorage

from ._resources import configure_workspace_resources

if TYPE_CHECKING:
    from egis_agentkit.service.resource._catalog import ResourceService


class SandboxManager(OpenSandboxWorkspaceManager):
    """在原生 Manager 之上绑定项目，保留其缓存、重连和回收语义。"""

    def __init__(
        self,
        workspace_root: str | Path,
        settings: OpenSandboxConfig,
        resources: ResourceService | None = None,
    ):
        """注入配置与资源服务，凭据缺失时明确失败。"""
        self.files = ProjectFileStorage(Path(workspace_root))
        self.settings = settings
        self.resources = resources
        self._active_workspaces: dict[str, int] = {}
        self._project_binding: ContextVar[tuple[str, str] | None] = ContextVar(
            "sandbox_project_binding", default=None
        )
        api_key = os.environ.get(settings.api_key_env)
        if not api_key:
            raise ValueError(f"缺少 OpenSandbox 凭据环境变量 {settings.api_key_env}")
        super().__init__(
            image=settings.image,
            api_key=api_key,
            domain=settings.domain,
            protocol=settings.protocol,
            request_timeout_seconds=settings.request_timeout_seconds,
            timeout_seconds=settings.timeout_seconds,
            gateway_port=settings.gateway_port,
            env=settings.env,
            resource=settings.resource,
            entrypoint=settings.entrypoint,
            extra_pip=settings.extra_pip,
            ttl=settings.ttl_seconds,
            sweep_interval=settings.sweep_interval_seconds,
        )

    async def get_workspace(
        self,
        user_id: str,
        agent_id: str,
        session_id: str,
        workspace_id: str | None = None,
        *,
        project_id: str | None = None,
    ) -> EgisOpenSandboxWorkspace:
        """取得当前项目的沙箱，缓存命中后仍检查实例归属。"""
        from egis_agentkit.extension._session_lock_override import (
            execution_scope,
            execution_setup_error,
        )

        # 1. 检查当前会话上下文，确定唯一项目绑定。
        error = execution_setup_error.get()
        if error is not None:
            raise error
        scope = execution_scope.get()
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
        # 项目目录由 ProjectFileManager 管理，此处只选择用户工作区的执行环境。
        runtime_id = sandbox_binding_for(user_id, project_id, agent_id, session_id)
        token = self._project_binding.set((project_id, session_id))
        try:
            # 2. 缓存对象仍在不代表服务端容器仍在；仅实例 404 才允许重新构建。
            await self._refresh_cached(runtime_id)
            workspace = await super().get_workspace(
                user_id, agent_id, session_id, workspace_id=runtime_id
            )
        finally:
            self._project_binding.reset(token)
        # 3. 缓存命中后仍核对归属，禁止跨用户接入。
        if workspace.egis_user_id != user_id or workspace.egis_project_id != project_id:
            raise Forbidden("工作区实例与用户或项目不一致")
        await configure_workspace_resources(workspace, self.resources, scope)
        return workspace

    async def _refresh_cached(self, runtime_id):
        """续期缓存实例；只移除已确认不存在且没有执行者的句柄。"""
        from opensandbox.exceptions import SandboxApiException

        expired = None
        async with self._lock:
            cached = self._cache.get(runtime_id)
            if cached is None:
                return
            workspace, _ = cached
            try:
                await workspace.renew()
            except SandboxApiException as exc:
                if exc.status_code != 404:
                    raise
                if self._active_workspaces.get(runtime_id):
                    raise Conflict("沙箱实例已失效，请等待当前执行结束后重试") from exc
                expired, _ = self._cache.pop(runtime_id)
            else:
                self._cache[runtime_id] = (workspace, time.monotonic())
        if expired is not None:
            await self._safe_close(expired)

    @asynccontextmanager
    async def keep_alive(self, workspace):
        """从回复开始到结束持有使用计数，续期失败时终止执行而不重放命令。"""
        identifier = workspace.runtime_binding_id
        # 1. 先在回收锁内登记使用者，不能租用已经被其他请求替换的句柄。
        async with self._lock:
            entry = self._cache.get(identifier)
            if entry is None or entry[0] is not workspace:
                raise Conflict("工作区实例已关闭，请重试当前消息")
            self._active_workspaces[identifier] = (
                self._active_workspaces.get(identifier, 0) + 1
            )
        try:
            await workspace.renew()
            # 2. 子任务失败会取消当前执行；正常结束或用户取消时停止续期任务。
            try:
                async with asyncio.TaskGroup() as group:
                    renewal = group.create_task(self._renew_while_active(workspace))
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

    async def _renew_while_active(self, workspace):
        """按 Nacos 配置的间隔续期；不忽略服务端失败或无限重试。"""
        while True:
            await asyncio.sleep(self.settings.keepalive_interval_seconds)
            await workspace.renew()

    async def on_reply(self, workspace, handler, agent, input_kwargs, next_handler):
        """保护整个回复流，项目产物回调和原生事件仍由原处理链执行。"""
        async with self.keep_alive(workspace):
            async for event in handler(agent, input_kwargs, next_handler):
                yield event

    async def _sweep_once(self):
        """仅暂停超过空闲 TTL 且没有执行者的实例。"""
        now = time.monotonic()
        async with self._lock:
            identifiers = [
                key
                for key, (_, accessed) in self._cache.items()
                if not self._active_workspaces.get(key) and now - accessed > self._ttl
            ]
            expired = [self._cache.pop(key)[0] for key in identifiers]
        await asyncio.gather(*(self._safe_close(workspace) for workspace in expired))

    async def refresh_resources(self, workspace):
        """工具执行前刷新管理侧授权，不把业务查询交给 Core。"""
        from egis_agentkit.extension._session_lock_override import execution_scope

        await configure_workspace_resources(
            workspace, self.resources, execution_scope.get()
        )

    async def close(self, workspace_id):
        """按用户工作区身份关闭其运行视图，具体回收仍调用原生方法。"""
        async with self._lock:
            identifiers = [
                key
                for key, (workspace, _) in self._cache.items()
                if workspace.workspace_id == workspace_id
            ]
        for identifier in identifiers:
            await super().close(identifier)

    async def release_project(self, user_id, project_id):
        """项目删除时销毁该用户的运行实例，释放挂载后才允许删除宿主目录。"""
        from opensandbox import SandboxManager as ProviderManager
        from opensandbox.config.connection import ConnectionConfig
        from opensandbox.models.sandboxes import SandboxFilter, SandboxState

        # 1. 项目管理已持有项目及会话锁；先移出缓存并关闭本进程的 SDK 句柄。
        async with self._lock:
            identifiers = [
                key
                for key, (workspace, _) in self._cache.items()
                if (workspace.egis_user_id, workspace.egis_project_id)
                == (user_id, project_id)
            ]
            entries = [self._cache.pop(key)[0] for key in identifiers]
        await asyncio.gather(*(workspace.close() for workspace in entries))
        manager = await ProviderManager.create(
            connection_config=ConnectionConfig(
                domain=self._domain,
                api_key=self._api_key,
                protocol=self._protocol,
                request_timeout=timedelta(seconds=self._request_timeout_seconds),
                use_server_proxy=self.settings.use_server_proxy,
            )
        )
        metadata = {
            "agentkit.user.hash": EgisOpenSandboxWorkspace._metadata_hash(
                user_id.encode()
            ),
            "agentkit.project.id": project_id,
        }
        try:
            # 2. 同时寻找暂停及其他进程创建的实例，分页收集后再销毁以免漏项。
            page = None
            bindings = []
            while True:
                result = await manager.list_sandbox_infos(
                    SandboxFilter(
                        states=[SandboxState.RUNNING, SandboxState.PAUSED],
                        metadata=metadata,
                        page=page,
                    )
                )
                bindings.extend(result.sandbox_infos)
                if not result.pagination.has_next_page:
                    break
                page = result.pagination.page + 1
            if any(
                any(
                    (info.metadata or {}).get(key) != value
                    for key, value in metadata.items()
                )
                for info in bindings
            ):
                raise Forbidden("沙箱归属与待删除项目不一致")
            # 3. 只有显式删除项目才销毁沙箱；正常关闭和 TTL 仍沿用原生暂停语义。
            for identifier in {info.id for info in bindings}:
                await manager.kill_sandbox(identifier)
        finally:
            await manager.close()

    async def _build_and_start(
        self, *, workspace_id: str, user_id: str, agent_id: str
    ) -> EgisOpenSandboxWorkspace:
        """只定制实例构造，创建/恢复仍交给原生 Workspace.initialize。"""
        binding = self._project_binding.get()
        if binding is None:
            raise RuntimeError("创建沙箱缺少项目绑定上下文")
        project_id, session_id = binding
        workspace = EgisOpenSandboxWorkspace(
            user_id=user_id,
            agent_id=agent_id,
            project_id=project_id,
            session_id=session_id,
            files=self.files,
            read_guard_factory=ReadToolGuardMiddleware,
            mcp_client_factory=AuthorizedGatewayMCPClient,
            settings=self.settings,
            workspace_id=workspace_id_for(user_id),
            runtime_binding_id=workspace_id,
            image=self._image,
            api_key=self._api_key,
            domain=self._domain,
            protocol=self._protocol,
            request_timeout_seconds=self._request_timeout_seconds,
            timeout_seconds=self._timeout_seconds,
            gateway_port=self._gateway_port,
            env=self._env,
            resource=self._resource,
            entrypoint=self._entrypoint,
            extra_pip=self._extra_pip,
        )
        try:
            await workspace.initialize()
            # 恢复旧实例不会自动延长其有效期，初始化完成后显式续期。
            await workspace.renew()
        except BaseException:
            await workspace.close()
            raise
        return workspace
