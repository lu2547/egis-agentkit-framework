"""实现 EgisSandboxWorkspace 内部检查：实例、通道、环境、网关和 NAS。

核心能力
- check_ready：每次核对实例和通道，按配置间隔执行轻量/完整检查。
- 网络重试只重新连接原实例；运行时、挂载和权限错误不触发重建。
- 探针使用项目内部保留目录和随机文件，结束、失败或取消均清理自己的文件。
"""

import asyncio
import errno
import logging
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

from egis_agentkit.client.sandbox import SandboxConnectionError, SandboxInstanceNotFound
from egis_agentkit.errors import (
    Forbidden,
    NotFound,
    UploadNameConflict,
    WorkspaceProbeError,
)

if TYPE_CHECKING:
    from .._egis_sandbox_workspace import EgisSandboxWorkspace

logger = logging.getLogger(__name__)


class _EgisSandboxChecks:
    """只由 EgisSandboxWorkspace 持有，保存检查状态并执行内部校验。"""

    def __init__(self, workspace: "EgisSandboxWorkspace"):
        self.workspace = workspace
        self._lock = asyncio.Lock()
        self._checked_light = None
        self._checked_full = None
        self.status = {"is_ready": False, "stage": "instance", "reason": "unchecked"}

    async def check_ready(self, *, full=False):
        """先核验实例，再按检查预算确认工具和存储可用，成功才发布就绪状态。"""
        async with self._lock:
            workspace, policy = self.workspace, self.workspace.probe_settings
            stage = "instance"
            try:
                # 1. 控制面与执行面分开确认；仅重试查询/同实例重连。
                instance = await self._check_connection()
                if instance.id != workspace.sandbox_id or any(
                    instance.metadata.get(k) != v
                    for k, v in workspace.sandbox_metadata.items()
                ):
                    raise WorkspaceProbeError(
                        "instance", "identity_mismatch", retryable=False
                    )
                if instance.state != "running":
                    raise WorkspaceProbeError(
                        "instance",
                        "not_running"
                        if instance.state == "paused"
                        else "state_unavailable",
                        retryable=instance.state == "paused",
                    )
                now = time.monotonic()
                deep = (
                    full
                    or self._checked_full is None
                    or now - self._checked_full >= policy.full_interval_seconds
                )
                light = (
                    deep
                    or self._checked_light is None
                    or now - self._checked_light >= policy.interval_seconds
                )
                # 2. 轻量检查验证真正的命令执行和网关，不把续期当成工具可用。
                if light:
                    stage = "network"
                    await self._run_bounded(stage, self._check_command())
                    stage = "runtime"
                    await self._run_bounded(stage, self._check_gateway())
                # 3. 完整检查验证当前依赖和持久挂载，不在每个工具调用前重复执行。
                if deep:
                    stage = "runtime"
                    await self._run_bounded(stage, self._check_runtime())
                    stage = "storage"
                    await self._run_bounded(stage, self._check_storage())
                    self._checked_full = time.monotonic()
                if light:
                    self._checked_light = time.monotonic()
            except BaseException as exc:
                self._checked_light = self._checked_full = None
                self.status = {
                    "is_ready": False,
                    "stage": getattr(exc, "stage", stage),
                    "reason": getattr(
                        exc,
                        "reason",
                        "missing"
                        if isinstance(exc, SandboxInstanceNotFound)
                        else "unavailable",
                    ),
                    "retryable": getattr(exc, "retryable", False),
                }
                if isinstance(exc, Exception):
                    logger.warning(
                        "工作区就绪检查失败 stage=%s reason=%s retryable=%s",
                        self.status["stage"],
                        self.status["reason"],
                        self.status["retryable"],
                    )
                raise
            else:
                self.status = {
                    "is_ready": True,
                    "stage": "ready",
                    "reason": None,
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                }

    async def _check_connection(self):
        policy = self.workspace.probe_settings
        for attempt in range(policy.retry_attempts):
            try:
                async with asyncio.timeout(policy.timeout_seconds):
                    return await self.workspace.client.check_connection(
                        reconnect=attempt > 0
                    )
            except (SandboxConnectionError, TimeoutError):
                if attempt + 1 == policy.retry_attempts:
                    raise WorkspaceProbeError(
                        "network", "unavailable", retryable=True
                    ) from None
                await asyncio.sleep(policy.retry_delay_seconds)

    async def _run_bounded(self, stage, operation):
        try:
            async with asyncio.timeout(self.workspace.probe_settings.timeout_seconds):
                await operation
        except WorkspaceProbeError:
            raise
        except SandboxInstanceNotFound:
            raise
        except SandboxConnectionError:
            raise WorkspaceProbeError(
                "network", "unavailable", retryable=True
            ) from None
        except TimeoutError:
            raise WorkspaceProbeError(stage, "timeout", retryable=True) from None
        except UploadNameConflict:
            raise WorkspaceProbeError(
                stage, "probe_collision", retryable=True
            ) from None
        except (Forbidden, NotFound, OSError) as exc:
            reason = (
                "permission"
                if isinstance(exc, Forbidden)
                or getattr(exc, "errno", None)
                in (errno.EACCES, errno.EPERM, errno.EROFS)
                else "capacity"
                if getattr(exc, "errno", None) in (errno.ENOSPC, errno.EDQUOT)
                else "mount_mismatch"
                if isinstance(exc, (NotFound, FileNotFoundError))
                else "io_unavailable"
            )
            raise WorkspaceProbeError(
                stage, reason, retryable=reason == "io_unavailable"
            ) from None

    async def _check_command(self):
        result = await self.workspace.get_backend().exec_shell(
            self.workspace.probe_settings.command,
            cwd=self.workspace.workdir,
            timeout=self.workspace.probe_settings.timeout_seconds,
        )
        if not result.ok():
            raise WorkspaceProbeError("network", "command_unavailable", retryable=True)

    async def _check_gateway(self):
        if (
            self.workspace._gateway is None
            or not await self.workspace._gateway.health()
        ):
            raise WorkspaceProbeError("runtime", "gateway_unavailable", retryable=True)

    async def _check_runtime(self):
        workspace = self.workspace
        for template in workspace.probe_settings.runtime_commands:
            argv = [
                part.replace("{gateway_python}", workspace._gateway_python).replace(
                    "{workdir}", workspace.workdir
                )
                for part in template
            ]
            result = await workspace.get_backend().exec_shell(
                argv,
                cwd=workspace.workdir,
                timeout=workspace.probe_settings.timeout_seconds,
            )
            if not result.ok():
                raise WorkspaceProbeError(
                    "runtime", "environment_invalid", retryable=False
                )

    async def _check_storage(self):
        workspace = self.workspace
        files, client = workspace.egis_files, workspace.client
        uid, pid = workspace.egis_user_id, workspace.egis_project_id
        for area in ("work", "artifacts"):
            token = uuid4().hex
            initial, changed = token.encode(), (token + ":sandbox").encode()
            created, abandoned = threading.Event(), threading.Event()
            try:
                # 宿主写、沙箱读；只检查可写性会漏掉错误挂载或空目录。
                path = await asyncio.to_thread(
                    self._write_host_probe, area, token, initial, created, abandoned
                )
                if await client.read_file(path) != initial:
                    raise WorkspaceProbeError(
                        "storage", "mount_mismatch", retryable=False
                    )
                # 沙箱写、宿主读，确认双方看到的是同一持久数据。
                await self._write_sandbox_probe(path, initial, changed)
                actual = await asyncio.to_thread(
                    files.read_probe_file, uid, pid, area, token, len(changed)
                )
                if actual != changed:
                    raise WorkspaceProbeError(
                        "storage", "mount_mismatch", retryable=False
                    )
            finally:
                # 已取消但仍被内核阻塞的 NAS 写入，完成后由写入线程自行清理。
                abandoned.set()
                if created.is_set():
                    async with asyncio.timeout(
                        workspace.probe_settings.timeout_seconds
                    ):
                        await asyncio.to_thread(
                            files.remove_probe_file, uid, pid, area, token
                        )

    def _write_host_probe(self, area, token, content, created, abandoned):
        """只有独占创建成功才取得清理资格，禁止覆盖或删除其他运行者的文件。"""
        workspace = self.workspace
        files, uid, pid = (
            workspace.egis_files,
            workspace.egis_user_id,
            workspace.egis_project_id,
        )
        path = files.write_probe_file(uid, pid, area, token, content)
        created.set()
        if abandoned.is_set():
            files.remove_probe_file(uid, pid, area, token)
        return path

    async def _write_sandbox_probe(self, path, initial, changed):
        """只打开已有探针，不带 O_CREAT；迟到请求无法重建已经被清理的路径。"""
        workspace = self.workspace
        python = workspace.probe_settings.storage_python.replace(
            "{gateway_python}", workspace._gateway_python
        )
        program = """import os,sys
try:
    fd=os.open(sys.argv[1], os.O_RDWR | os.O_NOFOLLOW)
    with os.fdopen(fd, 'r+b') as stream:
        expected=sys.argv[2].encode()
        if stream.read(len(expected)+1) != expected:
            print('identity')
            sys.exit(1)
        stream.seek(0)
        stream.write(sys.argv[3].encode())
        stream.truncate()
except OSError as exc:
    print(exc.errno)
    sys.exit(1)
"""
        result = await workspace.get_backend().exec_shell(
            [python, "-c", program, path, initial.decode(), changed.decode()],
            cwd=workspace.workdir,
            timeout=workspace.probe_settings.timeout_seconds,
        )
        if result.ok():
            return
        output = result.stdout.strip()
        if output in {
            str(code).encode() for code in (errno.EACCES, errno.EPERM, errno.EROFS)
        }:
            reason = "permission"
        elif output in {str(code).encode() for code in (errno.ENOSPC, errno.EDQUOT)}:
            reason = "capacity"
        elif output in (b"identity", str(errno.ENOENT).encode()):
            reason = "mount_mismatch"
        else:
            reason = "io_unavailable"
        raise WorkspaceProbeError(
            "storage", reason, retryable=reason == "io_unavailable"
        )
