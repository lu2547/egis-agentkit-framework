"""把中立沙箱通信契约映射到当前开发环境的 OpenSandbox SDK。

核心能力
- 实例发现、创建、恢复、续期和暂停；SDK 连接不进入 Core/Service。
- 完整分页查询和按实例销毁，管理连接在每次操作后关闭。
- 转换 argv、字节文件及 SDK 错误，保持错误和取消语义，不泄露上游正文。
"""

import base64
import hashlib
import json
import shlex
from contextlib import asynccontextmanager
from datetime import timedelta

import httpx
from agentscope.tool import ExecResult
from opensandbox import Sandbox, SandboxManager
from opensandbox.config.connection import ConnectionConfig
from opensandbox.exceptions import SandboxApiException, SandboxException
from opensandbox.models.execd import RunCommandOpts
from opensandbox.models.filesystem import WriteEntry
from opensandbox.models.sandboxes import Host, SandboxFilter, SandboxState, Volume

from egis_agentkit.client.nacos._settings import OpenSandboxConfig
from egis_agentkit.errors import AgentKitError, Conflict, Forbidden

from ._models import SandboxInstance, SandboxMount
from ._protocol import SandboxInstanceNotFound

UNKNOWN_EXIT_CODE = -1


class OpenSandboxClient:
    """一个 Client 持有一个执行连接；项目清理使用独立 Client 对象。"""

    def __init__(self, settings: OpenSandboxConfig, *, api_key: str):
        if not api_key:
            raise ValueError(f"缺少沙箱凭据环境变量 {settings.api_key_env}")
        self.settings, self._api_key = settings, api_key
        self._sandbox = None

    @property
    def instance_id(self) -> str | None:
        return self._sandbox.id if self._sandbox is not None else None

    def _connection(self):
        return ConnectionConfig(
            domain=self.settings.domain,
            api_key=self._api_key,
            protocol=self.settings.protocol,
            request_timeout=timedelta(seconds=self.settings.request_timeout_seconds),
            use_server_proxy=self.settings.use_server_proxy,
        )

    @staticmethod
    def _volumes(mounts):
        return [
            Volume(
                name=m.name,
                host=Host(path=m.source),
                mount_path=m.target,
                read_only=m.read_only,
            )
            for m in mounts
        ]

    def mount_fingerprint(self, mounts: list[SandboxMount]) -> str:
        """按提供方实际挂载请求计算身份，保持已有实例的挂载验证结果稳定。"""
        values = [
            volume.model_dump(mode="json", by_alias=True)
            for volume in self._volumes(mounts)
        ]
        return (
            base64.b32encode(
                hashlib.sha256(json.dumps(values, sort_keys=True).encode()).digest()
            )
            .decode()
            .rstrip("=")
        )

    @asynccontextmanager
    async def _request(self, *, file_path=None):
        """只有明确的实例 404 可触发重建；文件 404 保持文件缺失语义。"""
        try:
            yield
        except FileNotFoundError:
            raise
        except (SandboxApiException, httpx.HTTPStatusError) as exc:
            status = (
                exc.status_code
                if isinstance(exc, SandboxApiException)
                else exc.response.status_code
            )
            if status == 404:
                if file_path is not None:
                    raise FileNotFoundError(file_path) from None
                raise SandboxInstanceNotFound() from None
            if status == 403:
                raise Forbidden("沙箱服务拒绝当前访问") from None
            raise AgentKitError("沙箱服务请求失败", 502) from None
        except httpx.HTTPError:
            raise AgentKitError("沙箱连接超时或暂时不可用", 503) from None
        except SandboxException:
            raise AgentKitError("沙箱服务内部异常或就绪超时", 502) from None

    @asynccontextmanager
    async def _management(self):
        async with self._request():
            manager = await SandboxManager.create(connection_config=self._connection())
            try:
                yield manager
            finally:
                await manager.close()

    async def _list(self, metadata):
        """读完全部分页再向调用方返回，不能在后续页校验前开始删除。"""
        result = []
        async with self._management() as manager:
            page = None
            while True:
                response = await manager.list_sandbox_infos(
                    SandboxFilter(
                        states=[SandboxState.RUNNING, SandboxState.PAUSED],
                        metadata=metadata,
                        page=page,
                    )
                )
                result.extend(response.sandbox_infos)
                if not response.pagination.has_next_page:
                    return result
                page = response.pagination.page + 1

    @staticmethod
    def _instance(info):
        return SandboxInstance(
            info.id, str(info.status.state).lower(), dict(info.metadata or {})
        )

    async def find_instance(self, binding_id):
        values = await self._list({"agentscope.workspace.id": binding_id})
        return (
            self._instance(max(values, key=lambda value: value.created_at))
            if values
            else None
        )

    async def list_instances(self, metadata):
        return [self._instance(value) for value in await self._list(metadata)]

    async def create_instance(self, metadata, mounts):
        """只翻译提供方创建参数；挂载范围和身份由 Core 决定。"""
        async with self._request():
            self._sandbox = await Sandbox.create(
                image=self.settings.image,
                connection_config=self._connection(),
                metadata=metadata,
                timeout=timedelta(seconds=self.settings.timeout_seconds),
                ready_timeout=timedelta(seconds=self.settings.ready_timeout_seconds),
                env=self.settings.env,
                resource=self.settings.resource,
                entrypoint=self.settings.entrypoint,
                volumes=self._volumes(mounts),
            )

    async def connect_instance(self, instance):
        """只恢复确定的运行/暂停实例，不把其他状态当作新实例重复创建。"""
        async with self._request():
            if instance.state == "paused":
                self._sandbox = await Sandbox.resume(
                    sandbox_id=instance.id,
                    connection_config=self._connection(),
                    resume_timeout=timedelta(seconds=self.settings.timeout_seconds),
                )
            elif instance.state == "running":
                self._sandbox = await Sandbox.connect(
                    sandbox_id=instance.id,
                    connection_config=self._connection(),
                    connect_timeout=timedelta(seconds=self.settings.timeout_seconds),
                )
            else:
                raise Conflict("当前沙箱状态不支持恢复")

    async def renew_instance(self, timeout_seconds):
        async with self._request():
            await self._sandbox.renew(timedelta(seconds=timeout_seconds))

    async def close_instance(self):
        """暂停保留文件；即使暂停失败也释放本进程连接，失败继续向上抛出。"""
        sandbox, self._sandbox = self._sandbox, None
        if sandbox is not None:
            try:
                async with self._request():
                    await sandbox.pause()
            except SandboxInstanceNotFound:
                # 实例已过期等价于已关闭，不能阻止项目删除；其他错误仍传播。
                pass
            finally:
                async with self._request():
                    await sandbox.close()

    async def close(self):
        """释放未进入正常生命周期的通信句柄，不额外销毁底层实例。"""
        sandbox, self._sandbox = self._sandbox, None
        if sandbox is not None:
            async with self._request():
                await sandbox.close()

    async def destroy_instance(self, identifier):
        async with self._management() as manager:
            await manager.kill_sandbox(identifier)

    @staticmethod
    def _output_bytes(execution, stream):
        """SDK 输出按日志片段还原，紧邻的独立日志行补换行，原始换行保持不变。"""
        text = ""
        for item in getattr(execution.logs, stream):
            part = item.text or ""
            if text and part and not text[-1].isspace() and not part[0].isspace():
                text += "\n"
            text += part
        return text.encode()

    async def exec_command(self, command, *, cwd, timeout=None):
        async with self._request():
            result = await self._sandbox.commands.run(
                shlex.join(command),
                opts=RunCommandOpts(
                    working_directory=cwd,
                    timeout=timedelta(seconds=timeout) if timeout is not None else None,
                ),
            )
        return ExecResult(
            UNKNOWN_EXIT_CODE if result.exit_code is None else result.exit_code,
            self._output_bytes(result, "stdout"),
            self._output_bytes(result, "stderr"),
        )

    async def read_file(self, path):
        async with self._request(file_path=path):
            return await self._sandbox.files.read_bytes(path)

    async def write_file(self, path, content):
        async with self._request(file_path=path):
            await self._sandbox.files.write_files([WriteEntry(path=path, data=content)])
