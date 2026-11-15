"""继承原生 OpenSandbox Workspace，补充项目持久目录挂载。

核心能力
- project_mounts 仅公开当前项目，资料只读，工作和交付目录可写。
- 创建使用 SDK volumes；连接复用原生流程并核对企业绑定。
- execd 通过 OpenSandbox 服务代理访问，不依赖宿主机直连容器 IP。
- renew 延长当前实例的服务端有效期，不重建或重放工具操作。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
from datetime import timedelta
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING
from uuid import uuid4

from agentscope.skill import Skill
from agentscope.workspace import OpenSandboxWorkspace
from agentscope.workspace._gateway_client import GatewayMCPClient

from egis_agentkit.client.nacos._settings import OpenSandboxConfig
from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.storage.project._files import ProjectFileStorage

from ._identity import sandbox_binding_for, workspace_id_for
from ._runtime_mixin import WorkspaceRuntimeMixin

if TYPE_CHECKING:
    from opensandbox.models.sandboxes import Volume


class EgisOpenSandboxWorkspace(WorkspaceRuntimeMixin, OpenSandboxWorkspace):
    """保留原生环境与 Backend，挂载企业项目的持久数据。"""

    def __init__(
        self,
        *,
        user_id: str,
        agent_id: str,
        project_id: str,
        files: ProjectFileStorage,
        settings: OpenSandboxConfig,
        session_id: str = "",
        runtime_binding_id: str | None = None,
        read_guard_factory=None,
        mcp_client_factory=GatewayMCPClient,
        **kwargs,
    ):
        self._bind_runtime(
            user_id=user_id,
            agent_id=agent_id,
            project_id=project_id,
            session_id=session_id,
            files=files,
            read_guard_factory=read_guard_factory,
        )
        self.runtime_binding_id = runtime_binding_id or sandbox_binding_for(
            user_id, project_id, agent_id, session_id
        )
        logical_id = workspace_id_for(user_id)
        if (
            kwargs.get("workspace_id") is not None
            and kwargs["workspace_id"] != logical_id
        ):
            raise Forbidden("工作区标识与用户不一致")
        kwargs["workspace_id"] = logical_id
        self._mcp_client_factory = mcp_client_factory
        self.settings = settings
        self._skill_publication_lock = asyncio.Lock()
        self._bootstrap_cmd_timeout = settings.bootstrap_timeout_seconds
        super().__init__(**kwargs)
        mounts = [
            mount.model_dump(mode="json", by_alias=True)
            for mount in self.project_mounts()
        ]
        self._binding_metadata = {
            # 外部身份可以包含 Unicode 或邮箱，不能直接作为 provider label。
            "agentkit.user.hash": self._metadata_hash(user_id.encode()),
            "agentkit.project.id": project_id,
            # 服务端 metadata 遵循 label 长度约束；Base32 保留完整摘要且仅含字母数字。
            "agentkit.mount.hash": self._metadata_hash(
                json.dumps(mounts, sort_keys=True).encode()
            ),
        }
        self.sandbox_metadata.update(self._binding_metadata)
        self.sandbox_metadata["agentkit.workspace.id"] = self.workspace_id

    @staticmethod
    def _metadata_hash(content: bytes) -> str:
        """完整 SHA256 以无填充 Base32 编码，符合 OpenSandbox label 约束。"""
        return base64.b32encode(hashlib.sha256(content).digest()).decode().rstrip("=")

    def project_mounts(self) -> list[Volume]:
        """构造限定项目的三个挂载，宿主机和应用可使用不同根目录。"""
        from opensandbox.models.sandboxes import Host, Volume

        target = self.egis_files.project_root(self.egis_user_id, self.egis_project_id)
        relative = target.relative_to(self.egis_files.root)
        source = PurePosixPath(self.settings.host_root).joinpath(*relative.parts)
        return [
            Volume(
                name="project-inputs",
                host=Host(path=str(source)),
                mount_path=str(target),
                read_only=self.settings.inputs_read_only,
            ),
            Volume(
                name="project-work",
                host=Host(path=str(source / ".work")),
                mount_path=str(target / ".work"),
            ),
            Volume(
                name="project-artifacts",
                host=Host(path=str(source / "artifacts")),
                mount_path=str(target / "artifacts"),
            ),
        ]

    def _connection_config(self):
        """沿用原生连接配置，增加服务代理选项。"""
        connection = super()._connection_config()
        connection.use_server_proxy = self.settings.use_server_proxy
        return connection

    async def renew(self):
        """延长现有沙箱的到期时间；实例不存在时保留 SDK 错误供 Manager 处理。"""
        if self._sandbox is None:
            raise RuntimeError("工作区尚未初始化，不能续期沙箱")
        await self._sandbox.renew(timedelta(seconds=self.timeout_seconds))

    async def _create_sandbox(self):
        """原生创建接口未暴露 volumes，在此补充项目挂载。"""
        from opensandbox import Sandbox

        return await Sandbox.create(
            image=self.image,
            connection_config=self._connection_config(),
            metadata={
                **self.sandbox_metadata,
                "agentscope.workspace.id": self.runtime_binding_id,
            },
            timeout=timedelta(seconds=self.timeout_seconds),
            ready_timeout=timedelta(seconds=self.settings.ready_timeout_seconds),
            env=self.env,
            resource=self.resource,
            entrypoint=self.entrypoint,
            volumes=self.project_mounts(),
        )

    async def _find_existing_sandbox(self):
        """重连前核对持久身份，拒绝把其他用户或不同挂载接入当前环境。"""
        from opensandbox import SandboxManager
        from opensandbox.models.sandboxes import SandboxFilter, SandboxState

        manager = await SandboxManager.create(
            connection_config=self._connection_config()
        )
        try:
            result = await manager.list_sandbox_infos(
                SandboxFilter(
                    states=[SandboxState.RUNNING, SandboxState.PAUSED],
                    metadata={"agentscope.workspace.id": self.runtime_binding_id},
                )
            )
        finally:
            await manager.close()
        existing = max(
            result.sandbox_infos, key=lambda info: info.created_at, default=None
        )
        if existing is not None and any(
            (existing.metadata or {}).get(key) != value
            for key, value in self._binding_metadata.items()
        ):
            raise Forbidden("已有沙箱的用户、项目或挂载与当前绑定不一致")
        return existing

    async def _restore_mcp_specs(self):
        """每轮根据当前授权构建声明，不恢复可能包含旧凭据的配置文件。"""
        return {}

    async def _setup_mcp_gateway(self):
        """复用原生网关，只允许请求和响应临时内容经过内存文件系统。"""
        await super()._setup_mcp_gateway()
        result = await self.get_backend().exec_shell(
            ["stat", "-f", "-c", "%T", self.settings.gateway_tmp_dir]
        )
        if not result.ok() or result.stdout.strip() != b"tmpfs":
            raise Conflict("MCP 网关临时目录必须是 tmpfs，禁止凭据临时内容落盘")
        self._gateway.tmp_dir = self.settings.gateway_tmp_dir

    async def _save_mcp_file(self):
        """受管连接凭据只发送给沙箱网关，不持久化到 .mcp。"""

    async def list_mcps(self, *, agent_id=None, session_id=None):
        """协调获授权声明，实际连接和工具调用复用原生沙箱网关。"""
        aid, sid = self._mcp_partition(agent_id, session_id)
        declared = self._selected_mcps
        async with self.egis_mcp_reconcile:
            previous = self.egis_mcp_clients.setdefault((aid, sid), {})
            desired = {
                client.name: client.model_dump(mode="json") for client in declared
            }
            for name, old in list(previous.items()):
                if desired.get(name) != old:
                    await super().remove_mcp(name, agent_id=aid, session_id=sid)
            self._mcp_specs[(aid, sid)] = declared
            self.egis_mcp_clients[(aid, sid)] = desired
            clients = await super().list_mcps(agent_id=aid, session_id=sid)
            # 原生网关创建普通代理；保留同一注册身份，补充企业权限来源标签。
            result = []
            for client in clients:
                if not isinstance(client, self._mcp_client_factory):
                    client = self._mcp_client_factory.model_validate(
                        client.model_dump()
                    )
                    client.attach(
                        self._gateway, agent_id=aid, session_id=sid, connected=True
                    )
                    self._mcp_instances[(aid, sid)][client.name] = client
                result.append(client)
            return result

    def _skill_destination(self, agent_id: str, checksum: str) -> str:
        """统一校验 Agent 分区和内容身份，发布与撤销使用同一目标。"""
        if (
            agent_id in (".", "..")
            or not re.fullmatch(r"[\w.@-]+", agent_id)
            or not re.fullmatch(r"[a-f0-9]{64}", checksum)
        ):
            raise Forbidden("技能运行分区或内容校验值无效")
        return self.get_backend().join_path(self.workdir, "skills", agent_id, checksum)

    async def publish_skill(
        self, skill: Skill, *, agent_id: str, checksum: str
    ) -> Skill:
        """先完整暂存再安装技能，已完成的同一内容直接复用。"""
        # 1. 校验目标及受管源，不挂载整个公共缓存。
        destination = self._skill_destination(agent_id, checksum)
        source = Path(skill.dir)
        if source.is_symlink() or not source.resolve().is_relative_to(
            self.egis_files.root
        ):
            raise Forbidden("技能缓存不在受管目录")
        backend = self.get_backend()
        marker = destination + ".complete"
        async with self._skill_publication_lock:
            # 2. 完成标记只在安装成功后写入；重用时不删除使用中的脚本。
            complete = await backend.file_exists(marker)
            if not complete or await backend.read_file(marker) != checksum.encode():
                staging = destination + ".staging-" + uuid4().hex
                try:
                    await self._copy_skill(source, staging)
                    # 未完成内容可能来自上次中断，完整暂存后才清理并替换。
                    await backend.delete_path(destination)
                    moved = await backend.exec_shell(["mv", staging, destination])
                    if not moved.ok():
                        raise Conflict("无法安装沙箱技能目录")
                    await backend.write_file(marker, checksum.encode())
                finally:
                    await backend.delete_path(staging)
        return Skill(
            skill.name, skill.description, destination, skill.markdown, skill.updated_at
        )

    async def _copy_skill(self, source: Path, staging: str) -> None:
        """按总量限制读取受管内容，仅写入当前发布的暂存目录。"""
        backend = self.get_backend()
        result = await backend.exec_shell(["mkdir", "-p", staging])
        if not result.ok():
            raise Conflict("无法创建沙箱技能暂存目录")
        total = 0
        for path in sorted(source.rglob("*")):
            if path.is_symlink():
                raise Forbidden("技能内容不允许符号链接")
            if not path.is_file():
                continue
            relative = path.relative_to(source).as_posix()
            content, _ = await asyncio.to_thread(
                self.egis_files._read_root,
                source,
                relative,
                max_bytes=self.settings.max_skill_bytes - total,
            )
            total += len(content)
            if total > self.settings.max_skill_bytes:
                raise Conflict("技能内容超过沙箱发布限制")
            target = backend.join_path(staging, relative)
            result = await backend.exec_shell(["mkdir", "-p", backend.dirname(target)])
            if not result.ok():
                raise Conflict("无法创建沙箱技能子目录")
            await backend.write_file(target, content)

    async def unpublish_skill(self, *, agent_id: str, checksum: str) -> None:
        """授权失效时移除运行副本，不删除原资源及宿主缓存。"""
        destination = self._skill_destination(agent_id, checksum)
        async with self._skill_publication_lock:
            await self.get_backend().delete_path(destination)
            await self.get_backend().delete_path(destination + ".complete")

    async def close(self):
        """先注销企业连接，再由原生能力暂停沙箱并释放 SDK 句柄。"""
        try:
            await self._close_all_mcp_instances()
        finally:
            self.egis_mcp_clients.clear()
            await super().close()
