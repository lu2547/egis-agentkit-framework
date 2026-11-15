"""可选真实 Docker 验收：仅创建和清理本测试唯一标识的沙箱与目录。"""

import asyncio
import json
import os
import shutil
import socket
import time
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENTKIT_TEST_OPENSANDBOX_ROOT"),
    reason="需要显式提供 OpenSandbox 服务、凭据和允许挂载的持久根目录",
)


@pytest.mark.asyncio
async def test_real_docker_tools_gateway_skills_and_resume(monkeypatch):
    from agentscope.mcp import StdioMCPConfig
    from agentscope.skill import Skill
    from opensandbox import SandboxManager as ProviderSandboxManager
    from opensandbox.config.connection import ConnectionConfig
    from opensandbox.models.sandboxes import SandboxFilter

    from egis_agentkit.client.nacos._settings import OpenSandboxConfig
    from egis_agentkit.extension._opensandbox_mcp_client_override import (
        AuthorizedGatewayMCPClient,
    )
    from egis_agentkit.service.workspace._sandbox_manager import (
        SandboxManager,
    )

    root = Path(os.environ["AGENTKIT_TEST_OPENSANDBOX_ROOT"]).resolve()
    settings = OpenSandboxConfig(
        domain=os.environ["AGENTKIT_TEST_OPENSANDBOX_DOMAIN"],
        host_root=str(root),
        **(
            {"image": os.environ["AGENTKIT_TEST_OPENSANDBOX_IMAGE"]}
            if os.environ.get("AGENTKIT_TEST_OPENSANDBOX_IMAGE")
            else {}
        ),
    )
    if os.environ.get("AGENTKIT_TEST_OPENSANDBOX_IMAGE"):
        from egis_agentkit.core.workspace._opensandbox_workspace import (
            EgisOpenSandboxWorkspace,
        )

        def reject_runtime_install(self):
            raise AssertionError("预构建镜像不能在会话请求内联网安装依赖")

        monkeypatch.setattr(
            EgisOpenSandboxWorkspace, "_bootstrap_commands", reject_runtime_install
        )
    project_id = "test-sandbox-" + uuid4().hex
    other_id = "test-sandbox-" + uuid4().hex
    manager = SandboxManager(root, settings)
    manager.files.initialize_project("sandbox-test", project_id)
    manager.files.initialize_project("sandbox-test", other_id)
    source = root / "common" / project_id
    source.mkdir(parents=True)
    (source / "SKILL.md").write_text("# Selected skill")
    probe = manager.files.project_root("sandbox-test", project_id)
    other_root = manager.files.project_root("sandbox-test", other_id)
    (probe / "input.txt").write_text("host input")
    (other_root / "secret.txt").write_text("other project")
    cleanup = await ProviderSandboxManager.create(
        connection_config=ConnectionConfig(
            domain=settings.domain,
            api_key=os.environ[settings.api_key_env],
            use_server_proxy=True,
        )
    )
    try:
        async with manager:
            workspace, same = await asyncio.gather(
                *[
                    manager.get_workspace(
                        "sandbox-test", "agent", "session", project_id=project_id
                    )
                    for _ in range(2)
                ]
            )
            assert workspace is same
            backend = workspace.get_backend()
            assert await backend.read_file(str(probe / "input.txt")) == b"host input"
            result = await backend.exec_shell(["cat", str(other_root / "secret.txt")])
            assert not result.ok()
            result = await backend.exec_shell(
                ["sh", "-c", "echo overwrite > input.txt"], cwd=str(probe)
            )
            assert not result.ok()
            tools = {tool.name: tool for tool in await workspace.list_tools()}
            chunks = [chunk async for chunk in await tools["Bash"](command="pwd")]
            assert str(probe / ".work") in str(chunks)
            chunks = [
                chunk
                async for chunk in await tools["Read"](
                    file_path=str(probe / "input.txt")
                )
            ]
            assert "host input" in str(chunks)
            await tools["Write"](
                file_path=str(probe / "artifacts/result.txt"), content="sandbox output"
            )
            assert (probe / "artifacts/result.txt").read_text() == "sandbox output"
            published = await workspace.publish_skill(
                Skill("selected", "Selected", str(source), "# Selected skill", 0),
                agent_id="agent",
                checksum="a" * 64,
            )
            assert published.dir.startswith("/workspace/skills/agent/")
            assert (
                await backend.read_file(published.dir + "/SKILL.md")
                == b"# Selected skill"
            )

            # MCP 进程运行在沙箱网关的 Python 中，返回容器主机名而非 API 宿主机。
            script = probe / ".work/mcp_probe.py"
            script.write_text(
                'from mcp.server.mcpserver import MCPServer\nimport socket\nm = MCPServer("probe")\n@m.tool()\ndef hostname() -> str:\n return socket.gethostname()\nm.run(transport="stdio")\n'
            )
            client = AuthorizedGatewayMCPClient(
                name="probe",
                is_stateful=True,
                mcp_config=StdioMCPConfig(
                    command=workspace._gateway_python, args=[str(script)]
                ),
            )
            client.attach(workspace._gateway, agent_id="agent", session_id="session")
            await client.connect()
            try:
                tool = await client.get_tool("hostname")
                assert tool._egis_mcp_client is client
                result = await tool()
                container_name = await backend.exec_shell(["hostname"])
                assert container_name.stdout.decode().strip() in str(result)
                assert socket.gethostname() not in str(result)
            finally:
                await client.close()
            assert not await backend.file_exists(workspace._mcp_file)
            assert workspace._gateway.tmp_dir == "/dev/shm"
            another = await manager.get_workspace(
                "sandbox-test", "other-agent", "other-session", project_id=project_id
            )
            assert another.workspace_id == workspace.workspace_id
            other_project = await manager.get_workspace(
                "sandbox-test", "agent", "other-project-session", project_id=other_id
            )
            assert other_project.workspace_id == workspace.workspace_id
            assert other_project.runtime_binding_id != workspace.runtime_binding_id
            assert (
                await other_project.get_backend().read_file(
                    str(other_root / "secret.txt")
                )
            ) == b"other project"
            assert not (
                await other_project.get_backend().exec_shell(
                    ["cat", str(probe / "input.txt")]
                )
            ).ok()
            other_bash = next(
                t for t in await other_project.list_tools() if t.name == "Bash"
            )
            other_pwd = [chunk async for chunk in await other_bash(command="pwd")]
            assert str(other_root / ".work") in str(other_pwd)

            assert another._sandbox.id != workspace._sandbox.id
            assert not await another.get_backend().file_exists(
                published.dir + "/SKILL.md"
            )
            status, body = await another._gateway.exec_request(
                "GET", "/mcps", params={"agent_id": "agent", "session_id": "session"}
            )
            assert status == 200 and json.loads(body) == []
            assert (
                await another.get_backend().read_file(
                    str(probe / "artifacts/result.txt")
                )
                == b"sandbox output"
            )
            sandbox_id = workspace._sandbox.id
            await manager.close(workspace.workspace_id)
            again = await manager.get_workspace(
                "sandbox-test", "agent", "session", project_id=project_id
            )
            assert again._sandbox.id == sandbox_id
            assert (
                await again.get_backend().read_file(str(probe / "artifacts/result.txt"))
                == b"sandbox output"
            )
            await manager.release_project("sandbox-test", project_id)
            manager.files.remove_project("sandbox-test", project_id)
            assert not probe.exists()
            print(
                "Docker 验收通过：真实工具、MCP、激活技能、隔离、只读资料、暂停重连和项目释放"
            )
    finally:
        # 正常关闭保留容器；测试才通过唯一 metadata 精确销毁自己的临时容器。
        if cleanup is not None:
            try:
                remaining = set()
                for pid in (project_id, other_id):
                    infos = await cleanup.list_sandbox_infos(
                        SandboxFilter(metadata={"agentkit.project.id": pid})
                    )
                    remaining.update(info.id for info in infos.sandbox_infos)
                for sandbox_id in remaining:
                    await cleanup.kill_sandbox(sandbox_id)
            finally:
                await cleanup.close()
        if probe.exists():
            shutil.rmtree(probe)
        shutil.rmtree(other_root)
        shutil.rmtree(source)


@pytest.mark.asyncio
async def test_real_docker_recovers_missing_sandbox_without_losing_project_files():
    from egis_agentkit.client.nacos._settings import OpenSandboxConfig
    from egis_agentkit.service.workspace._sandbox_manager import SandboxManager

    root = Path(os.environ["AGENTKIT_TEST_OPENSANDBOX_ROOT"]).resolve()
    settings = OpenSandboxConfig(
        domain=os.environ["AGENTKIT_TEST_OPENSANDBOX_DOMAIN"],
        host_root=str(root),
        image=os.environ.get("AGENTKIT_TEST_OPENSANDBOX_IMAGE", "python:3.11-slim"),
        keepalive_interval_seconds=0.05,
    )
    project_id = "test-recover-" + uuid4().hex
    manager = SandboxManager(root, settings)
    manager.files.initialize_project("sandbox-test", project_id)
    manager.files.write(
        "sandbox-test", project_id, "input.txt", b"persistent input", area="uploads"
    )
    manager.files.write(
        "sandbox-test", project_id, "report.txt", b"persistent result", area="artifacts"
    )
    async with manager:
        try:
            workspace = await manager.get_workspace(
                "sandbox-test", "agent", "session", project_id=project_id
            )
            original_id = workspace.sandbox_id
            async with manager.keep_alive(workspace):
                before = await workspace._sandbox.get_info()
                await asyncio.sleep(settings.keepalive_interval_seconds * 2)
                after = await workspace._sandbox.get_info()
                assert after.expires_at > before.expires_at
                manager._cache[workspace.runtime_binding_id] = (
                    workspace,
                    time.monotonic() - manager._ttl - 1,
                )
                await manager._sweep_once()
                result = await workspace.get_backend().exec_shell(["true"])
                assert result.ok()
            # 只销毁本测试独占的实例，模拟到期或外部删除。
            await workspace._sandbox.kill()
            recovered = await manager.get_workspace(
                "sandbox-test", "agent", "session", project_id=project_id
            )
            assert recovered.sandbox_id != original_id
            project_root = manager.files.project_root("sandbox-test", project_id)
            assert (
                await recovered.get_backend().read_file(str(project_root / "input.txt"))
                == b"persistent input"
            )
            assert (
                await recovered.get_backend().read_file(
                    str(project_root / "artifacts/report.txt")
                )
                == b"persistent result"
            )
        finally:
            await manager.release_project("sandbox-test", project_id)
            manager.files.remove_project("sandbox-test", project_id)
