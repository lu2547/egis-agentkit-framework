from pathlib import Path

import pytest
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import Read
from runtime_helpers import project_prompt
from test_domain import expert, identities

from egis_agentkit.bootstrap._middlewares import (
    build_tool_access_middleware,
)
from egis_agentkit.core.session._context import session_execution_context
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.session._preparation import SessionPreparationService
from egis_agentkit.service.session._session import SessionService


@pytest.mark.asyncio
async def test_uploaded_pdf_is_named_by_its_readable_project_path(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    tasks = SessionService(storage, tmp_path)
    task = await tasks.add_session(alice, source["id"], "private")
    pdf = (
        Path(__file__).parents[2] / "egis-agentkit-frontend/tests/fixtures/preview.pdf"
    ).read_bytes()
    target = tasks.files.write(
        alice.user_id, task["project_id"], "[初级] 第三期 文件.pdf", pdf, area="uploads"
    )
    archive = tasks.files.user_root(alice.user_id) / "uploads" / "[初级] 第三期 文件.pdf"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(pdf)
    second = await tasks.add_session(
        alice, source["id"], "private", project_id=task["project_id"]
    )
    internal = tasks.files.write(
        alice.user_id, task["project_id"], "internal-script.py", b"script"
    )
    final = tasks.files.write(
        alice.user_id,
        task["project_id"],
        "final-report.txt",
        b"report",
        area="artifacts",
    )
    foreign = tasks.files.write(
        alice.user_id, "other-project", "private.txt", b"private", area="uploads"
    )
    async with SessionPreparationService(storage, tmp_path).bind(second["session_id"]):
        scope = session_execution_context.get()
        policy = build_tool_access_middleware(alice, scope, PermissionService(storage))
        prompt = await project_prompt(scope, tmp_path).on_system_prompt(None, "system")
        assert str(target) in prompt and "pages" in prompt
        assert (
            "上传资料直接位于根目录" in prompt
            and ".work/" in prompt
            and "artifacts/" in prompt
        )
        assert str(archive) not in prompt
        assert str(internal) not in prompt and str(final) not in prompt

        async def native_check(**kwargs):
            return PermissionDecision(PermissionBehavior.ALLOW, "native read check")

        tool = Read(model_input_types=[])
        from agentscope.permission import (
            PermissionContext,
            PermissionEngine,
            PermissionMode,
        )

        native = PermissionEngine(PermissionContext(mode=PermissionMode.DONT_ASK))
        assert (
            await native.check_permission(tool, {"file_path": str(target)})
        ).behavior == PermissionBehavior.ALLOW

        decision = await policy.on_check_permission(
            None, {"tool": tool, "tool_input": {"file_path": str(target)}}, native_check
        )
        assert decision.behavior == PermissionBehavior.ALLOW
        for readable in (internal, final):
            assert (
                await policy.on_check_permission(
                    None,
                    {"tool": tool, "tool_input": {"file_path": str(readable)}},
                    native_check,
                )
            ).behavior == PermissionBehavior.ALLOW
        from types import SimpleNamespace

        write_denied = await policy.on_check_permission(
            None,
            {
                "tool": SimpleNamespace(name="Write"),
                "tool_input": {"file_path": str(target)},
            },
            native_check,
        )
        assert write_denied.behavior == PermissionBehavior.DENY
        result = await tool.call(str(target), pages="1-2")
        assert "Preview slide 1" in "\n".join(
            block.text for block in result.content if block.type == "text"
        )
        denied = await policy.on_check_permission(
            None,
            {"tool": tool, "tool_input": {"file_path": str(archive)}},
            native_check,
        )
        assert denied.behavior == PermissionBehavior.DENY
        assert (
            await policy.on_check_permission(
                None,
                {"tool": tool, "tool_input": {"file_path": str(foreign)}},
                native_check,
            )
        ).behavior == PermissionBehavior.DENY
