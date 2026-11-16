"""业务能力和中间件可脱离 HTTP App 独立导入，底层依赖保持单向。"""

import importlib

import pytest


@pytest.mark.parametrize(
    ("module", "symbol"),
    [
        ("egis_agentkit.bootstrap", "init_agentkit"),
        ("egis_agentkit.client.nacos", "nacos_bootstrap"),
        ("egis_agentkit.service.agent._agent_manager", "AgentManager"),
        ("egis_agentkit.core.agent._expert_definition", "ExpertDefinition"),
        ("egis_agentkit.core.mcp", "MCPAuthorization"),
        ("egis_agentkit.service.team._definition", "TeamDefinition"),
        ("egis_agentkit.service.access._permission", "PermissionService"),
        ("egis_agentkit.service.access._grant", "GrantService"),
        ("egis_agentkit.service.access._share", "ShareService"),
        ("egis_agentkit.service._model", "ModelService"),
        ("egis_agentkit.service.session._session", "SessionService"),
        ("egis_agentkit.service.project._project_manager", "ProjectManager"),
        ("egis_agentkit.service.resource._catalog", "ResourceService"),
        ("egis_agentkit.service._user", "UserService"),
        ("egis_agentkit.client.nacos", "NacosClient"),
        ("egis_agentkit.service.resource._hubs", "LocalSkillHub"),
        ("egis_agentkit.service.resource._hubs", "LocalMCPHub"),
        (
            "egis_agentkit.service.workspace._egis_sandbox_manager",
            "EgisSandboxManager",
        ),
        ("egis_agentkit.core.message_bus", "EgisMessageBus"),
    ],
)
def test_public_entry_points_load_without_application(module, symbol):
    try:
        exported = getattr(importlib.import_module(module), symbol)
    except (ImportError, AttributeError) as exc:
        pytest.fail(f"Missing framework entry point {module}.{symbol}: {exc}")
    assert callable(exported)


def test_storage_and_client_do_not_depend_on_middleware_or_services():
    import ast
    from pathlib import Path

    import egis_agentkit

    root = Path(egis_agentkit.__file__).parent
    violations = []
    for layer in ("storage", "client"):
        for path in (root / layer).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if (
                    isinstance(node, ast.ImportFrom)
                    and node.module
                    and (
                        node.module.startswith("egis_agentkit.middleware")
                        or node.module.startswith("egis_agentkit.service")
                    )
                ):
                    violations.append(
                        f"{path.relative_to(root)}:{node.lineno} {node.module}"
                    )
    assert not violations, "Lower-layer dependency violations: " + ", ".join(violations)


@pytest.mark.parametrize(
    "module",
    ["egis_agentkit.core.mcp._authorization", "egis_agentkit.service._user"],
)
def test_identity_and_credential_capabilities_use_bound_storage(module):
    import ast
    import inspect

    service = importlib.import_module(module)
    for node in ast.walk(ast.parse(inspect.getsource(service))):
        assert not (isinstance(node, ast.Attribute) and node.attr == "engine")
        assert not (isinstance(node, ast.keyword) and node.arg == "conn")
