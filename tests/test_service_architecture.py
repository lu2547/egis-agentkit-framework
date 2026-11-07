"""Public service and extension entry points are independent of the HTTP app."""

import importlib

import pytest


@pytest.mark.parametrize(
    ("module", "symbol"),
    [
        ("egis_agentkit.service.agent", "AgentService"),
        ("egis_agentkit.service.agent.package", "PackageService"),
        ("egis_agentkit.service.team", "TeamService"),
        ("egis_agentkit.service.access", "PermissionService"),
        ("egis_agentkit.service.access", "ShareService"),
        ("egis_agentkit.service.model", "ModelService"),
        ("egis_agentkit.service.session", "TaskService"),
        ("egis_agentkit.service.project", "ProjectService"),
        ("egis_agentkit.service.resource", "ResourceService"),
        ("egis_agentkit.service.user", "UserService"),
        ("egis_agentkit.integration.nacos", "NacosClient"),
        ("egis_agentkit.integration.nacos", "NacosHubManagement"),
        ("egis_agentkit.extensions.hub", "NacosSkillHub"),
        ("egis_agentkit.extensions.hub", "NacosMCPHub"),
        ("egis_agentkit.extensions.workspace", "EgisLocalWorkspaceManager"),
        ("egis_agentkit.extensions.message_bus", "EgisMessageBus"),
    ],
)
def test_public_entry_points_load_without_application(module, symbol):
    try:
        exported = getattr(importlib.import_module(module), symbol)
    except (ImportError, AttributeError) as exc:
        pytest.fail(f"Missing framework entry point {module}.{symbol}: {exc}")
    assert callable(exported)


def test_storage_and_integration_do_not_depend_on_runtime_extensions():
    import ast
    from pathlib import Path
    import egis_agentkit

    root = Path(egis_agentkit.__file__).parent
    violations = []
    for layer in ("storage", "integration"):
        for path in (root / layer).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.ImportFrom) and node.module and (
                    node.module.startswith("egis_agentkit.extensions")
                    or node.module.startswith("egis_agentkit.service")
                ):
                    violations.append(f"{path.relative_to(root)}:{node.lineno} {node.module}")
    assert not violations, "Lower-layer dependency violations: " + ", ".join(violations)
