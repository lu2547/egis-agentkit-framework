"""Public service and extension entry points are independent of the HTTP app."""

import importlib

import pytest


@pytest.mark.parametrize(
    ("module", "symbol"),
    [
        ("egis_agentkit.bootstrap", "init_agentkit"),
        ("egis_agentkit.backend.nacos", "nacos_bootstrap"),
        ("egis_agentkit.service.agent._query", "AgentQuery"),
        ("egis_agentkit.service.agent._expert_definition", "ExpertDefinition"),
        ("egis_agentkit.service.team._definition", "TeamDefinition"),
        ("egis_agentkit.service.access._permission", "PermissionService"),
        ("egis_agentkit.service.access._grant", "GrantService"),
        ("egis_agentkit.service.access._share", "ShareService"),
        ("egis_agentkit.service._model", "ModelService"),
        ("egis_agentkit.service._session", "SessionService"),
        ("egis_agentkit.service._project", "ProjectService"),
        ("egis_agentkit.service.resource._catalog", "ResourceService"),
        ("egis_agentkit.service._user", "UserService"),
        ("egis_agentkit.backend.nacos", "NacosClient"),
        ("egis_agentkit.extension._resource_hubs_impl", "LocalSkillHub"),
        ("egis_agentkit.extension._resource_hubs_impl", "LocalMCPHub"),
        (
            "egis_agentkit.extension._workspace_manager_impl",
            "EgisLocalWorkspaceManager",
        ),
        ("egis_agentkit.extension._message_bus_impl", "EgisMessageBus"),
    ],
)
def test_public_entry_points_load_without_application(module, symbol):
    try:
        exported = getattr(importlib.import_module(module), symbol)
    except (ImportError, AttributeError) as exc:
        pytest.fail(f"Missing framework entry point {module}.{symbol}: {exc}")
    assert callable(exported)


def test_storage_and_backend_do_not_depend_on_runtime_extensions():
    import ast
    from pathlib import Path

    import egis_agentkit

    root = Path(egis_agentkit.__file__).parent
    violations = []
    for layer in ("storage", "backend"):
        for path in (root / layer).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if (
                    isinstance(node, ast.ImportFrom)
                    and node.module
                    and (
                        node.module.startswith("egis_agentkit.extension")
                        or node.module.startswith("egis_agentkit.service")
                    )
                ):
                    violations.append(
                        f"{path.relative_to(root)}:{node.lineno} {node.module}"
                    )
    assert not violations, "Lower-layer dependency violations: " + ", ".join(violations)


@pytest.mark.parametrize("module", ["resource._connector_auth", "_user"])
def test_identity_and_credential_services_use_bound_storage(module):
    import ast
    import inspect

    service = importlib.import_module(f"egis_agentkit.service.{module}")
    for node in ast.walk(ast.parse(inspect.getsource(service))):
        assert not (isinstance(node, ast.Attribute) and node.attr == "engine")
        assert not (isinstance(node, ast.keyword) and node.arg == "conn")
