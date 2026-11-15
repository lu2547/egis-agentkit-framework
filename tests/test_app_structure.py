"""The HTTP application stays a composition root with domain controllers."""

import ast
import importlib
from pathlib import Path

import egis_agentkit_app
import pytest


def test_application_root_contains_only_composition_and_controller_packages():
    root = Path(egis_agentkit_app.__file__).parent
    entries = {entry.name for entry in root.iterdir() if entry.name != "__pycache__"}
    assert entries == {"__init__.py", "__main__.py", "bootstrap", "controller"}


def test_business_controllers_own_their_http_models():
    root = Path(egis_agentkit_app.__file__).parent / "controller"
    for domain in ("agent", "team", "session", "project", "resource", "access", "user"):
        package = root / domain
        assert (package / "__init__.py").is_file(), domain
        assert (package / "_model.py").is_file(), domain
    assert not (root / "_router.py").exists()
    assert {p.name for p in root.glob("*.py")} == {"__init__.py"}
    assert (root / "base" / "_dependency.py").is_file()
    assert (root / "base" / "_response.py").is_file()
    assert (root / "base" / "_exception.py").is_file()


def test_framework_http_adapters_do_not_depend_on_application():
    modules = (
        "egis_agentkit.client.identity",
        "egis_agentkit.extension.http._http_identity_middleware_impl",
        "egis_agentkit.extension.http._stream_ready_middleware_impl",
    )
    for name in modules:
        spec = importlib.util.find_spec(name)
        assert spec is not None, name
        path = Path(spec.origin)
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("egis_agentkit_app")
    assert callable(importlib.import_module(modules[0]).JWTIdentity)


def test_cli_loads_the_existing_project_env_after_directory_migration(monkeypatch):
    module_name = "egis_agentkit_app.bootstrap._cli"
    assert importlib.util.find_spec(module_name) is not None
    cli = importlib.import_module(module_name)
    paths = []
    monkeypatch.setattr(cli, "load_dotenv", lambda path: paths.append(path))
    monkeypatch.setattr("sys.argv", ["egis-agentkit", "--help"])
    with pytest.raises(SystemExit) as ended:
        cli.main()
    assert ended.value.code == 0
    assert paths == [Path(egis_agentkit_app.__file__).resolve().parents[2] / ".env"]
