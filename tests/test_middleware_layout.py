"""Middleware 只承载流程增强，消息总线能力仍归 Core。"""

import ast
import importlib.util
from pathlib import Path

import egis_agentkit.middleware


def test_middleware_root_exports_components_without_flat_implementations():
    root = Path(egis_agentkit.middleware.__file__).parent
    assert {file.name for file in root.glob("*.py")} == {"__init__.py"}
    packages = {
        file.name
        for file in root.iterdir()
        if file.is_dir() and file.name != "__pycache__"
    }
    assert packages == {"agent", "team", "resource", "session", "workspace", "http"}
    for package in packages:
        assert (root / package / "__init__.py").is_file()


def test_message_bus_is_a_core_capability_without_middleware_dependencies():
    spec = importlib.util.find_spec("egis_agentkit.core.message_bus")
    assert spec is not None
    from egis_agentkit.core.message_bus import EgisMessageBus, LeaseLostError

    assert EgisMessageBus.__module__ == "egis_agentkit.core.message_bus._message_bus"
    assert issubclass(LeaseLostError, RuntimeError)
    for file in Path(spec.origin).parent.glob("*.py"):
        for node in ast.walk(ast.parse(file.read_text())):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("egis_agentkit.middleware")
