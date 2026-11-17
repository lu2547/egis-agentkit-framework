"""约束中间件目录及依赖方向，防止业务能力重新混入流程接入层。"""

import ast
from importlib.util import resolve_name
from pathlib import Path

import egis_agentkit

ROOT = Path(egis_agentkit.__file__).parent


def _imports(path):
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom):
            package = ".".join(path.parent.relative_to(ROOT.parent).parts)
            name = (
                resolve_name("." * node.level + (node.module or ""), package)
                if node.level
                else node.module
            )
            if name:
                yield name
                yield from (f"{name}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)


def test_middleware_package_replaces_legacy_extension():
    assert (ROOT / "middleware" / "__init__.py").is_file()
    assert not (ROOT / "extension").exists()


def test_capability_packages_have_explicit_boundaries_for_distribution():
    packages = {path.parent for path in ROOT.rglob("*.py")}
    missing = [
        str(path.relative_to(ROOT))
        for path in packages
        if not (path / "__init__.py").is_file()
    ]
    assert not missing, "发布包缺少明确的包入口：" + ", ".join(sorted(missing))


def test_internal_import_targets_exist_including_type_checking_blocks():
    violations = []
    for path in ROOT.rglob("*.py"):
        package = ".".join(path.parent.relative_to(ROOT.parent).parts)
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.ImportFrom):
                continue
            name = (
                resolve_name("." * node.level + (node.module or ""), package)
                if node.level
                else node.module
            )
            if not name or not name.startswith("egis_agentkit"):
                continue
            target = ROOT.parent.joinpath(*name.split("."))
            if (
                not target.with_suffix(".py").is_file()
                and not (target / "__init__.py").is_file()
            ):
                violations.append(f"{path.relative_to(ROOT)}:{node.lineno} -> {name}")
    assert not violations, "迁移后存在失效的内部导入：" + ", ".join(violations)


def test_middleware_contains_only_native_or_http_middleware():
    root = ROOT / "middleware"
    assert root.is_dir()
    allowed_bases = {
        "MiddlewareBase",
        "ToolMiddlewareBase",
        "ReMeMiddleware",
        "Protocol",
    }
    http_middleware = {"IdentityGuard", "StreamReady"}
    violations = []
    for path in root.rglob("*.py"):
        for node in ast.parse(path.read_text()).body:
            if not isinstance(node, ast.ClassDef):
                continue
            bases = {ast.unparse(base) for base in node.bases}
            if not bases.intersection(allowed_bases) and not (
                path.parent.name == "http" and node.name in http_middleware
            ):
                violations.append(f"{path.relative_to(ROOT)}:{node.name}")
    assert not violations, "非中间件能力混入 Middleware：" + ", ".join(violations)


def test_core_client_storage_do_not_depend_on_services_or_middleware():
    violations = []
    forbidden = (
        "egis_agentkit.service",
        "egis_agentkit.middleware",
        "egis_agentkit.extension",
        "egis_agentkit.bootstrap",
    )
    for layer in ("core", "client", "storage"):
        for path in (ROOT / layer).rglob("*.py"):
            for name in _imports(path):
                if name.startswith(forbidden):
                    violations.append(f"{path.relative_to(ROOT)} -> {name}")
    assert not violations, "底层反向依赖：" + ", ".join(violations)


def test_services_do_not_construct_concrete_middleware():
    violations = []
    for path in (ROOT / "service").rglob("*.py"):
        for name in _imports(path):
            if name.startswith(("egis_agentkit.middleware", "egis_agentkit.extension")):
                violations.append(f"{path.relative_to(ROOT)} -> {name}")
    assert not violations, "中间件应由 Bootstrap 装配：" + ", ".join(violations)


def test_service_dependencies_have_no_cycles():
    modules = {
        ".".join(path.relative_to(ROOT.parent).with_suffix("").parts): path
        for path in (ROOT / "service").rglob("*.py")
    }
    edges = {
        name: set(_imports(path)).intersection(modules)
        for name, path in modules.items()
    }
    checked = set()

    def visit(name, chain):
        assert name not in chain, "Service 循环依赖：" + " -> ".join([*chain, name])
        if name in checked:
            return
        for dependency in edges[name]:
            visit(dependency, [*chain, name])
        checked.add(name)

    for name in edges:
        visit(name, [])


def test_middleware_assembly_delegates_storage_operations_to_services():
    path = ROOT / "bootstrap" / "_middlewares.py"
    violations = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if any(
                isinstance(part, ast.Attribute) and part.attr == "storage"
                for part in ast.walk(node.func.value)
            ):
                violations.append(f"{node.lineno}: {ast.unparse(node.func)}")
    assert not violations, "装配函数不能直接执行存储操作：" + ", ".join(violations)
