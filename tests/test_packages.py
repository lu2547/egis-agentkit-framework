"""Protect version isolation, staging, trusted paths and real artifact behavior."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from egis_agentkit.packages import PackageLoader, package_digest


def release(root, version, *, body=None, reference="test.report"):
    folder = root / version
    folder.mkdir(parents=True)
    (folder / "manifest.json").write_text(
        json.dumps(
            {
                "assembly_ref": reference,
                "version": version,
                "entrypoint": "package:register",
                "framework_api_version": 1,
            }
        )
    )
    (folder / "helper.py").write_text(f"VALUE = {version!r}\n")
    (folder / "package.py").write_text(
        body
        or "from egis_agentkit.packages import Assembly\nfrom .helper import VALUE\nasync def tools(scope): return [VALUE]\ndef register(): return Assembly(tools_factory=tools)\n"
    )
    return folder


@pytest.mark.asyncio
async def test_versions_coexist_idempotent_load_and_disable_preserves_old_instance(
    tmp_path,
):
    loader = PackageLoader([tmp_path])
    first = release(tmp_path, "1.0.0")
    second = release(tmp_path, "1.1.0")
    first_asset = await loader.load(first)
    old = loader.get("test.report", "1.0.0")
    await loader.load(second)
    assert await old.tools_factory(None) == ["1.0.0"]
    assert await loader.get("test.report", "1.1.0").tools_factory(None) == ["1.1.0"]
    assert (await loader.load(first)).checksum_sha256 == first_asset.checksum_sha256
    assert loader.get("test.report", "1.0.0") is old
    loader.disable("test.report", "1.0.0")
    with pytest.raises(KeyError):
        loader.get("test.report", "1.0.0")
    assert await old.tools_factory(None) == ["1.0.0"]
    assert len(loader.status()) == 2


@pytest.mark.asyncio
async def test_failed_new_import_leaves_registry_and_modules_untouched(tmp_path):
    loader = PackageLoader([tmp_path])
    await loader.load(release(tmp_path, "1.0.0"))
    old = loader.get("test.report", "1.0.0")
    before = {m for m in sys.modules if m.startswith("_egis_assembly_")}
    broken = release(
        tmp_path,
        "1.1.0",
        body='from . import helper\nraise RuntimeError("broken candidate")\n',
    )
    with pytest.raises(ValueError):
        await loader.load(broken)
    assert loader.get("test.report", "1.0.0") is old
    with pytest.raises(KeyError):
        loader.get("test.report", "1.1.0")
    assert {m for m in sys.modules if m.startswith("_egis_assembly_")} == before
    assert len(loader.status()) == 1


@pytest.mark.asyncio
async def test_trusted_roots_expected_hash_and_same_version_immutability(tmp_path):
    trusted = tmp_path / "trusted"
    trusted.mkdir()
    outside = release(tmp_path / "outside", "1.0.0")
    loader = PackageLoader([trusted])
    with pytest.raises(ValueError):
        await loader.load(outside)
    (trusted / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError):
        await loader.load(trusted / "escape")
    valid = release(trusted, "1.0.0")
    checksum = package_digest(valid)
    with pytest.raises(ValueError):
        await loader.load(valid, expected_sha256="0" * 64)
    await loader.load(valid, expected_sha256=checksum)
    old = loader.get("test.report", "1.0.0")
    (valid / "helper.py").write_text('VALUE = "changed"\n')
    with pytest.raises(ValueError):
        await loader.load(valid)
    assert loader.get("test.report", "1.0.0") is old
    assert await old.tools_factory(None) == ["1.0.0"]


def test_digest_ignores_caches_and_manifest_digest_self_field(tmp_path):
    root = release(tmp_path, "1.0.0")
    original = package_digest(root)
    (root / "__pycache__").mkdir()
    (root / "__pycache__" / "ignored").write_bytes(b"cache")
    (root / "ignored.pyc").write_bytes(b"cache")
    manifest = json.loads((root / "manifest.json").read_text())
    manifest["checksum_sha256"] = "a" * 64
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2))
    assert package_digest(root) == original
    (root / "helper.py").write_text('VALUE = "other"\n')
    assert package_digest(root) != original


@pytest.mark.asyncio
async def test_manifest_requirement_and_return_contract_are_checked(tmp_path):
    folder = release(tmp_path, "1.0.0")
    manifest = json.loads((folder / "manifest.json").read_text())
    manifest["requirements"] = {"a-not-installed-egis-package": ">=999"}
    (folder / "manifest.json").write_text(json.dumps(manifest))
    loader = PackageLoader([tmp_path])
    with pytest.raises(ValueError):
        await loader.load(folder)
    assert loader.status() == []
    manifest["requirements"] = {}
    (folder / "manifest.json").write_text(json.dumps(manifest))
    (folder / "package.py").write_text('def register(): return {"tools": []}\n')
    with pytest.raises(ValueError):
        await loader.load(folder)
    assert loader.status() == []


@pytest.mark.asyncio
async def test_business_demo_native_tool_writes_versioned_work_and_artifact(tmp_path):
    from agentscope.tool import ToolBase, ToolChunk

    root = (
        Path(__file__).resolve().parents[3]
        / "demo/agentscope-demo/agentkit_demo/releases"
    )
    loader = PackageLoader([root])
    scope = SimpleNamespace(
        user_id="alice",
        agent_id="agent-a",
        session_id="session-a",
        definition={},
        snapshot={},
        workdir=tmp_path / "work",
        artifacts_dir=tmp_path / "artifacts",
    )
    for version in ("1.0.0", "1.1.0"):
        await loader.load(root / "egis-demo" / version)
    old = loader.get("egis-demo", "1.0.0")
    tool = (await old.tools_factory(scope))[0]
    assert isinstance(tool, ToolBase)
    result = await tool(
        title="Delivery review", findings=["Completed review", "Three actions remain"]
    )
    assert isinstance(result, ToolChunk)
    assert (
        (scope.artifacts_dir / "report.md").read_text()
        == "# Delivery review\n\nPackage version: 1.0.0\n\n## Findings\n\n- Completed review\n- Three actions remain\n"
    )
    assert (scope.workdir / "report.md").read_text() == (
        scope.artifacts_dir / "report.md"
    ).read_text()
    assert result.metadata["artifacts"] == [{"path": "report.md", "area": "artifacts"}]
    assert str(tmp_path) not in result.model_dump_json()
    newtool = (await loader.get("egis-demo", "1.1.0").tools_factory(scope))[0]
    await newtool(title="New review", findings=["Current"])
    assert "Package version: 1.1.0" in (scope.artifacts_dir / "report.md").read_text()
    assert tool.version == "1.0.0"
    middlewares = await old.middlewares_factory(scope, None)
    assert len(middlewares) == 1
    assert "1.0.0" in await middlewares[0].on_system_prompt(None, "System")


@pytest.mark.asyncio
async def test_loaded_version_keeps_lazy_relative_import_bytes_after_source_drift(
    tmp_path,
):
    folder = release(
        tmp_path,
        "1.0.0",
        body="from egis_agentkit.packages import Assembly\nasync def tools(scope):\n from .helper import VALUE\n return [VALUE]\ndef register(): return Assembly(tools_factory=tools)\n",
    )
    loader = PackageLoader([tmp_path])
    await loader.load(folder)
    pinned = loader.get("test.report", "1.0.0")
    (folder / "helper.py").write_text('VALUE="mutated source"\n')
    assert await pinned.tools_factory(None) == ["1.0.0"]


@pytest.mark.asyncio
async def test_nested_python_package_import_and_concurrent_load_are_isolated(tmp_path):
    import asyncio

    folder = release(tmp_path, "1.0.0")
    (folder / "package.py").unlink()
    (folder / "package").mkdir()
    (folder / "package" / "__init__.py").write_text(
        "from egis_agentkit.packages import Assembly\nfrom .helper import VALUE\nasync def tools(scope):return [VALUE]\ndef register():return Assembly(tools_factory=tools)\n"
    )
    (folder / "package" / "helper.py").write_text('VALUE="nested"\n')
    loader = PackageLoader([tmp_path])
    original_path = list(sys.path)
    assets = await asyncio.gather(*(loader.load(folder) for _ in range(4)))
    assert len({a.checksum_sha256 for a in assets}) == 1
    assert len(loader.status()) == 1
    assert await loader.get("test.report", "1.0.0").tools_factory(None) == ["nested"]
    assert sys.path == original_path


@pytest.mark.asyncio
async def test_demo_artifact_write_never_follows_existing_leaf_symlink(tmp_path):
    root = (
        Path(__file__).resolve().parents[3]
        / "demo/agentscope-demo/agentkit_demo/releases"
    )
    loader = PackageLoader([root])
    await loader.load(root / "egis-demo" / "1.0.0")
    outside = tmp_path / "outside.md"
    outside.write_text("outside stays intact")
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    (artifacts / "report.md").symlink_to(outside)
    scope = SimpleNamespace(workdir=tmp_path / "work", artifacts_dir=artifacts)
    tool = (await loader.get("egis-demo", "1.0.0").tools_factory(scope))[0]
    await tool(title="Safe", findings=["Bound path"])
    assert outside.read_text() == "outside stays intact"
    assert not (artifacts / "report.md").is_symlink()
    assert "# Safe" in (artifacts / "report.md").read_text()


@pytest.mark.asyncio
async def test_invalid_factory_contract_and_api_version_never_publish(tmp_path):
    folder = release(
        tmp_path,
        "1.0.0",
        body="from egis_agentkit.packages import Assembly\ndef tools(scope):return []\ndef register():return Assembly(tools_factory=tools)\n",
    )
    loader = PackageLoader([tmp_path])
    with pytest.raises(ValueError):
        await loader.load(folder)
    assert loader.status() == []
    (folder / "package.py").write_text(
        "from egis_agentkit.packages import Assembly\ndef register():return Assembly()\n"
    )
    manifest = json.loads((folder / "manifest.json").read_text())
    manifest["framework_api_version"] = True
    (folder / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        await loader.load(folder)
    assert loader.status() == []
