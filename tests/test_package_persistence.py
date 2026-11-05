from pathlib import Path

import pytest
from test_domain import identities

from egis_agentkit.packages import PackageLoader
from egis_agentkit.services.packages import PackageService


@pytest.mark.asyncio
async def test_package_descriptor_can_restore_after_app_restart(storage):
    _, admin, _, _ = await identities(storage)
    releases = (
        Path(__file__).resolve().parents[3]
        / "demo/agentscope-demo/agentkit_demo/releases"
    )
    first = PackageService(storage, PackageLoader([releases]))
    await first.register(admin, releases / "egis-demo/1.0.0")
    replacement = PackageLoader([releases])
    await PackageService(storage, replacement).restore_loaded()
    assert replacement.get("egis-demo", "1.0.0").tools_factory is not None


@pytest.mark.asyncio
async def test_registered_on_one_node_lazily_resolves_on_another_and_disable_is_shared(
    storage, tmp_path
):
    from test_packages import release

    from egis_agentkit.errors import Conflict, NotFound

    _, admin, _, _ = await identities(storage)
    folder = release(tmp_path, "1.0.0")
    a = PackageService(storage, PackageLoader([tmp_path]))
    b = PackageService(storage, PackageLoader([tmp_path]))
    await a.register(admin, folder)
    pinned = await b.resolve("test.report", "1.0.0")
    assert await pinned.tools_factory(None) == ["1.0.0"]
    await a.disable(admin, "test.report", "1.0.0")
    with pytest.raises(NotFound):
        await b.resolve("test.report", "1.0.0")
    assert await pinned.tools_factory(None) == ["1.0.0"]
    with pytest.raises(Conflict):
        await a.register(admin, folder)


@pytest.mark.asyncio
async def test_failed_registration_does_not_resolve_staged_package(
    storage, tmp_path, monkeypatch
):
    from contextlib import asynccontextmanager

    from test_packages import release

    from egis_agentkit.errors import NotFound

    _, admin, _, _ = await identities(storage)
    service = PackageService(storage, PackageLoader([tmp_path]))
    folder = release(tmp_path, "1.0.0")
    original = storage.transaction

    @asynccontextmanager
    async def fail():
        raise RuntimeError("simulated database failure")
        yield

    monkeypatch.setattr(storage, "transaction", fail)
    with pytest.raises(RuntimeError):
        await service.register(admin, folder)
    monkeypatch.setattr(storage, "transaction", original)
    with pytest.raises(NotFound):
        await service.resolve("test.report", "1.0.0")


@pytest.mark.asyncio
async def test_concurrent_registration_is_idempotent_across_nodes(storage, tmp_path):
    import asyncio

    from test_packages import release

    _, admin, _, _ = await identities(storage)
    folder = release(tmp_path, "1.0.0")
    first = PackageService(storage, PackageLoader([tmp_path]))
    second = PackageService(storage, PackageLoader([tmp_path]))
    results = await asyncio.gather(
        first.register(admin, folder), second.register(admin, folder)
    )
    assert results[0]["checksum_sha256"] == results[1]["checksum_sha256"]
    assert await (await second.resolve("test.report", "1.0.0")).tools_factory(None) == [
        "1.0.0"
    ]


@pytest.mark.asyncio
async def test_disable_during_lazy_load_blocks_new_resolution(
    storage, tmp_path, monkeypatch
):
    import asyncio

    from test_packages import release

    from egis_agentkit.errors import NotFound

    _, admin, _, _ = await identities(storage)
    folder = release(tmp_path, "1.0.0")
    a = PackageService(storage, PackageLoader([tmp_path]))
    b = PackageService(storage, PackageLoader([tmp_path]))
    await a.register(admin, folder)
    started, proceed = asyncio.Event(), asyncio.Event()
    original = b.loader.load

    async def delayed(*args, **kwargs):
        started.set()
        await proceed.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(b.loader, "load", delayed)
    pending = asyncio.create_task(b.resolve("test.report", "1.0.0"))
    await started.wait()
    await a.disable(admin, "test.report", "1.0.0")
    proceed.set()
    with pytest.raises(NotFound):
        await pending


@pytest.mark.asyncio
async def test_register_same_package_returns_persisted_path(storage, tmp_path):
    import shutil

    from test_packages import release

    _, admin, _, _ = await identities(storage)
    first_path = release(tmp_path, "1.0.0")
    second_path = tmp_path / "other"
    shutil.copytree(first_path, second_path)
    first = PackageService(storage, PackageLoader([tmp_path]))
    second = PackageService(storage, PackageLoader([tmp_path]))
    await first.register(admin, first_path)
    result = await second.register(admin, second_path)
    assert result["download_url"] == first_path.resolve().as_uri()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "changed"])
async def test_unavailable_registered_bytes_are_safe_conflict_and_pinned_object_survives(
    storage, tmp_path, damage
):
    import shutil

    from test_packages import release

    from egis_agentkit.errors import Conflict

    _, admin, _, _ = await identities(storage)
    folder = release(tmp_path, "1.0.0")
    first = PackageService(storage, PackageLoader([tmp_path]))
    second = PackageService(storage, PackageLoader([tmp_path]))
    await first.register(admin, folder)
    pinned = await first.resolve("test.report", "1.0.0")
    if damage == "missing":
        shutil.rmtree(folder)
    else:
        (folder / "helper.py").write_text('VALUE="changed"\n')
    with pytest.raises(Conflict, match="发布包文件不可用或完整性校验失败") as exc:
        await second.resolve("test.report", "1.0.0")
    assert str(tmp_path) not in str(exc.value)
    assert exc.value.__cause__ is None
    assert await pinned.tools_factory(None) == ["1.0.0"]
