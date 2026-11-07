import asyncio
import io
import zipfile

import pytest
from sqlalchemy import select, text
from test_domain import identities
from test_resources import Registry, service

from egis_agentkit.errors import NotFound
from egis_agentkit.service.resource.service import _archive_files


def bundle(version="1.2.3"):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "demo/SKILL.md",
            f"---\nname: demo\ndescription: Demo\nversion: {version}\nmetadata:\n  author: Demo\n  labels: [one, two]\ncustom: retain\n---\n# Original",
        )
        archive.writestr("demo/assets/logo.bin", b"attachment")
    return output.getvalue()


@pytest.mark.asyncio
async def test_import_enqueue_is_durable_owner_only_and_registration_is_deferred(
    storage, tmp_path
):
    from egis_agentkit.service.resource.imports import SkillImportService

    _, _, alice, bob = await identities(storage)
    registry = Registry()
    resources, http = await service(storage, tmp_path, registry)
    queue = SkillImportService(storage, resources)
    async with http:
        job = await queue.enqueue(
            alice,
            io.BytesIO(bundle()),
            filename="demo.zip",
            category="writing",
            avatar="book",
        )
        assert job["status"] == "queued" and not registry.calls
        with pytest.raises(NotFound):
            await queue.get(bob, job["job_id"])
        assert await queue.list(bob) == []
        # A new instance can recover the durable upload.
        recovery = SkillImportService(storage, resources)
        assert await recovery.run_once()
        done = await recovery.get(alice, job["job_id"])
        assert done["status"] == "succeeded" and done["progress"] == 100
        assert done["resource_version"] == "1.2.3"
        detail = await resources.detail(alice, "skill", done["resource_id"], "1.2.3")
        assert detail["payload"]["category"] == "writing"
        assert "name: demo" in detail["payload"]["markdown"]
        assert detail["payload"]["files"] == [
            {"path": "SKILL.md", "size": len(_archive_files(bundle())["SKILL.md"])},
            {"path": "assets/logo.bin", "size": 10},
        ]


@pytest.mark.asyncio
async def test_import_claim_fencing_recovery_and_retry_without_duplicate_asset(
    storage, tmp_path
):
    from egis_agentkit.service.resource.imports import SkillImportService
    from egis_agentkit.storage import tables as t

    _, _, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    first, second = (
        SkillImportService(storage, resources),
        SkillImportService(storage, resources),
    )
    job = await first.enqueue(alice, io.BytesIO(bundle()), filename="demo.zip")
    claims = await asyncio.gather(first.claim(), second.claim())
    claimed = next(c for c in claims if c)
    assert sum(c is not None for c in claims) == 1
    async with storage.transaction() as conn:
        await conn.execute(
            t.skill_imports.update().values(
                lease_until=text("now() - interval '1 second'")
            )
        )
    recovered = await second.claim()
    assert recovered["lease_token"] != claimed["lease_token"]
    assert not await first._update(claimed, stage="stale", progress=90)
    async with http:
        await second.process(recovered)
        # Simulate a lost completion checkpoint after the asset committed.
        async with storage.transaction() as conn:
            await conn.execute(
                t.skill_imports.update().values(
                    status="queued", result=None, archive=bundle()
                )
            )
        await first.run_once()
        assert (await first.get(alice, job["job_id"]))["status"] == "succeeded"
        async with storage.engine.connect() as conn:
            assert len((await conn.execute(select(t.assets))).all()) == 1


@pytest.mark.asyncio
async def test_skill_edit_preserves_attachments_and_frontmatter_version(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    async with http:
        original = await resources.upload_skill(
            alice, bundle(), category="writing", avatar="book"
        )
        edited = (
            _archive_files(bundle())["SKILL.md"]
            .decode()
            .replace("1.2.3", "1.2.4")
            .replace("# Original", "# Edited")
        )
        saved = await resources.save_skill(
            alice,
            {
                "resource_id": original["resource_id"],
                "base_version": "1.2.3",
                "markdown": edited,
                "category": "writing",
                "avatar": "book",
            },
        )
        content = await resources.download_skill(alice, saved["resource_id"], "1.2.4")
        files = _archive_files(content)
        assert files["assets/logo.bin"] == b"attachment"
        assert files["SKILL.md"].decode() == edited
        assert b"metadata:\n  author: Demo" in files["SKILL.md"]
        detail = await resources.detail(alice, "skill", saved["resource_id"], "1.2.4")
        assert detail["payload"]["markdown"] == edited
        with pytest.raises(ValueError, match="version"):
            await resources.upload_skill(alice, bundle(), "9.9.9")


@pytest.mark.asyncio
async def test_invalid_import_fails_as_job_and_oversize_is_rejected_before_queue(
    storage, tmp_path
):
    from egis_agentkit.service.resource.imports import SkillImportService

    _, _, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    queue = SkillImportService(storage, resources)
    job = await queue.enqueue(alice, io.BytesIO(b"not zip"), filename="invalid.zip")
    async with http:
        await queue.run_once()
    failed = await queue.get(alice, job["job_id"])
    assert failed["status"] == "failed" and failed["error"]
    assert (await queue.retry(alice, job["job_id"]))["status"] == "queued"
    resources.policies = type("Policy", (), {"max_skill_archive_bytes": 3})()
    with pytest.raises(ValueError, match="limit"):
        await queue.enqueue(alice, io.BytesIO(b"1234"), filename="large.zip")


@pytest.mark.asyncio
async def test_nested_metadata_version_is_canonical_and_export_is_exact(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    original = b'---\nname: deck\ndescription: Test\nmetadata:\n  version: "6.8.0"\n  copyright: "Author"\n  labels: [one, two]\n---\n\n# Body\n'
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("SKILL.md", original)
        archive.writestr("attachments/keep.txt", b"keep")
    async with http:
        saved = await resources.upload_skill(alice, stream.getvalue())
        assert saved["resource_version"] == "6.8.0"
        assert (await resources.detail(alice, "skill", saved["resource_id"], "6.8.0"))[
            "payload"
        ]["markdown"].encode() == original
        exported = _archive_files(
            await resources.download_skill(alice, saved["resource_id"], "6.8.0")
        )
        assert exported["SKILL.md"] == original
        with pytest.raises(ValueError, match="version"):
            await resources.save_skill(
                alice,
                {
                    "markdown": original.decode().replace(
                        "description: Test", "version: 9.0.0\ndescription: Test"
                    )
                },
            )


def test_manifest_delimiter_inside_metadata_does_not_truncate_frontmatter():
    from egis_agentkit.integration.nacos.registry import NacosHubManagement

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "SKILL.md",
            '---\nmetadata:\n  marker: "---"\nname: custom-marker\nversion: 1.0.0\n---\n# Body',
        )
    assert NacosHubManagement(None)._skill_name(output.getvalue()) == "custom-marker"


@pytest.mark.asyncio
async def test_recovery_resumes_after_remote_upload_checkpoint_without_registering_twice(
    storage, tmp_path
):
    import httpx

    from egis_agentkit.service.resource.imports import SkillImportService
    from egis_agentkit.storage import tables as t

    class InterruptedPublish(Registry):
        interrupted = False

        def request(self, request):
            if request.url.path.endswith("/force-publish") and not self.interrupted:
                self.interrupted = True
                return httpx.Response(503)
            return super().request(request)

    _, _, alice, _ = await identities(storage)
    registry = InterruptedPublish()
    resources, http = await service(storage, tmp_path, registry)
    worker = SkillImportService(storage, resources)
    job = await worker.enqueue(alice, io.BytesIO(bundle()), filename="resume.zip")
    async with http:
        await worker.run_once()
        assert (await worker.get(alice, job["job_id"]))["stage"] == "retrying"
        async with storage.transaction() as conn:
            await conn.execute(
                t.skill_imports.update().values(available_at=text("now()"))
            )
        await SkillImportService(storage, resources).run_once()
        assert (await worker.get(alice, job["job_id"]))["status"] == "succeeded"
        assert (
            sum(call.url.path.endswith("/skills/upload") for call in registry.calls)
            == 1
        )


@pytest.mark.asyncio
async def test_import_http_accepts_upload_metadata_and_routes_owner_only(
    storage, tmp_path
):
    from types import SimpleNamespace

    import httpx
    from egis_agentkit_app.manager_router import router
    from egis_agentkit_app.security import current_user
    from fastapi import FastAPI

    from egis_agentkit.service.resource.imports import SkillImportService

    _, _, alice, bob = await identities(storage)
    registry = Registry()
    resources, http = await service(storage, tmp_path, registry)
    queue = SkillImportService(storage, resources)
    app = FastAPI()
    app.state.egis_services = SimpleNamespace(resources=resources, skill_imports=queue)
    app.include_router(router)
    app.dependency_overrides[current_user] = lambda: alice
    async with (
        http,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client,
    ):
        response = await client.post(
            "/manager/skills/imports",
            files={"file": ("metadata.zip", bundle(), "application/zip")},
            data={"category": "writing", "avatar": "book"},
        )
        assert response.status_code == 202, response.text
        job = response.json()
        assert job["category"] == "writing" and job["avatar"] == "book"
        assert job["status"] == "queued" and registry.calls == []
        assert "archive" not in job and "lease_token" not in job
        assert (await client.get("/manager/skills/imports/" + job["job_id"])).json()[
            "job_id"
        ] == job["job_id"]
        app.dependency_overrides[current_user] = lambda: bob
        assert (await client.get("/manager/skills/imports")).json() == {"items": []}


@pytest.mark.asyncio
async def test_lost_lease_cancels_processing_without_killing_queue_polling(
    storage, tmp_path
):
    from egis_agentkit.service.resource.imports import SkillImportService
    from egis_agentkit.storage import tables as t

    _, _, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    queue = SkillImportService(storage, resources)
    queue.lease_seconds = 0.06
    started = asyncio.Event()

    async def slow_upload(*args, **kwargs):
        started.set()
        await asyncio.sleep(10)

    resources.upload_skill = slow_upload
    await queue.enqueue(alice, io.BytesIO(bundle()), filename="lease.zip")
    async with http:
        processing = asyncio.create_task(queue.run_once())
        await asyncio.wait_for(started.wait(), timeout=1)
        async with storage.transaction() as conn:
            await conn.execute(t.skill_imports.update().values(lease_token="new-owner"))
        assert await asyncio.wait_for(processing, timeout=1) is True
        assert not processing.cancelled()


@pytest.mark.asyncio
async def test_system_skill_editor_can_preserve_archive_without_a_use_grant(
    storage, tmp_path
):
    from test_resources import put_asset

    _, admin, _, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    await put_asset(storage)
    async with http:
        detail = await resources.detail(admin, "skill", "report", "1.0.0")
        assert detail["is_editable"] and not detail["is_usable"]
        assert detail["payload"]["markdown"].endswith("# Report")
        result = await resources.save_skill(
            admin,
            {
                "resource_id": "report",
                "base_version": "1.0.0",
                "markdown": "---\nname: report\nversion: 1.0.1\n---\n# Edit",
            },
        )
        # Check the registry source directly; edit permission does not grant use.
        assert result["resource_version"] == "1.0.1"
        asset = await resources._asset("skill", "report", "1.0.1")
        assert {item["path"] for item in asset["payload"]["files"]} == {
            "SKILL.md",
            "references/checklist.md",
        }


@pytest.mark.asyncio
async def test_skill_import_and_edit_yaml_processing_stays_off_event_loop(
    storage, tmp_path, monkeypatch
):
    import threading

    from egis_agentkit.service.resource import service as resource_module

    event_loop_thread = threading.get_ident()
    parse_front = resource_module._skill_front
    dump_front = resource_module.yaml.safe_dump
    observed = set()

    def parse_in_worker(markdown):
        assert threading.get_ident() != event_loop_thread, (
            "Skill YAML parsing blocked the event loop"
        )
        observed.add("parse")
        return parse_front(markdown)

    def dump_in_worker(*args, **kwargs):
        assert threading.get_ident() != event_loop_thread, (
            "Skill YAML rewriting blocked the event loop"
        )
        observed.add("dump")
        return dump_front(*args, **kwargs)

    monkeypatch.setattr(resource_module, "_skill_front", parse_in_worker)
    monkeypatch.setattr(resource_module.yaml, "safe_dump", dump_in_worker)
    _, _, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    async with http:
        imported = await resources.upload_skill(alice, bundle())
        await resources.save_skill(
            alice,
            {
                "resource_id": imported["resource_id"],
                "base_version": "1.2.3",
                "markdown": _archive_files(bundle())["SKILL.md"]
                .decode()
                .replace("1.2.3", "1.2.4"),
            },
        )
    assert observed == {"parse", "dump"}
