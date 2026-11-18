import asyncio
import io
import zipfile

import pytest
from sqlalchemy import select, text
from test_domain import identities

from egis_agentkit.core.skill._archive import read_archive_files
from egis_agentkit.errors import NotFound
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill import _skill_front


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
    from egis_agentkit.service.resource._skill_import import SkillImportService

    _, _, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    queue = SkillImportService(storage, resources)
    uploaded = bundle()
    job = await queue.enqueue(
        alice,
        io.BytesIO(uploaded),
        filename="demo.zip",
        category="writing",
        avatar="book",
    )
    assert job["status"] == "queued"
    from egis_agentkit.storage import tables as t

    assert "archive" not in t.skill_imports.c
    assert "archive_location" not in job
    async with storage.engine.connect() as conn:
        location = await conn.scalar(select(t.skill_imports.c.archive_location))
    assert queue.uploads.read(job["job_id"], location) == uploaded
    with pytest.raises(NotFound):
        await queue.get(bob, job["job_id"])
    assert await queue.list(bob) == []
    # A new instance can recover the durable upload.
    recovery = SkillImportService(storage, resources)
    assert await recovery.run_once()
    done = await recovery.get(alice, job["job_id"])
    assert done["status"] == "succeeded" and done["progress"] == 100
    assert done["resource_version"] == "1.2.3"
    async with storage.engine.connect() as conn:
        assert await conn.scalar(select(t.skill_imports.c.archive_location)) is None
    detail = await resources.detail(alice, "skill", done["resource_id"], "1.2.3")
    assert detail["payload"]["category"] == "writing"
    assert "name: demo" in detail["payload"]["markdown"]
    assert detail["payload"]["files"] == [
        {"path": "SKILL.md", "size": len(read_archive_files(bundle())["SKILL.md"])},
        {"path": "assets/logo.bin", "size": 10},
    ]


@pytest.mark.asyncio
async def test_import_claim_fencing_recovery_and_retry_without_duplicate_asset(
    storage, tmp_path
):
    from egis_agentkit.service.resource._skill_import import SkillImportService
    from egis_agentkit.storage import tables as t

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
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
    await second.process(recovered)
    # Simulate a lost completion checkpoint after the asset committed.
    async with storage.transaction() as conn:
        await conn.execute(
            t.skill_imports.update().values(
                status="queued",
                result=None,
                archive_location=first.uploads.write(job["job_id"], bundle()),
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
    resources = ResourceService(storage, tmp_path)
    original = await resources.upload_skill(
        alice, bundle(), category="writing", avatar="book"
    )
    edited = (
        read_archive_files(bundle())["SKILL.md"]
        .decode()
        .replace("1.2.3", "1.2.4")
        .replace("# Original", "# Edited")
    )
    data = {
        "resource_id": original["resource_id"],
        "markdown": edited,
        "category": "writing",
        "avatar": "book",
    }
    with pytest.raises(ValueError, match="base version"):
        await resources.save_skill(alice, data)
    saved = await resources.save_skill(alice, {**data, "base_version": "1.2.3"})
    content = await resources.download_skill(alice, saved["resource_id"], "1.2.4")
    files = read_archive_files(content)
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
    from egis_agentkit.service.resource._skill_import import SkillImportService

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    queue = SkillImportService(storage, resources)
    job = await queue.enqueue(alice, io.BytesIO(b"not zip"), filename="invalid.zip")
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
    resources = ResourceService(storage, tmp_path)
    original = b'---\nname: deck\ndescription: Test\nmetadata:\n  version: "6.8.0"\n  copyright: "Author"\n  labels: [one, two]\n---\n\n# Body\n'
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("SKILL.md", original)
        archive.writestr("attachments/keep.txt", b"keep")
    saved = await resources.upload_skill(alice, stream.getvalue())
    assert saved["resource_version"] == "6.8.0"
    assert (await resources.detail(alice, "skill", saved["resource_id"], "6.8.0"))[
        "payload"
    ]["markdown"].encode() == original
    exported = read_archive_files(
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
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "SKILL.md",
            '---\nmetadata:\n  marker: "---"\nname: custom-marker\nversion: 1.0.0\n---\n# Body',
        )
    front = _skill_front(read_archive_files(output.getvalue())["SKILL.md"].decode())
    assert front["name"] == "custom-marker"


@pytest.mark.asyncio
async def test_import_http_accepts_upload_metadata_and_routes_owner_only(
    storage, tmp_path
):
    from types import SimpleNamespace

    import httpx
    from egis_agentkit_app.bootstrap._router import manager_router as router
    from egis_agentkit_app.controller.base import current_user
    from fastapi import FastAPI

    from egis_agentkit.service.resource._skill_import import SkillImportService

    _, _, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    queue = SkillImportService(storage, resources)
    app = FastAPI()
    app.state.egis_services = SimpleNamespace(resources=resources, skill_imports=queue)
    app.include_router(router)
    app.dependency_overrides[current_user] = lambda: alice
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/manager/skills/imports",
            files={"file": ("metadata.zip", bundle(), "application/zip")},
            data={"category": "writing", "avatar": "book"},
        )
        assert response.status_code == 202, response.text
        job = response.json()
        assert job["category"] == "writing" and job["avatar"] == "book"
        assert job["status"] == "queued"
        assert "archive_location" not in job and "lease_token" not in job
        assert (await client.get("/manager/skills/imports/" + job["job_id"])).json()[
            "job_id"
        ] == job["job_id"]
        app.dependency_overrides[current_user] = lambda: bob
        assert (await client.get("/manager/skills/imports")).json() == {"items": []}


@pytest.mark.asyncio
async def test_lost_lease_cancels_processing_without_killing_queue_polling(
    storage, tmp_path
):
    from egis_agentkit.service.resource._skill_import import SkillImportService
    from egis_agentkit.storage import tables as t

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    queue = SkillImportService(storage, resources)
    queue.lease_seconds = 0.06
    started = asyncio.Event()

    async def slow_upload(*args, **kwargs):
        started.set()
        await asyncio.sleep(10)

    resources.upload_skill = slow_upload
    await queue.enqueue(alice, io.BytesIO(bundle()), filename="lease.zip")
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
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage)
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
    # Editing does not grant use.
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

    from egis_agentkit.service.resource import _catalog as resource_module

    event_loop_thread = threading.get_ident()
    parse_front = resource_module._skill_front
    observed = set()

    def parse_in_worker(markdown):
        assert threading.get_ident() != event_loop_thread, (
            "Skill YAML parsing blocked the event loop"
        )
        observed.add("parse")
        return parse_front(markdown)

    monkeypatch.setattr(resource_module, "_skill_front", parse_in_worker)
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    imported = await resources.upload_skill(alice, bundle())
    await resources.save_skill(
        alice,
        {
            "resource_id": imported["resource_id"],
            "base_version": "1.2.3",
            "markdown": read_archive_files(bundle())["SKILL.md"]
            .decode()
            .replace("1.2.3", "1.2.4"),
        },
    )
    assert observed == {"parse"}


@pytest.mark.asyncio
async def test_failed_nas_upload_does_not_enqueue_job(storage, tmp_path, monkeypatch):
    from egis_agentkit.service.resource._skill_import import SkillImportService

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    queue = SkillImportService(storage, resources)

    def fail(*args):
        raise OSError("NAS unavailable")

    monkeypatch.setattr(queue.uploads, "write", fail)
    with pytest.raises(OSError, match="NAS unavailable"):
        await queue.enqueue(alice, io.BytesIO(bundle()), filename="test.zip")
    assert await queue.list(alice) == []


@pytest.mark.asyncio
async def test_virtual_user_bearer_import_review_and_publication(storage, tmp_path):
    from types import SimpleNamespace

    from egis_agentkit_app.controller.resource._resource import router
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient

    from egis_agentkit.middleware.http._http_identity_middleware_impl import (
        IdentityGuard,
    )
    from egis_agentkit.service.resource._skill_import import SkillImportService

    _, admin, _, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    queue = SkillImportService(storage, resources)
    account = await queue.virtual_users.create(admin, "external catalog")
    app = FastAPI()
    app.state.egis_services = SimpleNamespace(
        virtual_users=queue.virtual_users, skill_imports=queue, resources=resources
    )
    app.include_router(router, prefix="/manager")
    app.add_middleware(IdentityGuard, identity=None, services=app.state.egis_services)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": "Bearer " + account["token"]},
    ) as client:
        assert (await client.get("/manager/skills/imports")).status_code == 200
        assert (await client.get("/manager/skills")).status_code == 403
        content = bundle()
        uploaded = await client.post(
            "/manager/skills/imports", files={"file": ("test.zip", content)}
        )
        assert uploaded.status_code == 202, uploaded.text
        job = uploaded.json()
        assert job["owner_type"] == "system" and job["uploader_type"] == "virtual"
        assert await queue.run_once()
        pending = await queue.list_reviews(admin)
        assert [row["uploaded_by"] for row in pending] == [account["user_id"]]
        assert await queue.download_review(admin, job["job_id"]) == (
            "test.zip",
            content,
        )
        assert not await storage.assets.list_versions("skill")
        assert (
            await queue.decide_batch(
                admin, [job["job_id"], "0" * 32], approved=True, reason=""
            )
        ) == [
            {"id": job["job_id"], "ok": True},
            {"id": "0" * 32, "ok": False, "detail": "技能导入不在待审批状态"},
        ]
        assert await queue.run_once()
        finished = (await client.get(f"/manager/skills/imports/{job['job_id']}")).json()
        assert finished["status"] == "succeeded"
        assert finished["reviewed_by"] == admin.user_id
        (asset,) = await storage.assets.list_versions("skill")
        assert asset["owner_type"] == "system" and asset["owner_id"] == "system"
        assert asset["created_by"] == account["user_id"]
        assert asset["payload"]["uploader_type"] == "virtual"
        assert not (queue.uploads.incoming / (job["job_id"] + ".zip")).exists()
        await queue.virtual_users.rotate(admin, account["user_id"])
        assert (await client.get("/manager/skills/imports")).status_code == 401


@pytest.mark.asyncio
async def test_development_skill_reset_keeps_other_records(storage, tmp_path):
    from test_domain import expert

    from egis_agentkit.service.agent._agent_manager import AgentManager
    from egis_agentkit.service.resource._skill_import import SkillImportService
    from egis_agentkit.storage import tables as t
    from egis_agentkit.storage._skill_reset import reset_skills

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    queue = SkillImportService(storage, resources)
    skill = await resources.upload_skill(alice, bundle())
    definition = expert()
    definition.payload["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": skill["resource_id"],
            "resource_version": skill["resource_version"],
            "resource_view_type": "private",
        }
    ]
    saved = await AgentManager(storage).add_definition(alice, definition)
    await queue.enqueue(alice, io.BytesIO(bundle()), filename="queued.zip")
    uploads = queue.uploads.root
    (uploads / "old.zip").write_bytes(bundle())
    project = tmp_path / "user" / alice.user_id / "projects" / "demo"
    project.mkdir(parents=True)
    (project / "source.md").write_text("keep")
    database = storage.engine.url.database
    preview = await reset_skills(storage.engine, tmp_path, uploads, database=database)
    assert preview.tables["ak_resource_assets"] == "1 条"
    assert preview.tables["ak_agent_definition_detail"] == "1 条"
    assert (uploads / "old.zip").exists()
    assert await storage.assets.get("skill", skill["resource_id"], "1.2.3")
    await reset_skills(storage.engine, tmp_path, uploads, database=database, apply=True)
    assert not await storage.assets.list_versions("skill")
    remaining = await storage.get_definition(saved["id"])
    assert remaining["payload"]["bindings"] == []
    assert remaining["payload"]["system_prompt"] == "旧版"
    assert await queue.list(alice) == []
    assert await storage.get_user(alice.user_id)
    assert (project / "source.md").read_text() == "keep"
    assert not uploads.exists()
    assert not (tmp_path / "user" / alice.user_id / "skills").exists()
    async with storage.engine.connect() as conn:
        assert await conn.scalar(select(t.skill_imports.c.owner_type)) is None
