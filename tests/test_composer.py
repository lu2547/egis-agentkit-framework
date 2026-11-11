"""Composer pages expose only currently callable options, without catalog hydration."""

from types import SimpleNamespace

import pytest
from agentscope.app.storage import SkillRecord
from test_domain import expert, identities
from test_resources import put_asset

from egis_agentkit.service._composer import ComposerService
from egis_agentkit.service._session import SessionService
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.agent._expert_definition import ExpertDefinition
from egis_agentkit.service.agent._personal_definition import PersonalDefinition
from egis_agentkit.service.agent._query import AgentQuery
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage._workspace import WorkspaceFiles


def composer(storage, tmp_path, *, tasks=None, projects=None, config=None):
    resources = ResourceService(storage, tmp_path)
    files = WorkspaceFiles(tmp_path)
    workspace = SimpleNamespace(files=files, sessions=tasks, projects=projects)
    return ComposerService(
        storage, AgentQuery(storage), resources, workspace, config_provider=config
    ), files


@pytest.mark.asyncio
async def test_expert_pages_are_authorized_searchable_and_cursor_scoped(
    storage, tmp_path
):
    _, admin, alice, _ = await identities(storage)
    service, _ = composer(storage, tmp_path)
    for index in range(23):
        row = expert(name=f"writer{index:02}")
        row.id = f"expert{index:02}"
        await ExpertDefinition(storage).build(alice, row)
    await PersonalDefinition(storage).build(alice)
    denied = expert("demo", "system", "denied")
    denied.id = "a-denied"
    await ExpertDefinition(storage).build(admin, denied)
    first = await service.options(alice, "experts")
    assert len(first["items"]) == 10
    assert first["next_cursor"]
    second = await service.options(alice, "experts", cursor=first["next_cursor"])
    last = await service.options(alice, "experts", cursor=second["next_cursor"])
    ids = [
        item["definition"]["id"]
        for page in (first, second, last)
        for item in page["items"]
    ]
    assert ids == [f"expert{index:02}" for index in range(23)]
    assert last["next_cursor"] is None
    assert (
        len((await service.options(alice, "experts", query="writer22"))["items"]) == 1
    )
    with pytest.raises(ValueError, match="cursor"):
        await service.options(
            alice, "experts", query="changed", cursor=first["next_cursor"]
        )


@pytest.mark.asyncio
async def test_context_pages_include_uploads_and_artifacts_but_no_work_scripts(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def guard(user, project_id):
        assert user == alice and project_id == "project"
        yield

    service, files = composer(storage, tmp_path, projects=SimpleNamespace(guard=guard))
    for index in range(13):
        files.write(
            "alice", "project", f"input{index:02}.csv", b"input", area="uploads"
        )
    files.write("alice", "project", "report.pdf", b"output", area="artifacts")
    files.write("alice", "project", "private.py", b"script", area="work")
    page = await service.options(alice, "context", project_id="project")
    following = await service.options(
        alice, "context", project_id="project", cursor=page["next_cursor"]
    )
    items = page["items"] + following["items"]
    assert len(page["items"]) == 10
    assert len(items) == 14 and following["next_cursor"] is None
    assert {item["reference"] for item in items} == {
        f"input{index:02}.csv" for index in range(13)
    } | {"artifacts/report.pdf"}
    assert (await service.options(alice, "context"))["items"] == []


@pytest.mark.asyncio
async def test_context_prefers_readable_deliveries_and_searches_labels_and_paths(
    storage, tmp_path
):
    from contextlib import asynccontextmanager

    _, _, alice, _ = await identities(storage)

    @asynccontextmanager
    async def guard(user, project_id):
        yield

    service, files = composer(storage, tmp_path, projects=SimpleNamespace(guard=guard))
    delivery = "面向 AI-Coding 的知识中心建设.pptx"
    generated = "scratch/ai_coding_knowledge_center_20261107_223808-a964f31821cf.pdf"
    upload = "原始_资料-1234abcd.csv"
    files.write("alice", "project", delivery, b"slides", area="artifacts")
    files.write("alice", "project", generated, b"pdf", area="artifacts")
    files.write("alice", "project", upload, b"data", area="uploads")

    page = await service.options(alice, "context", project_id="project", limit=2)
    assert [(row["display_name"], row["reference"]) for row in page["items"]] == [
        (delivery, f"artifacts/{delivery}"),
        (upload, upload),
    ]
    rest = await service.options(
        alice, "context", project_id="project", cursor=page["next_cursor"], limit=2
    )
    expected = [("ai coding knowledge center.pdf", f"artifacts/{generated}")]
    assert [
        (row["display_name"], row["reference"]) for row in rest["items"]
    ] == expected
    assert rest["next_cursor"] is None
    for query in ("ai coding knowledge center", "20261107_223808"):
        result = await service.options(
            alice, "context", project_id="project", query=query
        )
        assert [
            (row["display_name"], row["reference"]) for row in result["items"]
        ] == expected


@pytest.mark.asyncio
async def test_only_enabled_installed_authorized_skills_are_offered(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    service, _ = composer(storage, tmp_path)
    for rid, owner, enabled in [
        ("allowed", "alice", True),
        ("disabled", "alice", False),
        ("denied", "demo", True),
        ("uninstalled", "alice", True),
    ]:
        await put_asset(
            storage,
            rid=rid,
            owner_type="user" if owner == "alice" else "system",
            owner_id=owner,
        )
        if rid != "uninstalled":
            await storage.upsert_skill(
                "alice",
                SkillRecord(
                    id=service.resources._native_id(alice, "skill", rid, "1.0.0"),
                    user_id="alice",
                    name=rid,
                    description="demo",
                    markdown="",
                    hub_id=service.resources.skill_hub.hub_id,
                    card_id=rid,
                    version="1.0.0",
                    enabled=enabled,
                ),
            )
    assert [
        item["id"] for item in (await service.options(alice, "skills"))["items"]
    ] == ["allowed@1.0.0"]
    assert await service.options(alice, "commands") == {
        "items": [],
        "next_cursor": None,
    }


@pytest.mark.asyncio
async def test_existing_nonpersonal_session_cannot_offer_invites(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    tasks = SessionService(storage, tmp_path)
    source = await ExpertDefinition(storage).build(alice, expert())
    task = await tasks.create(alice, source["id"], "private")
    service, _ = composer(storage, tmp_path, tasks=tasks)
    assert (await service.options(alice, "experts", session_id=task["session_id"]))[
        "items"
    ] == []
    assert (await service.options(alice, "skills", session_id=task["session_id"]))[
        "items"
    ] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("unavailable", ["disabled", "revoked"])
async def test_expert_with_unavailable_dependency_is_not_callable(
    storage, tmp_path, unavailable
):
    from sqlalchemy import update

    from egis_agentkit.storage import tables as t
    from egis_agentkit.storage.model import Grant

    _, admin, alice, _ = await identities(storage)
    service, _ = composer(storage, tmp_path)
    await put_asset(storage)
    grant = Grant(
        principal_type="user",
        principal_id="alice",
        resource_type="skill",
        resource_id="report",
        resource_view_type="system",
        resource_owner_type="system",
        resource_owner_id="demo",
    )
    await GrantService(storage).grant(admin, grant)
    definition = expert()
    definition.payload["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": "report",
            "resource_version": "1.0.0",
            "resource_view_type": "system",
        }
    ]
    await ExpertDefinition(storage).build(alice, definition)
    assert len((await service.options(alice, "experts"))["items"]) == 1
    if unavailable == "revoked":
        await GrantService(storage).revoke(admin, grant.id)
    else:
        async with storage.transaction() as conn:
            await conn.execute(
                update(t.assets)
                .where(t.assets.c.resource_id == "report")
                .values(is_enabled=False)
            )
    assert (await service.options(alice, "experts"))["items"] == []


@pytest.mark.asyncio
async def test_context_metadata_pages_do_not_scan_aggregate_file_content(
    storage, tmp_path
):
    from contextlib import asynccontextmanager

    _, _, alice, _ = await identities(storage)

    @asynccontextmanager
    async def guard(user, project_id):
        yield

    service, files = composer(storage, tmp_path, projects=SimpleNamespace(guard=guard))
    # Every input is under the upload cap; their combined bytes must not limit
    # metadata discovery. Sparse files keep this regression cheap.
    for index in range(21):
        target = files.write(
            "alice", "project", f"input{index:02}.csv", b"input", area="uploads"
        )
        with target.open("r+b") as stream:
            stream.truncate(10 * 1024 * 1024)
    files.write("alice", "project", "folder/a.txt", b"nested", area="uploads")
    files.write("alice", "project", "folder.md", b"root", area="uploads")
    pages = []
    cursor = None
    while True:
        page = await service.options(
            alice, "context", project_id="project", cursor=cursor
        )
        pages.append(page)
        cursor = page["next_cursor"]
        if not cursor:
            break
    references = [item["reference"] for page in pages for item in page["items"]]
    assert references == sorted(
        [f"input{index:02}.csv" for index in range(21)] + ["folder/a.txt", "folder.md"]
    )
    assert [len(page["items"]) for page in pages] == [10, 10, 3]


@pytest.mark.asyncio
async def test_nonuser_personal_session_does_not_offer_invites(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    tasks = SessionService(storage, tmp_path)
    source = await PersonalDefinition(storage).build(alice)
    task = await tasks.create(alice, source["id"], "private")

    async def session(user, sid):
        record = await tasks.session(user, sid)
        return SimpleNamespace(
            origin=SimpleNamespace(type="scheduled"),
            state=record.state,
        )

    service, _ = composer(storage, tmp_path, tasks=SimpleNamespace(session=session))
    await ExpertDefinition(storage).build(alice, expert())
    assert (await service.options(alice, "experts", session_id=task["session_id"]))[
        "items"
    ] == []


@pytest.mark.asyncio
async def test_composer_uses_configured_option_bounds(storage, tmp_path):
    from test_app_http import app_snapshot

    _, _, alice, _ = await identities(storage)
    config = SimpleNamespace(
        snapshot=app_snapshot(
            tmp_path,
            session={
                "composer": {
                    "default_page_size": 1,
                    "max_page_size": 2,
                    "candidate_batch_size": 1,
                    "max_query_length": 4,
                }
            },
        )
    )
    await ExpertDefinition(storage).build(alice, expert(name="first"))
    await ExpertDefinition(storage).build(alice, expert(name="second"))
    service, _ = composer(storage, tmp_path, config=config)
    page = await service.options(alice, "experts")
    assert len(page["items"]) == 1
    assert page["next_cursor"]
    with pytest.raises(ValueError, match="2"):
        await service.options(alice, "experts", limit=3)
    with pytest.raises(ValueError, match="4"):
        await service.options(alice, "experts", query="abcde")
