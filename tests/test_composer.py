"""Composer pages expose only currently callable options, without catalog hydration."""

from importlib import import_module
from types import SimpleNamespace

import pytest
from agentscope.app.storage import SkillRecord
from test_domain import expert, identities
from test_resources import put_asset

from egis_agentkit.service.agent.definition import AgentService
from egis_agentkit.service.resource.service import ResourceService
from egis_agentkit.service.workspace.files import WorkspaceFiles


def composer(storage, tmp_path, *, tasks=None, projects=None):
    cls = getattr(
        import_module("egis_agentkit.service.session.composer"), "ComposerService"
    )
    resources = ResourceService(storage, None, None, None, tmp_path)
    files = WorkspaceFiles(tmp_path)
    workspace = SimpleNamespace(files=files, tasks=tasks, projects=projects)
    return cls(storage, AgentService(storage), resources, workspace), files


@pytest.mark.asyncio
async def test_expert_pages_are_authorized_searchable_and_cursor_scoped(
    storage, tmp_path
):
    _, admin, alice, _ = await identities(storage)
    service, _ = composer(storage, tmp_path)
    for index in range(23):
        row = expert(name=f"writer{index:02}")
        row.id = f"expert{index:02}"
        await service.agents.save(alice, row)
    await service.agents.personal(alice)
    denied = expert("demo", "system", "denied")
    denied.id = "a-denied"
    await service.agents.save(admin, denied)
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

    async def session(user, sid):
        return SimpleNamespace(
            origin=SimpleNamespace(type="user"),
            state=SimpleNamespace(
                middle_context={
                    "egis_agentkit": {
                        "is_team": False,
                        "project_id": "project",
                        "snapshot": {
                            "root": {
                                "definition_type": "agent",
                                "agent_type": "expert",
                            },
                            "root_key": "root",
                            "nodes": {"root": {"data": {"agent_type": "expert"}}},
                        },
                    }
                }
            ),
        )

    service, _ = composer(storage, tmp_path, tasks=SimpleNamespace(session=session))
    await service.agents.save(alice, expert())
    assert (await service.options(alice, "experts", session_id="session"))[
        "items"
    ] == []
    assert (await service.options(alice, "skills", session_id="session"))["items"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("unavailable", ["disabled", "revoked"])
async def test_expert_with_unavailable_dependency_is_not_callable(
    storage, tmp_path, unavailable
):
    from sqlalchemy import update

    from egis_agentkit.models import Grant
    from egis_agentkit.service.access.authorization import PermissionService
    from egis_agentkit.storage import tables as t

    _, admin, alice, _ = await identities(storage)
    service, _ = composer(storage, tmp_path)
    await put_asset(storage)
    permissions = PermissionService(storage)
    grant = Grant(
        principal_type="user",
        principal_id="alice",
        resource_type="skill",
        resource_id="report",
        resource_view_type="system",
        resource_owner_type="system",
        resource_owner_id="demo",
    )
    await permissions.grant(admin, grant)
    definition = expert()
    definition.payload["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": "report",
            "resource_version": "1.0.0",
            "resource_view_type": "system",
        }
    ]
    await service.agents.save(alice, definition)
    assert len((await service.options(alice, "experts"))["items"]) == 1
    if unavailable == "revoked":
        await permissions.revoke(admin, grant.id)
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
    root = {"definition_type": "agent", "agent_type": "personal"}

    async def session(user, sid):
        return SimpleNamespace(
            origin=SimpleNamespace(type="scheduled"),
            state=SimpleNamespace(
                middle_context={
                    "egis_agentkit": {
                        "is_team": False,
                        "project_id": "project",
                        "snapshot": {
                            "root": root,
                            "root_key": "root",
                            "nodes": {"root": {"data": root}},
                        },
                    }
                }
            ),
        )

    service, _ = composer(storage, tmp_path, tasks=SimpleNamespace(session=session))
    await service.agents.save(alice, expert())
    assert (await service.options(alice, "experts", session_id="session"))[
        "items"
    ] == []
