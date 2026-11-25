"""Resource reads batch SQL while preserving visible versions and live permissions."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from agentscope.tool import Bash, Edit, Glob, Grep, Read, Write
from sqlalchemy import event
from test_domain import identities

from egis_agentkit.core.skill._archive import calculate_files_digest
from egis_agentkit.errors import Forbidden
from egis_agentkit.service._admin_page import AdminPage
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import Asset
from egis_agentkit.storage.skill._version import SkillVersionStorage


@contextmanager
def sql_queries(storage):
    queries = []

    def capture(_conn, _cursor, statement, _params, _context, _many):
        queries.append(statement)

    event.listen(storage.engine.sync_engine, "before_cursor_execute", capture)
    try:
        yield queries
    finally:
        event.remove(storage.engine.sync_engine, "before_cursor_execute", capture)


async def seed_owned_skills(storage, owner, count, *, prefix="skill"):
    async with storage.definition_write() as write:
        for index in range(count):
            files = {"SKILL.md": f"# Skill {index}".encode()}
            asset = dict(resource_id=f"{prefix}-{index:03}", resource_version="1.0.0",
                         owner_type="user", owner_id=owner.user_id, checksum_sha256=calculate_files_digest(files))
            SkillVersionStorage(storage.test_workspace_root).publish(asset, files, max_bytes=1024)
            await write.assets.insert_asset(
                Asset(
                    resource_type="skill",
                    resource_id=f"{prefix}-{index:03}",
                    resource_version="1.0.0",
                    publication_revision=1,
                    owner_type="user",
                    owner_id=owner.user_id,
                    checksum_sha256=asset["checksum_sha256"],
                    payload={"display_name": f"Skill {index}", "category": "Reports"},
                ),
                actor=owner.user_id,
            )


@pytest.mark.parametrize("count", [1, 10, 50])
async def test_owned_resource_lists_and_admin_pages_use_bounded_sql(
    storage, tmp_path, count
):
    _, admin, alice, bob = await identities(storage)
    await seed_owned_skills(storage, alice, count)
    await seed_owned_skills(storage, bob, 1, prefix="foreign")
    resources = ResourceService(storage, tmp_path)
    for method, budget in (("list", 10), ("catalog_page", 11), ("admin_list", 12)):
        with sql_queries(storage) as queries:
            if method == "catalog_page":
                result = await resources.catalog_page(alice, "skill", limit=50)
                rows = result["items"]
                assert result["categories"] == ["Reports"]
                assert result["has_shareable"]
                assert result["next_cursor"] is None
            elif method == "admin_list":
                rows = (
                    await resources.admin_list(
                        admin, "skill", page=AdminPage(1, count + 1)
                    )
                )["items"]
            else:
                rows = await resources.list(alice, "skill")
        expected_count = count + 1 if method == "admin_list" else count
        assert len(rows) == expected_count
        owned = [row for row in rows if row["owner_id"] == alice.user_id]
        assert [row["resource_id"] for row in owned] == [
            f"skill-{i:03}" for i in range(count)
        ]
        if method != "admin_list":
            assert all(
                row["is_usable"] and row["is_editable"] and row["can_share"]
                for row in rows
            )
            assert all(row["owner_display_name"] == alice.username for row in rows)
        print(f"RESOURCE_SQL {method} count={count} statements={len(queries)}")
        assert len(queries) <= budget, (
            f"{method}({count}) executed {len(queries)} SQL statements"
        )


async def test_builtin_seed_batches_reads_locks_and_writes(storage, tmp_path):
    resources = ResourceService(storage, tmp_path)
    with sql_queries(storage) as queries:
        await resources.seed_builtin_tools()
    assert len(await resources._versions("tool")) == len(
        resources.tool_registry.catalog
    )
    print(f"RESOURCE_SQL first_seed statements={len(queries)}")
    assert len(queries) <= 6, f"first seed executed {len(queries)} SQL statements"
    with sql_queries(storage) as queries:
        await resources.seed_builtin_tools()
    print(f"RESOURCE_SQL unchanged_seed statements={len(queries)}")
    assert len(queries) <= 4, f"unchanged seed executed {len(queries)} SQL statements"


async def test_workspace_default_tools_batch_asset_reads_and_recheck_disable(
    storage, tmp_path
):
    users, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.seed_builtin_tools()
    scope = SimpleNamespace(
        user_id=alice.user_id,
        project_id="project",
        agent_id="agent",
        session_id="session",
        definition={"agent_type": "personal", "payload": {}},
    )

    class Workspace:
        egis_user_id, egis_project_id = alice.user_id, "project"
        egis_agent_id, egis_session_id = "agent", "session"

        async def get_builtin_tools(self):
            return [Bash(), Read(), Write(), Edit(), Glob(), Grep()]

        def configure_workspace_resources(self, *, tools, mcps):
            self.tools, self.mcps = tools, mcps

        async def list_mcps(self, **context):
            return []

    workspace = Workspace()
    with sql_queries(storage) as queries:
        await resources.prepare_workspace(alice, scope, workspace)
    assert {tool.name for tool in workspace.tools} == {
        "Bash",
        "Read",
        "Write",
        "Edit",
        "Glob",
        "Grep",
    }
    assert workspace.mcps == []
    print(f"RESOURCE_SQL default_assembly statements={len(queries)}")
    assert len(queries) <= 3, f"default assembly executed {len(queries)} SQL statements"
    await users.update_user(admin, alice.model_copy(update={"is_enabled": False}))
    with pytest.raises(Forbidden):
        await resources.prepare_workspace(alice, scope, workspace)


async def test_protected_connector_pages_batch_user_isolated_credential_status(
    storage, tmp_path
):
    from cryptography.fernet import Fernet

    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path, credential_key=Fernet.generate_key())
    for index in range(12):
        await resources.save_mcp(
            admin,
            {
                "resource_id": f"connector-{index:02}",
                "config": {
                    "transport": "streamable_http",
                    "url": "https://mcp.example.com/",
                    "auth_type": "api_key",
                },
            },
        )
    await resources.mcp_auth_action(
        alice, "connector-00", "set_api_key", key="alice-only"
    )
    await resources.mcp_auth_action(bob, "connector-01", "set_api_key", key="bob-only")
    for user, authorized_id in ((alice, "connector-00"), (bob, "connector-01")):
        with sql_queries(storage) as queries:
            result = await resources.catalog_page(user, "mcp")
        assert len(result["items"]) == 10
        assert {
            row["resource_id"] for row in result["items"] if row["is_authorized"]
        } == {authorized_id}
        assert all(
            row["is_usable"] and not row["is_installed"] for row in result["items"]
        )
        print(f"RESOURCE_SQL protected_connector_page statements={len(queries)}")
        assert len(queries) <= 7, (
            f"protected connector page executed {len(queries)} SQL statements"
        )
    asset = await resources._asset("mcp", "connector-00", None)
    with sql_queries(storage) as queries:
        assert (await resources.mcp_authorization.status(alice, asset))["authorized"]
    assert len(queries) == 1
    with sql_queries(storage) as queries:
        assert not (
            await resources.mcp_authorization.status(alice, asset, credential_row=None)
        )["authorized"]
    assert queries == []


async def test_personal_connector_assembly_batches_live_asset_and_install_reads(
    storage, tmp_path
):
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    for index in range(10):
        rid = f"connector-{index:02}"
        await resources.save_mcp(
            admin,
            {
                "resource_id": rid,
                "config": {
                    "transport": "streamable_http",
                    "url": "https://mcp.example.com/",
                },
            },
        )
        await resources.personal.add_resource(alice, "mcp", rid)
    refs = [
        {"resource_type": "mcp", "resource_id": f"connector-{i:02}"} for i in range(10)
    ]
    scope = SimpleNamespace(
        user_id=alice.user_id, definition={"agent_type": "personal", "payload": {}}
    )
    with sql_queries(storage) as queries:
        clients = await resources.mcps_for_refs(alice, refs, authorization_scope=scope)
    assert len(clients) == 10 and len({client.name for client in clients}) == 10
    print(
        f"RESOURCE_SQL personal_connector_assembly count=10 statements={len(queries)}"
    )
    assert len(queries) <= 5, f"10 MCP assembly executed {len(queries)} SQL statements"
    await resources.update_installation(alice, "mcp", "connector-00", False)
    clients = await resources.mcps_for_refs(
        alice, refs, authorization_scope=scope, skip_unavailable=True
    )
    assert len(clients) == 9


async def test_skill_descriptor_assembly_batches_versions_and_personal_selection(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await seed_owned_skills(storage, alice, 10)
    for index in range(10):
        await resources.personal.add_resource(alice, "skill", f"skill-{index:03}")
    refs = [
        {
            "resource_type": "skill",
            "resource_id": f"skill-{i:03}",
            "resource_version": "1.0.0",
            "resource_view_type": "private",
        }
        for i in range(10)
    ]
    scope = SimpleNamespace(
        user_id=alice.user_id, definition={"agent_type": "personal", "payload": {}}
    )
    with sql_queries(storage) as queries:
        descriptors = await resources.skill_descriptors(
            alice, refs, authorization_scope=scope
        )
    assert len(descriptors) == 10 and len({row.name for row in descriptors}) == 10
    assert all(not row.markdown for row in descriptors)
    print(f"RESOURCE_SQL personal_skill_assembly count=10 statements={len(queries)}")
    assert len(queries) <= 6, (
        f"10 skill descriptors executed {len(queries)} SQL statements"
    )
    await resources.set_asset_enabled(alice, "skill", "skill-000", "1.0.0", False)
    with pytest.raises(Forbidden):
        await resources.skill_descriptors(alice, refs, authorization_scope=scope)


async def test_expert_tool_resolution_batches_assets_and_rechecks_root(
    storage, tmp_path
):
    from egis_agentkit.service.agent._agent_manager import AgentManager
    from egis_agentkit.storage.model import AgentDefinition

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    schema = {"type": "object", "properties": {}}
    refs = []
    for index in range(10):
        name = f"Deployed{index}"

        def create(scope, tool_name=name):
            return SimpleNamespace(name=tool_name, input_schema=schema)

        resources.tool_registry.deploy(
            name, "1.0.0", create, description=name, input_schema=schema
        )
        refs.append(
            {
                "resource_type": "tool",
                "resource_id": name,
                "resource_version": "1.0.0",
                "resource_view_type": "system",
            }
        )
    await resources.seed_builtin_tools()
    root = await AgentManager(storage).add_definition(
        alice,
        AgentDefinition(
            owner_type="user",
            owner_id=alice.user_id,
            agent_name="tools",
            agent_display_name="Tools",
            payload={"bindings": refs},
        ),
    )
    root["resource_view_type"] = "private"
    scope = SimpleNamespace(
        user_id=alice.user_id,
        session_id="session",
        definition=root,
        root_definition=root,
    )
    with sql_queries(storage) as queries:
        result = await resources.tool_registry.resolve(alice, scope)
    assert [tool.name for tool in result] == [f"Deployed{i}" for i in range(10)]
    print(f"RESOURCE_SQL expert_tool_assembly count=10 statements={len(queries)}")
    assert len(queries) <= 6, (
        f"10 deployed tools executed {len(queries)} SQL statements"
    )
    await AgentManager(storage).delete_definition(alice, root["id"])
    with pytest.raises(Forbidden):
        await resources.tool_registry.resolve(alice, scope)


async def test_shared_pages_deduplicate_grants_keep_shared_version_and_recheck_revocation(
    storage, tmp_path
):
    from egis_agentkit.service.access._grant import GrantService
    from egis_agentkit.service.access._share import ShareService
    from egis_agentkit.storage.model import Group, ShareTarget

    users, admin, alice, bob = await identities(storage)
    resources, grants = ResourceService(storage, tmp_path), GrantService(storage)
    await seed_owned_skills(storage, bob, 12)
    shares = ShareService(storage, publish_skill=resources.publish_shared_skill)
    direct = []
    for index in range(12):
        for principal_type, principal_id in (("user", alice.user_id), ("group", "company")):
            review = await shares.submit(bob, "skill", f"skill-{index:03}", target=ShareTarget(principal_type=principal_type, principal_id=principal_id))
            await shares.decide(admin, review["review_id"], True, "")
            direct.append(next(row for row in await grants.list_grants(admin) if row["source_review_id"] == review["review_id"]))
    async with storage.definition_write() as write:
        for index in range(12):
            await write.assets.insert_asset(
                Asset(
                    resource_type="skill",
                    resource_id=f"skill-{index:03}",
                    resource_version="2.0.0",
                    publication_revision=2,
                    owner_type="user",
                    owner_id=bob.user_id,
                    checksum_sha256="b" * 64,
                    payload={"category": "Private updates"},
                ),
                actor=bob.user_id,
            )
    with sql_queries(storage) as queries:
        page = await resources.catalog_page(alice, "skill")
    print(f"RESOURCE_SQL shared_skill_page statements={len(queries)}")
    assert len(queries) <= 10
    assert [row["resource_id"] for row in page["items"]] == [
        f"skill-{i:03}" for i in range(10)
    ]
    assert all(
        row["resource_version"] == "1.0.0"
        and row["is_usable"]
        and not row["is_editable"]
        for row in page["items"]
    )
    assert page["categories"] == ["Reports"]
    last = await resources.catalog_page(alice, "skill", cursor=page["next_cursor"])
    assert [row["resource_id"] for row in last["items"]] == ["skill-010", "skill-011"]
    assert last["next_cursor"] is None
    await grants.revoke(admin, direct[0]["id"])
    assert "skill-000" in {
        row["resource_id"] for row in await resources.list(alice, "skill")
    }
    await grants.revoke(admin, direct[1]["id"])
    assert "skill-000" not in {
        row["resource_id"] for row in await resources.list(alice, "skill")
    }
    for row in direct[2::2]:
        await grants.revoke(admin, row["id"])
    await users.save_group(
        admin, Group(group_id="company", group_name="Company", is_enabled=False)
    )
    assert await resources.list(alice, "skill") == []
    assert (await resources.catalog_page(alice, "skill"))["items"] == []


async def test_loaded_mcp_status_rejects_another_user_or_connection(storage, tmp_path):
    from cryptography.fernet import Fernet

    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path, credential_key=Fernet.generate_key())
    for rid in ("first", "second"):
        await resources.save_mcp(
            admin,
            {
                "resource_id": rid,
                "config": {
                    "transport": "streamable_http",
                    "url": "https://mcp.example.com/",
                    "auth_type": "api_key",
                },
            },
        )
    await resources.mcp_auth_action(bob, "first", "set_api_key", key="bob-only")
    asset = await resources._asset("mcp", "first", None)
    row = await storage.mcp_credentials.read(
        bob.user_id, resources.mcp_authorization._connection(asset)
    )
    assert not (
        await resources.mcp_authorization.status(alice, asset, credential_row=row)
    )["authorized"]
    other = await resources._asset("mcp", "second", None)
    assert not (
        await resources.mcp_authorization.status(bob, other, credential_row=row)
    )["authorized"]


async def test_skill_detail_and_install_reuse_the_selected_asset(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_skill(alice, {"resource_id": "report", "markdown": "# Report"})
    with sql_queries(storage) as queries:
        card = await resources.detail(alice, "skill", "report", "1.0.0")
    assert card["payload"]["markdown"] == "# Report"
    assert card["is_usable"] and card["can_share"] and not card["is_installed"]
    print(f"RESOURCE_SQL skill_detail statements={len(queries)}")
    assert len(queries) <= 6
    with sql_queries(storage) as queries:
        installed = await resources.install(alice, "skill", "report")
    assert (
        installed["is_installed"] and installed["is_enabled"] and installed["is_usable"]
    )
    print(f"RESOURCE_SQL skill_first_install statements={len(queries)}")
    # Includes the first-use creation of the personal Agent definition.
    assert len(queries) <= 18
    with sql_queries(storage) as queries:
        installed = await resources.install(alice, "skill", "report")
    assert installed["is_installed"] and installed["is_enabled"]
    print(f"RESOURCE_SQL skill_existing_personal_install statements={len(queries)}")
    assert len(queries) <= 9
    with sql_queries(storage) as queries:
        updated = await resources.update_installation(alice, "skill", "report", False)
    assert (
        updated["is_installed"] and not updated["is_enabled"] and updated["is_usable"]
    )
    print(f"RESOURCE_SQL skill_update_installation statements={len(queries)}")
    assert len(queries) <= 6


async def test_late_visible_skill_page_uses_candidate_ids_instead_of_scanning_history(
    storage,
):
    import json
    from pathlib import Path

    from sqlalchemy import text
    from test_sql_query_plans import capture_selects, explain, nodes

    _, _, alice, _ = await identities(storage)
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO ak_skill_summary
              (skill_id, owner_type, owner_id, source_user_id, owner_version, shared_version, created_by, updated_by)
            SELECT CASE WHEN n <= 50000 THEN 'a-hidden-' || lpad(n::text,6,'0')
                        WHEN n <= 50005 THEN 'z-shared-' || n::text ELSE 'z-owned-' || n::text END,
                   'user', CASE WHEN n > 50005 THEN 'alice' ELSE 'bob' END, 'bob', 2,
                   CASE WHEN n > 50000 AND n <= 50005 THEN 1 ELSE NULL END, 'test', 'test'
            FROM generate_series(1,50010) n
        """)
        )
        await conn.execute(
            text("""
            INSERT INTO ak_skill_detail
              (skill_id, version, resource_version, checksum_sha256, payload, category, created_by, updated_by)
            SELECT summary.skill_id, revision, revision::text || '.0.0', repeat('a',64), '{}'::jsonb,
                   CASE WHEN revision=1 THEN 'Shared' ELSE 'Own' END, 'test', 'test'
            FROM ak_skill_summary summary CROSS JOIN generate_series(1,2) revision
        """)
        )
        await conn.execute(
            text("""
            INSERT INTO ak_resource_permission
              (id, principal_type, principal_id, resource_type, resource_id, resource_view_type,
               resource_owner_type, resource_owner_id, grant_source_type, origin_key, granted_by, granted_at)
            SELECT 'grant-' || skill_id, CASE WHEN skill_id LIKE 'z-shared-%' THEN 'group' ELSE 'user' END,
                   CASE WHEN skill_id LIKE 'z-shared-%' THEN 'company' ELSE 'unrelated-' || skill_id END,
                   'skill', skill_id, 'shared', 'user', 'bob', 'manual', 'manual', 'test', now()
            FROM ak_skill_summary WHERE owner_id='bob'
        """)
        )
        for relation in (
            "ak_skill_summary",
            "ak_skill_detail",
            "ak_resource_permission",
            "ak_users",
        ):
            # Only relations created by this isolated fixture are analyzed.
            await conn.execute(text("ANALYZE " + relation))
    with capture_selects(storage) as statements:
        rows = await storage.assets.skills.page_catalog(
            alice, after=None, limit=10, query="", category="", mine=False
        )
        metadata = await storage.assets.skills.catalog_metadata(alice)
    assert len(rows) == 10
    assert sum(row["resource_version"] == "1.0.0" for row in rows) == 5
    assert sum(row["resource_version"] == "2.0.0" for row in rows) == 5
    assert metadata == {"categories": ["Own", "Shared"], "has_shareable": True}
    plans = [await explain(storage, statement) for statement in statements]
    Path("/tmp/resource-skill-catalog-plan.json").write_text(
        json.dumps(plans, indent=2)
    )
    examined = []
    for plan in plans:
        visits = [
            (node["Actual Rows"] + node.get("Rows Removed by Filter", 0))
            * node["Actual Loops"]
            for node in nodes(plan["Plan"])
            if node.get("Relation Name") == "ak_skill_summary"
        ]
        examined.append(max(visits, default=0))
    print(
        "RESOURCE_SKILL_PLAN "
        + json.dumps(
            {
                "summary_rows_examined": examined,
                "milliseconds": [plan["Execution Time"] for plan in plans],
            }
        )
    )
    assert max(examined) <= 50, (
        f"Only ten skills are visible; summary scans examined {examined} rows"
    )


async def test_skill_candidate_union_preserves_private_policy_and_admin_system_pointer(
    storage, tmp_path
):
    from egis_agentkit.service.access._grant import GrantService
    from egis_agentkit.storage.model import Grant

    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(
        storage, tmp_path, policies=SimpleNamespace(allow_private_self_use=False)
    )
    await seed_owned_skills(storage, alice, 1)
    async with storage.definition_write() as write:
        for revision in (1, 2):
            await write.assets.insert_asset(
                Asset(
                    resource_type="skill",
                    resource_id="platform",
                    resource_version=f"{revision}.0.0",
                    publication_revision=revision,
                    owner_type="system",
                    owner_id="system",
                    checksum_sha256="a" * 64,
                    is_enabled=revision == 1,
                    payload={"category": "Platform"},
                ),
                actor=admin.user_id,
            )
    own = (await resources.catalog_page(alice, "skill"))["items"]
    assert [row["resource_id"] for row in own] == ["skill-000"]
    assert own[0]["is_editable"] and not own[0]["is_usable"]
    platform = (await resources.catalog_page(admin, "skill"))["items"]
    assert [row["resource_id"] for row in platform] == ["platform"]
    assert platform[0]["resource_version"] == "2.0.0"
    assert (
        platform[0]["is_editable"]
        and not platform[0]["is_available"]
        and not platform[0]["is_usable"]
    )
    await resources.set_asset_enabled(
        admin, "skill", "platform", "2.0.0", True, admin=True
    )
    await GrantService(storage).grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=alice.user_id,
            resource_type="skill",
            resource_id="platform",
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="system",
        ),
    )
    page = await resources.catalog_page(alice, "skill")
    assert [row["resource_id"] for row in page["items"]] == ["platform", "skill-000"]
    assert (
        page["items"][0]["resource_version"] == "2.0.0"
        and page["items"][0]["is_usable"]
    )
    assert not page["items"][1]["is_usable"]
