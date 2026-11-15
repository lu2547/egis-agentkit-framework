"""应用 HMAC 可见库不能直接变成用户权限；身份和组授权每次重新检查。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_domain import identities


def provider(grants):
    from egis_agentkit.client.nacos._knowledge_settings import KnowledgeConfig

    return SimpleNamespace(
        snapshot=SimpleNamespace(
            knowledge=KnowledgeConfig.model_validate(
                {
                    "enabled": True,
                    "connection": {
                        "baseUrl": "https://kb.example/knowledge",
                        "appId": "agentkit",
                    },
                    "access": {"grants": grants},
                }
            )
        )
    )


def peer():
    from egis_agentkit.client.knowledge._models import KnowledgeBase, QueryResult

    return SimpleNamespace(
        list_knowledge_bases=AsyncMock(
            return_value=[
                KnowledgeBase(id="kb_1", name="规则库"),
                KnowledgeBase(id="secret", name="未授权库"),
            ]
        ),
        query=AsyncMock(return_value=QueryResult(total=0)),
    )


@pytest.mark.asyncio
async def test_admin_has_no_implicit_access_and_query_scope_is_intersected(storage):
    from egis_agentkit.errors import Forbidden
    from egis_agentkit.service.knowledge import KnowledgeManager

    _, admin, alice, _ = await identities(storage)
    client = peer()
    config = provider(
        [{"principalType": "group", "principalId": "company", "kbIds": ["kb_1"]}]
    )
    manager = KnowledgeManager(storage, config, client)
    with pytest.raises(Forbidden):
        await manager.search(admin.user_id, "问题")
    assert not client.query.called
    await manager.search(alice.user_id, "问题")
    assert client.query.call_args.args[1] == ["kb_1"]
    from egis_agentkit.service._user import UserService
    from egis_agentkit.storage.model import Group

    await UserService(storage).save_group(
        admin, Group(group_id="company", group_name="停用", is_enabled=False)
    )
    with pytest.raises(Forbidden):
        await manager.search(alice.user_id, "再次查询")
    assert client.query.await_count == 1


@pytest.mark.asyncio
async def test_revocation_during_retrieval_prevents_returning_sources(storage):
    from egis_agentkit.client.nacos._knowledge_settings import KnowledgeAccess
    from egis_agentkit.errors import Forbidden
    from egis_agentkit.service.knowledge import KnowledgeManager

    _, _, alice, _ = await identities(storage)
    client = peer()
    config = provider(
        [{"principalType": "user", "principalId": alice.user_id, "kbIds": ["kb_1"]}]
    )

    async def revoke(*args, **kwargs):
        config.snapshot.knowledge = config.snapshot.knowledge.model_copy(
            update={"access": KnowledgeAccess()}
        )
        from egis_agentkit.client.knowledge._models import QueryResult

        return QueryResult(total=0)

    client.query.side_effect = revoke
    with pytest.raises(Forbidden):
        await KnowledgeManager(storage, config, client).search(alice.user_id, "问题")


@pytest.mark.asyncio
async def test_knowledge_catalog_hides_ungranted_application_resources(storage):
    from egis_agentkit.service.knowledge import KnowledgeManager

    _, admin, alice, _ = await identities(storage)
    client = peer()
    manager = KnowledgeManager(
        storage,
        provider(
            [{"principalType": "group", "principalId": "company", "kbIds": ["kb_1"]}]
        ),
        client,
    )
    assert await manager.list_knowledge_bases(admin.user_id) == []
    assert not client.list_knowledge_bases.called
    assert [base.id for base in await manager.list_knowledge_bases(alice.user_id)] == [
        "kb_1"
    ]
