import pytest
from test_domain import identities
from test_resources import Registry, service

from egis_agentkit.catalog_icons import catalog_icon_id


def test_catalog_icon_ids_cannot_reference_remote_or_arbitrary_paths():
    assert catalog_icon_id("expert:fullstack") == "expert:fullstack"
    assert catalog_icon_id(None) == "book"
    for value in (
        "https://example.com/x.svg",
        "../private/logo",
        "skill:unknown",
        {},
        "",
    ):
        with pytest.raises(ValueError):
            catalog_icon_id(value)


@pytest.mark.asyncio
async def test_skill_avatar_is_persisted_per_immutable_version(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    registry = Registry()
    resources, client = await service(storage, tmp_path, registry)
    try:
        first = await resources.save_skill(
            alice,
            {
                "resource_id": "avatar-skill",
                "resource_version": "1.0.0",
                "display_name": "图标测试",
                "markdown": "# Method",
                "avatar": "skill:ppt",
            },
        )
        assert first["payload"]["avatar"] == "skill:ppt"
        second = await resources.save_skill(
            alice,
            {
                "resource_id": "avatar-skill",
                "resource_version": "1.1.0",
                "display_name": "图标测试",
                "markdown": "# Method 2",
                "avatar": "expert:fullstack",
            },
        )
        assert second["payload"]["avatar"] == "expert:fullstack"
        detail = await resources.detail(alice, "skill", "avatar-skill", "1.0.0")
        assert detail["payload"]["avatar"] == "skill:ppt"
        with pytest.raises(ValueError):
            await resources.save_skill(
                alice,
                {
                    "resource_id": "avatar-skill",
                    "resource_version": "1.2.0",
                    "markdown": "# Method",
                    "avatar": "javascript:alert(1)",
                },
            )
    finally:
        await client.aclose()
