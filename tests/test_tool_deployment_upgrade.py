"""纯模块移动的部署登记迁移必须通过原有不可变版本校验，且拒绝修改后的内容。"""

import hashlib
import json

import pytest
from sqlalchemy import update

from egis_agentkit.bootstrap._demo_tools import deploy_demo_report_tools
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import Asset
from egis_agentkit.storage.model import _tables as t

OLD = "egis_agentkit.extension.demo._demo_report_tool_impl:deploy_demo_report_tools.<locals>.create"


async def setup_deployments(storage, tmp_path):
    resources = ResourceService(storage, tmp_path)
    deploy_demo_report_tools(resources.tool_registry, None)
    for version in ("1.0.0", "1.1.0"):
        current = resources.tool_registry.catalog["CreateDemoReport", version]
        values = current.model_dump()
        values["entrypoint"] = OLD
        values["checksum_sha256"] = hashlib.sha256(
            json.dumps([current.payload, OLD], sort_keys=True).encode()
        ).hexdigest()
        async with storage.definition_write() as write:
            await write.assets.insert_asset(Asset.model_validate(values), actor="system")
    return resources


@pytest.mark.asyncio
async def test_preview_then_apply_keeps_payload_versions_and_passes_normal_seed(
    storage, tmp_path
):
    resources = await setup_deployments(storage, tmp_path)
    from egis_agentkit.storage.migration._tool_deployments import align_tool_deployments

    async with storage.engine.begin() as conn:
        plan = await align_tool_deployments(
            conn, resources.tool_registry.catalog, apply=False
        )
        assert len(plan) == 2
    assert (await storage.assets.get("tool", "CreateDemoReport", "1.0.0"))[
        "entrypoint"
    ] == OLD
    async with storage.engine.begin() as conn:
        await align_tool_deployments(conn, resources.tool_registry.catalog, apply=True)
    await resources.tool_registry.seed()
    async with storage.engine.begin() as conn:
        assert (
            await align_tool_deployments(
                conn, resources.tool_registry.catalog, apply=True
            )
            == []
        )
    assert (await storage.assets.get("tool", "CreateDemoReport", "1.0.0"))[
        "payload"
    ] == resources.tool_registry.catalog["CreateDemoReport", "1.0.0"].payload


@pytest.mark.asyncio
async def test_tampered_asset_aborts_entire_upgrade(storage, tmp_path):
    resources = await setup_deployments(storage, tmp_path)
    from egis_agentkit.storage.migration._tool_deployments import align_tool_deployments

    async with storage.engine.begin() as conn:
        await conn.execute(
            update(t.assets)
            .where(t.assets.c.resource_version == "1.1.0")
            .values(checksum_sha256="0" * 64)
        )
    with pytest.raises(ValueError):
        async with storage.engine.begin() as conn:
            await align_tool_deployments(
                conn, resources.tool_registry.catalog, apply=True
            )
    assert (await storage.assets.get("tool", "CreateDemoReport", "1.0.0"))[
        "entrypoint"
    ] == OLD
