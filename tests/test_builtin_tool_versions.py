"""SDK 升级用新的内置工具版本登记，保留已有不可变记录。"""

import hashlib
import json

import agentscope
import pytest
from agentscope.tool import Bash

from egis_agentkit.errors import Conflict
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import Asset


async def test_sdk_upgrade_registers_new_builtin_version_without_overwriting(
    storage, tmp_path
):
    resources = ResourceService(storage, tmp_path)
    current = resources.tool_registry.catalog[resources.tool_registry.native[Bash]]
    previous = current.model_dump()
    previous["resource_version"] = "1.0.0"
    previous["payload"] = {
        **previous["payload"],
        "description": "Previously registered SDK tool description",
    }
    previous["checksum_sha256"] = hashlib.sha256(
        json.dumps(
            [previous["payload"], previous["entrypoint"]], sort_keys=True
        ).encode()
    ).hexdigest()
    async with storage.definition_write() as write:
        await write.assets.insert_asset(Asset.model_validate(previous), actor="system")
    await resources.seed_builtin_tools()
    assert resources.tool_registry.native[Bash] == ("Bash", agentscope.__version__)
    assert (await storage.assets.get("tool", "Bash", "1.0.0"))[
        "checksum_sha256"
    ] == previous["checksum_sha256"]
    assert (await storage.assets.get("tool", "Bash", agentscope.__version__))[
        "checksum_sha256"
    ] == current.checksum_sha256
    # 同一 SDK 版本的不可变内容校验仍然保留。
    resources.tool_registry.catalog[resources.tool_registry.native[Bash]] = (
        current.model_copy(update={"checksum_sha256": "0" * 64})
    )
    with pytest.raises(Conflict):
        await resources.seed_builtin_tools()
