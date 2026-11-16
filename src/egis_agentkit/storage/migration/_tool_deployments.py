"""显式迁移已知模块移动的示例工具部署地址，不绕过运行时版本校验。

先锁定并核验全部候选记录；内容、归属或原校验值不同则整批拒绝。
预览不修改数据；应用只更新部署地址及其校验值，版本和资源内容保持一致。
"""

import hashlib
import json

from sqlalchemy import func, select, update

from egis_agentkit.storage.model import _tables as t

_MOVED_ENTRYPOINT = "egis_agentkit.extension.demo._demo_report_tool_impl:deploy_demo_report_tools.<locals>.create"


async def align_tool_deployments(connection, catalog, *, apply=False):
    """按当前受信部署清单核验纯地址移动；调用方管理预览/应用事务。"""
    statement = (
        select(t.assets)
        .where(
            t.assets.c.resource_type == "tool",
            t.assets.c.entrypoint == _MOVED_ENTRYPOINT,
        )
        .with_for_update()
    )
    rows = (await connection.execute(statement)).mappings().all()
    changes = []
    # 1. 全部验证通过后才写入，不能在后面的资源校验失败前部分更新。
    for row in rows:
        target = catalog.get((row["resource_id"], row["resource_version"]))
        before = hashlib.sha256(
            json.dumps([row["payload"], row["entrypoint"]], sort_keys=True).encode()
        ).hexdigest()
        if (
            target is None
            or (row["owner_type"], row["owner_id"]) != ("system", "system")
            or row["payload"] != target.payload
            or row["checksum_sha256"] != before
        ):
            raise ValueError("工具部署内容或原校验值不一致，拒绝自动迁移")
        changes.append(
            {
                "resource_id": row["resource_id"],
                "resource_version": row["resource_version"],
                "previous_entrypoint": row["entrypoint"],
                "entrypoint": target.entrypoint,
                "checksum_sha256": target.checksum_sha256,
            }
        )
    # 2. 仅登记同一工具实现的新部署位置，保留所有版本和授权引用。
    if apply:
        for change in changes:
            await connection.execute(
                update(t.assets)
                .where(
                    t.assets.c.resource_type == "tool",
                    t.assets.c.resource_id == change["resource_id"],
                    t.assets.c.resource_version == change["resource_version"],
                )
                .values(
                    entrypoint=change["entrypoint"],
                    checksum_sha256=change["checksum_sha256"],
                    updated_by="system",
                    updated_at=func.now(),
                )
            )
    return changes
