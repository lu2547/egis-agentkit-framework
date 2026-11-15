"""把既有技能 ZIP 从 PostgreSQL 迁移到 NAS，成功验证后才删除二进制表。

核心能力
- 预览只检查版本是否具备 NAS 定位或可迁移源，不写文件和数据库。
- 执行逐包校验、原子写 NAS、更新版本元数据，逐版本读回核对。
- NAS 或校验失败由外层事务回滚，旧包表保留；重试复用已写完整包。
"""

import asyncio

from sqlalchemy import inspect, select, text

from egis_agentkit.client.nacos._settings import SkillArchiveConfig
from egis_agentkit.core.skill._archive import calculate_archive_digest
from egis_agentkit.errors import Conflict
from egis_agentkit.storage.model import _tables as t

LEGACY_SKILL_ARCHIVES = "ak_skill_archives"


def _qualified(connection, schema, table):
    quote = connection.dialect.identifier_preparer.quote_identifier
    return f"{quote(schema)}.{quote(table)}"


async def migrate_skill_archives(
    connection, schema, *, archive_store=None, limits=None, apply=False
):
    """只在全部已发布版本可从 NAS 校验读取后移除旧包表，不提供运行时回退。"""
    limits = limits or SkillArchiveConfig()
    names = set(
        await connection.run_sync(
            lambda sync: inspect(sync).get_table_names(schema=schema)
        )
    )
    legacy = LEGACY_SKILL_ARCHIVES in names
    assets = _qualified(connection, schema, "ak_resource_assets")
    old = _qualified(connection, schema, LEGACY_SKILL_ARCHIVES)
    available = (
        "a.payload ?& ARRAY['archive_path','archive_sha256','archive_size_bytes']"
    )
    if legacy:
        available += f" OR EXISTS (SELECT 1 FROM {old} s WHERE s.checksum_sha256=a.checksum_sha256)"
    # 1. 预览检查每个版本都有来源，不因为旧表空缺就误报已升级。
    missing = await connection.scalar(
        text(
            f"SELECT EXISTS (SELECT 1 FROM {assets} a WHERE a.resource_type='skill' AND NOT ({available}))"
        )
    )
    if missing:
        if apply:
            raise Conflict("技能版本缺少可迁移的归档或 NAS 定位，保留原数据")
        return "inconsistent"
    state = "pending" if legacy else "current"
    if not apply:
        return state
    if archive_store is None:
        if legacy:
            raise ValueError("技能归档升级必须提供已挂载的 NAS 文件存储")
        return state
    # 2. 每次处理一个包，先验证内容身份，再写 NAS 并更新所有引用版本。
    if legacy:
        await connection.execute(text(f"LOCK TABLE {old} IN SHARE ROW EXCLUSIVE MODE"))
        after = ""
        while True:
            record = (
                await connection.execute(
                    text(
                        f"SELECT checksum_sha256,archive FROM {old} WHERE checksum_sha256 > :after ORDER BY checksum_sha256 LIMIT 1"
                    ),
                    {"after": after},
                )
            ).first()
            if record is None:
                break
            content = bytes(record.archive)
            digest = await asyncio.to_thread(calculate_archive_digest, content, limits)
            if digest != record.checksum_sha256:
                raise Conflict("旧技能包的内容摘要不一致，保留二进制归档表")
            location = await asyncio.to_thread(archive_store.write_archive, content)
            verified = await asyncio.to_thread(archive_store.read_archive, location)
            if (
                await asyncio.to_thread(calculate_archive_digest, verified, limits)
                != digest
            ):
                raise Conflict("NAS 技能包读回校验失败")
            await connection.execute(
                t.assets.update()
                .where(
                    t.assets.c.resource_type == "skill",
                    t.assets.c.checksum_sha256 == digest,
                )
                .values(
                    payload=t.assets.c.payload.op("-")("original_markdown").op("||")(
                        location
                    )
                )
            )
            after = record.checksum_sha256
    # 3. 核对全部版本的 NAS 引用；中途失败会回滚元数据，旧包表仍然存在。
    async with connection.stream(
        select(t.assets.c.checksum_sha256, t.assets.c.payload).where(
            t.assets.c.resource_type == "skill"
        )
    ) as records:
        async for record in records:
            content = await asyncio.to_thread(
                archive_store.read_archive, record.payload
            )
            if (
                await asyncio.to_thread(calculate_archive_digest, content, limits)
                != record.checksum_sha256
            ):
                raise Conflict("NAS 技能内容与已发布版本不一致")
    if legacy:
        await connection.execute(text(f"DROP TABLE {old} RESTRICT"))
    return "upgraded" if legacy else "current"
