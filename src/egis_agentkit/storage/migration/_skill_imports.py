"""迁移技能导入任务的上传文件：NAS 保存字节，任务表只保存文件定位。

核心能力
- 预览上传字段及任务来源完整性，不写 NAS 或数据库。
- 保留任务状态、租约和重试次数，逐包写 NAS 并读回校验。
- 全部任务的文件引用验证成功后删除二进制列；异常由外层事务回滚。
"""

import asyncio
import json

from sqlalchemy import inspect, text

from egis_agentkit.errors import Conflict

IMPORT_TABLE = "ak_skill_imports"


def _qualified(connection, schema):
    quote = connection.dialect.identifier_preparer.quote_identifier
    return f"{quote(schema)}.{quote(IMPORT_TABLE)}"


async def migrate_skill_import_files(
    connection, schema, *, archive_store=None, apply=False
):
    """显式迁移上传文件，不解析尚未领取的 ZIP，不改变原有任务处理结果。"""
    names = await connection.run_sync(
        lambda sync: inspect(sync).get_table_names(schema=schema)
    )
    if IMPORT_TABLE not in names:
        return "absent"
    columns = {
        col["name"]
        for col in await connection.run_sync(
            lambda sync: inspect(sync).get_columns(IMPORT_TABLE, schema=schema)
        )
    }
    legacy = "archive" in columns
    located = "archive_location" in columns
    table = _qualified(connection, schema)
    # 1. 待处理或失败任务必须具备上传来源，成功任务允许已释放上传引用。
    source = []
    if legacy:
        source.append("archive IS NOT NULL")
    if located:
        source.append("archive_location IS NOT NULL")
    missing = not source or await connection.scalar(
        text(
            f"SELECT EXISTS (SELECT 1 FROM {table} WHERE status <> 'succeeded' AND NOT ({' OR '.join(source)}))"
        )
    )
    if missing:
        if apply:
            raise Conflict("技能导入任务缺少上传来源，保留原数据")
        return "inconsistent"
    state = "pending" if legacy else "current"
    if not apply:
        return state
    if archive_store is None:
        if legacy:
            raise ValueError("技能导入升级必须提供已挂载的 NAS 文件存储")
        return state
    # 2. 一次迁移一份上传，避免将整个导入队列的文件装入内存。
    await connection.execute(text(f"LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"))
    if not located:
        await connection.execute(
            text(f"ALTER TABLE {table} ADD COLUMN archive_location jsonb")
        )
    if legacy:
        after = ""
        while True:
            record = (
                await connection.execute(
                    text(
                        f"SELECT job_id,archive FROM {table} WHERE archive IS NOT NULL AND job_id > :after ORDER BY job_id LIMIT 1"
                    ),
                    {"after": after},
                )
            ).first()
            if record is None:
                break
            location = await asyncio.to_thread(
                archive_store.write_archive, bytes(record.archive)
            )
            await connection.execute(
                text(
                    f"UPDATE {table} SET archive_location=CAST(:location AS jsonb) WHERE job_id=:id"
                ),
                {"id": record.job_id, "location": json.dumps(location)},
            )
            after = record.job_id
    # 3. 所有已定位包必须读回通过原始字节摘要校验，最后才删除上传二进制列。
    after = ""
    while True:
        record = (
            await connection.execute(
                text(
                    f"SELECT job_id,archive_location FROM {table} WHERE archive_location IS NOT NULL AND job_id > :after ORDER BY job_id LIMIT 1"
                ),
                {"after": after},
            )
        ).first()
        if record is None:
            break
        await asyncio.to_thread(archive_store.read_archive, record.archive_location)
        after = record.job_id
    if legacy:
        await connection.execute(
            text(f"ALTER TABLE {table} DROP COLUMN archive RESTRICT")
        )
    return "upgraded" if legacy else "current"
