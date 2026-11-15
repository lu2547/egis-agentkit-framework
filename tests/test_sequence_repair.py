"""导入显式主键后，升级操作补齐序列，避免后续写入主键冲突。"""

import pytest
from sqlalchemy import text


@pytest.mark.asyncio
async def test_sequence_repair_preserves_rows_and_never_moves_a_sequence_back(storage):
    from egis_agentkit.storage.migration._sequences import repair_sequences

    async with storage.engine.begin() as connection:
        schema = await connection.scalar(text("SELECT current_schema()"))
        await connection.execute(
            text("CREATE TABLE sequence_probe (id bigserial PRIMARY KEY)")
        )
        await connection.execute(text("INSERT INTO sequence_probe (id) VALUES (100)"))
        # 1. 预览发现落后，不消耗序列，也不修改记录。
        states = await repair_sequences(connection, schema)
        assert states["sequence_probe.id"] == "pending"
        assert not await connection.scalar(
            text("SELECT is_called FROM sequence_probe_id_seq")
        )
        # 2. 修复后隐式插入必须落在已有主键之后。
        states = await repair_sequences(connection, schema, apply=True)
        assert states["sequence_probe.id"] == "repaired"
        assert (
            await connection.scalar(
                text("INSERT INTO sequence_probe DEFAULT VALUES RETURNING id")
            )
            == 101
        )
        # 3. 已经领先的序列保持原值，重复执行无副作用。
        await connection.execute(
            text("SELECT setval('sequence_probe_id_seq', 200, true)")
        )
        states = await repair_sequences(connection, schema, apply=True)
        assert states["sequence_probe.id"] == "current"
        assert (
            await connection.scalar(text("SELECT nextval('sequence_probe_id_seq')"))
            == 201
        )
        assert await connection.scalar(text("SELECT count(*) FROM sequence_probe")) == 2
