-- 1.0.2：已有 schema 的索引增量。仅索引 DDL，不修改表、字段或业务行。
-- 用 psql -X -v ON_ERROR_STOP=1 -f 执行；每条语句自动提交。
-- 禁止 BEGIN/COMMIT、psql -1 或迁移工具的外层事务，CONCURRENTLY 不支持事务块。
-- 连接的数据库和 search_path 必须与应用一致，详见本目录 README.md。

SET lock_timeout = '5s';

DO $preflight$
DECLARE
    expected record;
    existing record;
BEGIN
    IF current_schema() IS NULL THEN
        RAISE EXCEPTION '没有有效的目标 schema，请显式设置 search_path';
    END IF;
    -- 固定到选中的单个 schema，避免 DROP 落到 search_path 中的其他 schema。
    PERFORM set_config('search_path', format('%I', current_schema()), false);
    IF NOT pg_try_advisory_lock(727140203) THEN
        RAISE EXCEPTION '另一个查询索引维护任务正在运行';
    END IF;
    FOR expected IN
        SELECT * FROM (VALUES
            ('sessions', 'project_id'),
            ('ak_project_file_changes', 'change_id'),
            ('ak_resource_permission', 'revoked_at')
        ) AS required(table_name, column_name)
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = expected.table_name
              AND column_name = expected.column_name
        ) THEN
            RAISE EXCEPTION '缺少字段：schema=%, table=%, column=%；请先确认基线表结构',
                current_schema(), expected.table_name, expected.column_name;
        END IF;
    END LOOP;
    -- 同名旧索引只有定义完全符合预期时才允许移除。
    FOR expected IN
        SELECT * FROM (VALUES
            ('ix_sessions_updated_at', 'sessions', 'updated_at'),
            ('ix_ak_project_file_changes_project_id', 'ak_project_file_changes', 'project_id')
        ) AS retired(index_name, table_name, column_name)
    LOOP
        SELECT tab.relname AS table_name, am.amname AS method,
               i.indisunique, i.indnkeyatts, i.indnatts,
               pg_get_indexdef(i.indexrelid, 1, true) AS first_column,
               pg_get_expr(i.indpred, i.indrelid) AS predicate
        INTO existing
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_namespace ns ON ns.oid = idx.relnamespace
        JOIN pg_class tab ON tab.oid = i.indrelid
        JOIN pg_am am ON am.oid = idx.relam
        WHERE ns.nspname = current_schema() AND idx.relname = expected.index_name;
        IF FOUND AND (
            existing.table_name <> expected.table_name OR existing.method <> 'btree'
            OR existing.indisunique OR existing.indnkeyatts <> 1 OR existing.indnatts <> 1
            OR existing.first_column <> expected.column_name OR existing.predicate IS NOT NULL
        ) THEN
            RAISE EXCEPTION '旧索引 % 的定义与预期不一致，未移除', expected.index_name;
        END IF;
    END LOOP;
END;
$preflight$;

-- 当前用户的主会话分页：先取覆盖索引中的本页 ID，再按主键读取正文。
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_sessions_main_user_updated
    ON sessions (user_id, updated_at DESC) INCLUDE (id)
    WHERE source = 'user' AND project_id IS NOT NULL;

-- 单项目变更记录的游标分页。
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_ak_project_changes_page
    ON ak_project_file_changes (project_id, change_id DESC);

-- 活动授权按资源及当前用户/组定位，覆盖依赖授权。
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_ak_permission_active_resource_principal
    ON ak_resource_permission (resource_type, resource_id, principal_type, principal_id)
    WHERE revoked_at IS NULL;

-- IF NOT EXISTS 不保证已有同名索引正确；新索引有效且定义一致后才能移除旧索引。
DO $validate$
DECLARE
    expected record;
    existing record;
BEGIN
    FOR expected IN
        SELECT * FROM (VALUES
            ('ix_sessions_main_user_updated', 'sessions', 2, 3,
             '(user_id,updated_atdesc)include(id)', 'source=''user''andproject_idisnotnull'),
            ('ix_ak_project_changes_page', 'ak_project_file_changes', 2, 2,
             '(project_id,change_iddesc)', ''),
            ('ix_ak_permission_active_resource_principal', 'ak_resource_permission', 4, 4,
             '(resource_type,resource_id,principal_type,principal_id)', 'revoked_atisnull')
        ) AS required(index_name, table_name, key_count, column_count, keys, predicate)
    LOOP
        SELECT tab.relname AS table_name, am.amname AS method,
               i.indisvalid, i.indisready, i.indisunique, i.indnkeyatts, i.indnatts,
               lower(regexp_replace(pg_get_indexdef(i.indexrelid), '[[:space:]"]', '', 'g')) AS definition,
               lower(regexp_replace(coalesce(pg_get_expr(i.indpred, i.indrelid), ''),
                   '[[:space:]"()]|::text', '', 'g')) AS predicate
        INTO existing
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_namespace ns ON ns.oid = idx.relnamespace
        JOIN pg_class tab ON tab.oid = i.indrelid
        JOIN pg_am am ON am.oid = idx.relam
        WHERE ns.nspname = current_schema() AND idx.relname = expected.index_name;
        IF NOT FOUND THEN
            RAISE EXCEPTION '新索引 % 缺失，保留旧索引', expected.index_name;
        END IF;
        IF NOT existing.indisvalid OR NOT existing.indisready OR existing.indisunique
            OR existing.table_name <> expected.table_name OR existing.method <> 'btree'
            OR existing.indnkeyatts <> expected.key_count OR existing.indnatts <> expected.column_count
            OR strpos(existing.definition, expected.keys) = 0
            OR existing.predicate <> expected.predicate
        THEN
            RAISE EXCEPTION '新索引 % 无效或定义不一致，保留旧索引', expected.index_name;
        END IF;
    END LOOP;
END;
$validate$;

DROP INDEX CONCURRENTLY IF EXISTS ix_sessions_updated_at;
DROP INDEX CONCURRENTLY IF EXISTS ix_ak_project_file_changes_project_id;

SELECT pg_advisory_unlock(727140203);
RESET lock_timeout;
