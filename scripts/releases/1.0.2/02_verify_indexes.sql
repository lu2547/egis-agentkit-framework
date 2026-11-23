-- 只读核验：连接与 01_query_indexes.sql 相同的数据库和单一 schema。
-- 预期 5 行 passed 全为 true；前三行还展示实际定义供发布记录保存。
WITH expected(index_name, should_exist) AS (VALUES
    ('ix_sessions_main_user_updated', true),
    ('ix_ak_project_changes_page', true),
    ('ix_ak_permission_active_resource_principal', true),
    ('ix_sessions_updated_at', false),
    ('ix_ak_project_file_changes_project_id', false)
)
SELECT current_database() AS database_name, current_schema() AS schema_name,
       expected.index_name,
       CASE WHEN expected.should_exist
           THEN coalesce(i.indisvalid AND i.indisready, false)
           ELSE idx.oid IS NULL
       END AS passed,
       pg_get_indexdef(idx.oid) AS definition
FROM expected
LEFT JOIN pg_namespace ns ON ns.nspname = current_schema()
LEFT JOIN pg_class idx ON idx.relnamespace = ns.oid AND idx.relname = expected.index_name
LEFT JOIN pg_index i ON i.indexrelid = idx.oid
ORDER BY expected.should_exist DESC, expected.index_name;
