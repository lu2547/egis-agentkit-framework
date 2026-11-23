-- 1.0.3 只读核验：使用与 01、02 相同的数据库和单一 search_path。
-- 预期 38 行 passed 全为 true；保留实际定义作为发布记录。
-- 包含 1.0.2 查询索引前置检查；不会修改表、索引、约束或业务数据。
WITH expected_columns(table_name, column_name, column_type, required) AS (VALUES
    ('schedules', 'project_id', 'character varying(64)', false),
    ('schedules', 'next_run_at', 'timestamp with time zone', false),
    ('schedules', 'deleted_at', 'timestamp with time zone', false),
    ('ak_schedule_runs', 'id', 'character varying(64)', true),
    ('ak_schedule_runs', 'schedule_id', 'character varying(255)', true),
    ('ak_schedule_runs', 'schedule_name', 'text', true),
    ('ak_schedule_runs', 'project_id', 'character varying(64)', true),
    ('ak_schedule_runs', 'trigger_type', 'character varying(16)', true),
    ('ak_schedule_runs', 'scheduled_for', 'timestamp with time zone', false),
    ('ak_schedule_runs', 'status', 'character varying(16)', true),
    ('ak_schedule_runs', 'created_at', 'timestamp with time zone', true),
    ('ak_schedule_runs', 'started_at', 'timestamp with time zone', false),
    ('ak_schedule_runs', 'finished_at', 'timestamp with time zone', false),
    ('ak_schedule_runs', 'input_snapshot', 'jsonb', true),
    ('ak_schedule_runs', 'session_id', 'character varying(255)', false),
    ('ak_schedule_runs', 'reply_id', 'character varying(255)', false),
    ('ak_schedule_runs', 'summary', 'text', false),
    ('ak_schedule_runs', 'error_code', 'character varying(64)', false),
    ('ak_schedule_runs', 'error_message', 'text', false),
    ('ak_schedule_runs', 'archived_at', 'timestamp with time zone', false),
    ('ak_schedule_runs', 'deleted_at', 'timestamp with time zone', false)
), expected_constraints(table_name, constraint_name, normalized_definition) AS (VALUES
    ('ak_schedule_runs', 'ak_schedule_runs_pkey', 'primarykeyid'),
    ('ak_schedule_runs', 'ak_schedule_runs_schedule_id_fkey',
     'foreignkeyschedule_idreferencesschedulesid'),
    ('ak_schedule_runs', 'ak_schedule_runs_trigger_type_check',
     'checktrigger_type=anyarray[''scheduled'',''backfill'',''test''][]'),
    ('ak_schedule_runs', 'ak_schedule_runs_scheduled_for_check',
     'checktrigger_type=''test''=scheduled_forisnull'),
    ('ak_schedule_runs', 'ak_schedule_runs_status_check',
     'checkstatus=anyarray[''queued'',''running'',''succeeded'',''failed''][]'),
    ('ak_agent_personal_resources', 'ak_agent_personal_resources_resource_type_check',
     'checkresource_type=anyarray[''skill'',''mcp'',''tool''][]')
), expected_indexes(index_name, table_name, is_unique, keys, predicate) AS (VALUES
    ('ix_schedules_user_project', 'schedules', false,
     '(user_id,project_id)', 'project_idisnotnullanddeleted_atisnull'),
    ('ix_schedules_due', 'schedules', false,
     '(next_run_at,id)', 'deleted_atisnullandnext_run_atisnotnull'),
    ('ix_ak_schedule_runs_history', 'ak_schedule_runs', false,
     '(schedule_id,created_atdesc,id)', ''),
    ('ix_ak_schedule_runs_queue', 'ak_schedule_runs', false,
     '(created_at,id)', 'status=''queued'''),
    ('ix_ak_schedule_runs_running', 'ak_schedule_runs', false,
     '(started_at)', 'status=''running'''),
    ('uq_ak_schedule_runs_automatic_time', 'ak_schedule_runs', true,
     '(schedule_id,scheduled_for)', 'scheduled_forisnotnull'),
    ('ix_sessions_main_user_updated', 'sessions', false,
     '(user_id,updated_atdesc)include(id)', 'source=''user''andproject_idisnotnull'),
    ('ix_ak_project_changes_page', 'ak_project_file_changes', false,
     '(project_id,change_iddesc)', ''),
    ('ix_ak_permission_active_resource_principal', 'ak_resource_permission', false,
     '(resource_type,resource_id,principal_type,principal_id)', 'revoked_atisnull')
), checks AS (
    SELECT 'column' AS kind, e.table_name || '.' || e.column_name AS object_name,
           coalesce(format_type(a.atttypid, a.atttypmod) = e.column_type
                    AND a.attnotnull = e.required AND a.attgenerated = ''
                    AND d.oid IS NULL, false) AS passed,
           format_type(a.atttypid, a.atttypmod) ||
               CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE ' NULL' END AS definition
    FROM expected_columns e
    LEFT JOIN pg_namespace ns ON ns.nspname = current_schema()
    LEFT JOIN pg_class tab ON tab.relnamespace = ns.oid AND tab.relname = e.table_name
    LEFT JOIN pg_attribute a ON a.attrelid = tab.oid AND a.attname = e.column_name
                                AND a.attnum > 0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid = tab.oid AND d.adnum = a.attnum
    UNION ALL
    SELECT 'constraint', e.table_name || '.' || e.constraint_name,
           coalesce(p.convalidated AND NOT p.condeferrable AND
               lower(regexp_replace(pg_get_constraintdef(p.oid),
                   '[[:space:]"()]|::text|::character varying', '', 'g')) =
                   e.normalized_definition, false),
           pg_get_constraintdef(p.oid)
    FROM expected_constraints e
    LEFT JOIN pg_namespace ns ON ns.nspname = current_schema()
    LEFT JOIN pg_class tab ON tab.relnamespace = ns.oid AND tab.relname = e.table_name
    LEFT JOIN pg_constraint p ON p.conrelid = tab.oid AND p.conname = e.constraint_name
    UNION ALL
    SELECT 'index', e.index_name,
           coalesce(i.indisvalid AND i.indisready AND i.indisunique = e.is_unique
               AND tab.relname = e.table_name AND am.amname = 'btree'
               AND split_part(split_part(lower(regexp_replace(pg_get_indexdef(idx.oid),
                   '[[:space:]"]', '', 'g')), 'usingbtree', 2), 'where', 1) = e.keys
               AND lower(regexp_replace(coalesce(pg_get_expr(i.indpred, i.indrelid), ''),
                   '[[:space:]"()]|::text|::character varying', '', 'g')) = e.predicate,
               false),
           pg_get_indexdef(idx.oid)
    FROM expected_indexes e
    LEFT JOIN pg_namespace ns ON ns.nspname = current_schema()
    LEFT JOIN pg_class idx ON idx.relnamespace = ns.oid AND idx.relname = e.index_name
    LEFT JOIN pg_index i ON i.indexrelid = idx.oid
    LEFT JOIN pg_class tab ON tab.oid = i.indrelid
    LEFT JOIN pg_am am ON am.oid = idx.relam
    UNION ALL
    SELECT 'retired index', old.index_name, idx.oid IS NULL, pg_get_indexdef(idx.oid)
    FROM (VALUES ('ix_sessions_updated_at'), ('ix_ak_project_file_changes_project_id'))
        AS old(index_name)
    LEFT JOIN pg_namespace ns ON ns.nspname = current_schema()
    LEFT JOIN pg_class idx ON idx.relnamespace = ns.oid AND idx.relname = old.index_name
)
SELECT current_database() AS database_name, current_schema() AS schema_name,
       kind, object_name, passed, definition
FROM checks
ORDER BY kind, object_name;
