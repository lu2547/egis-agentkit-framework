-- 1.0.3：个人助理资源选择增加可选工具；保留已有 Skill/MCP 选择和启停状态。
-- 先执行 01_schedules.sql；在应用连接的数据库和单一 schema 中执行，可重复执行。
-- psql -X -v ON_ERROR_STOP=1 -f；本文件自带事务，不添加外层事务。
BEGIN;
SET LOCAL lock_timeout = '5s';
DO $target$
BEGIN
    IF current_schema() IS NULL THEN
        RAISE EXCEPTION '没有有效的目标 schema，请显式设置 search_path';
    END IF;
    PERFORM set_config('search_path', format('%I', current_schema()), true);
    PERFORM pg_advisory_xact_lock(727140201);
END;
$target$;

ALTER TABLE ak_agent_personal_resources
    DROP CONSTRAINT ak_agent_personal_resources_resource_type_check,
    ADD CONSTRAINT ak_agent_personal_resources_resource_type_check
        CHECK (resource_type IN ('skill','mcp','tool'));
COMMIT;
