-- 1.0.3：在原生 schedules 上增加项目定时任务字段，新增运行记录及调度索引。
-- 基线为已执行 1.0.2/01_query_indexes.sql 的 1.0.2；首次安装使用 scripts/init/pg.sql。
-- 在应用连接的数据库和单一 schema 中执行，可重复执行；不改写已有计划或消息。
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

ALTER TABLE schedules
    ADD COLUMN IF NOT EXISTS project_id VARCHAR(64),
    ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX IF NOT EXISTS ix_schedules_user_project
    ON schedules (user_id, project_id)
    WHERE project_id IS NOT NULL AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_schedules_due
    ON schedules (next_run_at, id)
    WHERE deleted_at IS NULL AND next_run_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS ak_schedule_runs (
    id VARCHAR(64) PRIMARY KEY,
    schedule_id VARCHAR(255) NOT NULL REFERENCES schedules (id),
    schedule_name TEXT NOT NULL,
    project_id VARCHAR(64) NOT NULL,
    trigger_type VARCHAR(16) NOT NULL,
    scheduled_for TIMESTAMP WITH TIME ZONE,
    status VARCHAR(16) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE,
    finished_at TIMESTAMP WITH TIME ZONE,
    input_snapshot JSONB NOT NULL,
    session_id VARCHAR(255),
    reply_id VARCHAR(255),
    summary TEXT,
    error_code VARCHAR(64),
    error_message TEXT,
    archived_at TIMESTAMP WITH TIME ZONE,
    deleted_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT ak_schedule_runs_trigger_type_check
        CHECK (trigger_type IN ('scheduled', 'backfill', 'test')),
    CONSTRAINT ak_schedule_runs_scheduled_for_check
        CHECK ((trigger_type = 'test') = (scheduled_for IS NULL)),
    CONSTRAINT ak_schedule_runs_status_check
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed'))
);

CREATE INDEX IF NOT EXISTS ix_ak_schedule_runs_history
    ON ak_schedule_runs (schedule_id, created_at DESC, id);

CREATE INDEX IF NOT EXISTS ix_ak_schedule_runs_queue
    ON ak_schedule_runs (created_at, id) WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS ix_ak_schedule_runs_running
    ON ak_schedule_runs (started_at) WHERE status = 'running';

CREATE UNIQUE INDEX IF NOT EXISTS uq_ak_schedule_runs_automatic_time
    ON ak_schedule_runs (schedule_id, scheduled_for) WHERE scheduled_for IS NOT NULL;

COMMIT;
