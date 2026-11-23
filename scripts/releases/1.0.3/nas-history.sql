-- 增量升级：为 NAS 原始消息日志创建提交位置和写入序列。
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

CREATE SEQUENCE IF NOT EXISTS ak_session_history_write_fence AS BIGINT;

CREATE TABLE IF NOT EXISTS ak_session_history (
    session_id VARCHAR(255) PRIMARY KEY REFERENCES sessions (id) ON DELETE CASCADE,
    user_id VARCHAR(255) NOT NULL,
    committed_bytes BIGINT NOT NULL CHECK (committed_bytes >= 0)
);

CREATE TABLE IF NOT EXISTS ak_session_history_gc (
    fence BIGINT PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    retry_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
