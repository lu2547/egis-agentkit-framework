-- Additive migration for withdrawal approvals and immutable review context.
-- Run ONLY after selecting the intended database and schema; this file is not auto-applied.
-- Equivalent managed upgrade: egis_agentkit.storage._schema.upgrade_schema(engine, ..., apply=True).
-- Existing approval status, recipient permissions and publication pointers are preserved.
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
ALTER TABLE ak_approve
  ADD COLUMN IF NOT EXISTS original_review_id varchar(64) REFERENCES ak_approve(review_id),
  ADD COLUMN IF NOT EXISTS content_snapshot jsonb,
  DROP CONSTRAINT IF EXISTS ck_ak_approve_target,
  DROP CONSTRAINT IF EXISTS ck_ak_share_target,
  DROP CONSTRAINT IF EXISTS ck_ak_approve_original;
ALTER TABLE ak_approve
  ADD CONSTRAINT ck_ak_approve_target CHECK (
    (review_type IN ('share','revoke') AND share_scope_type IS NOT NULL
     AND share_scope_type IN ('user','group') AND principal_id IS NOT NULL
     AND principal_id NOT IN ('','*')) OR
    (review_type='update' AND share_scope_type IS NULL AND principal_id IS NULL)),
  ADD CONSTRAINT ck_ak_approve_original CHECK (
    (review_type='revoke') = (original_review_id IS NOT NULL));
CREATE UNIQUE INDEX IF NOT EXISTS uq_ak_approve_pending_revoke
  ON ak_approve(original_review_id)
  WHERE review_type='revoke' AND review_status_type='pending';
COMMIT;
-- content_snapshot remains NULL for legacy applications. Their content endpoint explicitly
-- reports origin='reconstructed': immutable expert/skill versions are resolved as recorded,
-- while an old mutable MCP configuration cannot be recovered and must not be called frozen.
-- New submissions persist root definition + dependency versions, file manifest/checksums,
-- and safe MCP configuration/revision. Skill archives remain in verified version storage.
