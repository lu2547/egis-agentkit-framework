-- 仅用于首次初始化，在平台已提供的 PostgreSQL 空 schema 中执行。

-- 执行前选定目标数据库及 search_path；本文件不创建角色、数据库或 schema。

-- 范围：AgentScope 原生 5 表、AgentKit 25 表、集群消息总线 5 表，共 35 表。

-- 定义来源：storage/_schema.py、storage/model/_tables.py、storage/_remote_agent.py、storage/_schedule.py、storage/_message_bus.py。

-- 同名表已存在时整笔事务失败并回滚；不覆盖数据、不执行旧库迁移。

BEGIN;

-- 1. 原生运行记录、凭据、计划和协作表及索引。

CREATE TABLE agents (
    user_id VARCHAR(255) NOT NULL,
    source VARCHAR(16) NOT NULL,
    id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    payload JSON NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ix_agents_created_at ON agents (created_at);

CREATE INDEX ix_agents_source ON agents (source);

CREATE INDEX ix_agents_updated_at ON agents (updated_at);

CREATE INDEX ix_agents_user_id ON agents (user_id);

CREATE INDEX ix_agents_user_source ON agents (user_id, source);

CREATE TABLE credentials (
    user_id VARCHAR(255) NOT NULL,
    id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    payload JSON NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ix_credentials_created_at ON credentials (created_at);

CREATE INDEX ix_credentials_updated_at ON credentials (updated_at);

CREATE INDEX ix_credentials_user_id ON credentials (user_id);

CREATE TABLE schedules (
    user_id VARCHAR(255) NOT NULL,
    agent_id VARCHAR(255) NOT NULL,
    id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    payload JSON NOT NULL,
    project_id VARCHAR(64),
    next_run_at TIMESTAMP WITH TIME ZONE,
    deleted_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id)
);

CREATE INDEX ix_schedules_agent_id ON schedules (agent_id);

CREATE INDEX ix_schedules_created_at ON schedules (created_at);

CREATE INDEX ix_schedules_updated_at ON schedules (updated_at);

CREATE INDEX ix_schedules_user_id ON schedules (user_id);

CREATE INDEX ix_schedules_user_project
    ON schedules (user_id, project_id)
    WHERE project_id IS NOT NULL AND deleted_at IS NULL;

CREATE INDEX ix_schedules_due
    ON schedules (next_run_at, id)
    WHERE deleted_at IS NULL AND next_run_at IS NOT NULL;

CREATE TABLE ak_schedule_runs (
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

CREATE INDEX ix_ak_schedule_runs_history
    ON ak_schedule_runs (schedule_id, created_at DESC, id);

CREATE INDEX ix_ak_schedule_runs_queue
    ON ak_schedule_runs (created_at, id) WHERE status = 'queued';

CREATE INDEX ix_ak_schedule_runs_running
    ON ak_schedule_runs (started_at) WHERE status = 'running';

CREATE UNIQUE INDEX uq_ak_schedule_runs_automatic_time
    ON ak_schedule_runs (schedule_id, scheduled_for) WHERE scheduled_for IS NOT NULL;

CREATE TABLE sessions (
    user_id VARCHAR(255) NOT NULL,
    agent_id VARCHAR(255) NOT NULL,
    source VARCHAR(16) NOT NULL,
    source_schedule_id VARCHAR(255),
    team_id VARCHAR(255),
    id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    payload JSON NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ix_sessions_agent_id ON sessions (agent_id);

CREATE INDEX ix_sessions_created_at ON sessions (created_at);

CREATE INDEX ix_sessions_source ON sessions (source);

CREATE INDEX ix_sessions_source_schedule_id ON sessions (source_schedule_id);

CREATE INDEX ix_sessions_team_id ON sessions (team_id);


CREATE INDEX ix_sessions_user_agent ON sessions (user_id, agent_id);

CREATE INDEX ix_sessions_user_id ON sessions (user_id);

CREATE TABLE teams (
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    payload JSON NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ix_teams_created_at ON teams (created_at);

CREATE INDEX ix_teams_updated_at ON teams (updated_at);

CREATE INDEX ix_teams_user_id ON teams (user_id);

-- 2. 企业用户、定义、权限、资源和项目等业务表及索引。

CREATE TABLE ak_agent_definition_summary (
    id VARCHAR(64) NOT NULL,
    owner_type VARCHAR(16) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    agent_type VARCHAR(16) NOT NULL,
    agent_name VARCHAR(128) NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE,
    owner_version BIGINT NOT NULL,
    shared_version BIGINT,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id),
    CHECK (owner_type IN ('user','system')),
    CHECK (owner_version > 0),
    CHECK (agent_type IN ('personal','expert','remote','orchestrator'))
);

CREATE INDEX ix_ak_agent_catalog_active ON ak_agent_definition_summary (created_at DESC, id DESC) WHERE deleted_at IS NULL;

CREATE INDEX ix_ak_agent_owner_active ON ak_agent_definition_summary (owner_type, owner_id, agent_type) WHERE deleted_at IS NULL;

CREATE UNIQUE INDEX uq_ak_agent_definition_name ON ak_agent_definition_summary (owner_type, owner_id, agent_name);

CREATE UNIQUE INDEX uq_ak_personal_definition ON ak_agent_definition_summary (owner_id) WHERE owner_type='user' AND agent_type='personal';

CREATE TABLE ak_agent_remote_runs (
    run_id VARCHAR(64) NOT NULL,
    conversation_id VARCHAR(128) NOT NULL,
    connection_id VARCHAR(128) NOT NULL,
    connection_config JSONB NOT NULL,
    request_digest VARCHAR(64) NOT NULL,
    deadline TIMESTAMP WITH TIME ZONE NOT NULL,
    is_submitted BOOLEAN DEFAULT 'false' NOT NULL,
    result JSONB,
    reported_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (run_id)
);

CREATE INDEX ix_ak_agent_remote_runs_conversation_id ON ak_agent_remote_runs (conversation_id);

CREATE TABLE ak_approve (
    review_id VARCHAR(64) NOT NULL,
    resource_type VARCHAR(16) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    review_type VARCHAR(16) DEFAULT 'share' NOT NULL,
    original_review_id VARCHAR(64) REFERENCES ak_approve (review_id),
    content_snapshot JSONB,
    owner_id VARCHAR(128) NOT NULL,
    review_status_type VARCHAR(16) NOT NULL,
    is_current BOOLEAN NOT NULL,
    resource_version VARCHAR(128) NOT NULL,
    share_scope_type VARCHAR(16),
    principal_id VARCHAR(128),
    submitted_by VARCHAR(128) NOT NULL,
    submitted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    reviewed_by VARCHAR(128),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    review_reason TEXT NOT NULL,
    PRIMARY KEY (review_id),
    CHECK (review_status_type IN ('pending','approved','rejected','cancelled')),
    CHECK (resource_type IN ('agent','team','skill','mcp')),
    CHECK (resource_version <> ''),
    CHECK (NOT is_current OR review_status_type='approved'),
    CONSTRAINT ck_ak_approve_original CHECK ((review_type = 'revoke') = (original_review_id IS NOT NULL)),
    CONSTRAINT ck_ak_approve_target CHECK ((review_type IN ('share','revoke') AND share_scope_type IS NOT NULL AND share_scope_type IN ('user','group') AND principal_id IS NOT NULL AND principal_id NOT IN ('', '*')) OR (review_type = 'update' AND share_scope_type IS NULL AND principal_id IS NULL))
);

CREATE INDEX ix_ak_approve_resource_version ON ak_approve (resource_type, resource_id, resource_version);

CREATE UNIQUE INDEX uq_ak_approve_pending_update ON ak_approve (resource_type, resource_id) WHERE review_type = 'update' AND review_status_type = 'pending';

CREATE UNIQUE INDEX uq_ak_approve_pending_revoke ON ak_approve (original_review_id) WHERE review_type = 'revoke' AND review_status_type = 'pending';

CREATE TABLE ak_feedback (
    id VARCHAR(64) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    username TEXT NOT NULL,
    project_id VARCHAR(128) NOT NULL,
    project_name TEXT NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    session_name TEXT NOT NULL,
    reply_id VARCHAR(128) NOT NULL,
    agent_id VARCHAR(128) NOT NULL,
    definition_id VARCHAR(128) NOT NULL,
    agent_display_name TEXT NOT NULL,
    comment TEXT NOT NULL,
    question_text TEXT NOT NULL,
    reply_text TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ix_ak_feedback_created ON ak_feedback (created_at DESC, id DESC);

CREATE UNIQUE INDEX uq_ak_feedback_user_reply ON ak_feedback (user_id, session_id, reply_id);

CREATE TABLE ak_groups (
    group_id VARCHAR(128) NOT NULL,
    group_name VARCHAR(128) NOT NULL,
    is_enabled BOOLEAN DEFAULT true NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (group_id)
);

CREATE TABLE ak_mcp_credentials (
    user_id VARCHAR(128) NOT NULL,
    connection_id VARCHAR(128) NOT NULL,
    credential BYTEA,
    oauth_state_hash VARCHAR(64),
    oauth_pending BYTEA,
    oauth_expires_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (user_id, connection_id),
    UNIQUE (oauth_state_hash)
);

CREATE TABLE ak_project_file_changes (
    change_id BIGSERIAL NOT NULL,
    project_id VARCHAR(128) NOT NULL,
    session_id VARCHAR(128),
    reply_id VARCHAR(128),
    area VARCHAR(16) NOT NULL,
    path TEXT NOT NULL,
    change_type VARCHAR(16) NOT NULL,
    observed_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    size BIGINT,
    previous_size BIGINT,
    PRIMARY KEY (change_id)
);

CREATE INDEX ix_ak_project_change_path ON ak_project_file_changes (project_id, area, path, change_id);

CREATE INDEX ix_ak_project_change_reply ON ak_project_file_changes (project_id, session_id, reply_id, area, change_id);

CREATE INDEX ix_ak_project_changes_page ON ak_project_file_changes (project_id, change_id DESC);

CREATE TABLE ak_project_file_fingerprints (
    project_id VARCHAR(128) NOT NULL,
    area VARCHAR(16) NOT NULL,
    path TEXT NOT NULL,
    digest VARCHAR(64) NOT NULL,
    size BIGINT NOT NULL,
    PRIMARY KEY (project_id, area, path)
);

CREATE TABLE ak_projects (
    id VARCHAR(64) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    project_name VARCHAR(128) NOT NULL,
    status_type VARCHAR(16) DEFAULT 'active' NOT NULL,
    is_default BOOLEAN DEFAULT false NOT NULL,
    scan_has_baseline BOOLEAN DEFAULT false NOT NULL,
    scan_error TEXT,
    scan_error_at TIMESTAMP WITH TIME ZONE,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id),
    CHECK (status_type IN ('active','deleting'))
);

CREATE INDEX ix_ak_projects_owner_id ON ak_projects (owner_id);

CREATE UNIQUE INDEX uq_ak_default_project ON ak_projects (owner_id) WHERE is_default;

CREATE TABLE ak_resource_assets (
    resource_type VARCHAR(16) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    resource_version VARCHAR(128) NOT NULL,
    publication_revision BIGINT,
    owner_type VARCHAR(16) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    checksum_sha256 VARCHAR(64) NOT NULL,
    entrypoint TEXT,
    is_enabled BOOLEAN NOT NULL,
    is_shared_current BOOLEAN DEFAULT false NOT NULL,
    payload JSONB NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (resource_type, resource_id, resource_version),
    CHECK (resource_type IN ('skill','tool','mcp')),
    CHECK (publication_revision > 0)
);

CREATE UNIQUE INDEX uq_ak_asset_publication_revision ON ak_resource_assets (resource_type, resource_id, publication_revision);

CREATE UNIQUE INDEX uq_ak_asset_shared_current ON ak_resource_assets (resource_type, resource_id) WHERE is_shared_current;

-- Skill 按稳定 ID 保存归属和发布指针，包版本、摘要和文件清单落在明细。

CREATE TABLE ak_skill_summary (
    skill_id VARCHAR(128) NOT NULL,
    owner_type VARCHAR(16) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    source_user_id VARCHAR(128) NOT NULL,
    is_enabled BOOLEAN DEFAULT true NOT NULL,
    owner_version BIGINT NOT NULL,
    shared_version BIGINT,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (skill_id),
    CHECK (owner_type IN ('user','system')),
    CHECK (owner_version > 0)
);

CREATE INDEX ix_ak_skill_owner ON ak_skill_summary (owner_type, owner_id, skill_id);

CREATE TABLE ak_skill_detail (
    skill_id VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL,
    resource_version VARCHAR(128) NOT NULL,
    checksum_sha256 VARCHAR(64) NOT NULL,
    entrypoint TEXT,
    is_enabled BOOLEAN DEFAULT true NOT NULL,
    display_name TEXT,
    description TEXT,
    category TEXT,
    avatar TEXT,
    avatar_checksum VARCHAR(64),
    files JSONB,
    import_job_id VARCHAR(128),
    payload JSONB NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (skill_id, version),
    CHECK (version > 0),
    UNIQUE (skill_id, resource_version),
    FOREIGN KEY(skill_id) REFERENCES ak_skill_summary (skill_id) ON DELETE CASCADE
);

ALTER TABLE ak_skill_summary ADD CONSTRAINT fk_ak_skill_owner_version FOREIGN KEY(skill_id, owner_version) REFERENCES ak_skill_detail (skill_id, version) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ak_skill_summary ADD CONSTRAINT fk_ak_skill_shared_version FOREIGN KEY(skill_id, shared_version) REFERENCES ak_skill_detail (skill_id, version) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE ak_resource_permission (
    id VARCHAR(64) NOT NULL,
    principal_type VARCHAR(16) NOT NULL,
    principal_id VARCHAR(128) NOT NULL,
    resource_type VARCHAR(16) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    resource_view_type VARCHAR(16) NOT NULL,
    resource_owner_type VARCHAR(16) NOT NULL,
    resource_owner_id VARCHAR(128) NOT NULL,
    grant_source_type VARCHAR(16) NOT NULL,
    source_review_id VARCHAR(64),
    origin_key VARCHAR(192) DEFAULT 'manual' NOT NULL,
    root_grant_id VARCHAR(64),
    parent_grant_id VARCHAR(64),
    agent_id VARCHAR(64),
    team_id VARCHAR(64),
    granted_by VARCHAR(128) NOT NULL,
    granted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    revoked_by VARCHAR(128),
    revoked_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id),
    CHECK (principal_type IN ('user','group') AND principal_id NOT IN ('', '*')),
    CONSTRAINT ck_ak_resource_permission_view CHECK (resource_view_type IN ('private','system','shared')),
    CONSTRAINT ck_ak_resource_permission_source CHECK (grant_source_type IN ('owner','manual','share','dependency')),
    CHECK (resource_type IN ('agent','team','skill','mcp')),
    CHECK (grant_source_type != 'share' OR source_review_id IS NOT NULL),
    CONSTRAINT ck_ak_resource_permission_dependency CHECK ((grant_source_type = 'dependency') = (root_grant_id IS NOT NULL AND parent_grant_id IS NOT NULL AND agent_id IS NOT NULL)),
    FOREIGN KEY(root_grant_id) REFERENCES ak_resource_permission (id),
    FOREIGN KEY(parent_grant_id) REFERENCES ak_resource_permission (id)
);

CREATE INDEX ix_ak_resource_permission_parent_grant_id ON ak_resource_permission (parent_grant_id);

CREATE INDEX ix_ak_resource_permission_resource_principal ON ak_resource_permission (resource_type, resource_id, principal_type, principal_id, resource_view_type) WHERE revoked_at IS NULL AND root_grant_id IS NULL;

CREATE INDEX ix_ak_permission_active_resource_principal ON ak_resource_permission (resource_type, resource_id, principal_type, principal_id) WHERE revoked_at IS NULL;

CREATE INDEX ix_ak_resource_permission_root_grant_id ON ak_resource_permission (root_grant_id);

CREATE UNIQUE INDEX uq_ak_resource_permission_origin ON ak_resource_permission (principal_type, principal_id, resource_type, resource_id, resource_view_type, resource_owner_type, resource_owner_id, grant_source_type, origin_key) WHERE revoked_at IS NULL;

-- 导入和上架审批共用任务表；ZIP 暂存于 NAS，任务表保存完整性元数据。

CREATE TABLE ak_skill_imports (
    job_id VARCHAR(64) NOT NULL,
    uploader_id VARCHAR(128) NOT NULL,
    filename VARCHAR(255) NOT NULL,
    owner_type VARCHAR(16) DEFAULT 'user' NOT NULL,
    uploader_type VARCHAR(16) DEFAULT 'user' NOT NULL,
    reviewed_by VARCHAR(128),
    reviewed_at TIMESTAMP WITH TIME ZONE,
    review_reason TEXT,
    category VARCHAR(80) NOT NULL,
    avatar VARCHAR(64) NOT NULL,
    archive_location JSONB,
    status VARCHAR(16) DEFAULT 'queued' NOT NULL,
    stage VARCHAR(32) DEFAULT 'queued' NOT NULL,
    progress BIGINT DEFAULT '0' NOT NULL,
    attempts BIGINT DEFAULT '0' NOT NULL,
    lease_token VARCHAR(64),
    lease_until TIMESTAMP WITH TIME ZONE,
    available_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    resource_version VARCHAR(128),
    error TEXT,
    result JSONB,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (job_id),
    CHECK (status IN ('queued','running','pending_approval','rejected','succeeded','failed')),
    CHECK (owner_type IN ('user','system')),
    CHECK (uploader_type IN ('user','admin','virtual')),
    CHECK (progress BETWEEN 0 AND 100)
);

CREATE INDEX ix_ak_skill_import_claim ON ak_skill_imports (status, available_at);

CREATE INDEX ix_ak_skill_import_uploader ON ak_skill_imports (uploader_id, created_at);

CREATE TABLE ak_users (
    user_id VARCHAR(128) NOT NULL,
    username VARCHAR(128) NOT NULL,
    group_ids JSONB DEFAULT '[]'::jsonb NOT NULL,
    role_type VARCHAR(16) DEFAULT 'user' NOT NULL,
    is_enabled BOOLEAN DEFAULT true NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (user_id),
    CHECK (role_type IN ('user','admin')),
    CHECK (jsonb_typeof(group_ids)='array')
);

CREATE INDEX ix_ak_user_groups ON ak_users USING gin (group_ids);

CREATE TABLE ak_virtual_users (
    user_id VARCHAR(128) NOT NULL,
    username VARCHAR(128) NOT NULL,
    credential_sha256 VARCHAR(64) NOT NULL,
    is_enabled BOOLEAN DEFAULT true NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (user_id)
);

CREATE TABLE ak_agent_definition_detail (
    definition_id VARCHAR(64) NOT NULL,
    version BIGINT NOT NULL,
    agent_name VARCHAR(128) NOT NULL,
    agent_display_name VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (definition_id, version),
    CHECK (version > 0),
    FOREIGN KEY(definition_id) REFERENCES ak_agent_definition_summary (id)
);

CREATE TABLE ak_agent_personal_resources (
    user_id VARCHAR(128) NOT NULL,
    agent_id VARCHAR(64) NOT NULL,
    resource_type VARCHAR(16) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    is_enabled BOOLEAN NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (agent_id, resource_type, resource_id),
    CHECK (resource_type IN ('skill','mcp','tool')),
    FOREIGN KEY(agent_id) REFERENCES ak_agent_definition_summary (id)
);

CREATE INDEX ix_ak_agent_personal_resources_user_type ON ak_agent_personal_resources (user_id, resource_type);

CREATE TABLE ak_agent_invite_candidates (
    user_id VARCHAR(128) NOT NULL REFERENCES ak_users (user_id) ON DELETE CASCADE,
    definition_id VARCHAR(64) NOT NULL REFERENCES ak_agent_definition_summary (id) ON DELETE CASCADE,
    resource_view_type VARCHAR(16) NOT NULL,
    PRIMARY KEY (user_id, definition_id, resource_view_type),
    CHECK (resource_view_type IN ('private','system','shared'))
);

CREATE TABLE ak_project_reply_artifacts (
    project_id VARCHAR(128) NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    reply_id VARCHAR(128) NOT NULL,
    path TEXT NOT NULL,
    digest VARCHAR(64) NOT NULL,
    PRIMARY KEY (project_id, session_id, reply_id, path),
    FOREIGN KEY(project_id) REFERENCES ak_projects (id) ON DELETE CASCADE
);

CREATE TABLE ak_team_definition_summary (
    id VARCHAR(64) NOT NULL,
    owner_type VARCHAR(16) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    team_name VARCHAR(128) NOT NULL,
    orchestrator_id VARCHAR(64) NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE,
    owner_version BIGINT NOT NULL,
    shared_version BIGINT,
    created_by VARCHAR(128) NOT NULL,
    updated_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (id),
    CHECK (owner_type IN ('user','system')),
    CHECK (owner_version > 0),
    FOREIGN KEY(orchestrator_id) REFERENCES ak_agent_definition_summary (id)
);

CREATE INDEX ix_ak_team_catalog_active ON ak_team_definition_summary (created_at DESC, id DESC) WHERE deleted_at IS NULL;

CREATE INDEX ix_ak_team_owner_active ON ak_team_definition_summary (owner_type, owner_id) WHERE deleted_at IS NULL;

CREATE UNIQUE INDEX uq_ak_team_definition_name ON ak_team_definition_summary (owner_type, owner_id, team_name);

CREATE TABLE ak_team_definition_detail (
    definition_id VARCHAR(64) NOT NULL,
    version BIGINT NOT NULL,
    team_name VARCHAR(128) NOT NULL,
    team_display_name VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    PRIMARY KEY (definition_id, version),
    CHECK (version > 0),
    FOREIGN KEY(definition_id) REFERENCES ak_team_definition_summary (id)
);

-- 3. 从会话状态生成项目归属列，与 storage/_schema.py 的运行时查询一致。

ALTER TABLE sessions ADD COLUMN project_id text GENERATED ALWAYS AS (payload #>> '{state,middle_context,egis_agentkit,project_id}') STORED;

CREATE INDEX ix_sessions_project_user ON sessions (project_id, user_id) WHERE project_id IS NOT NULL;

CREATE INDEX ix_sessions_main_user_updated ON sessions (user_id, updated_at DESC) INCLUDE (id) WHERE source='user' AND project_id IS NOT NULL;

-- 4. cluster 消息总线的 PostgreSQL 状态；Kafka Topic 由独立脚本初始化。

CREATE TABLE ak_bus_keys (
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    seq BIGINT NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (kind, key)
);

CREATE TABLE ak_bus_entries (
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    seq BIGINT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (kind, key, seq)
);

CREATE TABLE ak_bus_registry (
    key TEXT NOT NULL,
    field TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (key, field)
);

CREATE TABLE ak_bus_leases (
    key TEXT PRIMARY KEY,
    token TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE ak_bus_broadcast_payloads (
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    key TEXT NOT NULL,
    payload JSONB NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX ak_bus_keys_expiry_idx
    ON ak_bus_keys (expires_at) WHERE expires_at IS NOT NULL;

CREATE INDEX ak_bus_leases_expiry_idx
    ON ak_bus_leases (expires_at);

CREATE INDEX ak_bus_broadcast_payloads_expiry_idx
    ON ak_bus_broadcast_payloads (expires_at);

-- 5. NAS 历史文件的已提交字节数及跨进程写入序号。

CREATE SEQUENCE ak_session_history_write_fence AS BIGINT;

CREATE TABLE ak_session_history (
    session_id VARCHAR(255) PRIMARY KEY REFERENCES sessions (id) ON DELETE CASCADE,
    user_id VARCHAR(255) NOT NULL,
    committed_bytes BIGINT NOT NULL CHECK (committed_bytes >= 0)
);

CREATE TABLE ak_session_history_gc (
    fence BIGINT PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    retry_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
