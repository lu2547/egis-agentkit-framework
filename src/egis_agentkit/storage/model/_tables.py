"""企业业务表独立于 AgentScope 的原生表。"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()

# 反馈保留问题和回答快照，删除原会话后仍能由管理员处理。
feedback = Table(
    "ak_feedback",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("user_id", String(128), nullable=False),
    Column("username", Text, nullable=False),
    Column("project_id", String(128), nullable=False),
    Column("project_name", Text, nullable=False),
    Column("session_id", String(128), nullable=False),
    Column("session_name", Text, nullable=False),
    Column("reply_id", String(128), nullable=False),
    Column("agent_id", String(128), nullable=False),
    Column("definition_id", String(128), nullable=False),
    Column("agent_display_name", Text, nullable=False),
    Column("comment", Text, nullable=False),
    Column("question_text", Text, nullable=False),
    Column("reply_text", Text, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
)
Index(
    "uq_ak_feedback_user_reply",
    feedback.c.user_id,
    feedback.c.session_id,
    feedback.c.reply_id,
    unique=True,
)
Index(
    "ix_ak_feedback_created",
    feedback.c.created_at.desc(),
    feedback.c.id.desc(),
)


def audit_columns():
    return [
        Column("created_by", String(128), nullable=False),
        Column("updated_by", String(128), nullable=False),
        Column(
            "created_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=text("now()"),
        ),
        Column(
            "updated_at",
            DateTime(timezone=True),
            nullable=False,
            server_default=text("now()"),
        ),
    ]


users = Table(
    "ak_users",
    metadata,
    Column("user_id", String(128), primary_key=True),
    Column("username", String(128), nullable=False),
    Column("group_ids", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column("role_type", String(16), nullable=False, server_default="user"),
    Column("is_enabled", Boolean, nullable=False, server_default=text("true")),
    *audit_columns(),
    CheckConstraint("role_type IN ('user','admin')"),
    CheckConstraint("jsonb_typeof(group_ids)='array'"),
)
Index("ix_ak_user_groups", users.c.group_ids, postgresql_using="gin")
virtual_users = Table(
    "ak_virtual_users",
    metadata,
    Column("user_id", String(128), primary_key=True),
    Column("username", String(128), nullable=False),
    Column("credential_sha256", String(64), nullable=False),
    Column("is_enabled", Boolean, nullable=False, server_default=text("true")),
    *audit_columns(),
)
groups = Table(
    "ak_groups",
    metadata,
    Column("group_id", String(128), primary_key=True),
    Column("group_name", String(128), nullable=False),
    Column("is_enabled", Boolean, nullable=False, server_default=text("true")),
    *audit_columns(),
)
agent_definition_summary = Table(
    "ak_agent_definition_summary",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("owner_type", String(16), nullable=False),
    Column("owner_id", String(128), nullable=False),
    Column("agent_type", String(16), nullable=False),
    Column("agent_name", String(128), nullable=False),
    Column("deleted_at", DateTime(timezone=True)),
    Column("owner_version", BigInteger, nullable=False),
    Column("shared_version", BigInteger),
    *audit_columns(),
    CheckConstraint("owner_type IN ('user','system')"),
    CheckConstraint("owner_version > 0"),
    CheckConstraint("agent_type IN ('personal','expert','remote','orchestrator')"),
)
Index(
    "uq_ak_agent_definition_name",
    agent_definition_summary.c.owner_type,
    agent_definition_summary.c.owner_id,
    agent_definition_summary.c.agent_name,
    unique=True,
)
Index(
    "uq_ak_personal_definition",
    agent_definition_summary.c.owner_id,
    unique=True,
    postgresql_where=text("owner_type='user' AND agent_type='personal'"),
)
# 主表保存生命周期和当前名称索引，版本表保存可执行配置。
agent_definition_detail = Table(
    "ak_agent_definition_detail",
    metadata,
    Column(
        "definition_id",
        String(64),
        ForeignKey("ak_agent_definition_summary.id"),
        primary_key=True,
    ),
    Column("version", BigInteger, primary_key=True),
    Column("agent_name", String(128), nullable=False),
    Column("agent_display_name", String(128), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_by", String(128), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    CheckConstraint("version > 0"),
)
Index(
    "ix_ak_agent_catalog_active",
    agent_definition_summary.c.created_at.desc(),
    agent_definition_summary.c.id.desc(),
    postgresql_where=agent_definition_summary.c.deleted_at.is_(None),
)
Index(
    "ix_ak_agent_owner_active",
    agent_definition_summary.c.owner_type,
    agent_definition_summary.c.owner_id,
    agent_definition_summary.c.agent_type,
    postgresql_where=agent_definition_summary.c.deleted_at.is_(None),
)
agent_personal_resources = Table(
    "ak_agent_personal_resources",
    metadata,
    Column("user_id", String(128), nullable=False),
    Column(
        "agent_id",
        String(64),
        ForeignKey("ak_agent_definition_summary.id"),
        primary_key=True,
    ),
    Column("resource_type", String(16), primary_key=True),
    Column("resource_id", String(128), primary_key=True),
    Column("is_enabled", Boolean, nullable=False),
    *audit_columns(),
    CheckConstraint("resource_type IN ('skill','mcp','tool')"),
)
Index(
    "ix_ak_agent_personal_resources_user_type",
    agent_personal_resources.c.user_id,
    agent_personal_resources.c.resource_type,
)

agent_invite_candidates = Table(
    "ak_agent_invite_candidates",
    metadata,
    Column(
        "user_id", String(128), ForeignKey("ak_users.user_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "definition_id", String(64),
        ForeignKey("ak_agent_definition_summary.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("resource_view_type", String(16), primary_key=True),
    CheckConstraint("resource_view_type IN ('private','system','shared')"),
)

team_definition_summary = Table(
    "ak_team_definition_summary",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("owner_type", String(16), nullable=False),
    Column("owner_id", String(128), nullable=False),
    Column("team_name", String(128), nullable=False),
    Column(
        "orchestrator_id",
        String(64),
        ForeignKey("ak_agent_definition_summary.id"),
        nullable=False,
    ),
    Column("deleted_at", DateTime(timezone=True)),
    Column("owner_version", BigInteger, nullable=False),
    Column("shared_version", BigInteger),
    *audit_columns(),
    CheckConstraint("owner_type IN ('user','system')"),
    CheckConstraint("owner_version > 0"),
)
Index(
    "uq_ak_team_definition_name",
    team_definition_summary.c.owner_type,
    team_definition_summary.c.owner_id,
    team_definition_summary.c.team_name,
    unique=True,
)
team_definition_detail = Table(
    "ak_team_definition_detail",
    metadata,
    Column(
        "definition_id",
        String(64),
        ForeignKey("ak_team_definition_summary.id"),
        primary_key=True,
    ),
    Column("version", BigInteger, primary_key=True),
    Column("team_name", String(128), nullable=False),
    Column("team_display_name", String(128), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_by", String(128), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    CheckConstraint("version > 0"),
)
Index(
    "ix_ak_team_catalog_active",
    team_definition_summary.c.created_at.desc(),
    team_definition_summary.c.id.desc(),
    postgresql_where=team_definition_summary.c.deleted_at.is_(None),
)
Index(
    "ix_ak_team_owner_active",
    team_definition_summary.c.owner_type,
    team_definition_summary.c.owner_id,
    postgresql_where=team_definition_summary.c.deleted_at.is_(None),
)
assets = Table(
    "ak_resource_assets",
    metadata,
    Column("resource_type", String(16), primary_key=True),
    Column("resource_id", String(128), primary_key=True),
    Column("resource_version", String(128), primary_key=True),
    Column("publication_revision", BigInteger),
    Column("owner_type", String(16), nullable=False),
    Column("owner_id", String(128), nullable=False),
    Column("checksum_sha256", String(64), nullable=False),
    Column("entrypoint", Text),
    Column("is_enabled", Boolean, nullable=False),
    Column("is_shared_current", Boolean, nullable=False, server_default=text("false")),
    Column("payload", JSONB, nullable=False),
    *audit_columns(),
    CheckConstraint("resource_type IN ('skill','tool','mcp')"),
    CheckConstraint("publication_revision > 0"),
)
Index(
    "uq_ak_asset_publication_revision",
    assets.c.resource_type,
    assets.c.resource_id,
    assets.c.publication_revision,
    unique=True,
)
Index(
    "uq_ak_asset_shared_current",
    assets.c.resource_type,
    assets.c.resource_id,
    unique=True,
    postgresql_where=assets.c.is_shared_current,
)
skill_summary = Table(
    "ak_skill_summary",
    metadata,
    Column("skill_id", String(128), primary_key=True),
    Column("owner_type", String(16), nullable=False),
    Column("owner_id", String(128), nullable=False),
    Column("source_user_id", String(128), nullable=False),
    Column("is_enabled", Boolean, nullable=False, server_default=text("true")),
    Column("owner_version", BigInteger, nullable=False),
    Column("shared_version", BigInteger),
    *audit_columns(),
    CheckConstraint("owner_type IN ('user','system')"),
    CheckConstraint("owner_version > 0"),
    ForeignKeyConstraint(
        ("skill_id", "owner_version"),
        ("ak_skill_detail.skill_id", "ak_skill_detail.version"),
        name="fk_ak_skill_owner_version",
        deferrable=True,
        initially="DEFERRED",
        use_alter=True,
    ),
    ForeignKeyConstraint(
        ("skill_id", "shared_version"),
        ("ak_skill_detail.skill_id", "ak_skill_detail.version"),
        name="fk_ak_skill_shared_version",
        deferrable=True,
        initially="DEFERRED",
        use_alter=True,
    ),
)
Index(
    "ix_ak_skill_owner",
    skill_summary.c.owner_type,
    skill_summary.c.owner_id,
    skill_summary.c.skill_id,
)
skill_detail = Table(
    "ak_skill_detail",
    metadata,
    Column(
        "skill_id",
        String(128),
        ForeignKey("ak_skill_summary.skill_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("version", BigInteger, primary_key=True),
    Column("resource_version", String(128), nullable=False),
    Column("checksum_sha256", String(64), nullable=False),
    Column("entrypoint", Text),
    Column("is_enabled", Boolean, nullable=False, server_default=text("true")),
    Column("display_name", Text),
    Column("description", Text),
    Column("category", Text),
    Column("avatar", Text),
    Column("avatar_checksum", String(64)),
    Column("files", JSONB),
    Column("import_job_id", String(128)),
    Column("payload", JSONB, nullable=False),
    *audit_columns(),
    CheckConstraint("version > 0"),
    UniqueConstraint("skill_id", "resource_version"),
)
resource_permission = Table(
    "ak_resource_permission",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("principal_type", String(16), nullable=False),
    Column("principal_id", String(128), nullable=False),
    Column("resource_type", String(16), nullable=False),
    Column("resource_id", String(128), nullable=False),
    Column("resource_view_type", String(16), nullable=False),
    Column("resource_owner_type", String(16), nullable=False),
    Column("resource_owner_id", String(128), nullable=False),
    Column("grant_source_type", String(16), nullable=False),
    Column("source_review_id", String(64)),
    Column("origin_key", String(192), nullable=False, server_default="manual"),
    Column("root_grant_id", ForeignKey("ak_resource_permission.id"), index=True),
    Column("parent_grant_id", ForeignKey("ak_resource_permission.id"), index=True),
    Column("agent_id", String(64)),
    Column("team_id", String(64)),
    Column("granted_by", String(128), nullable=False),
    Column("granted_at", DateTime(timezone=True), nullable=False),
    Column("revoked_by", String(128)),
    Column("revoked_at", DateTime(timezone=True)),
    CheckConstraint(
        "principal_type IN ('user','group') AND principal_id NOT IN ('', '*')"
    ),
    CheckConstraint(
        "resource_view_type IN ('private','system','shared')",
        name="ck_ak_resource_permission_view",
    ),
    CheckConstraint(
        "grant_source_type IN ('owner','manual','share','dependency')",
        name="ck_ak_resource_permission_source",
    ),
    CheckConstraint("resource_type IN ('agent','team','skill','mcp')"),
    CheckConstraint("grant_source_type != 'share' OR source_review_id IS NOT NULL"),
    CheckConstraint(
        "(grant_source_type = 'dependency') = (root_grant_id IS NOT NULL AND parent_grant_id IS NOT NULL AND agent_id IS NOT NULL)",
        name="ck_ak_resource_permission_dependency",
    ),
)
Index(
    "uq_ak_resource_permission_origin",
    *[
        resource_permission.c[c]
        for c in [
            "principal_type",
            "principal_id",
            "resource_type",
            "resource_id",
            "resource_view_type",
            "resource_owner_type",
            "resource_owner_id",
            "grant_source_type",
            "origin_key",
        ]
    ],
    unique=True,
    postgresql_where=text("revoked_at IS NULL"),
)
Index(
    "ix_ak_resource_permission_resource_principal",
    resource_permission.c.resource_type,
    resource_permission.c.resource_id,
    resource_permission.c.principal_type,
    resource_permission.c.principal_id,
    resource_permission.c.resource_view_type,
    postgresql_where=resource_permission.c.revoked_at.is_(None)
    & resource_permission.c.root_grant_id.is_(None),
)
approvals = Table(
    "ak_approve",
    metadata,
    Column("review_id", String(64), primary_key=True),
    Column("resource_type", String(16), nullable=False),
    Column("resource_id", String(128), nullable=False),
    Column("review_type", String(16), nullable=False, server_default="share"),
    Column("owner_id", String(128), nullable=False),
    Column("review_status_type", String(16), nullable=False),
    Column("is_current", Boolean, nullable=False),
    Column("resource_version", String(128), nullable=False),
    Column("share_scope_type", String(16)),
    Column("principal_id", String(128)),
    Column("submitted_by", String(128), nullable=False),
    Column("submitted_at", DateTime(timezone=True), nullable=False),
    Column("reviewed_by", String(128)),
    Column("reviewed_at", DateTime(timezone=True)),
    Column("review_reason", Text, nullable=False),
    CheckConstraint(
        "review_status_type IN ('pending','approved','rejected','cancelled')"
    ),
    CheckConstraint("resource_type IN ('agent','team','skill','mcp')"),
    CheckConstraint("resource_version <> ''"),
    CheckConstraint("NOT is_current OR review_status_type='approved'"),
    CheckConstraint(
        "(review_type = 'share' AND share_scope_type IS NOT NULL "
        "AND share_scope_type IN ('user','group') AND principal_id IS NOT NULL "
        "AND principal_id NOT IN ('', '*')) OR "
        "(review_type = 'update' AND share_scope_type IS NULL AND principal_id IS NULL)",
        name="ck_ak_approve_target",
    ),
)
Index(
    "ix_ak_approve_resource_version",
    approvals.c.resource_type,
    approvals.c.resource_id,
    approvals.c.resource_version,
)
Index(
    "uq_ak_approve_pending_update",
    approvals.c.resource_type,
    approvals.c.resource_id,
    unique=True,
    postgresql_where=text("review_type = 'update' AND review_status_type = 'pending'"),
)

projects = Table(
    "ak_projects",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("owner_id", String(128), nullable=False, index=True),
    Column("project_name", String(128), nullable=False),
    Column("status_type", String(16), nullable=False, server_default="active"),
    Column("is_default", Boolean, nullable=False, server_default=text("false")),
    Column("scan_has_baseline", Boolean, nullable=False, server_default=text("false")),
    Column("scan_error", Text),
    Column("scan_error_at", DateTime(timezone=True)),
    *audit_columns(),
    CheckConstraint("status_type IN ('active','deleting')"),
)
Index(
    "uq_ak_default_project",
    projects.c.owner_id,
    unique=True,
    postgresql_where=text("is_default"),
)
# File observations belong to projects, independently of session lifetime.
file_fingerprints = Table(
    "ak_project_file_fingerprints",
    metadata,
    Column("project_id", String(128), primary_key=True),
    Column("area", String(16), primary_key=True),
    Column("path", Text, primary_key=True),
    Column("digest", String(64), nullable=False),
    Column("size", BigInteger, nullable=False),
)
file_changes = Table(
    "ak_project_file_changes",
    metadata,
    Column("change_id", BigInteger, primary_key=True, autoincrement=True),
    Column("project_id", String(128), nullable=False),
    Column("session_id", String(128)),
    Column("reply_id", String(128)),
    Column("area", String(16), nullable=False),
    Column("path", Text, nullable=False),
    Column("change_type", String(16), nullable=False),
    Column(
        "observed_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column("size", BigInteger),
    Column("previous_size", BigInteger),
)

Index(
    "ix_ak_project_changes_page",
    file_changes.c.project_id,
    file_changes.c.change_id.desc(),
)

Index(
    "ix_ak_project_change_reply",
    file_changes.c.project_id,
    file_changes.c.session_id,
    file_changes.c.reply_id,
    file_changes.c.area,
    file_changes.c.change_id,
)

Index(
    "ix_ak_project_change_path",
    file_changes.c.project_id,
    file_changes.c.area,
    file_changes.c.path,
    file_changes.c.change_id,
)

# 上传包保存在 NAS；任务通过文件定位恢复，不依赖进程本地磁盘或数据库二进制。
skill_imports = Table(
    "ak_skill_imports",
    metadata,
    Column("job_id", String(64), primary_key=True),
    Column("uploader_id", String(128), nullable=False),
    Column("filename", String(255), nullable=False),
    Column("owner_type", String(16), nullable=False, server_default="user"),
    Column("uploader_type", String(16), nullable=False, server_default="user"),
    Column("reviewed_by", String(128)),
    Column("reviewed_at", DateTime(timezone=True)),
    Column("review_reason", Text),
    Column("category", String(80), nullable=False),
    Column("avatar", String(64), nullable=False),
    Column("archive_location", JSONB(none_as_null=True)),
    Column("status", String(16), nullable=False, server_default="queued"),
    Column("stage", String(32), nullable=False, server_default="queued"),
    Column("progress", BigInteger, nullable=False, server_default="0"),
    Column("attempts", BigInteger, nullable=False, server_default="0"),
    Column("lease_token", String(64)),
    Column("lease_until", DateTime(timezone=True)),
    Column(
        "available_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column("resource_id", String(128), nullable=False),
    Column("resource_version", String(128)),
    Column("error", Text),
    Column("result", JSONB),
    *audit_columns(),
    CheckConstraint(
        "status IN ('queued','running','pending_approval','rejected','succeeded','failed')"
    ),
    CheckConstraint("owner_type IN ('user','system')"),
    CheckConstraint("uploader_type IN ('user','admin','virtual')"),
    CheckConstraint("progress BETWEEN 0 AND 100"),
)
Index("ix_ak_skill_import_claim", skill_imports.c.status, skill_imports.c.available_at)
Index("ix_ak_skill_import_uploader", skill_imports.c.uploader_id, skill_imports.c.created_at)

reply_artifacts = Table(
    "ak_project_reply_artifacts",
    metadata,
    Column(
        "project_id",
        String(128),
        ForeignKey("ak_projects.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("session_id", String(128), primary_key=True),
    Column("reply_id", String(128), primary_key=True),
    Column("path", Text, primary_key=True),
    Column("digest", String(64), nullable=False),
)


mcp_credentials = Table(
    "ak_mcp_credentials",
    metadata,
    Column("user_id", String(128), primary_key=True),
    Column("connection_id", String(128), primary_key=True),
    Column("credential", LargeBinary, nullable=True),
    Column("oauth_state_hash", String(64), unique=True),
    Column("oauth_pending", LargeBinary),
    Column("oauth_expires_at", DateTime(timezone=True)),
)
