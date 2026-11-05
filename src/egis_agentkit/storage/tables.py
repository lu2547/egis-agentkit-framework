"""Six additive enterprise tables, separate metadata from upstream tables."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    MetaData,
    String,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()


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
groups = Table(
    "ak_groups",
    metadata,
    Column("group_id", String(128), primary_key=True),
    Column("group_name", String(128), nullable=False),
    Column("is_enabled", Boolean, nullable=False, server_default=text("true")),
    *audit_columns(),
)
definitions = Table(
    "ak_definitions",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("definition_type", String(16), nullable=False),
    Column("owner_type", String(16), nullable=False),
    Column("owner_id", String(128), nullable=False),
    Column("agent_type", String(16)),
    Column("agent_name", String(128)),
    Column("agent_display_name", String(128)),
    Column("team_name", String(128)),
    Column("team_display_name", String(128)),
    Column("is_enabled", Boolean, nullable=False),
    Column("config_version", BigInteger, nullable=False),
    Column("payload", JSONB, nullable=False),
    *audit_columns(),
    CheckConstraint("owner_type IN ('user','system')"),
    CheckConstraint("config_version > 0"),
    CheckConstraint(
        "(definition_type='agent' AND agent_type IN ('personal','expert','orchestrator') AND agent_name IS NOT NULL AND agent_display_name IS NOT NULL AND team_name IS NULL AND team_display_name IS NULL) OR (definition_type='team' AND agent_type IS NULL AND team_name IS NOT NULL AND team_display_name IS NOT NULL AND agent_name IS NULL AND agent_display_name IS NULL)"
    ),
)
Index(
    "uq_ak_agent_name",
    definitions.c.owner_type,
    definitions.c.owner_id,
    definitions.c.agent_name,
    unique=True,
    postgresql_where=text("definition_type='agent'"),
)
Index(
    "uq_ak_team_name",
    definitions.c.owner_type,
    definitions.c.owner_id,
    definitions.c.team_name,
    unique=True,
    postgresql_where=text("definition_type='team'"),
)
Index(
    "uq_ak_personal",
    definitions.c.owner_id,
    unique=True,
    postgresql_where=text("owner_type='user' AND agent_type='personal'"),
)
assets = Table(
    "ak_resource_assets",
    metadata,
    Column("resource_type", String(16), primary_key=True),
    Column("resource_id", String(128), primary_key=True),
    Column("resource_version", String(128), primary_key=True),
    Column("owner_type", String(16), nullable=False),
    Column("owner_id", String(128), nullable=False),
    Column("download_url", Text, nullable=False),
    Column("checksum_sha256", String(64), nullable=False),
    Column("entrypoint", Text),
    Column("is_enabled", Boolean, nullable=False),
    Column("payload", JSONB, nullable=False),
    *audit_columns(),
    CheckConstraint("resource_type IN ('assembly','skill','tool','middleware','mcp')"),
)
grants = Table(
    "ak_resource_grants",
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
    Column("granted_by", String(128), nullable=False),
    Column("granted_at", DateTime(timezone=True), nullable=False),
    Column("revoked_by", String(128)),
    Column("revoked_at", DateTime(timezone=True)),
    CheckConstraint("principal_type IN ('user','group','all_users')"),
    CheckConstraint("resource_view_type IN ('system','shared')"),
    CheckConstraint("grant_source_type IN ('manual','share')"),
    CheckConstraint("principal_type != 'all_users' OR principal_id = '*'"),
    CheckConstraint("grant_source_type != 'share' OR source_review_id IS NOT NULL"),
)
Index(
    "uq_ak_active_grant",
    *[
        grants.c[c]
        for c in [
            "principal_type",
            "principal_id",
            "resource_type",
            "resource_id",
            "resource_view_type",
            "resource_owner_type",
            "resource_owner_id",
            "grant_source_type",
        ]
    ],
    unique=True,
    postgresql_where=text("revoked_at IS NULL"),
)
reviews = Table(
    "ak_share_reviews",
    metadata,
    Column("review_id", String(64), primary_key=True),
    Column("resource_type", String(16), nullable=False),
    Column("resource_id", String(128), nullable=False),
    Column("owner_id", String(128), nullable=False),
    Column("review_status_type", String(16), nullable=False),
    Column("is_current", Boolean, nullable=False),
    Column("shared_version", BigInteger, nullable=False),
    Column("share_scope_type", String(16), nullable=False),
    Column("source_versions", JSONB, nullable=False),
    Column("snapshot", JSONB, nullable=False),
    Column("submitted_by", String(128), nullable=False),
    Column("submitted_at", DateTime(timezone=True), nullable=False),
    Column("reviewed_by", String(128)),
    Column("reviewed_at", DateTime(timezone=True)),
    Column("review_reason", Text, nullable=False),
    CheckConstraint(
        "review_status_type IN ('pending','approved','rejected','cancelled')"
    ),
    CheckConstraint("NOT is_current OR review_status_type='approved'"),
    CheckConstraint("share_scope_type='all_users'"),
)
Index(
    "uq_ak_current_share",
    reviews.c.resource_type,
    reviews.c.resource_id,
    unique=True,
    postgresql_where=text("is_current"),
)
Index(
    "uq_ak_pending_share",
    reviews.c.resource_type,
    reviews.c.resource_id,
    unique=True,
    postgresql_where=text("review_status_type='pending'"),
)

projects = Table(
    "ak_projects",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("owner_id", String(128), nullable=False, index=True),
    Column("project_name", String(128), nullable=False),
    Column("status_type", String(16), nullable=False, server_default="active"),
    Column("is_default", Boolean, nullable=False, server_default=text("false")),
    *audit_columns(),
    CheckConstraint("status_type IN ('active','deleting')"),
)
Index(
    "uq_ak_default_project",
    projects.c.owner_id,
    unique=True,
    postgresql_where=text("is_default"),
)
project_sessions = Table(
    "ak_project_sessions",
    metadata,
    Column("session_id", String(128), primary_key=True),
    Column(
        "project_id",
        String(64),
        ForeignKey("ak_projects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    ),
)

# File observations belong to projects, independently of session lifetime.
file_baselines = Table(
    "ak_file_baselines",
    metadata,
    Column("project_id", String(128), primary_key=True),
    Column("user_id", String(128), nullable=False),
)
file_fingerprints = Table(
    "ak_file_fingerprints",
    metadata,
    Column("project_id", String(128), primary_key=True),
    Column("area", String(16), primary_key=True),
    Column("path", Text, primary_key=True),
    Column("digest", String(64), nullable=False),
    Column("size", BigInteger, nullable=False),
)
file_changes = Table(
    "ak_file_changes",
    metadata,
    Column("change_id", BigInteger, primary_key=True, autoincrement=True),
    Column("project_id", String(128), nullable=False, index=True),
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

file_scan_errors = Table(
    "ak_file_scan_errors",
    metadata,
    Column("project_id", String(128), primary_key=True),
    Column("message", Text, nullable=False),
    Column(
        "observed_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
)
