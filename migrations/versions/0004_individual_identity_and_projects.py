"""Individual identity, project ownership, quotas and audit.

Revision ID: 0004
Revises: bb26b07cb641
"""

import sqlalchemy as sa
from alembic import op

from control_plane.database import UTCDateTime

revision = "0004"
down_revision = "bb26b07cb641"
branch_labels = None
depends_on = None

OWNED_TABLES = ("jobs", "campaigns", "datasets", "dataset_versions", "dataset_files", "artifacts")
IDENTITY_TABLES = (
    "users",
    "projects",
    "access_tokens",
    "login_throttles",
    "project_memberships",
    "project_storage_locks",
    "audit_events",
)


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("username", sa.String(128), nullable=False, unique=True),
        sa.Column("password_hash", sa.String(512)),
        sa.Column("display_name", sa.String(128), nullable=False),
        sa.Column("is_admin", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("oidc_subject", sa.String(512), unique=True),
        sa.Column("created_at", UTCDateTime(), nullable=False),
    )
    op.create_table(
        "projects",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("queue_limit", sa.Integer(), nullable=False),
        sa.Column("cpu_limit", sa.Float(), nullable=False),
        sa.Column("memory_limit_mb", sa.Integer(), nullable=False),
        sa.Column("storage_limit_bytes", sa.BigInteger(), nullable=False),
        sa.Column("dispatch_count", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", UTCDateTime(), nullable=False),
        sa.CheckConstraint("queue_limit > 0 AND cpu_limit > 0 AND memory_limit_mb > 0"),
        sa.CheckConstraint("storage_limit_bytes > 0"),
    )
    op.create_table(
        "access_tokens",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("user_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("token_hash", sa.String(64), unique=True, nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id")),
        sa.Column("role", sa.String(16)),
        sa.Column("created_at", UTCDateTime(), nullable=False),
        sa.Column("expires_at", UTCDateTime(), nullable=False),
        sa.Column("revoked_at", UTCDateTime()),
    )
    for column in ("user_id", "project_id", "expires_at"):
        op.create_index(f"ix_access_tokens_{column}", "access_tokens", [column])
    op.create_table(
        "login_throttles",
        sa.Column("key", sa.String(64), primary_key=True),
        sa.Column("failures", sa.Integer(), nullable=False),
        sa.Column("window_start", UTCDateTime(), nullable=False),
    )
    op.create_table(
        "project_memberships",
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), primary_key=True),
        sa.Column("user_id", sa.String(36), sa.ForeignKey("users.id"), primary_key=True),
        sa.Column("role", sa.String(16), nullable=False),
        sa.CheckConstraint("role IN ('viewer', 'operator', 'admin')"),
    )
    op.create_table(
        "project_storage_locks",
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id"), primary_key=True),
    )
    op.create_table(
        "audit_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("actor_id", sa.String(36), sa.ForeignKey("users.id")),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id")),
        sa.Column("action", sa.String(128), nullable=False),
        sa.Column("resource_id", sa.String(128)),
        sa.Column("created_at", UTCDateTime(), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
    )
    for column in ("actor_id", "project_id", "created_at"):
        op.create_index(f"ix_audit_events_{column}", "audit_events", [column])
    for name in OWNED_TABLES:
        with op.batch_alter_table(name) as table:
            table.add_column(sa.Column("project_id", sa.String(36), nullable=True))
            table.create_foreign_key(f"{name}_project_id_fkey", "projects", ["project_id"], ["id"])
            table.create_index(f"ix_{name}_project_id", ["project_id"])
            if name in {"jobs", "campaigns", "datasets"}:
                table.add_column(sa.Column("created_by", sa.String(36), nullable=True))
                table.create_foreign_key(f"{name}_created_by_fkey", "users", ["created_by"], ["id"])


def downgrade() -> None:
    for name in reversed(OWNED_TABLES):
        with op.batch_alter_table(name) as table:
            if name in {"jobs", "campaigns", "datasets"}:
                table.drop_constraint(f"{name}_created_by_fkey", type_="foreignkey")
                table.drop_column("created_by")
            table.drop_index(f"ix_{name}_project_id")
            table.drop_constraint(f"{name}_project_id_fkey", type_="foreignkey")
            table.drop_column("project_id")
    for name in reversed(IDENTITY_TABLES):
        op.drop_table(name)
