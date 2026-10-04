"""Atomic compute groups and bounded interactive sessions."""

import sqlalchemy as sa
from alembic import op

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def identity_columns():
    return [
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id")),
        sa.Column("created_by", sa.String(36), sa.ForeignKey("users.id")),
        sa.Column("idempotency_key", sa.String(256), unique=True),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    ]


def upgrade() -> None:
    op.create_table(
        "compute_groups",
        *identity_columns(),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("nodes", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("specification", sa.JSON(), nullable=False),
        sa.Column("next_sequence", sa.Integer(), nullable=False),
    )
    op.create_index("ix_compute_groups_project_id", "compute_groups", ["project_id"])
    op.create_table(
        "group_members",
        sa.Column("group_id", sa.String(36), sa.ForeignKey("compute_groups.id"), primary_key=True),
        sa.Column("rank", sa.Integer(), primary_key=True),
        sa.Column("job_id", sa.String(36), sa.ForeignKey("jobs.id"), nullable=False),
    )
    op.create_index("ix_group_member_job", "group_members", ["job_id"], unique=True)
    op.create_table(
        "collective_rounds",
        sa.Column("group_id", sa.String(36), sa.ForeignKey("compute_groups.id"), primary_key=True),
        sa.Column("sequence", sa.Integer(), primary_key=True),
        sa.Column("operation", sa.String(16), nullable=False),
        sa.Column("contributions", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=True),
    )
    op.create_table(
        "interactive_sessions",
        *identity_columns(),
        sa.Column("job_id", sa.String(36), sa.ForeignKey("jobs.id"), nullable=False, unique=True),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("specification", sa.JSON(), nullable=False),
        sa.Column("idle_seconds", sa.Integer(), nullable=False),
        sa.Column("next_sequence", sa.Integer(), nullable=False),
        sa.Column("last_activity_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_interactive_sessions_project_id", "interactive_sessions", ["project_id"])
    op.create_table(
        "session_cells",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id")),
        sa.Column(
            "session_id", sa.String(36), sa.ForeignKey("interactive_sessions.id"), nullable=False
        ),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("idempotency_key", sa.String(256), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
    )
    op.create_index("ix_session_cells_project_id", "session_cells", ["project_id"])
    op.create_index(
        "ix_session_cell_sequence", "session_cells", ["session_id", "sequence"], unique=True
    )
    op.create_index(
        "ix_session_cell_key", "session_cells", ["session_id", "idempotency_key"], unique=True
    )


def downgrade() -> None:
    for table in (
        "session_cells",
        "interactive_sessions",
        "collective_rounds",
        "group_members",
        "compute_groups",
    ):
        op.drop_table(table)
