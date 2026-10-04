"""Durable periodic jobs and dependency completion policies."""

import sqlalchemy as sa
from alembic import op

from control_plane.database import UTCDateTime

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "dependency_policy",
            sa.String(32),
            nullable=False,
            server_default=sa.text("'all_succeeded'"),
        ),
    )
    op.create_table(
        "job_schedules",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id")),
        sa.Column("created_by", sa.String(36), sa.ForeignKey("users.id")),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("cron", sa.String(128), nullable=False),
        sa.Column("timezone", sa.String(128), nullable=False),
        sa.Column("specification", sa.JSON(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("catch_up", sa.Boolean(), nullable=False),
        sa.Column("next_run_at", UTCDateTime(), nullable=False),
        sa.Column("next_check_at", UTCDateTime(), nullable=False),
        sa.Column("last_error", sa.String(256)),
        sa.Column("created_at", UTCDateTime(), nullable=False),
    )
    for column in ("project_id", "next_run_at"):
        op.create_index(f"ix_job_schedules_{column}", "job_schedules", [column])
    op.create_table(
        "schedule_fires",
        sa.Column(
            "schedule_id", sa.String(36), sa.ForeignKey("job_schedules.id"), primary_key=True
        ),
        sa.Column("scheduled_at", UTCDateTime(), primary_key=True),
        sa.Column("job_id", sa.String(36), sa.ForeignKey("jobs.id"), nullable=False, unique=True),
        sa.Column("created_at", UTCDateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("schedule_fires")
    op.drop_table("job_schedules")
    op.drop_column("jobs", "dependency_policy")
