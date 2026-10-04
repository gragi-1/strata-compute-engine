"""Durable data-driven workflow expansion and coordinator join nodes."""

import sqlalchemy as sa
from alembic import op

from control_plane.database import UTCDateTime

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "execution_kind", sa.String(16), nullable=False, server_default=sa.text("'container'")
        ),
    )
    op.create_table(
        "workflow_expansions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id")),
        sa.Column("created_by", sa.String(36), sa.ForeignKey("users.id")),
        sa.Column("campaign_id", sa.String(36), sa.ForeignKey("campaigns.id"), nullable=False),
        sa.Column("source_job_id", sa.String(36), sa.ForeignKey("jobs.id"), nullable=False),
        sa.Column(
            "gate_job_id", sa.String(36), sa.ForeignKey("jobs.id"), nullable=False, unique=True
        ),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("artifact_name", sa.String(128), nullable=False),
        sa.Column("template", sa.JSON(), nullable=False),
        sa.Column("max_jobs", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("generated_count", sa.Integer(), nullable=False),
        sa.Column("manifest_sha256", sa.String(64)),
        sa.Column("next_check_at", UTCDateTime(), nullable=False),
        sa.Column("last_error", sa.String(256)),
        sa.Column("created_at", UTCDateTime(), nullable=False),
    )
    for column in ("project_id", "campaign_id"):
        op.create_index(f"ix_workflow_expansions_{column}", "workflow_expansions", [column])


def downgrade() -> None:
    op.drop_table("workflow_expansions")
    op.drop_column("jobs", "execution_kind")
