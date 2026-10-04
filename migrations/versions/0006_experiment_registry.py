"""Experiment registry and immutable attempt provenance."""

import sqlalchemy as sa
from alembic import op

from control_plane.database import UTCDateTime

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("expected_image_digest", sa.String(71)))
    op.add_column(
        "job_attempts",
        sa.Column("provenance", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
    )
    op.create_table(
        "experiments",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id")),
        sa.Column("created_by", sa.String(36), sa.ForeignKey("users.id")),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("created_at", UTCDateTime(), nullable=False),
    )
    op.create_index("ix_experiments_project_id", "experiments", ["project_id"])
    op.create_table(
        "experiment_runs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id")),
        sa.Column("experiment_id", sa.String(36), sa.ForeignKey("experiments.id"), nullable=False),
        sa.Column("job_id", sa.String(36), sa.ForeignKey("jobs.id"), unique=True, nullable=False),
        sa.Column("created_by", sa.String(36), sa.ForeignKey("users.id")),
        sa.Column("source_revision", sa.String(64)),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("specification", sa.JSON(), nullable=False),
        sa.Column("metrics", sa.JSON(), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("idempotency_key", sa.String(256), unique=True),
        sa.Column("replay_of", sa.String(36), sa.ForeignKey("experiment_runs.id")),
        sa.Column("created_at", UTCDateTime(), nullable=False),
    )
    for column in ("project_id", "experiment_id"):
        op.create_index(f"ix_experiment_runs_{column}", "experiment_runs", [column])


def downgrade() -> None:
    op.drop_table("experiment_runs")
    op.drop_table("experiments")
    op.drop_column("job_attempts", "provenance")
    op.drop_column("jobs", "expected_image_digest")
