"""Exclusive NVIDIA device inventory, assignment history and project GPU budgets."""

import sqlalchemy as sa
from alembic import op

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs", sa.Column("gpu_required", sa.Integer(), nullable=False, server_default="0")
    )
    op.add_column(
        "jobs", sa.Column("gpu_memory_mb", sa.Integer(), nullable=False, server_default="0")
    )
    op.add_column(
        "job_attempts", sa.Column("gpu_ids", sa.JSON(), nullable=False, server_default="[]")
    )
    op.add_column(
        "projects", sa.Column("gpu_limit", sa.Integer(), nullable=False, server_default="8")
    )
    op.create_table(
        "gpu_devices",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("worker_id", sa.String(128), sa.ForeignKey("workers.id"), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("memory_mb", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("allocated_to", sa.String(36), sa.ForeignKey("job_attempts.id")),
    )
    op.create_index("ix_gpu_devices_worker_id", "gpu_devices", ["worker_id"])
    op.create_index("ix_gpu_devices_allocated_to", "gpu_devices", ["allocated_to"])


def downgrade() -> None:
    op.drop_table("gpu_devices")
    op.drop_column("projects", "gpu_limit")
    op.drop_column("job_attempts", "gpu_ids")
    op.drop_column("jobs", "gpu_memory_mb")
    op.drop_column("jobs", "gpu_required")
