"""Persist execution/helper resource charges for exact reservation release."""

import sqlalchemy as sa
from alembic import op

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "job_attempts", sa.Column("cpu_reserved", sa.Float(), nullable=False, server_default="0")
    )
    op.add_column(
        "job_attempts",
        sa.Column("memory_reserved_mb", sa.Integer(), nullable=False, server_default="0"),
    )
    # Earlier agents charged only the submitted workload, including currently live attempts.
    op.execute(
        """UPDATE job_attempts SET
        cpu_reserved = (SELECT cpu_required FROM jobs WHERE jobs.id = job_attempts.job_id),
        memory_reserved_mb =
            (SELECT memory_required_mb FROM jobs WHERE jobs.id = job_attempts.job_id)"""
    )


def downgrade() -> None:
    op.drop_column("job_attempts", "memory_reserved_mb")
    op.drop_column("job_attempts", "cpu_reserved")
