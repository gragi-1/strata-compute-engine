"""Bound container concurrency independently of requested CPU fractions.

Revision ID: 0002
Revises: 7b38f4d2f5cb
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "7b38f4d2f5cb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workers", sa.Column("running_jobs", sa.Integer(), nullable=False, server_default="0")
    )
    op.execute("""UPDATE workers SET running_jobs = (
        SELECT COUNT(*) FROM job_attempts WHERE worker_id = workers.id
        AND status IN ('SCHEDULED', 'RUNNING', 'CANCEL_REQUESTED')
    )""")


def downgrade() -> None:
    op.drop_column("workers", "running_jobs")
