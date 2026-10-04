"""Durable admission and shared scheduling fence for maintenance windows."""

import sqlalchemy as sa
from alembic import op

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "admission",
        sa.Column("accepting_jobs", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.add_column(
        "admission",
        sa.Column("scheduling_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.add_column(
        "admission", sa.Column("reason", sa.String(256), nullable=False, server_default="")
    )
    op.add_column("admission", sa.Column("changed_at", sa.DateTime(timezone=True)))
    op.add_column("admission", sa.Column("changed_by", sa.String(36), sa.ForeignKey("users.id")))


def downgrade() -> None:
    for name in ("changed_by", "changed_at", "reason", "scheduling_enabled", "accepting_jobs"):
        op.drop_column("admission", name)
