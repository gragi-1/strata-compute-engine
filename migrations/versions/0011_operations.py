"""Durable operational status and restart-aware maintenance scheduling."""

import sqlalchemy as sa
from alembic import op

from control_plane.database import UTCDateTime

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "maintenance_states",
        sa.Column("name", sa.String(64), primary_key=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("next_run_at", UTCDateTime(), nullable=False),
        sa.Column("started_at", UTCDateTime()),
        sa.Column("succeeded_at", UTCDateTime()),
        sa.Column("last_error", sa.String(256)),
        sa.Column("result", sa.JSON(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("maintenance_states")
