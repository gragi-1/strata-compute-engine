"""Atomic event outbox and leased webhook delivery."""

import sqlalchemy as sa
from alembic import op

from control_plane.database import UTCDateTime

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("job_events", sa.Column("event_uid", sa.String(36)))
    with op.batch_alter_table("job_events") as batch:
        batch.create_unique_constraint("uq_job_events_event_uid", ["event_uid"])
    op.create_table(
        "webhooks",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id")),
        sa.Column("created_by", sa.String(36), sa.ForeignKey("users.id")),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("target", sa.String(64), nullable=False),
        sa.Column("events", sa.JSON(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", UTCDateTime(), nullable=False),
    )
    op.create_index("ix_webhooks_project_id", "webhooks", ["project_id"])
    op.create_table(
        "webhook_deliveries",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("webhook_id", sa.String(36), sa.ForeignKey("webhooks.id"), nullable=False),
        sa.Column(
            "event_uid", sa.String(36), sa.ForeignKey("job_events.event_uid"), nullable=False
        ),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("attempt_limit", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", UTCDateTime(), nullable=False),
        sa.Column("lease_token", sa.String(36)),
        sa.Column("lease_until", UTCDateTime()),
        sa.Column("last_status", sa.Integer()),
        sa.Column("last_error", sa.String(128)),
        sa.Column("completed_at", UTCDateTime()),
        sa.Column("created_at", UTCDateTime(), nullable=False),
    )
    op.create_index("ix_webhook_deliveries_webhook_id", "webhook_deliveries", ["webhook_id"])
    op.create_index("ix_webhook_delivery_due", "webhook_deliveries", ["status", "next_attempt_at"])
    op.create_index(
        "uq_webhook_delivery_event", "webhook_deliveries", ["webhook_id", "event_uid"], unique=True
    )


def downgrade() -> None:
    op.drop_table("webhook_deliveries")
    op.drop_table("webhooks")
    with op.batch_alter_table("job_events") as batch:
        batch.drop_constraint("uq_job_events_event_uid", type_="unique")
        batch.drop_column("event_uid")
