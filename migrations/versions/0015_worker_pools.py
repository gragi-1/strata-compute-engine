"""Durable bounded Docker host pool policies and provisioning intents."""

import sqlalchemy as sa
from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "worker_pools",
        sa.Column("id", sa.String(48), primary_key=True),
        sa.Column("host_id", sa.String(128), nullable=False, unique=True),
        sa.Column("configuration_hash", sa.String(64), nullable=False),
        sa.Column("image", sa.String(256), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("cpu_per_worker", sa.Float(), nullable=False),
        sa.Column("memory_per_worker_mb", sa.Integer(), nullable=False),
        sa.Column("host_cpu_budget", sa.Float(), nullable=False),
        sa.Column("host_memory_budget_mb", sa.Integer(), nullable=False),
        sa.Column("hard_limit", sa.Integer(), nullable=False),
        sa.Column("minimum", sa.Integer(), nullable=False),
        sa.Column("maximum", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("desired", sa.Integer(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error", sa.String(128)),
        sa.CheckConstraint("minimum >= 0 AND maximum >= minimum AND maximum <= hard_limit"),
    )
    op.create_table(
        "provisioned_workers",
        sa.Column("worker_id", sa.String(128), primary_key=True),
        sa.Column("pool_id", sa.String(48), sa.ForeignKey("worker_pools.id"), nullable=False),
        sa.Column("phase", sa.String(16), nullable=False),
        sa.Column("container_id", sa.String(64)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("next_check_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("idle_since", sa.DateTime(timezone=True)),
        sa.Column("removed_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.String(128)),
    )
    op.create_index("ix_provisioned_workers_pool_id", "provisioned_workers", ["pool_id"])


def downgrade() -> None:
    op.drop_table("provisioned_workers")
    op.drop_table("worker_pools")
