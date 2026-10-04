"""Stable deployment identity for independently scoped host cleanup."""

from uuid import uuid4

import sqlalchemy as sa
from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("admission", sa.Column("cluster_id", sa.String(36)))
    op.execute(sa.text("UPDATE admission SET cluster_id=:cluster").bindparams(cluster=str(uuid4())))
    with op.batch_alter_table("admission") as batch:
        batch.alter_column("cluster_id", existing_type=sa.String(36), nullable=False)


def downgrade() -> None:
    op.drop_column("admission", "cluster_id")
