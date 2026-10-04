"""Explicit federated identity mappings and browser-bound login transactions."""

import sqlalchemy as sa
from alembic import op

from control_plane.database import UTCDateTime

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "oidc_identities",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("issuer", sa.String(512), nullable=False),
        sa.Column("subject", sa.String(255), nullable=False),
        sa.Column("user_id", sa.String(36), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", UTCDateTime(), nullable=False),
    )
    op.create_index("uq_oidc_subject", "oidc_identities", ["issuer", "subject"], unique=True)
    op.create_index("ix_oidc_identities_user_id", "oidc_identities", ["user_id"])
    op.create_table(
        "oidc_states",
        sa.Column("state_hash", sa.String(64), primary_key=True),
        sa.Column("browser_hash", sa.String(64), nullable=False),
        sa.Column("address_hash", sa.String(64), nullable=False),
        sa.Column("nonce_hash", sa.String(64), nullable=False),
        sa.Column("encrypted_verifier", sa.String(256), nullable=False),
        sa.Column("created_at", UTCDateTime(), nullable=False),
        sa.Column("expires_at", UTCDateTime(), nullable=False),
        sa.Column("consumed_at", UTCDateTime()),
    )
    op.create_index("ix_oidc_states_address_hash", "oidc_states", ["address_hash"])
    op.create_index("ix_oidc_states_expires_at", "oidc_states", ["expires_at"])
    op.create_table(
        "oidc_handoffs",
        sa.Column("token_hash", sa.String(64), primary_key=True),
        sa.Column(
            "identity_id", sa.String(36), sa.ForeignKey("oidc_identities.id"), nullable=False
        ),
        sa.Column("browser_hash", sa.String(64), nullable=False),
        sa.Column("created_at", UTCDateTime(), nullable=False),
        sa.Column("expires_at", UTCDateTime(), nullable=False),
        sa.Column("consumed_at", UTCDateTime()),
    )
    op.create_index("ix_oidc_handoffs_expires_at", "oidc_handoffs", ["expires_at"])


def downgrade() -> None:
    op.drop_table("oidc_handoffs")
    op.drop_table("oidc_states")
    op.drop_table("oidc_identities")
