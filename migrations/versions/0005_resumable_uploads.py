"""Persistent resumable transfer sessions and verified parts."""

import sqlalchemy as sa
from alembic import op

from control_plane.database import UTCDateTime

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "upload_sessions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id")),
        sa.Column(
            "version_id", sa.String(36), sa.ForeignKey("dataset_versions.id"), nullable=False
        ),
        sa.Column("created_by", sa.String(36), sa.ForeignKey("users.id")),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("total_bytes", sa.BigInteger(), nullable=False),
        sa.Column("received_bytes", sa.BigInteger(), nullable=False),
        sa.Column("expected_sha256", sa.String(64)),
        sa.Column("chunk_bytes", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("file_id", sa.String(36), sa.ForeignKey("dataset_files.id")),
        sa.Column("created_at", UTCDateTime(), nullable=False),
        sa.Column("expires_at", UTCDateTime(), nullable=False),
        sa.CheckConstraint(
            "total_bytes >= 0 AND received_bytes >= 0 AND received_bytes <= total_bytes"
        ),
    )
    for column in ("project_id", "version_id", "expires_at"):
        op.create_index(f"ix_upload_sessions_{column}", "upload_sessions", [column])
    op.create_table(
        "upload_parts",
        sa.Column(
            "upload_id", sa.String(36), sa.ForeignKey("upload_sessions.id"), primary_key=True
        ),
        sa.Column("offset", sa.BigInteger(), primary_key=True),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("upload_parts")
    op.drop_table("upload_sessions")
