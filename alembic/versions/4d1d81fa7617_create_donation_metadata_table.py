"""create donation metadata table

Revision ID: 4d1d81fa7617
Revises: d4b3730f33de
Create Date: 2022-05-04 22:21:59.526501-07:00

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
from stv_services.data_store.model import Timestamp, epoch

revision = "4d1d81fa7617"
down_revision = "d4b3730f33de"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "donation_metadata",
        sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
        sa.Column("created_date", Timestamp, index=True, nullable=False),
        sa.Column("modified_date", Timestamp, index=True, nullable=False),
        sa.Column("published_date", Timestamp, index=True, default=epoch),
        sa.Column("item_type", sa.Text, index=True, nullable=False),
        sa.Column("donor_email", sa.Text, index=True, nullable=False),
        sa.Column("order_id", sa.Text, index=True, default=""),
        sa.Column("order_date", Timestamp, index=True, default=epoch),
        sa.Column("line_item_ids", sa.Text, index=True, default=""),
        sa.Column("form_name", sa.Text, index=True, default=""),
        sa.Column("form_owner_email", sa.Text, index=True, default=""),
        sa.Column("ref_code", sa.Text, index=True, default=""),
        sa.Column("attribution_id", sa.Text, index=True, default=""),
        sa.Index("ix_donation_metadata_uuid_hash", "uuid", postgresql_using="hash"),
    )


def downgrade():
    op.drop_table("donation_metadata")
