"""create donation metadata table

Revision ID: 4d1d81fa7617
Revises: d4b3730f33de
Create Date: 2022-05-04 22:21:59.526501-07:00

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as psql

# revision identifiers, used by Alembic.
from stv_services.data_store.model import Timestamp

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
        sa.Column("donor_email", sa.Text, index=True, nullable=False),
        sa.Column("item_type", sa.Text, index=True, nullable=False),
        sa.Column("external_uuid", sa.Text, index=True, default=""),
        sa.Column("recurrence_data", psql.JSONB, default=""),
        sa.Column("form_name", sa.Text, index=True, default=""),
        sa.Column("form_owner_email", sa.Text, index=True, default=""),
        sa.Column("ref_codes", psql.JSONB, default={}),
    )


def downgrade():
    op.drop_table("donation_metadata")
