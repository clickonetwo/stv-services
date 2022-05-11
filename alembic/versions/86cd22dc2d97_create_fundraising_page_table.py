"""create fundraising page table

Revision ID: 86cd22dc2d97
Revises: aa71ccf58f66
Create Date: 2022-04-09 16:03:01.694272-07:00

"""
import sqlalchemy as sa
from alembic import op

from stv_services.data_store.model import epoch

# field type for timestamp with timezone
Timestamp = sa.TIMESTAMP(timezone=True)

# revision identifiers, used by Alembic.
revision = "86cd22dc2d97"
down_revision = "aa71ccf58f66"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "fundraising_page_info",
        sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
        sa.Column("created_date", Timestamp, nullable=False),
        sa.Column("modified_date", Timestamp, index=True, nullable=False),
        sa.Column("updated_date", Timestamp, index=True, default=epoch),
        sa.Column("origin_system", sa.Text, index=True, default=""),
        sa.Column("title", sa.Text, index=True, nullable=False),
        sa.Column("attribution_id", sa.Text, index=True, default=""),
        sa.Index("ix_fundraising_page_info_uuid_hash", "uuid", postgresql_using="hash"),
    )


def downgrade():
    op.drop_table("fundraising_page_info")
