"""create donation table

Revision ID: aa71ccf58f66
Revises: d8a07c79e3a1
Create Date: 2022-04-08 22:32:49.803094-07:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql


# revision identifiers, used by Alembic.
revision = "aa71ccf58f66"
down_revision = "d8a07c79e3a1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "donation_info",
        sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
        sa.Column(
            "created_date", sa.TIMESTAMP(timezone=True), index=True, nullable=False
        ),
        sa.Column(
            "modified_date", sa.TIMESTAMP(timezone=True), index=True, nullable=False
        ),
        sa.Column("amount", sa.Text, nullable=False),
        sa.Column("recurrence_data", psql.JSONB, nullable=False),
        sa.Column("donor_id", sa.Text, index=True, nullable=False),
        sa.Column("fundraising_page_id", sa.Text, index=True, nullable=False),
    )


def downgrade():
    op.drop_table("donation_info")
