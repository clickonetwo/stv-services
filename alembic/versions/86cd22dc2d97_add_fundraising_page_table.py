"""add fundraising page table

Revision ID: 86cd22dc2d97
Revises: aa71ccf58f66
Create Date: 2022-04-09 16:03:01.694272-07:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "86cd22dc2d97"
down_revision = "aa71ccf58f66"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "fundraising_page_info",
        sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
        sa.Column("created_date", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "modified_date", sa.TIMESTAMP(timezone=True), index=True, nullable=False
        ),
        sa.Column("origin_system", sa.Text, index=True, nullable=True),
        sa.Column("title", sa.Text, index=True, nullable=False),
    )


def downgrade():
    op.drop_table("fundraising_page_info")
