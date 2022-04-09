"""create config table

Revision ID: 5c5ae127fd95
Revises: 
Create Date: 2022-04-06 00:04:57.266831-07:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql


# revision identifiers, used by Alembic.
revision = "5c5ae127fd95"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "configuration",
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("value", psql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("key"),
    )


def downgrade():
    op.drop_table("configuration")
