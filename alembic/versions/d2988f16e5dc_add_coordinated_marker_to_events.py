"""add coordinated marker to events

Revision ID: d2988f16e5dc
Revises: 8dd71067cef3
Create Date: 2022-08-09 20:49:07.094570-07:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d2988f16e5dc"
down_revision = "8dd71067cef3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "event_info", sa.Column("is_coordinated", sa.Boolean(), nullable=True)
    )
    op.execute("update event_info set is_coordinated = false;")


def downgrade():
    op.drop_column("event_info", "is_coordinated")
