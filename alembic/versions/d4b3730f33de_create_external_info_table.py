"""create external info table

Revision ID: d4b3730f33de
Revises: 5d10012919b9
Create Date: 2022-04-18 11:21:47.118303-07:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d4b3730f33de"
down_revision = "5d10012919b9"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "external_info",
        sa.Column("email", sa.Text, primary_key=True, nullable=False),
        sa.Column("shifts_2020", sa.Integer, default=0),
        sa.Column("events_2020", sa.Integer, default=0),
        sa.Column("connect_2020", sa.Text, default=""),
        sa.Column("assigns_2020", sa.Text, default=""),
        sa.Column("notes_2020", sa.Text, default=""),
        sa.Column("history_2020", sa.Text, default=""),
        sa.Column("fundraise_2020", sa.Boolean, default=False),
        sa.Column("doorknock_2020", sa.Boolean, default=False),
        sa.Column("phonebank_2020", sa.Boolean, default=False),
        sa.Column("recruit_2020", sa.Boolean, default=False),
        sa.Column("delegate_ga_2020", sa.Boolean, default=False),
        sa.Column("delegate_pa_2020", sa.Boolean, default=False),
        sa.Column("delegate_az_2020", sa.Boolean, default=False),
        sa.Column("delegate_fl_2020", sa.Boolean, default=False),
    )


def downgrade():
    op.drop_table("external_info")
