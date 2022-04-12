"""create Airtable person-map table

Revision ID: ac6f23ee1ad3
Revises: 86cd22dc2d97
Create Date: 2022-04-10 12:54:18.506220-07:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "ac6f23ee1ad3"
down_revision = "86cd22dc2d97"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "person_map",
        sa.Column("record_id", sa.Text, primary_key=True, nullable=False),
        sa.Column(
            "uuid",
            sa.Text,
            sa.ForeignKey("person_info.uuid"),
            index=True,
            nullable=True,
        ),
        sa.Column("is_contact", sa.Boolean, nullable=False),
        sa.Column("is_volunteer", sa.Boolean, nullable=False),
        sa.Column("is_fundraiser", sa.Boolean, nullable=False),
        sa.Column("contact_record_id", sa.Text, index=True, nullable=True),
        sa.Column("volunteer_record_id", sa.Text, index=True, nullable=True),
        sa.Column("fundraiser_record_id", sa.Text, index=True, nullable=True),
        sa.Column(
            "contact_last_updated",
            sa.TIMESTAMP(timezone=True),
            index=True,
            nullable=False,
        ),
        sa.Column(
            "volunteer_last_updated",
            sa.TIMESTAMP(timezone=True),
            index=True,
            nullable=False,
        ),
        sa.Column(
            "fundraising_last_updated",
            sa.TIMESTAMP(timezone=True),
            index=True,
            nullable=False,
        ),
    )


def downgrade():
    op.drop_table("person_map")
