"""create person-related tables

Revision ID: d8a07c79e3a1
Revises: 5c5ae127fd95
Create Date: 2022-04-07 20:38:59.932855-07:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as psql


# revision identifiers, used by Alembic.
revision = "d8a07c79e3a1"
down_revision = "5c5ae127fd95"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "person_info",
        sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
        sa.Column("created_date", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "modified_date", sa.TIMESTAMP(timezone=True), index=True, nullable=False
        ),
        sa.Column("email", sa.Text, unique=True, index=True, nullable=False),
        sa.Column("email_status", sa.Text, nullable=True),
        sa.Column("phone", sa.Text, nullable=True),
        sa.Column("phone_type", sa.Text, nullable=True),
        sa.Column("phone_status", sa.Text, nullable=True),
        sa.Column("given_name", sa.Text, index=True, nullable=True),
        sa.Column("family_name", sa.Text, index=True, nullable=True),
        sa.Column("street_address", sa.Text, nullable=True),
        sa.Column("locality", sa.Text, nullable=True),
        sa.Column("region", sa.Text, nullable=True),
        sa.Column("postal_code", sa.Text, nullable=True),
        sa.Column("country", sa.Text, nullable=True),
        sa.Column("custom_fields", psql.JSONB, nullable=True),
        sa.Column("tags", psql.JSONB, nullable=True),
    )
    op.create_table(
        "contact_map",
        sa.Column("record_id", sa.Text, primary_key=True, nullable=False),
        sa.Column(
            "uuid",
            sa.Text,
            sa.ForeignKey("person_info.uuid"),
            index=True,
            nullable=True,
        ),
        sa.Column(
            "last_updated", sa.TIMESTAMP(timezone=True), index=True, nullable=False
        ),
    )
    op.create_table(
        "volunteer_map",
        sa.Column("record_id", sa.Text, primary_key=True, nullable=False),
        sa.Column(
            "uuid",
            sa.Text,
            sa.ForeignKey("person_info.uuid"),
            index=True,
            nullable=True,
        ),
        sa.Column(
            "last_updated", sa.TIMESTAMP(timezone=True), index=True, nullable=False
        ),
    )


def downgrade():
    op.drop_table("volunteer_map")
    op.drop_table("contact_map")
    op.drop_table("person_info")
