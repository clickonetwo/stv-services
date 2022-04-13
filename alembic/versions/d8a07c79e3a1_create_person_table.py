"""create person table

Revision ID: d8a07c79e3a1
Revises: 5c5ae127fd95
Create Date: 2022-04-07 20:38:59.932855-07:00

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as psql

# field type for timestamp with timezone
Timestamp = sa.TIMESTAMP(timezone=True)

# revision identifiers, used by Alembic.
revision = "d8a07c79e3a1"
down_revision = "5c5ae127fd95"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "person_info",
        sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
        sa.Column("created_date", Timestamp, index=True, nullable=False),
        sa.Column("modified_date", Timestamp, index=True, nullable=False),
        sa.Column("email", sa.Text, unique=True, index=True, nullable=False),
        sa.Column("email_status", sa.Text, nullable=True),
        sa.Column("phone", sa.Text, index=True, nullable=True),
        sa.Column("phone_type", sa.Text, nullable=True),
        sa.Column("phone_status", sa.Text, nullable=True),
        sa.Column("given_name", sa.Text, index=True, nullable=True),
        sa.Column("family_name", sa.Text, index=True, nullable=True),
        sa.Column("street_address", sa.Text, nullable=True),
        sa.Column("locality", sa.Text, nullable=True),
        sa.Column("region", sa.Text, index=True, nullable=True),
        sa.Column("postal_code", sa.Text, index=True, nullable=True),
        sa.Column("country", sa.Text, nullable=True),
        sa.Column("custom_fields", psql.JSONB, nullable=True),
        sa.Column("total_2020", sa.Integer, index=True, default=-1),
        sa.Column("summary_2020", sa.Text, default=""),
        sa.Column("total_2021", sa.Integer, index=True, default=-1),
        sa.Column("summary_2021", sa.Text, default=""),
        sa.Column("is_contact", sa.Boolean, index=True, default=False),
        sa.Column("contact_record_id", sa.Text, index=True, nullable=True),
        sa.Column("contact_last_updated", Timestamp, index=True, nullable=True),
        sa.Column("is_volunteer", sa.Boolean, index=True, default=False),
        sa.Column("volunteer_record_id", sa.Text, index=True, nullable=True),
        sa.Column("volunteer_last_updated", Timestamp, index=True, nullable=True),
        sa.Column("is_funder", sa.Boolean, index=True, default=False),
        sa.Column("funder_record_id", sa.Text, index=True, nullable=True),
        sa.Column("funder_last_updated", Timestamp, index=True, nullable=True),
    )


def downgrade():
    op.drop_table("person_info")
