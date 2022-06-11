"""Create attendance and attendee tables

Revision ID: 8dd71067cef3
Revises: ba114bfc4f08
Create Date: 2022-06-06 18:32:32.638455-07:00

"""
from alembic import op
import sqlalchemy as sa


from stv_services.data_store.model import epoch

# field type for timestamp with timezone
Timestamp = sa.TIMESTAMP(timezone=True)

# revision identifiers, used by Alembic.
revision = "8dd71067cef3"
down_revision = "ba114bfc4f08"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "attendance_info",
        sa.Column("uuid", sa.Integer, primary_key=True, nullable=False),
        sa.Column("created_date", Timestamp, index=True, nullable=False),
        sa.Column("modified_date", Timestamp, index=True, nullable=False),
        sa.Column("updated_date", Timestamp, index=True, default=epoch),
        sa.Column("event_id", sa.Integer, index=True, nullable=False),
        sa.Column("event_type", sa.Text, index=True, nullable=False),
        sa.Column("timeslot_id", sa.Integer, index=True, nullable=False),
        sa.Column("email", sa.Text, index=True, nullable=False),
        sa.Column("person_id", sa.Text, index=True, default=""),
        sa.Column("status", sa.Text, nullable=False),
    )


def downgrade():
    op.drop_table("attendance_info")
