"""create submission table

Revision ID: 5d10012919b9
Revises: 86cd22dc2d97
Create Date: 2022-04-13 09:29:06.704971-07:00

"""
from alembic import op
import sqlalchemy as sa

# field type for timestamp with timezone
Timestamp = sa.TIMESTAMP(timezone=True)

# revision identifiers, used by Alembic.
revision = "5d10012919b9"
down_revision = "86cd22dc2d97"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "submission_info",
        sa.Column("uuid", sa.Text, primary_key=True, nullable=False),
        sa.Column("created_date", Timestamp, nullable=False),
        sa.Column("modified_date", Timestamp, index=True, nullable=False),
        sa.Column("person_id", sa.Text, index=True, nullable=False),
        sa.Column("form_id", sa.Text, index=True, nullable=False),
    )


def downgrade():
    op.drop_table("submission_info")
