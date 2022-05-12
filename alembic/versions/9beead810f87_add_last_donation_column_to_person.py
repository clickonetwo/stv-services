"""Add last donation column to person

Revision ID: 9beead810f87
Revises: 4d1d81fa7617
Create Date: 2022-05-11 17:24:38.549419-07:00

"""
import sqlalchemy as sa
from alembic import op

from stv_services.data_store import model

# revision identifiers, used by Alembic.
revision = "9beead810f87"
down_revision = "4d1d81fa7617"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "person_info",
        sa.Column("last_donation", model.Timestamp, nullable=True),
    )
    op.execute(sa.update(model.person_info).values(last_donation=model.epoch))


def downgrade():
    op.drop_column("person_info", "last_donation")
