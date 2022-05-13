"""Add supporter page flag to person

Revision ID: f6ac7ccc4302
Revises: 9beead810f87
Create Date: 2022-05-12 17:20:52.883695-07:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
from stv_services.data_store import model

revision = "f6ac7ccc4302"
down_revision = "9beead810f87"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "person_info", sa.Column("funder_has_page", sa.Boolean(), nullable=True)
    )
    op.execute(sa.update(model.person_info).values(funder_has_page=False))


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_person_info_funder_has_page"), table_name="person_info")
    op.drop_column("person_info", "funder_has_page")
    # ### end Alembic commands ###
