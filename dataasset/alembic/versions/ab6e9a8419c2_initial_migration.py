"""Initial migration

Revision ID: ab6e9a8419c2
Revises: 
Create Date: 2022-10-24 19:03:02.256850

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ab6e9a8419c2"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "data_asset",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=256), nullable=True),
        sa.Column("as_of", sa.DateTime(), nullable=True),
        sa.Column("last_poke", sa.DateTime(), nullable=True),
        sa.Column("data_as_of", sa.DateTime(), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("data_asset")
    # ### end Alembic commands ###
