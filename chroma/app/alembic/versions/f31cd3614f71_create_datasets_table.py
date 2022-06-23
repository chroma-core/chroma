"""create datasets table

Revision ID: f31cd3614f71
Revises: 
Create Date: 2022-06-19 14:15:44.729718

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f31cd3614f71'
down_revision = None
branch_labels = None
depends_on = None

# bumping so this file shows up in commit
def upgrade() -> None:
    op.create_table(
        'datasets',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String(), nullable=False)
    )


def downgrade() -> None:
    op.drop_table('datasets')
