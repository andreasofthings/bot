"""add_rss_delivery_queue_table

Revision ID: b1c2d3e4f5a6
Revises: 0cd01d66c010
Create Date: 2026-10-04 17:50:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b1c2d3e4f5a6'
down_revision: Union[str, Sequence[str], None] = '0cd01d66c010'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('rss_delivery_queue',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('subscriber_id', sa.String(length=255), nullable=False),
        sa.Column('feed_id', sa.Integer(), nullable=False),
        sa.Column('entry_id', sa.String(length=512), nullable=False),
        sa.Column('title', sa.String(length=512), nullable=False),
        sa.Column('link', sa.String(length=1024), nullable=False),
        sa.Column('summary', sa.Text(), nullable=True),
        sa.Column('matches', sa.JSON(), nullable=True),
        sa.Column('feed_name', sa.String(length=255), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=False, server_default='pending'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('delivered_at', sa.DateTime(), nullable=True),
        sa.Column('attempts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['feed_id'], ['rss_feeds.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_rss_delivery_queue_status'), 'rss_delivery_queue', ['status'], unique=False)
    op.create_index(op.f('ix_rss_delivery_queue_created_at'), 'rss_delivery_queue', ['created_at'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_rss_delivery_queue_created_at'), table_name='rss_delivery_queue')
    op.drop_index(op.f('ix_rss_delivery_queue_status'), table_name='rss_delivery_queue')
    op.drop_table('rss_delivery_queue')
