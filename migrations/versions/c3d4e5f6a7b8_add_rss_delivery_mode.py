"""add_rss_delivery_mode

Revision ID: c3d4e5f6a7b8
Revises: b1c2d3e4f5a6
Create Date: 2026-10-21 09:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, Sequence[str], None] = 'b1c2d3e4f5a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Subscriptions choose how matched articles are delivered.
    op.add_column(
        'rss_subscriptions',
        sa.Column('delivery_mode', sa.String(length=20), nullable=False, server_default='timeline')
    )
    # Summary output language directive for the LLM ('auto' mirrors the source articles).
    op.add_column(
        'rss_subscriptions',
        sa.Column('summary_language', sa.String(length=8), nullable=False, server_default='auto')
    )
    # Anchors the summary cadence: next digest is due one interval after this timestamp.
    op.add_column(
        'rss_subscriptions',
        sa.Column('last_summary_at', sa.DateTime(), nullable=True)
    )
    # Queue items snapshot the delivery mode at enqueue time (like feed_name / matches).
    op.add_column(
        'rss_delivery_queue',
        sa.Column('delivery_mode', sa.String(length=20), nullable=False, server_default='timeline')
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('rss_delivery_queue', 'delivery_mode')
    op.drop_column('rss_subscriptions', 'last_summary_at')
    op.drop_column('rss_subscriptions', 'summary_language')
    op.drop_column('rss_subscriptions', 'delivery_mode')
