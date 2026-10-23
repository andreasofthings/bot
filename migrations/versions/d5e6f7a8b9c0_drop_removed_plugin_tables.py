"""drop_removed_plugin_tables

Drops the tables of the removed plugins (stock, hubspot, onboarding/licensing).
The RSS plugin and its tables are unaffected.

Revision ID: d5e6f7a8b9c0
Revises: c3d4e5f6a7b8
Create Date: 2026-10-23 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd5e6f7a8b9c0'
down_revision: Union[str, Sequence[str], None] = 'c3d4e5f6a7b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Children first, then the referenced users table.
    op.drop_table('license_codes')
    op.drop_index(op.f('ix_stock_subscriptions_ticker'), table_name='stock_subscriptions')
    op.drop_table('stock_subscriptions')
    op.drop_index(op.f('ix_hubspot_connections_portal_id'), table_name='hubspot_connections')
    op.drop_table('hubspot_connections')
    op.drop_table('users')


def downgrade() -> None:
    """Downgrade schema."""
    op.create_table('users',
    sa.Column('user_id', sa.String(length=255), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=True),
    sa.Column('company', sa.String(length=255), nullable=True),
    sa.Column('email', sa.String(length=255), nullable=True),
    sa.Column('consent_given', sa.Boolean(), nullable=False),
    sa.Column('tier', sa.String(length=50), nullable=False),
    sa.Column('onboarding_state', sa.String(length=50), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('user_id')
    )
    op.create_table('hubspot_connections',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('portal_id', sa.String(length=100), nullable=False),
    sa.Column('access_token', sa.String(length=512), nullable=False),
    sa.Column('account_name', sa.String(length=255), nullable=True),
    sa.Column('token_type', sa.String(length=50), nullable=False),
    sa.Column('scopes', sa.Text(), nullable=True),
    sa.Column('data_hosting_location', sa.String(length=50), nullable=True),
    sa.Column('timezone', sa.String(length=100), nullable=True),
    sa.Column('currency', sa.String(length=20), nullable=True),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('connected_by', sa.String(length=255), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_hubspot_connections_portal_id'), 'hubspot_connections', ['portal_id'], unique=True)
    op.create_table('stock_subscriptions',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('user_id', sa.String(length=255), nullable=False),
    sa.Column('room_id', sa.String(length=255), nullable=False),
    sa.Column('ticker', sa.String(length=50), nullable=False),
    sa.Column('indicator', sa.String(length=50), nullable=False),
    sa.Column('parameter_1', sa.Integer(), nullable=True),
    sa.Column('parameter_2', sa.Integer(), nullable=True),
    sa.Column('condition_type', sa.String(length=50), nullable=False),
    sa.Column('threshold', sa.Float(), nullable=False),
    sa.Column('last_triggered', sa.DateTime(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.user_id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_stock_subscriptions_ticker'), 'stock_subscriptions', ['ticker'], unique=False)
    op.create_table('license_codes',
    sa.Column('code', sa.String(length=255), nullable=False),
    sa.Column('is_used', sa.Boolean(), nullable=False),
    sa.Column('used_by', sa.String(length=255), nullable=True),
    sa.Column('used_at', sa.DateTime(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['used_by'], ['users.user_id'], ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('code')
    )
