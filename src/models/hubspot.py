from datetime import datetime
from sqlalchemy import String, Boolean, DateTime, func, Text
from sqlalchemy.orm import Mapped, mapped_column
from src.models.base import Base


class HubSpotConnection(Base):
    """Represents a HubSpot CRM connection, credentials, and verification details."""
    __tablename__ = "hubspot_connections"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    portal_id: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    access_token: Mapped[str] = mapped_column(String(512), nullable=False)
    account_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    token_type: Mapped[str] = mapped_column(String(50), default="private_app")
    scopes: Mapped[str | None] = mapped_column(Text, nullable=True)
    data_hosting_location: Mapped[str | None] = mapped_column(String(50), nullable=True)
    timezone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(20), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    connected_by: Mapped[str | None] = mapped_column(String(255), nullable=True)  # Matrix user ID who configured it
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<HubSpotConnection portal_id={self.portal_id} is_active={self.is_active}>"
