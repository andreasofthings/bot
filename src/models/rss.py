from datetime import datetime
from typing import List, Optional
from sqlalchemy import String, DateTime, ForeignKey, func, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.models.base import Base

class RSSFeed(Base):
    """Represents an external RSS/Atom feed source that is monitored."""
    __tablename__ = "rss_feeds"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String(512), unique=True, index=True, nullable=False)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_polled: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    subscriptions: Mapped[List["RSSSubscription"]] = relationship(
        "RSSSubscription", back_populates="feed", cascade="all, delete-orphan"
    )
    history: Mapped[List["RSSHistory"]] = relationship(
        "RSSHistory", back_populates="feed", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<RSSFeed id={self.id} url={self.url} name={self.name}>"


class RSSSubscription(Base):
    """Links a feed to a target room/user with granular entity filters."""
    __tablename__ = "rss_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    subscriber_id: Mapped[str] = mapped_column(String(255), nullable=False)  # Matrix Room ID or User ID
    subscriber_type: Mapped[str] = mapped_column(String(50), nullable=False)  # 'room' or 'user'
    feed_id: Mapped[int] = mapped_column(ForeignKey("rss_feeds.id", ondelete="CASCADE"), nullable=False)
    
    # Custom filters saved as JSON arrays
    keywords: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    companies: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    geographies: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    representatives: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    feed: Mapped["RSSFeed"] = relationship("RSSFeed", back_populates="subscriptions")

    def __repr__(self) -> str:
        return f"<RSSSubscription id={self.id} subscriber={self.subscriber_id} feed={self.feed_id}>"


class RSSHistory(Base):
    """Tracks unique identifiers of processed articles to prevent double-posting."""
    __tablename__ = "rss_history"

    entry_id: Mapped[str] = mapped_column(String(512), primary_key=True)  # Unique GUID or URL
    feed_id: Mapped[int] = mapped_column(ForeignKey("rss_feeds.id", ondelete="CASCADE"), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    # Relationships
    feed: Mapped["RSSFeed"] = relationship("RSSFeed", back_populates="history")

    def __repr__(self) -> str:
        return f"<RSSHistory entry_id={self.entry_id} feed={self.feed_id}>"
