from datetime import datetime
from sqlalchemy import String, DateTime, ForeignKey, func, Float, Integer
from sqlalchemy.orm import Mapped, mapped_column
from src.models.base import Base

class StockSubscription(Base):
    """Tracks threshold alerts configured by users for technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands)."""
    __tablename__ = "stock_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(255), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    room_id: Mapped[str] = mapped_column(String(255), nullable=False)  # Matrix room ID where the alert notification is posted
    ticker: Mapped[str] = mapped_column(String(50), index=True, nullable=False)  # e.g., TSLA, AAPL
    indicator: Mapped[str] = mapped_column(String(50), nullable=False)  # 'SMA', 'EMA', 'RSI', 'MACD', 'BOLLINGER'
    
    # Configuration parameters for indicators (e.g., periods)
    parameter_1: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Primary period (e.g., 14 for RSI, 50 for SMA)
    parameter_2: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Secondary period (e.g., 200 for SMA crossover)
    
    # Alert conditions: ABOVE, BELOW, CROSS_ABOVE, CROSS_BELOW
    condition_type: Mapped[str] = mapped_column(String(50), nullable=False)
    threshold: Mapped[float] = mapped_column(Float, nullable=False)  # Trigger value
    
    last_triggered: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)  # Used for cooldown control
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    def __repr__(self) -> str:
        return f"<StockSubscription id={self.id} user={self.user_id} ticker={self.ticker} indicator={self.indicator}>"
