from datetime import datetime
from sqlalchemy import String, Boolean, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from src.models.base import Base

class Room(Base):
    """Tracks rooms/channels the bot has joined, distinguishing DMs from public channels."""
    __tablename__ = "rooms"

    room_id: Mapped[str] = mapped_column(String(255), primary_key=True)  # Matrix Room ID (e.g., !roomid:matrix.org)
    room_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_dm: Mapped[bool] = mapped_column(Boolean, default=False)  # True for 1-on-1 conversations, False for channels
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    def __repr__(self) -> str:
        return f"<Room room_id={self.room_id} name={self.room_name} is_dm={self.is_dm}>"
