from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession, AsyncEngine
from src.config import load_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Global singletons
_engine: Optional[AsyncEngine] = None
_session_maker: Optional[async_sessionmaker] = None


def get_engine() -> AsyncEngine:
    """Retrieves or initializes the global AsyncEngine singleton."""
    global _engine
    if _engine is None:
        settings = load_settings()
        db_url = settings.database_url
        
        # Hide credentials in logs (display only host/path)
        safe_url = db_url.split("@")[-1] if "@" in db_url else db_url
        logger.info("Initializing database connection pool", db_url=safe_url)
        
        # Configure dialect-specific connection arguments
        connect_args = {}
        if db_url.startswith("sqlite"):
            # SQLite requires check_same_thread=False to work across multiple asyncio task loops
            connect_args["check_same_thread"] = False
            
        _engine = create_async_engine(
            db_url,
            connect_args=connect_args,
            pool_recycle=3600,
            pool_pre_ping=True,  # Automatically verifies connection health on checkout
            echo=False
        )
    return _engine


def get_sessionmaker() -> async_sessionmaker:
    """Retrieves or initializes the global async session factory."""
    global _session_maker
    if _session_maker is None:
        engine = get_engine()
        _session_maker = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
    return _session_maker


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager supplying transactional database sessions.
    
    Automatically handles commits on success and rollback on exceptions, closing the session in all cases.
    """
    factory = get_sessionmaker()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.debug("Transaction rolled back due to error", error=str(e))
            raise
        finally:
            await session.close()


async def shutdown_db() -> None:
    """Disposes the global connection pool cleanly."""
    global _engine, _session_maker
    if _engine:
        logger.info("Closing database connection pool...")
        await _engine.dispose()
        _engine = None
        _session_maker = None
        logger.info("Database connection pool closed.")


def run_migrations() -> None:
    """Runs database migrations to latest head programmatically."""
    import os
    from alembic.config import Config
    from alembic import command

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    ini_path = os.path.join(project_root, "alembic.ini")
    
    logger.info("Running database migrations programmatically...", ini_path=ini_path)
    
    alembic_cfg = Config(ini_path)
    command.upgrade(alembic_cfg, "head")
    logger.info("Database migrations completed successfully.")
