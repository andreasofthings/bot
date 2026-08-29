import asyncio
import signal
import sys
from src.config import load_settings, load_cli_args
from src.utils.logger import configure_logging, get_logger
from src.core.plugin import PluginManager
from src.core.bot import MatrixBot

# We initialize a raw python logger temporarily until structlog is configured
import logging

logging.basicConfig(level=logging.INFO)
logger = get_logger(__name__)


async def main() -> None:
    # 1. Load config and initialize structured logging
    settings = load_settings()
    cli_args = load_cli_args()
    configure_logging()

    # 2. Check if only migrations are requested
    if cli_args.run_migrations:
        logger.info("Run migrations flag detected. Initiating database migrations...")
        from src.core.database import run_migrations
        await asyncio.to_thread(run_migrations)
        logger.info("Database migrations complete. Exiting.")
        return

    # 3. Auto-run database migrations on startup to ensure schema is synchronized
    logger.info("Verifying database schema is up to date...")
    from src.core.database import run_migrations
    await asyncio.to_thread(run_migrations)

    logger.info("Starting Matrix Bot Service...")

    # 2. Instantiate PluginManager and register base plugins
    plugin_manager = PluginManager()

    # Help plugin needs to know about other registered plugins

    from src.plugins.help import HelpPlugin
    from src.plugins.onboarding import OnboardingPlugin
    from src.plugins.stock import StockPlugin
    from src.plugins.rss import RSSPlugin

    help_plugin = HelpPlugin(plugin_manager)
    onboarding_plugin = OnboardingPlugin()
    stock_plugin = StockPlugin()
    rss_plugin = RSSPlugin()

    plugin_manager.register_plugin(help_plugin)
    plugin_manager.register_plugin(onboarding_plugin)
    plugin_manager.register_plugin(stock_plugin)
    plugin_manager.register_plugin(rss_plugin)

    # 3. Instantiate bot runner
    bot = MatrixBot(settings, plugin_manager)

    # 4. Configure graceful shutdown signal handling
    loop = asyncio.get_running_loop()

    def shutdown_handler() -> None:
        logger.info(
            "Shutdown signal received (SIGINT/SIGTERM). Stopping bot gracefully..."
        )
        # Schedule the async stop routine in the event loop
        asyncio.create_task(bot.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown_handler)
        except NotImplementedError:
            # Signal handlers are not fully supported on some platforms (e.g. Windows/some CI)
            pass

    # 5. Start bot connection and hold execution open
    try:
        await bot.start()

        # Keep main function alive while the connection sync loop is running
        while bot.is_running:
            await asyncio.sleep(1)

    except Exception as e:
        logger.exception("Matrix Bot service crashed", error=str(e))
        sys.exit(1)
    finally:
        # Guarantee cleanup runs if we break out of the sync loop
        await bot.stop()
        # Clean up database pool cleanly
        from src.core.database import shutdown_db
        await shutdown_db()
        logger.info("Matrix Bot Service shutdown complete.")


def entrypoint():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service interrupted via Keyboard. Exiting.")
        sys.exit(0)


if __name__ == "__main__":
    entrypoint()
