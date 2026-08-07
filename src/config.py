import argparse
import sys
from typing import List, Optional
from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # 1. Matrix Connection Config
    matrix_homeserver: str = "https://matrix.org"
    matrix_user_id: str
    matrix_password: Optional[str] = None
    matrix_access_token: Optional[str] = None
    matrix_device_id: str = "BOT_CLIENT"
    matrix_startup_rooms: str = ""  # Comma-separated room aliases or IDs

    # 2. Database Configuration (configurable via CLI or env)
    database_url: str = "sqlite+aiosqlite:///data/bot.db"

    # 3. Third-party APIs
    stock_api_key: Optional[str] = None
    gemini_api_key: Optional[str] = None

    # 4. Admin & Productization
    admin_room_id: Optional[str] = None
    admin_users: str = ""  # Comma-separated in env
    free_tier_limit: int = 2
    webhook_secret: Optional[str] = None

    # 5. Operations & Logging
    log_level: str = "INFO"
    json_logging: bool = False
    health_port: int = 8080

    # Pydantic Settings Config
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @property
    def admin_users_list(self) -> List[str]:
        """Returns the admin user IDs as a parsed list."""
        if not self.admin_users:
            return []
        return [user_id.strip() for user_id in self.admin_users.split(",") if user_id.strip()]

    @property
    def startup_rooms_list(self) -> List[str]:
        """Returns the startup room aliases or IDs as a parsed list."""
        if not self.matrix_startup_rooms:
            return []
        return [room.strip() for room in self.matrix_startup_rooms.split(",") if room.strip()]


class CLIArgs:
    """Holds command-line parsed options."""
    def __init__(self):
        self.env_file: str = ".env"
        self.database_url: Optional[str] = None
        self.log_level: Optional[str] = None
        self.health_port: Optional[int] = None
        self.run_migrations: bool = False


def parse_cli_args() -> CLIArgs:
    """Parses incoming command-line options using standard argparse."""
    parser = argparse.ArgumentParser(
        description="Matrix Multi-Capability Bot",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-e", "--env-file", type=str, default=".env", help="Path to dotenv config file")
    parser.add_argument("-d", "--database-url", type=str, help="Override database connection URL (e.g. postgresql+asyncpg://...)")
    parser.add_argument("-l", "--log-level", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Override log level")
    parser.add_argument("-p", "--health-port", type=int, help="Override health probe port")
    parser.add_argument("--run-migrations", action="store_true", help="Run database migrations and exit immediately")
    
    # Parse known arguments to avoid issues with test frameworks (like pytest flags)
    args, _ = parser.parse_known_args()
    
    cli = CLIArgs()
    cli.env_file = args.env_file
    cli.database_url = args.database_url
    cli.log_level = args.log_level
    cli.health_port = args.health_port
    cli.run_migrations = args.run_migrations
    return cli


def print_validation_error(error: ValidationError, env_file: str) -> None:
    """Outputs a polished, actionable configuration instruction block to stderr."""
    print("\n[ERROR] Configuration Error: Missing or invalid settings.", file=sys.stderr)
    print(f"Attempted to load configuration using env file: '{env_file}'\n", file=sys.stderr)
    print("The following validation errors occurred:", file=sys.stderr)
    for err in error.errors():
        field = " -> ".join(str(x) for x in err["loc"])
        print(f"  - {field}: {err['msg']} (type={err['type']})", file=sys.stderr)
    
    print("\nTo configure the bot, please copy '.env.example' to '.env' and fill in the values.", file=sys.stderr)
    print("Alternatively, pass the required variables or point to a custom config file:", file=sys.stderr)
    print("  uv run bot --env-file /path/to/.env\n", file=sys.stderr)
    print("For database overrides, you can also pass them directly:", file=sys.stderr)
    print("  uv run bot --database-url 'sqlite+aiosqlite:///custom.db'\n", file=sys.stderr)


# Global singletons
settings: Optional[Settings] = None
cli_args: Optional[CLIArgs] = None


def init_config() -> None:
    """Initializes command-line arguments and configuration settings."""
    global settings, cli_args
    cli_args = parse_cli_args()
    
    # Resolve Pydantic configuration overrides
    # CLI options take highest precedence
    kwargs = {}
    if cli_args.database_url:
        kwargs["database_url"] = cli_args.database_url
    if cli_args.log_level:
        kwargs["log_level"] = cli_args.log_level
    if cli_args.health_port:
        kwargs["health_port"] = cli_args.health_port
        
    try:
        # Load from environment and/or resolved .env path, injecting CLI overrides
        settings = Settings(_env_file=cli_args.env_file, **kwargs)
    except ValidationError as e:
        print_validation_error(e, cli_args.env_file)
        sys.exit(1)


def load_settings() -> Settings:
    """Gets or initializes the global Pydantic settings schema."""
    global settings
    if settings is None:
        init_config()
    return settings


def load_cli_args() -> CLIArgs:
    """Gets or parses the global command-line argument schema."""
    global cli_args
    if cli_args is None:
        init_config()
    return cli_args
