import abc
from typing import List, Dict, Tuple
from nio import AsyncClient, MatrixRoom, RoomMessageText
from src.utils.logger import get_logger

logger = get_logger(__name__)


class Plugin(abc.ABC):
    """Abstract Base Class for all bot capabilities (plugins)."""

    @property
    @abc.abstractmethod
    def plugin_id(self) -> str:
        """Unique identifier for the plugin (e.g., 'rss', 'stock')."""
        pass

    @property
    @abc.abstractmethod
    def commands(self) -> List[str]:
        """List of commands triggerable by this plugin (e.g., ['rss', 'rss_sub'])."""
        pass

    @abc.abstractmethod
    async def on_message(
        self,
        client: AsyncClient,
        room: MatrixRoom,
        event: RoomMessageText,
        command: str,
        args: List[str],
    ) -> None:
        """Callback invoked when a command registered by this plugin is detected."""
        pass

    async def on_tick(self, client: AsyncClient) -> None:
        """Periodic background callback triggered by the bot's scheduler."""
        pass

    @abc.abstractmethod
    def get_help(self) -> str:
        """Returns usage instructions/help documentation for this plugin's commands."""
        pass


class PluginManager:
    """Manages bot plugin lifecycles, command registration, and execution routing."""

    def __init__(self):
        self.plugins: Dict[str, Plugin] = {}
        self.command_map: Dict[str, Plugin] = {}

    def register_plugin(self, plugin: Plugin) -> None:
        """Registers a plugin and binds its commands to the router mapping."""
        logger.info(
            "Registering plugin", plugin_id=plugin.plugin_id, commands=plugin.commands
        )
        self.plugins[plugin.plugin_id] = plugin
        for cmd in plugin.commands:
            normalized_cmd = cmd.lower()
            if normalized_cmd in self.command_map:
                logger.warning(
                    "Command collision",
                    command=cmd,
                    existing_plugin=self.command_map[normalized_cmd].plugin_id,
                    new_plugin=plugin.plugin_id,
                )
            self.command_map[normalized_cmd] = plugin

    async def route_command(
        self,
        client: AsyncClient,
        room: MatrixRoom,
        event: RoomMessageText,
        command: str,
        args: List[str],
    ) -> bool:
        """Routes a parsed command and its arguments to the appropriate plugin."""
        normalized_cmd = command.lower()
        plugin = self.command_map.get(normalized_cmd)
        if not plugin:
            return False

        try:
            logger.debug(
                "Routing command to plugin", command=command, plugin_id=plugin.plugin_id
            )
            await plugin.on_message(client, room, event, command, args)
            return True
        except Exception as e:
            logger.exception(
                "Plugin execution failed",
                plugin_id=plugin.plugin_id,
                command=command,
                error=str(e),
            )
            # Send error feedback to room
            await client.room_send(
                room_id=room.room_id,
                message_type="m.room.message",
                content={
                    "msgtype": "m.text",
                    "body": f"An error occurred while executing command: {command}",
                },
            )
            return True

    def get_all_help(self) -> str:
        """Aggregates help text from all registered plugins."""
        help_texts = []
        for plugin in self.plugins.values():
            help_texts.append(
                f"### {plugin.plugin_id.upper()} Plugin\n{plugin.get_help()}"
            )
        return "\n\n".join(help_texts)
