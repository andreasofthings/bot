import time
from typing import List
from nio import AsyncClient, MatrixRoom, RoomMessageText
from src.core.plugin import Plugin, PluginManager


class HelpPlugin(Plugin):
    """System utility plugin providing !help and !status commands."""

    def __init__(self, plugin_manager: PluginManager):
        self._plugin_manager = plugin_manager
        self._start_time = time.time()

    @property
    def plugin_id(self) -> str:
        return "help"

    @property
    def commands(self) -> List[str]:
        return ["help", "status"]

    async def on_message(
        self,
        client: AsyncClient,
        room: MatrixRoom,
        event: RoomMessageText,
        command: str,
        args: List[str],
    ) -> None:
        cmd = command.lower()
        if cmd == "help":
            all_help = self._plugin_manager.get_all_help()

            # Simple HTML conversion for basic formatting
            html_body = f"<h4>Available Bot Commands</h4><p>{all_help.replace('\n', '<br>')}</p>"
            plain_body = f"Available Bot Commands:\n\n{all_help}"

            await client.room_send(
                room_id=room.room_id,
                message_type="m.room.message",
                content={
                    "msgtype": "m.text",
                    "format": "org.matrix.custom.html",
                    "body": plain_body,
                    "formatted_body": html_body,
                },
            )
        elif cmd == "status":
            uptime_seconds = int(time.time() - self._start_time)
            hours, remainder = divmod(uptime_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            uptime_str = f"{hours}h {minutes}m {seconds}s"

            html_body = (
                f"<h4>Bot Operational Status</h4>"
                f"<ul>"
                f"<li><b>Uptime:</b> {uptime_str}</li>"
                f"<li><b>Active Plugins:</b> {len(self._plugin_manager.plugins)}</li>"
                f"<li><b>Database Mode:</b> Active</li>"
                f"</ul>"
            )
            plain_body = f"Bot Operational Status:\n- Uptime: {uptime_str}\n- Active Plugins: {len(self._plugin_manager.plugins)}\n- Database Mode: Active"

            await client.room_send(
                room_id=room.room_id,
                message_type="m.room.message",
                content={
                    "msgtype": "m.text",
                    "format": "org.matrix.custom.html",
                    "body": plain_body,
                    "formatted_body": html_body,
                },
            )

    def get_help(self) -> str:
        return (
            "• <b>!help</b>: Lists all available commands and descriptions.<br>"
            "• <b>!status</b>: Reports bot uptime, plugins, and status."
        )
