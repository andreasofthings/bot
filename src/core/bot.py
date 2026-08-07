import asyncio
from typing import Optional, List
from nio import AsyncClient, MatrixRoom, RoomMessageText, InviteEvent, LoginResponse
from src.config import Settings
from src.core.plugin import PluginManager
from src.utils.logger import get_logger

logger = get_logger(__name__)


class MatrixBot:
    """Core Matrix bot client runner using matrix-nio."""

    def __init__(self, settings: Settings, plugin_manager: PluginManager):
        self.settings = settings
        self.plugin_manager = plugin_manager
        self.client: Optional[AsyncClient] = None
        self.is_running = False
        self._sync_task: Optional[asyncio.Task] = None
        self._ticker_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Initializes client, logs in, registers event callbacks, and starts sync loop."""
        logger.info(
            "Initializing Matrix client...",
            homeserver=self.settings.matrix_homeserver,
            user=self.settings.matrix_user_id,
        )

        self.client = AsyncClient(
            homeserver=self.settings.matrix_homeserver,
            user=self.settings.matrix_user_id,
            device_id=self.settings.matrix_device_id,
        )

        # Register Event Callbacks
        self.client.add_event_callback(self._on_room_message, RoomMessageText)
        self.client.add_event_callback(self._on_invite, InviteEvent)

        # Authentication Flow
        if self.settings.matrix_access_token:
            self.client.access_token = self.settings.matrix_access_token
            self.client.device_id = self.settings.matrix_device_id
            logger.info("Authenticated using access token")
        elif self.settings.matrix_password:
            logger.info("Attempting login via password...")
            response = await self.client.login(
                password=self.settings.matrix_password,
            )
            if isinstance(response, LoginResponse):
                logger.info(
                    "Successfully logged in via password", device_id=response.device_id
                )
            else:
                logger.error("Authentication failed", response=str(response))
                raise RuntimeError(f"Login failed: {response}")
        else:
            raise ValueError(
                "Invalid configuration: Provide either MATRIX_ACCESS_TOKEN or MATRIX_PASSWORD"
            )

        # Join configured startup rooms automatically
        for room_alias_or_id in self.settings.startup_rooms_list:
            logger.info("Attempting to join startup room", room=room_alias_or_id)
            try:
                await self.client.join(room_alias_or_id)
                logger.info("Successfully joined startup room", room=room_alias_or_id)
            except Exception as e:
                logger.error("Failed to join startup room", room=room_alias_or_id, error=str(e))

        self.is_running = True
        # Start the sync loop as a background task to allow other tasks to run concurrently
        self._sync_task = asyncio.create_task(self._run_sync_loop())
        self._ticker_task = asyncio.create_task(self._run_ticker_loop())

    async def _run_sync_loop(self) -> None:
        """Internal runner loop for syncing with the Matrix homeserver."""
        logger.info("Starting Matrix client sync loop...")
        try:
            # sync_forever handles reconnections internally
            await self.client.sync_forever(timeout=30000, full_state=True)
        except asyncio.CancelledError:
            logger.info("Sync loop task cancelled.")
        except Exception as e:
            logger.exception("Matrix sync loop crashed", error=str(e))
        finally:
            self.is_running = False

    async def _run_ticker_loop(self) -> None:
        """Background loop that ticks registered plugins periodically."""
        logger.info("Starting background scheduler loop...")
        while self.is_running:
            try:
                # Dispatch tick to all plugins
                for plugin in self.plugin_manager.plugins.values():
                    await plugin.on_tick(self.client)
            except Exception as e:
                logger.error("Error in scheduler tick", error=str(e))
            
            # Check every 30 seconds
            await asyncio.sleep(30)

    async def stop(self) -> None:
        """Gracefully disconnects and stops background sync tasks."""
        logger.info("Shutting down Matrix bot...")
        self.is_running = False

        if self._sync_task:
            logger.debug("Cancelling sync loop task...")
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
            self._sync_task = None

        if self._ticker_task:
            logger.debug("Cancelling background scheduler task...")
            self._ticker_task.cancel()
            try:
                await self._ticker_task
            except asyncio.CancelledError:
                pass
            self._ticker_task = None

        if self.client:
            logger.debug("Closing client session...")
            await self.client.close()
            self.client = None

        logger.info("Matrix bot shut down completed.")

    async def _on_invite(self, room: MatrixRoom, event: InviteEvent) -> None:
        """Automatically accepts incoming room invites."""
        logger.info("Received room invite", room_id=room.room_id, sender=event.sender)
        try:
            await self.client.join(room.room_id)
            logger.info("Accepted invite and joined room", room_id=room.room_id)
        except Exception as e:
            logger.error(
                "Failed to join room on invite", room_id=room.room_id, error=str(e)
            )

    async def _on_room_message(self, room: MatrixRoom, event: RoomMessageText) -> None:
        """Parses message body for commands and dispatches them to plugins or handles onboarding."""
        # Ignore messages sent by the bot itself
        if event.sender == self.client.user_id:
            return

        body = event.body.strip()
        
        # Check user onboarding status
        from src.core.database import get_db_session
        from src.models.user import User

        async with get_db_session() as session:
            user = await session.get(User, event.sender)

            # Identify platform source (Signal, WhatsApp, Matrix)
            platform = "Matrix"
            if "signal_" in event.sender:
                platform = "Signal"
            elif "whatsapp_" in event.sender:
                platform = "WhatsApp"

            is_completed = user is not None and user.onboarding_state == "COMPLETED"

            if not is_completed:
                # Check if it's a command that bypasses onboarding
                is_bypass = False
                if body.startswith("!"):
                    parts = body[1:].split()
                    if parts:
                        cmd = parts[0].lower()
                        if cmd in ["help", "status", "activate"]:
                            is_bypass = True

                if not is_bypass:
                    # Determine if this is a private context (DM)
                    is_dm = len(room.users) <= 2
                    
                    if is_dm:
                        from src.plugins.onboarding import handle_onboarding_message
                        await handle_onboarding_message(self.client, room, event, user, platform)
                        return
                    else:
                        # In a public channel, warn the user if they tried to run a command
                        if body.startswith("!"):
                            from src.plugins.onboarding import send_rich_message
                            html_msg = f"Hello <a href='https://matrix.to/#/{event.sender}'>{event.sender}</a>! Please start a private chat (DM) with me to complete onboarding before using commands."
                            plain_msg = f"Hello {event.sender}! Please start a private chat (DM) with me to complete onboarding before using commands."
                            await send_rich_message(self.client, room.room_id, plain_msg, html_msg)
                        return

        # If onboarding is completed, parse command
        if not body.startswith("!"):
            if self.settings.gemini_api_key:
                interpreted_command = await self._route_natural_language(room, event, body)
                if interpreted_command:
                    from src.plugins.onboarding import send_rich_message
                    html = f"<i>(Interpreted: <code>{interpreted_command}</code>)</i>"
                    plain = f"(Interpreted: {interpreted_command})"
                    await send_rich_message(self.client, room.room_id, plain, html)
                    body = interpreted_command
                else:
                    return
            else:
                return

        parts = body[1:].split()
        if not parts:
            return

        command = parts[0]
        args = parts[1:]

        logger.debug(
            "Command detected",
            command=command,
            args=args,
            sender=event.sender,
            room_id=room.room_id,
        )

        # Route to plugin manager
        await self.plugin_manager.route_command(self.client, room, event, command, args)

    async def _route_natural_language(self, room: MatrixRoom, event: RoomMessageText, message_body: str) -> Optional[str]:
        """Translates user natural language message into a structured bot command using Gemini API."""
        import httpx
        
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={self.settings.gemini_api_key}"
        
        system_instruction = (
            "You are the Natural Language Router for a Matrix capability bot.\n"
            "The bot supports these commands:\n"
            "- !help: Shows general command assistance.\n"
            "- !status: Shows current service status.\n"
            "- !activate <code>: Upgrades user licensing tier.\n"
            "- !forgetme: Wipes database user profile.\n"
            "- !rss list: Lists current RSS subscriptions.\n"
            "- !rss subscribe <url> [--keywords k1] [--companies c1] [--geo g1] [--representatives r1]: Subscribes room/user to feed URL with optional comma-separated relevance filters.\n"
            "- !rss unsubscribe <id_or_url>: Cancels RSS feed subscription.\n"
            "- !stock list: Lists active indicator threshold alerts.\n"
            "- !stock check <ticker> [indicator] [period]: Inspects a stock or its technical indicators on-demand.\n"
            "- !stock subscribe <ticker> <indicator> <period> <condition> <threshold>: Subscribes to stock indicator alerts.\n"
            "- !stock unsubscribe <id_or_ticker>: Cancels active stock alerts.\n\n"
            "Supported Indicators: RSI, SMA, EMA, MACD, BOLLINGER_HIGH, BOLLINGER_LOW\n"
            "Supported Conditions: ABOVE, BELOW, CROSS_ABOVE, CROSS_BELOW\n\n"
            "Your task is to translate user message intents into one of the structured commands above.\n"
            "If the user message maps to a command, output ONLY the structured command itself (e.g. '!stock check SAP.DE' or '!rss list'), starting with '!' and with no markdown, formatting, or extra text.\n"
            "If the message is a greeting, general chat, or does not map to any structured command, reply naturally with a conversational message explaining how you can help."
        )

        prompt = f"User Message: {message_body}"

        # Gemini v1beta API request body
        payload = {
            "contents": [{
                "parts": [{"text": prompt}]
            }],
            "systemInstruction": {
                "parts": [{"text": system_instruction}]
            },
            "generationConfig": {
                "temperature": 0.1,  # Low temperature for highly deterministic command routing
                "maxOutputTokens": 200
            }
        }

        try:
            async with httpx.AsyncClient(timeout=15.0) as http_client:
                resp = await http_client.post(url, json=payload)
                if resp.status_code == 200:
                    result = resp.json()
                    output_text = result["candidates"][0]["content"]["parts"][0]["text"].strip()
                    
                    if output_text.startswith("!"):
                        return output_text
                    else:
                        from src.plugins.onboarding import send_rich_message
                        await send_rich_message(self.client, room.room_id, output_text, output_text)
                        return None
                else:
                    logger.error("Gemini API error response", status=resp.status_code, body=resp.text)
                    from src.plugins.onboarding import send_rich_message
                    await send_rich_message(self.client, room.room_id, "API Error: Conversational router failed.", "❌ <b>API Error:</b> Natural language router request failed.")
                    return None
        except Exception as e:
            logger.exception("Failed to process natural language message via Gemini", error=str(e))
            return None
