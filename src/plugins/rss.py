import asyncio
import feedparser
import html
import httpx
import shlex
import re
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import select, delete, update, func
from sqlalchemy.orm import joinedload
from nio import (
    AsyncClient,
    MatrixRoom,
    RoomMessageText,
    RoomSendResponse,
    RoomSendError,
    RoomResolveAliasResponse,
)
from src.core.plugin import Plugin
from src.core.database import get_db_session
from src.models.rss import RSSFeed, RSSSubscription, RSSHistory, RSSQueueItem
from src.models.user import User
from src.config import load_settings
from src.fixtures.rss_feeds import get_default_rss_fixtures, find_fixture_by_slug_or_name
from src.utils.logger import get_logger

logger = get_logger(__name__)

async def send_rich_message(client: AsyncClient, room_id: str, plain: str, html: str) -> None:
    """Helper to send formatted HTML messages to a Matrix room.
    
    If room_id is an alias (starts with #), resolves it to a canonical room ID.
    Raises RuntimeError if delivery fails.
    """
    if room_id.startswith("#"):
        resolved = None
        if hasattr(client, "rooms"):
            for rid, r in client.rooms.items():
                if r.canonical_alias == room_id:
                    resolved = rid
                    break
        if not resolved:
            try:
                res_alias = await client.room_resolve_alias(room_id)
                if isinstance(res_alias, RoomResolveAliasResponse):
                    resolved = res_alias.room_id
                else:
                    logger.error("Failed to resolve room alias in send_rich_message", alias=room_id, error=str(res_alias))
            except Exception as e:
                logger.error("Error resolving room alias in send_rich_message", alias=room_id, error=str(e))
        if resolved:
            room_id = resolved

    response = await client.room_send(
        room_id=room_id,
        message_type="m.room.message",
        content={
            "msgtype": "m.text",
            "format": "org.matrix.custom.html",
            "body": plain,
            "formatted_body": html
        }
    )
    if isinstance(response, RoomSendError):
        logger.error(
            "Matrix room_send failed",
            room_id=room_id,
            error=response.message,
            status_code=getattr(response, "status_code", None)
        )
        raise RuntimeError(f"Matrix room_send failed ({getattr(response, 'status_code', 'unknown')}): {response.message}")

class RSSPlugin(Plugin):
    """RSS Syndication capability plugin. Ingests feeds into a queue and paces delivery predictably."""

    def __init__(self, settings: Optional[Any] = None):
        self.settings = settings or load_settings()
        self._poll_interval_seconds = getattr(self.settings, "rss_poll_interval_seconds", 3600)  # Default 60 minutes
        self._min_delivery_interval = getattr(self.settings, "rss_min_delivery_interval_seconds", 5)  # Min seconds between alerts
        self._max_delivery_interval = getattr(self.settings, "rss_max_delivery_interval_seconds", 3600)
        self._default_channel = getattr(self.settings, "rss_default_channel", None)
        # Summary mode settings (batched LLM digest delivery)
        self._summary_interval = getattr(self.settings, "rss_summary_interval_seconds", 3600)
        self._summary_model = getattr(self.settings, "rss_summary_model", "gemini-3.5-flash-lite")
        self._summary_max_articles = getattr(self.settings, "rss_summary_max_articles", 20)
        self._summary_max_output_tokens = getattr(self.settings, "rss_summary_max_output_tokens", 1024)
        self._room_id_cache: Dict[str, str] = {}

        self._last_poll_time: float = 0.0
        self._last_delivery_time: float = 0.0
        self._current_interval: float = 60.0
        self._is_running: bool = True
        self._dispatcher_task: Optional[asyncio.Task] = None
        self._summary_task: Optional[asyncio.Task] = None
        self._new_item_event = asyncio.Event()
        self._poll_lock = asyncio.Lock()

    @property
    def plugin_id(self) -> str:
        return "rss"

    @property
    def commands(self) -> List[str]:
        return ["rss", "help"]

    async def on_message(
        self, 
        client: AsyncClient, 
        room: MatrixRoom, 
        event: RoomMessageText, 
        command: str, 
        args: List[str]
    ) -> None:
        if command.lower() == "help" or not args or args[0].lower() in ["help", "--help", "-h"]:
            await self._send_usage(client, room.room_id)
            return

        sub_cmd = args[0].lower()

        # 1. Command: !rss list
        if sub_cmd == "list":
            await self._handle_list(client, room, event)
            return

        # 2. Command: !rss feeds (Lists curated default German news feeds)
        elif sub_cmd in ["feeds", "catalog", "available"]:
            await self._handle_feeds(client, room, event)
            return

        # 3. Command: !rss defaults / seed (Subscribes room to curated defaults)
        elif sub_cmd in ["defaults", "default", "seed"]:
            await self._handle_defaults(client, room, event)
            return

        # 4. Command: !rss subscribe <url_or_slug> [filters]
        elif sub_cmd == "subscribe":
            await self._handle_subscribe(client, room, event, args[1:])
            return

        # 5. Command: !rss unsubscribe <id_or_slug_or_url>
        elif sub_cmd == "unsubscribe":
            await self._handle_unsubscribe(client, room, event, args[1:])
            return

        # 6. Command: !rss queue / !rss status
        elif sub_cmd in ["queue", "status", "q"]:
            await self._handle_queue_status(client, room, event)
            return

        # 7. Command: !rss poll / !rss sync
        elif sub_cmd in ["poll", "sync", "refresh"]:
            await self._handle_poll_now(client, room, event)
            return

        # 8. Command: !rss mode <id_or_slug> <timeline|summary>
        elif sub_cmd == "mode":
            await self._handle_mode(client, room, event, args[1:])
            return

        # 9. Command: !rss language <id_or_slug> <auto|de|en|...>
        elif sub_cmd in ["language", "lang"]:
            await self._handle_language(client, room, event, args[1:])
            return

        else:
            await self._send_usage(client, room.room_id)

    async def _handle_list(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText) -> None:
        """Lists active subscriptions for the current context (checks room ID, room name, and canonical alias)."""
        # Collect identifiers that might represent this subscriber
        identifiers = [room.room_id]
        if room.name:
            identifiers.append(room.name)
        if room.canonical_alias:
            identifiers.append(room.canonical_alias)
            
        # If in a DM context, also check the sender's ID
        if len(room.users) <= 2:
            identifiers.append(event.sender)

        # Fallback: if we are in the main sauna room, also include "Product" subscriptions
        if "#sauna:pramari.de" in identifiers or "!tRiKSCYsVYKBQduVwT:pramari.de" in identifiers:
            identifiers.append("Product")

        async with get_db_session() as session:
            q = (
                select(RSSSubscription)
                .where(RSSSubscription.subscriber_id.in_(identifiers))
                .options(joinedload(RSSSubscription.feed))
            )
            res = await session.execute(q)
            subscriptions = res.scalars().all()

            if not subscriptions:
                await send_rich_message(
                    client, room.room_id, 
                    "No RSS subscriptions found for this context.", 
                    "No RSS subscriptions found for this context."
                )
                return

            html_lines = ["<b>Subscriptions in this room:</b><ul>"]
            plain_lines = ["Subscriptions in this room:"]
            for sub in subscriptions:
                feed_name = sub.feed.name or sub.feed.url
                feed_url = sub.feed.url
                mode_str = sub.delivery_mode or "timeline"
                if mode_str == "summary":
                    mode_str = f"summary/{sub.summary_language or 'auto'}"
                html_lines.append(f"<li>{sub.feed_id} - <a href='{feed_url}'>{feed_name}</a> ({mode_str}) (subscribed by <code>{sub.subscriber_id}</code>)</li>")
                plain_lines.append(f"  {sub.feed_id} - {feed_name} ({feed_url}) [{mode_str}] (subscribed by {sub.subscriber_id})")
            html_lines.append("</ul>")

            await send_rich_message(client, room.room_id, "\n".join(plain_lines), "".join(html_lines))

    async def _handle_feeds(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText) -> None:
        """Lists available curated German news RSS feed fixtures."""
        fixtures = get_default_rss_fixtures()
        html_lines = ["<b>📰 Curated Major German News Feeds (Fixtures):</b><ul>"]
        plain_lines = ["Curated Major German News Feeds (Fixtures):"]
        for f in fixtures:
            slug = f.get("slug", "")
            name = f.get("name", "")
            cat = f.get("category", "")
            url = f.get("url", "")
            html_lines.append(
                f"<li><b>{name}</b> [<code>{slug}</code>] &mdash; <i>{cat}</i><br>"
                f"Subscribe: <code>!rss subscribe {slug}</code> | <a href='{url}'>Feed URL</a></li>"
            )
            plain_lines.append(f"  • {name} [{slug}] ({cat}): !rss subscribe {slug}")
        html_lines.append(
            "</ul><p>💡 Tip: Use <code>!rss subscribe &lt;slug&gt;</code> to subscribe with optional filters, "
            "or <code>!rss defaults</code> to subscribe this room to all top German news feeds.</p>"
        )
        plain_lines.append("\nTip: Use '!rss subscribe <slug>' or '!rss defaults' to subscribe.")
        await send_rich_message(client, room.room_id, "\n".join(plain_lines), "".join(html_lines))

    async def _handle_defaults(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText) -> None:
        """Subscribes current room context to top default German news feeds."""
        subscriber_id = room.canonical_alias if room.canonical_alias else room.room_id
        if len(room.users) <= 2:
            subscriber_id = event.sender

        fixtures = get_default_rss_fixtures()
        # Top major German news outlets
        top_slugs = ["tagesschau", "zdf", "spiegel", "zeit", "faz", "sz", "handelsblatt", "heise"]
        selected_fixtures = [f for f in fixtures if f.get("slug") in top_slugs]

        async with get_db_session() as session:
            subscribed_names = []
            for f in selected_fixtures:
                q_feed = select(RSSFeed).where(RSSFeed.url == f["url"])
                res_f = await session.execute(q_feed)
                feed = res_f.scalar_one_or_none()
                if not feed:
                    feed = RSSFeed(url=f["url"], name=f["name"])
                    session.add(feed)
                    await session.flush()

                q_sub = select(RSSSubscription).where(
                    RSSSubscription.subscriber_id == subscriber_id,
                    RSSSubscription.feed_id == feed.id
                )
                res_s = await session.execute(q_sub)
                if not res_s.scalar_one_or_none():
                    sub = RSSSubscription(
                        subscriber_id=subscriber_id,
                        subscriber_type="room" if room.canonical_alias or len(room.users) > 2 else "user",
                        feed_id=feed.id
                    )
                    session.add(sub)
                    subscribed_names.append(feed.name or f["name"])

            if subscribed_names:
                msg = f"Subscribed {subscriber_id} to {len(subscribed_names)} default German news feeds:\n- " + "\n- ".join(subscribed_names)
                html = (
                    f"✅ <b>Subscribed to Default German News Feeds:</b><ul>"
                    + "".join(f"<li>{n}</li>" for n in subscribed_names)
                    + "</ul>"
                )
            else:
                msg = "Already subscribed to all top default German news feeds."
                html = "Already subscribed to all top default German news feeds."
            await send_rich_message(client, room.room_id, msg, html)

    async def _handle_subscribe(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText, sub_args: List[str]) -> None:
        """Subscribes the current room context to an RSS feed URL, parsing filter arguments."""
        settings = load_settings()
        if not sub_args:
            await send_rich_message(client, room.room_id, "Usage: !rss subscribe <url> [--keywords ...] [--companies ...] [--geo ...] [--representatives ...]", "Usage: <code>!rss subscribe &lt;url&gt; [--keywords ...]</code>")
            return

        feed_input = sub_args[0].strip()
        fixture = find_fixture_by_slug_or_name(feed_input)
        if fixture:
            feed_url = fixture["url"]
            preferred_name = fixture["name"]
        else:
            feed_url = feed_input
            preferred_name = None

        # Parse flags using argparse-like strategy via standard shlex
        keywords, companies, geos, reps = None, None, None, None
        delivery_mode = "timeline"
        summary_language = "auto"
        try:
            # We reconstruct the string and parse with shlex to support quotes in flags
            arg_line = " ".join(sub_args[1:])
            parsed_args = shlex.split(arg_line)
            
            i = 0
            while i < len(parsed_args):
                flag = parsed_args[i]
                if flag == "--keywords" and i + 1 < len(parsed_args):
                    keywords = [k.strip() for k in parsed_args[i+1].split(",") if k.strip()]
                    i += 2
                elif flag == "--companies" and i + 1 < len(parsed_args):
                    companies = [c.strip() for c in parsed_args[i+1].split(",") if c.strip()]
                    i += 2
                elif flag == "--geo" and i + 1 < len(parsed_args):
                    geos = [g.strip() for g in parsed_args[i+1].split(",") if g.strip()]
                    i += 2
                elif flag == "--representatives" and i + 1 < len(parsed_args):
                    reps = [r.strip() for r in parsed_args[i+1].split(",") if r.strip()]
                    i += 2
                elif flag == "--mode" and i + 1 < len(parsed_args):
                    delivery_mode = parsed_args[i+1].strip().lower()
                    i += 2
                elif flag in ("--lang", "--language") and i + 1 < len(parsed_args):
                    summary_language = parsed_args[i+1].strip().lower()
                    i += 2
                else:
                    i += 1
        except Exception as e:
            logger.error("Failed to parse filter flags", error=str(e))
            await send_rich_message(client, room.room_id, "Error parsing filter options.", "❌ <b>Error parsing filter options.</b> Make sure quotes are balanced.")
            return

        if delivery_mode not in ("timeline", "summary"):
            await send_rich_message(
                client, room.room_id,
                f"Unknown delivery mode '{delivery_mode}'. Use 'timeline' or 'summary'.",
                f"❌ Unknown delivery mode <code>{html.escape(delivery_mode)}</code>. Use <code>timeline</code> or <code>summary</code>."
            )
            return

        # Fetch subscriber target ID (prefer canonical alias, fallback to room ID)
        subscriber_id = room.canonical_alias if room.canonical_alias else room.room_id
        if len(room.users) <= 2:
            # In a DM, subscribe to the user ID
            subscriber_id = event.sender
            
        async with get_db_session() as session:
            # 1. Ensure user has not exceeded Free Tier limit
            if len(room.users) <= 2 or not room.canonical_alias:
                # Query user tier
                user_rec = await session.get(User, event.sender)
                if user_rec and user_rec.tier == "FREE":
                    # Check existing active subscriptions for this user
                    q_sub_count = select(RSSSubscription).where(RSSSubscription.subscriber_id == event.sender)
                    res_count = await session.execute(q_sub_count)
                    if len(res_count.scalars().all()) >= settings.free_tier_limit:
                        await send_rich_message(
                            client, room.room_id,
                            f"Free Tier Limit Exceeded. You can only have {settings.free_tier_limit} active subscriptions. Upgrade to Premium.",
                            f"❌ <b>Limit Exceeded:</b> Free Tier is restricted to {settings.free_tier_limit} active subscriptions. Activate Premium using <code>!activate &lt;code&gt;</code>."
                        )
                        return

            # 2. Get or create RSS Feed
            q_feed = select(RSSFeed).where(RSSFeed.url == feed_url)
            res_feed = await session.execute(q_feed)
            feed = res_feed.scalar_one_or_none()
            if not feed:
                # Try parsing feed to check if valid and extract name
                try:
                    parsed = feedparser.parse(feed_url)
                    feed_name = preferred_name or (parsed.feed.title if 'title' in parsed.feed else feed_url)
                except Exception:
                    feed_name = preferred_name or feed_url
                
                feed = RSSFeed(url=feed_url, name=feed_name)
                session.add(feed)
                await session.flush()
            elif preferred_name and (not feed.name or feed.name == feed.url):
                feed.name = preferred_name

            # 3. Create Subscription
            # Check if subscription already exists
            q_sub = select(RSSSubscription).where(
                RSSSubscription.subscriber_id == subscriber_id,
                RSSSubscription.feed_id == feed.id
            )
            res_sub = await session.execute(q_sub)
            subscription = res_sub.scalar_one_or_none()
            if subscription:
                await send_rich_message(client, room.room_id, "Already subscribed to this feed.", "Already subscribed to this feed.")
                return

            subscription = RSSSubscription(
                subscriber_id=subscriber_id,
                subscriber_type="room" if room.canonical_alias or len(room.users) > 2 else "user",
                feed_id=feed.id,
                keywords=keywords,
                companies=companies,
                geographies=geos,
                representatives=reps,
                delivery_mode=delivery_mode,
                summary_language=summary_language[:8] if summary_language else "auto"
            )
            session.add(subscription)
            
            html_msg = (
                f"✅ <b>Successfully Subscribed!</b><br>"
                f"• <b>Feed:</b> {feed.name or feed.url}<br>"
                f"• <b>Keywords:</b> {keywords or 'None'}<br>"
                f"• <b>Companies:</b> {companies or 'None'}<br>"
                f"• <b>Geography:</b> {geos or 'None'}<br>"
                f"• <b>Representatives:</b> {reps or 'None'}<br>"
                f"• <b>Mode:</b> {delivery_mode}" + (f" (language: {summary_language})" if delivery_mode == "summary" else "")
            )
            plain_msg = f"Successfully Subscribed!\n- Feed: {feed.name or feed.url}\n- Keywords: {keywords}\n- Companies: {companies}\n- Mode: {delivery_mode}" + (f" (language: {summary_language})" if delivery_mode == "summary" else "")
            await send_rich_message(client, room.room_id, plain_msg, html_msg)

    async def _handle_unsubscribe(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText, unsub_args: List[str]) -> None:
        """Removes an active RSS subscription by feed ID or URL."""
        if not unsub_args:
            await send_rich_message(client, room.room_id, "Usage: !rss unsubscribe <id_or_url>", "Usage: <code>!rss unsubscribe &lt;id_or_url&gt;</code>")
            return

        target = unsub_args[0].strip()
        subscriber_id = room.canonical_alias if room.canonical_alias else room.room_id
        if len(room.users) <= 2:
            subscriber_id = event.sender

        async with get_db_session() as session:
            # Check if target is integer ID
            feed_id = None
            if target.isdigit():
                feed_id = int(target)

            # Query subscription
            if feed_id:
                q = select(RSSSubscription).where(
                    RSSSubscription.subscriber_id == subscriber_id,
                    RSSSubscription.feed_id == feed_id
                )
            else:
                # Query feed first by URL or fixture slug/alias
                fixture = find_fixture_by_slug_or_name(target)
                target_url = fixture["url"] if fixture else target
                q_feed = select(RSSFeed).where(RSSFeed.url == target_url)
                res_feed = await session.execute(q_feed)
                feed_rec = res_feed.scalar_one_or_none()
                if not feed_rec:
                    await send_rich_message(client, room.room_id, "Subscription or feed URL not found.", "Subscription or feed URL not found.")
                    return
                q = select(RSSSubscription).where(
                    RSSSubscription.subscriber_id == subscriber_id,
                    RSSSubscription.feed_id == feed_rec.id
                )

            res = await session.execute(q)
            subscription = res.scalar_one_or_none()
            if not subscription:
                await send_rich_message(client, room.room_id, "No active subscription found.", "No active subscription found.")
                return

            await session.delete(subscription)
            await send_rich_message(client, room.room_id, "Successfully unsubscribed.", "Successfully unsubscribed.")

    async def _find_subscription(self, session, subscriber_id: str, target: str) -> Optional[RSSSubscription]:
        """Resolves a subscription for the current context by feed ID, URL, or fixture slug."""
        feed_id = None
        if target.isdigit():
            feed_id = int(target)

        if feed_id:
            q = select(RSSSubscription).where(
                RSSSubscription.subscriber_id == subscriber_id,
                RSSSubscription.feed_id == feed_id
            ).options(joinedload(RSSSubscription.feed))
        else:
            fixture = find_fixture_by_slug_or_name(target)
            target_url = fixture["url"] if fixture else target
            q_feed = select(RSSFeed).where(RSSFeed.url == target_url)
            res_feed = await session.execute(q_feed)
            feed_rec = res_feed.scalar_one_or_none()
            if not feed_rec:
                return None
            q = select(RSSSubscription).where(
                RSSSubscription.subscriber_id == subscriber_id,
                RSSSubscription.feed_id == feed_rec.id
            ).options(joinedload(RSSSubscription.feed))
        res = await session.execute(q)
        return res.scalar_one_or_none()

    async def _handle_mode(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText, mode_args: List[str]) -> None:
        """Switches an existing subscription between 'timeline' and 'summary' delivery."""
        if len(mode_args) < 2:
            await send_rich_message(
                client, room.room_id,
                "Usage: !rss mode <id_or_slug_or_url> <timeline|summary>",
                "Usage: <code>!rss mode &lt;id_or_slug_or_url&gt; &lt;timeline|summary&gt;</code>"
            )
            return

        target = mode_args[0].strip()
        new_mode = mode_args[1].strip().lower()
        if new_mode not in ("timeline", "summary"):
            await send_rich_message(
                client, room.room_id,
                f"Unknown delivery mode '{new_mode}'. Use 'timeline' or 'summary'.",
                f"❌ Unknown delivery mode <code>{html.escape(new_mode)}</code>. Use <code>timeline</code> or <code>summary</code>."
            )
            return

        subscriber_id = room.canonical_alias if room.canonical_alias else room.room_id
        if len(room.users) <= 2:
            subscriber_id = event.sender

        async with get_db_session() as session:
            subscription = await self._find_subscription(session, subscriber_id, target)
            if not subscription:
                await send_rich_message(client, room.room_id, "No active subscription found.", "No active subscription found.")
                return

            subscription.delivery_mode = new_mode
            if new_mode == "summary":
                # First digest goes out as soon as pending articles exist
                subscription.last_summary_at = None

            # Re-stamp pending queue items so in-flight articles follow the new mode
            q_update = (
                update(RSSQueueItem)
                .where(
                    RSSQueueItem.subscriber_id == subscription.subscriber_id,
                    RSSQueueItem.feed_id == subscription.feed_id,
                    RSSQueueItem.status == "pending"
                )
                .values(delivery_mode=new_mode)
            )
            await session.execute(q_update)
            await session.commit()
            # Capture display values before the session closes (feed relationship is joined-loaded)
            feed_name = subscription.feed.name if subscription.feed else subscription.feed_id

            msg = f"Delivery mode for '{feed_name}' set to '{new_mode}'."
            html_msg = f"✅ Delivery mode for <b>{html.escape(str(feed_name))}</b> set to <code>{new_mode}</code>."
            if new_mode == "summary":
                lang = subscription.summary_language or "auto"
                msg += f" Summaries are delivered every {self._summary_interval // 60} minutes (language: {lang})."
                html_msg += f" Summaries are delivered every {self._summary_interval // 60} minutes (language: <code>{html.escape(lang)}</code>)."
            await send_rich_message(client, room.room_id, msg, html_msg)

    async def _handle_language(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText, lang_args: List[str]) -> None:
        """Sets the summary language for an existing summary-mode subscription."""
        if len(lang_args) < 2:
            await send_rich_message(
                client, room.room_id,
                "Usage: !rss language <id_or_slug_or_url> <auto|de|en|fr|...>",
                "Usage: <code>!rss language &lt;id_or_slug_or_url&gt; &lt;auto|de|en|fr|...&gt;</code>"
            )
            return

        target = lang_args[0].strip()
        language = lang_args[1].strip().lower()
        if not language:
            await send_rich_message(client, room.room_id, "Language must not be empty.", "❌ Language must not be empty.")
            return

        subscriber_id = room.canonical_alias if room.canonical_alias else room.room_id
        if len(room.users) <= 2:
            subscriber_id = event.sender

        async with get_db_session() as session:
            subscription = await self._find_subscription(session, subscriber_id, target)
            if not subscription:
                await send_rich_message(client, room.room_id, "No active subscription found.", "No active subscription found.")
                return

            subscription.summary_language = language[:8]
            await session.commit()
            feed_name = subscription.feed.name if subscription.feed else subscription.feed_id

            msg = f"Summary language for '{feed_name}' set to '{language}'."
            html_msg = f"✅ Summary language for <b>{html.escape(str(feed_name))}</b> set to <code>{html.escape(language)}</code>."
            await send_rich_message(client, room.room_id, msg, html_msg)

    async def _handle_queue_status(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText) -> None:
        """Reports the current status of the RSS delivery queue and pacing rate."""
        async with get_db_session() as session:
            pending_q = (
                select(func.count(RSSQueueItem.id))
                .where(RSSQueueItem.status == "pending", RSSQueueItem.delivery_mode == "timeline")
            )
            summary_q = (
                select(func.count(RSSQueueItem.id))
                .where(RSSQueueItem.status == "pending", RSSQueueItem.delivery_mode == "summary")
            )
            delivered_q = select(func.count(RSSQueueItem.id)).where(RSSQueueItem.status == "delivered")
            
            pending_count = (await session.execute(pending_q)).scalar() or 0
            summary_count = (await session.execute(summary_q)).scalar() or 0
            delivered_count = (await session.execute(delivered_q)).scalar() or 0
            
            # Fetch next 3 pending articles (timeline items are paced one-by-one)
            next_items_q = (
                select(RSSQueueItem)
                .where(RSSQueueItem.status == "pending", RSSQueueItem.delivery_mode == "timeline")
                .order_by(RSSQueueItem.created_at.asc(), RSSQueueItem.id.asc())
                .limit(3)
            )
            next_items = (await session.execute(next_items_q)).scalars().all()

        now_ts = datetime.now().timestamp()
        time_until_next_poll = max(0, int((self._last_poll_time + self._poll_interval_seconds) - now_ts))
        mins_until_poll = time_until_next_poll // 60
        secs_until_poll = time_until_next_poll % 60

        rate_str = (
            f"1 article every {int(self._current_interval)}s"
            if self._current_interval < 60
            else f"1 article every {round(self._current_interval / 60, 1)}m ({int(self._current_interval)}s)"
        )

        html = [
            "<b>📊 RSS Delivery Queue & Pacing Status</b>",
            f"• <b>Pending Articles in Queue:</b> <code>{pending_count}</code>",
            f"• <b>Pending Awaiting Summary:</b> <code>{summary_count}</code>",
            f"• <b>Total Delivered:</b> <code>{delivered_count}</code>",
            f"• <b>Current Output Rate:</b> <code>{rate_str}</code>",
            f"• <b>Poll Cycle Interval:</b> <code>{self._poll_interval_seconds // 60} minutes</code>",
            f"• <b>Next Feed Poll In:</b> <code>{mins_until_poll}m {secs_until_poll}s</code>",
        ]

        if next_items:
            html.append("<br><b>Up Next in Queue:</b><ul>")
            for idx, item in enumerate(next_items, 1):
                html.append(f"<li>#{idx}: <b>{item.feed_name or 'Feed'}</b>: <a href='{item.link}'>{item.title}</a> (target: <code>{item.subscriber_id}</code>)</li>")
            html.append("</ul>")
        elif pending_count == 0:
            html.append("<br><i>Queue is currently empty. Next articles will be enqueued on the upcoming poll cycle.</i>")

        plain = (
            f"RSS Queue Status:\n"
            f"- Pending in Queue: {pending_count}\n"
            f"- Pending Awaiting Summary: {summary_count}\n"
            f"- Total Delivered: {delivered_count}\n"
            f"- Output Rate: {rate_str}\n"
            f"- Next Poll in: {mins_until_poll}m {secs_until_poll}s"
        )
        await send_rich_message(client, room.room_id, plain, "<br>".join(html))

    async def _handle_poll_now(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText) -> None:
        """Triggers an immediate poll of all subscribed feeds and recalculates pacing."""
        await send_rich_message(
            client, room.room_id,
            "Triggering RSS feed poll now...",
            "⏳ <b>Polling all subscribed RSS feeds now...</b>"
        )
        enqueued = await self.poll_all_feeds(client)
        msg_html = f"✅ <b>Feed poll completed!</b> Enqueued <code>{enqueued}</code> new articles for paced delivery."
        msg_plain = f"Feed poll completed. Enqueued {enqueued} new articles for paced delivery."
        await send_rich_message(client, room.room_id, msg_plain, msg_html)

    async def stop(self) -> None:
        """Gracefully cancels dispatcher worker and cleans up background loops."""
        logger.info("Stopping RSSPlugin background tasks...")
        self._is_running = False
        self._new_item_event.set()
        for task in (self._dispatcher_task, self._summary_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._dispatcher_task = None
        self._summary_task = None

    async def on_tick(self, client: AsyncClient) -> None:
        """Periodic background callback triggered by the bot's scheduler.
        Ensures the queue dispatcher is alive and triggers feed polling when interval elapses.
        """
        # 1. Ensure queue dispatcher worker is active
        if self._dispatcher_task is None or self._dispatcher_task.done():
            self._dispatcher_task = asyncio.create_task(self._run_queue_dispatcher(client))

        # 2. Ensure the summary scheduler worker is active
        if self._summary_task is None or self._summary_task.done():
            self._summary_task = asyncio.create_task(self._run_summary_scheduler(client))

        # 3. Check if polling interval has elapsed
        now_ts = datetime.now().timestamp()
        if self._last_poll_time == 0.0 or (now_ts - self._last_poll_time >= self._poll_interval_seconds):
            await self.poll_all_feeds(client)

    async def poll_all_feeds(self, client: Optional[AsyncClient] = None) -> int:
        """Polls external RSS feeds and enqueues matched articles into the delivery queue.

        Returns:
            The number of newly enqueued articles.
        """
        async with self._poll_lock:
            now_ts = datetime.now().timestamp()
            self._last_poll_time = now_ts
            logger.info("Executing RSS feed polling task...", poll_interval=self._poll_interval_seconds)

            # 1. Fetch active feeds with subscriptions
            async with get_db_session() as session:
                q_feeds = select(RSSFeed).join(RSSSubscription).group_by(RSSFeed.id)
                res_feeds = await session.execute(q_feeds)
                feeds = [{"id": f.id, "url": f.url, "name": f.name} for f in res_feeds.scalars().all()]

            if not feeds:
                logger.debug("No active RSS feeds to poll.")
                return 0

            new_articles_count = 0

            async with httpx.AsyncClient(timeout=15.0) as http_client:
                for feed_dict in feeds:
                    async with get_db_session() as session:
                        try:
                            feed = await session.get(RSSFeed, feed_dict["id"])
                            if not feed:
                                continue

                            logger.info("Polling feed URL", url=feed.url)
                            resp = await http_client.get(feed.url)
                            if resp.status_code != 200:
                                logger.warning("Feed poll status error", url=feed.url, status=resp.status_code)
                                continue

                            parsed = feedparser.parse(resp.text)
                            if not feed.name and "title" in parsed.feed:
                                feed.name = parsed.feed.title
                            feed.last_polled = datetime.now()

                            # Process up to 25 latest entries
                            for entry in parsed.entries[:25]:
                                entry_id = entry.id if "id" in entry else (entry.link if "link" in entry else entry.title)
                                if not entry_id:
                                    continue

                                # Check history to prevent duplicate ingestion
                                q_hist = select(RSSHistory).where(
                                    RSSHistory.entry_id == entry_id,
                                    RSSHistory.feed_id == feed.id
                                )
                                res_hist = await session.execute(q_hist)
                                if res_hist.scalar_one_or_none():
                                    continue

                                # Mark in history with nested transaction
                                try:
                                    async with session.begin_nested():
                                        history_item = RSSHistory(entry_id=entry_id, feed_id=feed.id)
                                        session.add(history_item)
                                except Exception:
                                    logger.debug("Duplicate RSS entry detected during concurrent write; skipping", entry_id=entry_id)
                                    continue

                                # Check matching subscriptions for this feed
                                q_subs = select(RSSSubscription).where(RSSSubscription.feed_id == feed.id)
                                res_subs = await session.execute(q_subs)
                                subscriptions = res_subs.scalars().all()

                                for sub in subscriptions:
                                    is_match, reasons = self._evaluate_relevance(entry, sub)
                                    if is_match:
                                        title = entry.get("title", "No Title")
                                        link = entry.get("link", "#")
                                        summary = entry.get("summary", entry.get("description", ""))
                                        
                                        # Queue article into delivery queue
                                        queue_item = RSSQueueItem(
                                            subscriber_id=sub.subscriber_id,
                                            feed_id=feed.id,
                                            entry_id=entry_id,
                                            title=title,
                                            link=link,
                                            summary=summary,
                                            matches=reasons,
                                            feed_name=feed.name or feed.url,
                                            delivery_mode=sub.delivery_mode or "timeline",
                                            status="pending"
                                        )
                                        session.add(queue_item)
                                        new_articles_count += 1
                                        logger.info(
                                            "Enqueued RSS article match",
                                            title=title,
                                            subscriber=sub.subscriber_id,
                                            feed=feed.name
                                        )

                            await session.commit()
                        except Exception as e:
                            logger.exception("Failed to poll feed", url=feed_dict["url"], error=str(e))

            if new_articles_count > 0:
                self._new_item_event.set()
                logger.info("RSS feed polling complete", enqueued=new_articles_count)
            else:
                logger.info("RSS feed polling complete: No new articles enqueued.")

            return new_articles_count

    async def _run_queue_dispatcher(self, client: AsyncClient) -> None:
        """Background worker that continuously outputs queued articles one-by-one at a calculated predictable rate."""
        logger.info("Starting RSS queue output dispatcher worker...")

        while self._is_running:
            try:
                # 1. Check pending items count (timeline items only; summary batches are handled
                # by the dedicated summary scheduler)
                async with get_db_session() as session:
                    count_q = select(func.count(RSSQueueItem.id)).where(
                        RSSQueueItem.status == "pending",
                        RSSQueueItem.delivery_mode == "timeline"
                    )
                    count_res = await session.execute(count_q)
                    pending_count = count_res.scalar() or 0

                # If queue is empty, wait for new items or timeout
                if pending_count == 0:
                    self._new_item_event.clear()
                    try:
                        await asyncio.wait_for(self._new_item_event.wait(), timeout=15.0)
                    except asyncio.TimeoutError:
                        pass
                    continue

                # 2. Calculate dynamic pacing interval
                now_ts = datetime.now().timestamp()
                time_until_next_poll = (self._last_poll_time + self._poll_interval_seconds) - now_ts
                if time_until_next_poll <= 0:
                    time_until_next_poll = self._poll_interval_seconds

                # Spread pending items evenly across the time remaining until the next poll
                ideal_interval = time_until_next_poll / pending_count

                # Clamp within sensible limits
                interval = max(self._min_delivery_interval, min(ideal_interval, self._max_delivery_interval))
                self._current_interval = interval

                # 3. Fetch the oldest pending article to deliver (timeline items only)
                item_to_deliver = None
                async with get_db_session() as session:
                    item_q = (
                        select(RSSQueueItem)
                        .where(RSSQueueItem.status == "pending", RSSQueueItem.delivery_mode == "timeline")
                        .order_by(RSSQueueItem.created_at.asc(), RSSQueueItem.id.asc())
                        .limit(1)
                    )
                    res_item = await session.execute(item_q)
                    item_to_deliver = res_item.scalar_one_or_none()
                    if item_to_deliver:
                        item_id = item_to_deliver.id
                        subscriber_id = item_to_deliver.subscriber_id
                        title = item_to_deliver.title
                        link = item_to_deliver.link
                        summary = item_to_deliver.summary
                        matches = item_to_deliver.matches
                        feed_name = item_to_deliver.feed_name

                if not item_to_deliver:
                    continue

                # 4. Deliver the article to Matrix
                logger.info(
                    "Delivering queued RSS article",
                    item_id=item_id,
                    title=title,
                    subscriber=subscriber_id,
                    pending_remaining=pending_count - 1,
                    pacing_interval_sec=round(interval, 1)
                )

                delivery_success = False
                delivery_error = None
                try:
                    await self._deliver_alert_from_queue(
                        client=client,
                        subscriber_id=subscriber_id,
                        feed_name=feed_name,
                        title=title,
                        link=link,
                        summary=summary,
                        matches=matches or []
                    )
                    delivery_success = True
                    self._last_delivery_time = datetime.now().timestamp()
                except Exception as e:
                    delivery_error = str(e)
                    logger.error("Failed to deliver queued RSS article", item_id=item_id, error=delivery_error)

                # 5. Update item status in database
                async with get_db_session() as session:
                    db_item = await session.get(RSSQueueItem, item_id)
                    if db_item:
                        db_item.attempts += 1
                        if delivery_success:
                            db_item.status = "delivered"
                            db_item.delivered_at = datetime.now()
                        else:
                            db_item.last_error = delivery_error
                            if db_item.attempts >= 5:
                                db_item.status = "failed"
                                logger.warning("RSS queue item marked as failed after max attempts", item_id=item_id)
                        await session.commit()

                # 6. Sleep for the paced interval before next delivery
                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                logger.info("RSS queue dispatcher loop cancelled.")
                break
            except Exception as e:
                logger.exception("Unexpected error in RSS queue dispatcher", error=str(e))
                await asyncio.sleep(10.0)

    async def _run_summary_scheduler(self, client: AsyncClient) -> None:
        """Background worker that periodically batches pending articles per summary subscription
        and delivers one LLM-generated digest message per (subscriber, feed).

        Runs independently of the timeline pacing loop. Skips silently when a subscription has
        no pending articles ('skip if no update happened').
        """
        logger.info("Starting RSS summary scheduler worker...", interval_seconds=self._summary_interval)

        while self._is_running:
            try:
                await self._process_summary_subscriptions(client)
            except asyncio.CancelledError:
                logger.info("RSS summary scheduler loop cancelled.")
                break
            except Exception as e:
                logger.exception("Unexpected error in RSS summary scheduler", error=str(e))
                await asyncio.sleep(10.0)

            await asyncio.sleep(30.0)

    async def _process_summary_subscriptions(self, client: AsyncClient) -> None:
        """Checks every summary-mode subscription and delivers due digests."""
        now = datetime.now()

        async with get_db_session() as session:
            subs_q = (
                select(RSSSubscription)
                .where(RSSSubscription.delivery_mode == "summary")
                .options(joinedload(RSSSubscription.feed))
            )
            subscriptions = (await session.execute(subs_q)).scalars().unique().all()

        for sub in subscriptions:
            if not self._is_running:
                break

            # 1. Respect the configured summary interval (per subscription)
            if sub.last_summary_at is not None:
                elapsed = (now - sub.last_summary_at).total_seconds()
                if elapsed < self._summary_interval:
                    continue

            # 2. Gather the pending batch for this (subscriber, feed), oldest first
            async with get_db_session() as session:
                items_q = (
                    select(RSSQueueItem)
                    .where(
                        RSSQueueItem.subscriber_id == sub.subscriber_id,
                        RSSQueueItem.feed_id == sub.feed_id,
                        RSSQueueItem.status == "pending",
                        RSSQueueItem.delivery_mode == "summary"
                    )
                    .order_by(RSSQueueItem.created_at.asc(), RSSQueueItem.id.asc())
                    .limit(self._summary_max_articles)
                )
                items = (await session.execute(items_q)).scalars().all()

            # 3. Skip if no update happened since the last summary
            if not items:
                continue

            await self._deliver_summary_batch(
                client,
                subscription_id=sub.id,
                feed=sub.feed,
                subscriber_id=sub.subscriber_id,
                language=sub.summary_language or "auto",
                items=items
            )

    async def _deliver_summary_batch(
        self,
        client: AsyncClient,
        subscription_id: int,
        feed: RSSFeed,
        subscriber_id: str,
        language: str,
        items: List[RSSQueueItem]
    ) -> None:
        """Generates (or falls back to) a digest for the given batch and delivers it as one message."""
        target_room_id = await self._resolve_target_room(client, subscriber_id)
        if not target_room_id:
            logger.warning("Could not resolve target room for summary subscriber", subscriber_id=subscriber_id)
            return

        feed_name = feed.name or feed.url
        articles = [
            {
                "title": item.title,
                "link": item.link,
                "summary": item.summary or ""
            }
            for item in items
        ]

        # 1. Attempt the LLM summary (None on failure or missing API key)
        summary_text = await self._generate_feed_summary(feed_name, language, articles)

        now = datetime.now()
        item_ids = [item.id for item in items]

        if summary_text is None:
            # 2. Failure path: count the attempt; after 3 failed attempts deliver unsummarized
            async with get_db_session() as session:
                await session.execute(
                    update(RSSQueueItem)
                    .where(RSSQueueItem.id.in_(item_ids))
                    .values(attempts=RSSQueueItem.attempts + 1)
                )
                await session.commit()
                refresh_q = select(RSSQueueItem.attempts).where(RSSQueueItem.id.in_(item_ids))
                attempts = [row[0] for row in (await session.execute(refresh_q)).all()]

            if attempts and min(attempts) < 3:
                logger.warning(
                    "RSS summary generation failed; will retry",
                    feed=feed_name,
                    subscriber=subscriber_id,
                    batch_size=len(items)
                )
                return

            logger.warning(
                "RSS summary generation failed repeatedly; delivering unsummarized digest",
                feed=feed_name,
                subscriber=subscriber_id
            )

        # 3. Compose and send the digest message
        summary_html = ""
        summary_plain = ""
        if summary_text:
            summary_html = f"<p>{html.escape(summary_text).replace(chr(10), '<br>')}</p>"
            summary_plain = f"\n{summary_text}\n"

        headline_html = "".join(
            f"<li><a href='{a['link']}'>{html.escape(a['title'])}</a></li>" for a in articles
        )
        headline_plain = "".join(
            f"{idx}. {a['title']}\n   {a['link']}\n" for idx, a in enumerate(articles, 1)
        )

        html_body = (
            f"📰 <b>{html.escape(str(feed_name))}</b> — <b>Summary</b> ({len(articles)} articles)"
            f"{summary_html}"
            f"<b>Headlines:</b><ul>{headline_html}</ul>"
        )
        plain_body = (
            f"📰 {feed_name} — Summary ({len(articles)} articles)"
            f"{summary_plain}"
            f"Headlines:\n{headline_plain}"
        )

        try:
            await send_rich_message(client, target_room_id, plain_body, html_body)
        except Exception as e:
            logger.error("Failed to deliver RSS summary digest", target=target_room_id, error=str(e))
            raise

        # 4. Mark the batch delivered and anchor the next summary interval
        async with get_db_session() as session:
            await session.execute(
                update(RSSQueueItem)
                .where(RSSQueueItem.id.in_(item_ids))
                .values(status="delivered", delivered_at=now)
            )
            db_sub = await session.get(RSSSubscription, subscription_id)
            if db_sub:
                db_sub.last_summary_at = now
            await session.commit()

        logger.info(
            "Delivered RSS summary digest",
            feed=feed_name,
            subscriber=subscriber_id,
            articles=len(articles),
            summarized=summary_text is not None
        )

    async def _generate_feed_summary(self, feed_name: str, language: str, articles: List[Dict[str, Any]]) -> Optional[str]:
        """Calls the Gemini API to summarize a batch of articles from one feed.

        Returns the summary text, or None on failure (missing API key, HTTP error, empty response).
        """
        api_key = getattr(self.settings, "gemini_api_key", None)
        if not api_key:
            logger.warning("GEMINI_API_KEY not configured; RSS summaries unavailable", feed=feed_name)
            return None

        article_lines = []
        for idx, article in enumerate(articles, 1):
            teaser = re.sub(r"<[^>]+>", "", article.get("summary") or "").strip()
            if len(teaser) > 300:
                teaser = teaser[:297] + "..."
            article_lines.append(
                f"{idx}. {article.get('title', 'No Title')}\n   Link: {article.get('link', '')}\n   Teaser: {teaser or '(none)'}"
            )
        prompt = "\n".join(article_lines)

        if language == "auto":
            language_directive = "Write the summary in the same language as the source articles."
        else:
            language_directive = f"Write the summary in {language}."

        system_instruction = (
            "You are a news digest writer for a Matrix chat bot.\n"
            f"Below are {len(articles)} recent headlines with teaser texts from the RSS feed '{feed_name}'.\n"
            "Write a concise summary of the events they describe: 2-4 short paragraphs, factual and neutral. "
            "Group related stories and highlight the most important developments. "
            "Do not invent facts that are not supported by the headlines and teasers. "
            "Do not append a headline list; the bot adds one itself. "
            "Output plain text only, no markdown formatting."
            f" {language_directive}"
        )

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self._summary_model}:generateContent?key={api_key}"
        payload = {
            "contents": [{
                "parts": [{"text": prompt}]
            }],
            "systemInstruction": {
                "parts": [{"text": system_instruction}]
            },
            "generationConfig": {
                "temperature": 0.3,
                "maxOutputTokens": self._summary_max_output_tokens
            }
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as http_client:
                resp = await http_client.post(url, json=payload)
                if resp.status_code != 200:
                    logger.error("Gemini summary API error", status=resp.status_code, body=resp.text[:500], model=self._summary_model)
                    return None

                result = resp.json()
                candidates = result.get("candidates") or []
                if not candidates:
                    logger.warning("Gemini summary returned no candidates", feed=feed_name)
                    return None
                parts = candidates[0].get("content", {}).get("parts") or []
                text = "".join(part.get("text", "") for part in parts).strip()
                return text or None
        except Exception as e:
            logger.error("Failed to call Gemini summary API", feed=feed_name, error=str(e))
            return None

    def _evaluate_relevance(self, entry: Any, sub: RSSSubscription) -> tuple[bool, List[str]]:
        """Evaluates whether a feed entry matches a subscription's filters."""
        # Clean text compilation (title + summary/description)
        title = entry.get("title", "")
        summary = entry.get("summary", entry.get("description", ""))
        full_text = f"{title} {summary}"

        # If no filters are defined, it's a catch-all subscription
        has_filters = any([sub.keywords, sub.companies, sub.geographies, sub.representatives])
        if not has_filters:
            return True, [""] # was: ["All content (no filters defined)"]

        matches = []

        # 1. Check Keywords (case-insensitive substring match)
        if sub.keywords:
            for kw in sub.keywords:
                if kw.lower() in full_text.lower():
                    matches.append(f"Keyword: '{kw}'")

        # Helper for word boundary regex matches (prevents matching "India" in "Indiana")
        def word_match(pattern: str, text: str) -> bool:
            escaped = re.escape(pattern)
            return bool(re.search(rf"\b{escaped}\b", text, re.IGNORECASE))

        # 2. Check Companies/Tickers
        if sub.companies:
            for co in sub.companies:
                if word_match(co, full_text):
                    matches.append(f"Company: '{co}'")

        # 3. Check Geographies
        if sub.geographies:
            for geo in sub.geographies:
                if word_match(geo, full_text):
                    matches.append(f"Geography: '{geo}'")

        # 4. Check Key Representatives
        if sub.representatives:
            for rep in sub.representatives:
                if word_match(rep, full_text):
                    matches.append(f"Representative: '{rep}'")

        if matches:
            return True, matches
        return False, []

    async def _resolve_target_room(self, client: AsyncClient, subscriber_id: str) -> Optional[str]:
        """Resolves a subscriber identifier (alias, room ID, user ID, room name) to a valid Matrix room ID."""
        if not subscriber_id:
            return None

        # 1. Direct canonical Matrix room ID
        if subscriber_id.startswith("!"):
            return subscriber_id

        # 2. Check cached room IDs
        if subscriber_id in self._room_id_cache:
            return self._room_id_cache[subscriber_id]

        # 3. If subscriber is a room alias (#...), resolve it
        if subscriber_id.startswith("#"):
            if hasattr(client, "rooms"):
                for r_id, room in client.rooms.items():
                    if room.canonical_alias == subscriber_id:
                        self._room_id_cache[subscriber_id] = r_id
                        return r_id
            try:
                res = await client.room_resolve_alias(subscriber_id)
                if isinstance(res, RoomResolveAliasResponse):
                    self._room_id_cache[subscriber_id] = res.room_id
                    return res.room_id
                else:
                    logger.warning("Could not resolve room alias via homeserver", alias=subscriber_id, res=str(res))
            except Exception as e:
                logger.error("Exception resolving room alias", alias=subscriber_id, error=str(e))

        # 4. Check client.rooms by name or canonical alias
        if hasattr(client, "rooms"):
            for room_id, room in client.rooms.items():
                if room.name == subscriber_id or room.canonical_alias == subscriber_id:
                    self._room_id_cache[subscriber_id] = room_id
                    return room_id

            # 5. Direct user DM
            if subscriber_id.startswith("@"):
                for room_id, room in client.rooms.items():
                    if len(room.users) <= 2 and subscriber_id in room.users:
                        self._room_id_cache[subscriber_id] = room_id
                        return room_id

        # 6. Fallbacks: default channel override or startup rooms list
        fallbacks = []
        if self._default_channel and self._default_channel != subscriber_id:
            fallbacks.append(self._default_channel)
        if self.settings.startup_rooms_list:
            for s_room in self.settings.startup_rooms_list:
                if s_room != subscriber_id and s_room not in fallbacks:
                    fallbacks.append(s_room)
        if "#sauna:pramari.de" != subscriber_id and "#sauna:pramari.de" not in fallbacks:
            fallbacks.append("#sauna:pramari.de")

        for fallback in fallbacks:
            resolved = await self._resolve_target_room(client, fallback)
            if resolved:
                self._room_id_cache[subscriber_id] = resolved
                return resolved

        return None

    async def _deliver_alert_from_queue(
        self,
        client: AsyncClient,
        subscriber_id: str,
        feed_name: Optional[str],
        title: str,
        link: str,
        summary: Optional[str],
        matches: List[str]
    ) -> None:
        """Sends formatted HTML alert details to the matched subscriber room or user DM."""
        target_room_id = await self._resolve_target_room(client, subscriber_id)
        if not target_room_id:
            logger.warning("Could not resolve target room for subscriber", subscriber_id=subscriber_id)
            raise RuntimeError(f"Could not resolve target room for subscriber: {subscriber_id}")

        summary_clean = ""
        if summary:
            summary_clean = re.sub(r"<[^>]+>", "", summary)[:300]
            if len(summary) > 300:
                summary_clean += "..."

        reasons_str = ", ".join(matches) if matches else "Subscription feed update"

        html_body = (
            f"📰 <b>{feed_name or 'News Feed'}</b><br>"
            f"<b><a href='{link}'>{title}</a></b><br>"
            f"<i>Matches: {reasons_str}</i>"
        )
        if summary_clean:
            html_body += f"<br><br><blockquote>{summary_clean}</blockquote>"

        plain_body = f"📰 {feed_name or 'News Feed'}: {title}\n{link}"
        if reasons_str:
            plain_body += f"\nMatches: {reasons_str}"

        try:
            await send_rich_message(client, target_room_id, plain_body, html_body)
        except Exception as e:
            logger.error("Failed to deliver RSS notification", target=target_room_id, error=str(e))
            raise

    async def _send_usage(self, client: AsyncClient, room_id: str) -> None:
        """Sends command assistance usage block."""
        await send_rich_message(
            client, room_id,
            "RSS Commands:\n"
            "- !rss queue: Displays current delivery queue size, output rate, and pacing status\n"
            "- !rss poll: Triggers an immediate poll of all feeds\n"
            "- !rss feeds: Lists curated major German news feeds\n"
            "- !rss list: Lists active subscriptions in this room\n"
            "- !rss defaults: Subscribes this room to all top German news feeds\n"
            "- !rss subscribe <url_or_slug> [--mode timeline|summary] [--lang <code>] [--keywords ...] [--companies ...] [--geo ...] [--representatives ...]\n"
            "- !rss unsubscribe <id_or_slug_or_url>: Removes an active subscription\n"
            "- !rss mode <id_or_slug_or_url> <timeline|summary>: Switches delivery mode (summary = batched AI digest)\n"
            "- !rss language <id_or_slug_or_url> <auto|de|en|...>: Sets the summary language\n"
            "- !help: Shows this usage guide",
            "<b>📰 RSS Capability Commands:</b><ul>"
            "<li><code>!rss queue</code>: Displays delivery queue status, dynamic rate pacing, and next poll ETA.</li>"
            "<li><code>!rss poll</code>: Triggers an immediate poll of all subscribed feeds.</li>"
            "<li><code>!rss feeds</code>: Lists curated major German news feeds (Tagesschau, Spiegel, FAZ, Zeit, etc.).</li>"
            "<li><code>!rss list</code>: Lists active subscriptions in this room.</li>"
            "<li><code>!rss defaults</code>: Quickly subscribes this room to top German news feeds.</li>"
            "<li><code>!rss subscribe &lt;url_or_slug&gt; [filters]</code>: Subscribes with optional filters and delivery mode (e.g. <code>!rss subscribe tagesschau</code> or <code>!rss subscribe heise --mode summary --lang de</code>).</li>"
            "<li><code>!rss unsubscribe &lt;id_or_slug_or_url&gt;</code>: Removes an active subscription.</li>"
            "<li><code>!rss mode &lt;id_or_slug_or_url&gt; &lt;timeline|summary&gt;</code>: timeline delivers each article paced; summary delivers one AI-generated digest per interval (default hourly).</li>"
            "<li><code>!rss language &lt;id_or_slug_or_url&gt; &lt;auto|de|en|...&gt;</code>: Sets the language of AI summaries (auto mirrors the source articles).</li>"
            "<li><code>!help</code>: Displays this help message.</li>"
            "</ul>"
        )

    def get_help(self) -> str:
        return (
            "• <b>!rss queue</b>: Displays delivery queue size, dynamic output rate, and next poll ETA.<br>"
            "• <b>!rss poll</b>: Triggers an immediate poll of all feeds.<br>"
            "• <b>!rss feeds</b>: Lists curated major German news feeds.<br>"
            "• <b>!rss defaults</b>: Subscribes this room to top German news feeds.<br>"
            "• <b>!rss list</b>: Lists active subscriptions for this room/context.<br>"
            "• <b>!rss subscribe &lt;url_or_slug&gt; [filters]</code>: Subscribes to an RSS feed URL or slug alias with filtering options (supports <code>--mode summary</code>).<br>"
            "• <b>!rss unsubscribe &lt;id_or_slug_or_url&gt;</b>: Unsubscribes from a feed.<br>"
            "• <b>!rss mode &lt;id_or_slug_or_url&gt; &lt;timeline|summary&gt;</b>: Switches between paced single articles and AI summary digests.<br>"
            "• <b>!rss language &lt;id_or_slug_or_url&gt; &lt;auto|de|en|...&gt;</b>: Sets the language of AI summaries."
        )
