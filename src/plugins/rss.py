import asyncio
import feedparser
import httpx
import shlex
import re
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import select, delete
from sqlalchemy.orm import joinedload
from nio import AsyncClient, MatrixRoom, RoomMessageText
from src.core.plugin import Plugin
from src.core.database import get_db_session
from src.models.rss import RSSFeed, RSSSubscription, RSSHistory
from src.models.user import User
from src.config import load_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

async def send_rich_message(client: AsyncClient, room_id: str, plain: str, html: str) -> None:
    """Helper to send formatted HTML messages to a Matrix room."""
    await client.room_send(
        room_id=room_id,
        message_type="m.room.message",
        content={
            "msgtype": "m.text",
            "format": "org.matrix.custom.html",
            "body": plain,
            "formatted_body": html
        }
    )

class RSSPlugin(Plugin):
    """RSS Syndication capability plugin. Parses feeds and runs entity relevance filters."""

    def __init__(self):
        self._last_tick_time = 0
        self._poll_interval_seconds = 900  # 15 minutes default

    @property
    def plugin_id(self) -> str:
        return "rss"

    @property
    def commands(self) -> List[str]:
        return ["rss"]

    async def on_message(
        self, 
        client: AsyncClient, 
        room: MatrixRoom, 
        event: RoomMessageText, 
        command: str, 
        args: List[str]
    ) -> None:
        if not args:
            await self._send_usage(client, room.room_id)
            return

        sub_cmd = args[0].lower()

        # 1. Command: !rss list
        if sub_cmd == "list":
            await self._handle_list(client, room, event)
            return

        # 2. Command: !rss subscribe <url> [filters]
        elif sub_cmd == "subscribe":
            await self._handle_subscribe(client, room, event, args[1:])
            return

        # 3. Command: !rss unsubscribe <id_or_url>
        elif sub_cmd == "unsubscribe":
            await self._handle_unsubscribe(client, room, event, args[1:])
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
                html_lines.append(f"<li>{sub.feed_id} - <a href='{feed_url}'>{feed_name}</a> (subscribed by <code>{sub.subscriber_id}</code>)</li>")
                plain_lines.append(f"  {sub.feed_id} - {feed_name} ({feed_url}) (subscribed by {sub.subscriber_id})")
            html_lines.append("</ul>")

            await send_rich_message(client, room.room_id, "\n".join(plain_lines), "".join(html_lines))

    async def _handle_subscribe(self, client: AsyncClient, room: MatrixRoom, event: RoomMessageText, sub_args: List[str]) -> None:
        """Subscribes the current room context to an RSS feed URL, parsing filter arguments."""
        settings = load_settings()
        if not sub_args:
            await send_rich_message(client, room.room_id, "Usage: !rss subscribe <url> [--keywords ...] [--companies ...] [--geo ...] [--representatives ...]", "Usage: <code>!rss subscribe &lt;url&gt; [--keywords ...]</code>")
            return

        feed_url = sub_args[0].strip()

        # Parse flags using argparse-like strategy via standard shlex
        keywords, companies, geos, reps = None, None, None, None
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
                else:
                    i += 1
        except Exception as e:
            logger.error("Failed to parse filter flags", error=str(e))
            await send_rich_message(client, room.room_id, "Error parsing filter options.", "❌ <b>Error parsing filter options.</b> Make sure quotes are balanced.")
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
                    feed_name = parsed.feed.title if 'title' in parsed.feed else feed_url
                except Exception:
                    feed_name = feed_url
                
                feed = RSSFeed(url=feed_url, name=feed_name)
                session.add(feed)
                await session.flush()

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
                representatives=reps
            )
            session.add(subscription)
            
            html_msg = (
                f"✅ <b>Successfully Subscribed!</b><br>"
                f"• <b>Feed:</b> {feed.name or feed.url}<br>"
                f"• <b>Keywords:</b> {keywords or 'None'}<br>"
                f"• <b>Companies:</b> {companies or 'None'}<br>"
                f"• <b>Geography:</b> {geos or 'None'}<br>"
                f"• <b>Representatives:</b> {reps or 'None'}"
            )
            plain_msg = f"Successfully Subscribed!\n- Feed: {feed.name or feed.url}\n- Keywords: {keywords}\n- Companies: {companies}"
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
                # Query feed first by URL
                q_feed = select(RSSFeed).where(RSSFeed.url == target)
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

    async def on_tick(self, client: AsyncClient) -> None:
        """Triggered periodically by the bot scheduler. Ingests and processes feeds."""
        now_ts = int(datetime.now().timestamp())
        if now_ts - self._last_tick_time < self._poll_interval_seconds:
            return  # Wait until polling interval elapsed
        
        self._last_tick_time = now_ts
        logger.info("Executing RSS feed polling task...")

        # 1. Fetch all feeds in a short-lived query session
        async with get_db_session() as session:
            q_feeds = select(RSSFeed).join(RSSSubscription).group_by(RSSFeed.id)
            res_feeds = await session.execute(q_feeds)
            feeds = [{"id": f.id, "url": f.url, "name": f.name} for f in res_feeds.scalars().all()]

        if not feeds:
            logger.debug("No active RSS feeds to poll.")
            return

        async with httpx.AsyncClient(timeout=10.0) as http_client:
            for feed_dict in feeds:
                # 2. Process each feed in its own isolated transaction session
                async with get_db_session() as session:
                    try:
                        # Fetch the feed record inside this session
                        feed = await session.get(RSSFeed, feed_dict["id"])
                        if not feed:
                            continue

                        logger.info("Polling feed URL", url=feed.url)
                        resp = await http_client.get(feed.url)
                        if resp.status_code != 200:
                            logger.warning("Feed poll status error", url=feed.url, status=resp.status_code)
                            continue

                        # Parse RSS feed content
                        parsed = feedparser.parse(resp.text)
                        
                        # Update feed metadata name if missing
                        if not feed.name and 'title' in parsed.feed:
                            feed.name = parsed.feed.title

                        # Update last polled time
                        feed.last_polled = datetime.now()

                        # Process feed items
                        for entry in parsed.entries[:15]:  # Limit to latest 15 items per poll
                            entry_id = entry.id if 'id' in entry else (entry.link if 'link' in entry else entry.title)
                            if not entry_id:
                                continue

                            # Check if item was already processed
                            q_hist = select(RSSHistory).where(
                                RSSHistory.entry_id == entry_id,
                                RSSHistory.feed_id == feed.id
                            )
                            res_hist = await session.execute(q_hist)
                            if res_hist.scalar_one_or_none():
                                continue  # Already processed

                            # Save to history with isolated nested transaction check
                            try:
                                async with session.begin_nested():
                                    history_item = RSSHistory(entry_id=entry_id, feed_id=feed.id)
                                    session.add(history_item)
                            except Exception:
                                logger.debug("Duplicate RSS entry detected during concurrent write; skipping", entry_id=entry_id)
                                continue

                            # Match against active subscriptions for this feed
                            q_subs = select(RSSSubscription).where(RSSSubscription.feed_id == feed.id)
                            res_subs = await session.execute(q_subs)
                            subscriptions = res_subs.scalars().all()

                            for sub in subscriptions:
                                is_match, reasons = self._evaluate_relevance(entry, sub)
                                if is_match:
                                    logger.info("Relevance match found!", entry_title=entry.get('title'), subscriber=sub.subscriber_id)
                                    await self._deliver_alert(client, sub.subscriber_id, feed, entry, reasons)

                    except Exception as e:
                        logger.exception("Failed to poll feed", url=feed_dict["url"], error=str(e))

    def _evaluate_relevance(self, entry: Any, sub: RSSSubscription) -> tuple[bool, List[str]]:
        """Evaluates whether a feed entry matches a subscription's filters."""
        # Clean text compilation (title + summary/description)
        title = entry.get("title", "")
        summary = entry.get("summary", entry.get("description", ""))
        full_text = f"{title} {summary}"

        # If no filters are defined, it's a catch-all subscription
        has_filters = any([sub.keywords, sub.companies, sub.geographies, sub.representatives])
        if not has_filters:
            return True, ["All content (no filters defined)"]

        matches = []

        # 1. Check Keywords (case-insensitive substring match)
        if sub.keywords:
            for kw in sub.keywords:
                if kw.lower() in full_text.lower():
                    matches.append(f"Keyword: '{kw}'")

        # Helper for word boundary regex matches (prevents matching "India" in "Indiana")
        def word_match(pattern: str, text: str) -> bool:
            # Escape pattern and enforce word boundaries
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

    async def _deliver_alert(self, client: AsyncClient, subscriber_id: str, feed: RSSFeed, entry: Any, reasons: List[str]) -> None:
        """Sends formatted HTML alert details to the matched subscriber room or user DM."""
        # Resolve target room ID
        target_room_id = None
        
        # If subscriber_id is room ID/alias directly
        if subscriber_id.startswith("!") or subscriber_id.startswith("#"):
            target_room_id = subscriber_id
        else:
            # Check if subscriber_id is "Product" or similar name
            # We can search room names or canonical aliases
            for room_id, room in client.rooms.items():
                if room.name == subscriber_id or room.canonical_alias == subscriber_id:
                    target_room_id = room_id
                    break
            
            # If not found, check if it's a registered user ID (send DM)
            if not target_room_id and subscriber_id.startswith("@"):
                # Search for a direct room containing the user
                for room_id, room in client.rooms.items():
                    if len(room.users) <= 2 and subscriber_id in room.users:
                        target_room_id = room_id
                        break
                        
        if not target_room_id:
            if subscriber_id == "Product":
                # Fallback mapping for prefilled subscriptions
                target_room_id = "#sauna:pramari.de"
            else:
                logger.warning("Could not resolve target room for subscriber", subscriber_id=subscriber_id)
                return

        title = entry.get("title", "No Title")
        link = entry.get("link", "#")
        summary = entry.get("summary", entry.get("description", ""))
        
        # Simple HTML tag stripping for summary snippet
        summary_clean = re.sub(r"<[^>]+>", "", summary)[:300]
        if len(summary) > 300:
            summary_clean += "..."

        reasons_str = ", ".join(reasons)

        html_body = (
            f"📰 <b>New Article Match: {feed.name or feed.url}</b><br>"
            f"<b><a href='{link}'>{title}</a></b><br>"
            f"<i>Matches: {reasons_str}</i><br><br>"
            f"Snippet:<br><blockquote>{summary_clean}</blockquote>"
        )
        plain_body = f"New Article Match: {feed.name or feed.url}\n{title}\nLink: {link}\nMatches: {reasons_str}"

        try:
            await send_rich_message(client, target_room_id, plain_body, html_body)
        except Exception as e:
            logger.error("Failed to deliver RSS notification", target=target_room_id, error=str(e))

    async def _send_usage(self, client: AsyncClient, room_id: str) -> None:
        """Sends command assistance usage block."""
        await send_rich_message(
            client, room_id,
            "RSS Commands:\n- !rss list\n- !rss subscribe <url> [--keywords ...] [--companies ...] [--geo ...] [--representatives ...]\n- !rss unsubscribe <id_or_url>",
            "<b>RSS capability commands:</b><ul>"
            "<li><code>!rss list</code>: Lists active subscriptions in the room.</li>"
            "<li><code>!rss subscribe &lt;url&gt; [filters]</code>: Subscribes target context with filters (comma-separated, e.g. <code>--companies Apple,Google</code>).</li>"
            "<li><code>!rss unsubscribe &lt;id_or_url&gt;</code>: Removes an active subscription.</li>"
            "</ul>"
        )

    def get_help(self) -> str:
        return (
            "• <b>!rss list</b>: Lists active subscriptions for this context.<br>"
            "• <b>!rss subscribe &lt;url&gt; [filters]</b>: Subscribes this channel/user to an RSS URL with filtering options.<br>"
            "• <b>!rss unsubscribe &lt;id_or_url&gt;</b>: Unsubscribes from a feed."
        )
