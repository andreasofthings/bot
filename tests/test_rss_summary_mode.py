import asyncio
import os
import uuid

import pytest

# Isolate this module from the configured live database: run migrations and all
# plugin DB access against a dedicated SQLite file (data/ is gitignored).
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///data/test_rss_summary.db"

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from nio import RoomResolveAliasResponse, RoomSendResponse
from sqlalchemy import select

from src.config import Settings
from src.core.database import close_engine, get_db_session, run_migrations
from src.fixtures.rss_feeds import find_fixture_by_slug_or_name
from src.models.rss import RSSFeed, RSSQueueItem, RSSSubscription
from src.plugins.rss import RSSPlugin


@pytest.fixture(autouse=True)
def setup_database():
    """Ensure database migrations run before each test."""
    run_migrations()


def run_async(coro_fn):
    """Executes an async test coroutine and disposes database connections cleanly."""
    async def _runner():
        try:
            return await coro_fn()
        finally:
            await close_engine()
    return asyncio.run(_runner())


def _make_plugin() -> RSSPlugin:
    return RSSPlugin(settings=Settings(
        matrix_user_id="@bot:matrix.org",
        rss_poll_interval_seconds=3600,
        matrix_startup_rooms="#sauna:pramari.de",
        gemini_api_key="test-api-key",
    ))


def _make_mock_client():
    mock_client = AsyncMock()
    mock_client.rooms = {}
    mock_client.room_resolve_alias.return_value = RoomResolveAliasResponse(
        room_alias="#sauna:pramari.de",
        room_id="!CuFFvooZfoEcYPNXOz:pramari.de",
        servers=["pramari.de"]
    )
    mock_client.room_send.return_value = RoomSendResponse(
        event_id="$event123",
        room_id="!CuFFvooZfoEcYPNXOz:pramari.de"
    )
    return mock_client


def _make_room_event():
    """Room with >2 users and no canonical alias so subscriber_id = room_id."""
    room = MagicMock()
    room.room_id = "#sauna:pramari.de"
    room.canonical_alias = None
    room.users = ["a", "b", "c"]
    event = MagicMock()
    event.sender = "@alice:matrix.org"
    return room, event


def _gemini_ok_response(text="The most important events of the day, condensed."):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "candidates": [{"content": {"parts": [{"text": text}]}}]
    }
    return resp


async def _cleanup_feed(feed_id):
    if feed_id:
        async with get_db_session() as session:
            f = await session.get(RSSFeed, feed_id)
            if f:
                await session.delete(f)
                await session.commit()


def test_rss_subscription_mode_defaults():
    """New subscriptions default to timeline/auto; explicit summary mode persists."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = None
        try:
            async with get_db_session() as session:
                feed = RSSFeed(url=f"https://example.com/mode_{unique_id}.xml", name="Mode Feed")
                session.add(feed)
                await session.flush()
                feed_id = feed.id

                default_sub = RSSSubscription(
                    subscriber_id="#room1:matrix.org", subscriber_type="room", feed_id=feed.id
                )
                summary_sub = RSSSubscription(
                    subscriber_id="#room2:matrix.org", subscriber_type="room", feed_id=feed.id,
                    delivery_mode="summary", summary_language="de"
                )
                session.add_all([default_sub, summary_sub])
                await session.commit()
                default_id, summary_id = default_sub.id, summary_sub.id

            async with get_db_session() as session:
                d = await session.get(RSSSubscription, default_id)
                s = await session.get(RSSSubscription, summary_id)
                assert d.delivery_mode == "timeline"
                assert d.summary_language == "auto"
                assert d.last_summary_at is None
                assert s.delivery_mode == "summary"
                assert s.summary_language == "de"
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)


def test_rss_subscribe_command_parses_mode_and_language_flags():
    """!rss subscribe tagesschau --mode summary --lang de persists both settings."""
    async def _test():
        fixture = find_fixture_by_slug_or_name("tagesschau")
        assert fixture, "tagesschau fixture must exist"
        feed_id = None
        try:
            # Get or create the feed (it may already exist from the startup fixtures)
            # so the handler does not attempt a network fetch
            async with get_db_session() as session:
                q = select(RSSFeed).where(RSSFeed.url == fixture["url"])
                feed = (await session.execute(q)).scalar_one_or_none()
                if not feed:
                    feed = RSSFeed(url=fixture["url"], name=fixture["name"])
                    session.add(feed)
                    await session.commit()
                feed_id = feed.id

            plugin = _make_plugin()
            client = _make_mock_client()
            room, event = _make_room_event()

            await plugin._handle_subscribe(client, room, event, ["tagesschau", "--mode", "summary", "--lang", "de"])

            async with get_db_session() as session:
                q = select(RSSSubscription).where(
                    RSSSubscription.subscriber_id == room.room_id,
                    RSSSubscription.feed_id == feed_id
                )
                sub = (await session.execute(q)).scalar_one_or_none()
                assert sub is not None
                assert sub.delivery_mode == "summary"
                assert sub.summary_language == "de"
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)


def test_rss_mode_command_switches_subscription():
    """!rss mode <id> summary switches the subscription and clears the summary anchor."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = None
        try:
            async with get_db_session() as session:
                feed = RSSFeed(url=f"https://example.com/switch_{unique_id}.xml", name="Switch Feed")
                session.add(feed)
                await session.flush()
                feed_id = feed.id
                sub = RSSSubscription(
                    subscriber_id="#sauna:pramari.de", subscriber_type="room", feed_id=feed.id,
                    delivery_mode="timeline", last_summary_at=datetime.now()  # noqa: DTZ005 - matches naive DB columns
                )
                session.add(sub)
                await session.commit()
                sub_id = sub.id

            plugin = _make_plugin()
            client = _make_mock_client()
            room, event = _make_room_event()

            await plugin._handle_mode(client, room, event, [str(feed_id), "summary"])

            async with get_db_session() as session:
                s = await session.get(RSSSubscription, sub_id)
                assert s.delivery_mode == "summary"
                assert s.last_summary_at is None
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)


def test_rss_language_command_sets_language():
    """!rss language <id> en updates the summary language."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = None
        try:
            async with get_db_session() as session:
                feed = RSSFeed(url=f"https://example.com/lang_{unique_id}.xml", name="Lang Feed")
                session.add(feed)
                await session.flush()
                feed_id = feed.id
                sub = RSSSubscription(
                    subscriber_id="#sauna:pramari.de", subscriber_type="room", feed_id=feed.id,
                    delivery_mode="summary", summary_language="auto"
                )
                session.add(sub)
                await session.commit()
                sub_id = sub.id

            plugin = _make_plugin()
            client = _make_mock_client()
            room, event = _make_room_event()

            await plugin._handle_language(client, room, event, [str(feed_id), "en"])

            async with get_db_session() as session:
                s = await session.get(RSSSubscription, sub_id)
                assert s.summary_language == "en"
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)


def test_rss_timeline_dispatcher_skips_summary_items():
    """The paced dispatcher must not deliver summary-mode items."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = None
        try:
            async with get_db_session() as session:
                feed = RSSFeed(url=f"https://example.com/skip_{unique_id}.xml", name="Skip Feed")
                session.add(feed)
                await session.flush()
                feed_id = feed.id
                item = RSSQueueItem(
                    subscriber_id="#sauna:pramari.de",
                    feed_id=feed.id,
                    entry_id=f"summary-item-{unique_id}",
                    title="Only Summary Article",
                    link="https://example.com/1",
                    delivery_mode="summary",
                    status="pending",
                )
                session.add(item)
                await session.commit()

            plugin = _make_plugin()
            client = _make_mock_client()

            # Run the dispatcher briefly; with only summary items pending it must stay idle
            task = asyncio.create_task(plugin._run_queue_dispatcher(client))
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=1.5)
            except TimeoutError:
                pass
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            client.room_send.assert_not_called()

            async with get_db_session() as session:
                db_item = await session.get(RSSQueueItem, item.id)
                assert db_item.status == "pending"
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)


def test_rss_summary_scheduler_delivers_single_digest():
    """Due summary subscriptions batch pending articles into one LLM digest message."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = None
        try:
            async with get_db_session() as session:
                feed = RSSFeed(url=f"https://example.com/digest_{unique_id}.xml", name="Digest Feed")
                session.add(feed)
                await session.flush()
                feed_id = feed.id
                sub = RSSSubscription(
                    subscriber_id="#sauna:pramari.de", subscriber_type="room", feed_id=feed.id,
                    delivery_mode="summary", summary_language="auto"
                )
                session.add(sub)
                await session.flush()
                item1 = RSSQueueItem(
                    subscriber_id="#sauna:pramari.de", feed_id=feed.id,
                    entry_id=f"digest-1-{unique_id}", title="First Event Happened",
                    link="https://example.com/1", summary="A major event occurred.",
                    delivery_mode="summary", status="pending"
                )
                item2 = RSSQueueItem(
                    subscriber_id="#sauna:pramari.de", feed_id=feed.id,
                    entry_id=f"digest-2-{unique_id}", title="Second Event Followed",
                    link="https://example.com/2", summary="Another event occurred.",
                    delivery_mode="summary", status="pending"
                )
                session.add_all([item1, item2])
                await session.commit()
                item1_id, item2_id = item1.id, item2.id

            plugin = _make_plugin()
            client = _make_mock_client()

            with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=_gemini_ok_response())):
                await plugin._process_summary_subscriptions(client)

            # Exactly one digest message delivered
            client.room_send.assert_called_once()
            content = client.room_send.call_args[1]["content"]
            assert "Summary (2 articles)" in content["body"]
            assert "The most important events of the day, condensed." in content["body"]
            assert "First Event Happened" in content["body"]
            assert "Second Event Followed" in content["body"]
            assert "First Event Happened" in content["formatted_body"]

            async with get_db_session() as session:
                i1 = await session.get(RSSQueueItem, item1_id)
                i2 = await session.get(RSSQueueItem, item2_id)
                assert i1.status == "delivered"
                assert i2.status == "delivered"
                subs = (await session.execute(
                    select(RSSSubscription).where(RSSSubscription.feed_id == feed_id)
                )).scalars().all()
                assert subs[0].last_summary_at is not None
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)


def test_rss_summary_scheduler_skips_when_no_pending_articles():
    """'Skip if no update happened': no pending articles means no message and no LLM call."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = None
        try:
            async with get_db_session() as session:
                feed = RSSFeed(url=f"https://example.com/quiet_{unique_id}.xml", name="Quiet Feed")
                session.add(feed)
                await session.flush()
                feed_id = feed.id
                sub = RSSSubscription(
                    subscriber_id="#sauna:pramari.de", subscriber_type="room", feed_id=feed.id,
                    delivery_mode="summary"
                )
                session.add(sub)
                await session.commit()
                sub_id = sub.id

            plugin = _make_plugin()
            client = _make_mock_client()

            gemini_post = AsyncMock(side_effect=AssertionError("LLM must not be called"))
            with patch("httpx.AsyncClient.post", new=gemini_post):
                await plugin._process_summary_subscriptions(client)

            client.room_send.assert_not_called()

            async with get_db_session() as session:
                s = await session.get(RSSSubscription, sub_id)
                assert s.last_summary_at is None
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)


def test_rss_summary_scheduler_respects_interval():
    """Summaries are not delivered before the configured interval has elapsed."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = None
        try:
            async with get_db_session() as session:
                feed = RSSFeed(url=f"https://example.com/interval_{unique_id}.xml", name="Interval Feed")
                session.add(feed)
                await session.flush()
                feed_id = feed.id
                sub = RSSSubscription(
                    subscriber_id="#sauna:pramari.de", subscriber_type="room", feed_id=feed.id,
                    delivery_mode="summary",
                    last_summary_at=datetime.now() - timedelta(seconds=60)  # noqa: DTZ005 - matches naive DB columns
                )
                session.add(sub)
                await session.flush()
                session.add(RSSQueueItem(
                    subscriber_id="#sauna:pramari.de", feed_id=feed.id,
                    entry_id=f"interval-1-{unique_id}", title="Pending Article",
                    link="https://example.com/1", delivery_mode="summary", status="pending"
                ))
                await session.commit()

            plugin = _make_plugin()
            client = _make_mock_client()

            gemini_post = AsyncMock(side_effect=AssertionError("LLM must not be called before interval"))
            with patch("httpx.AsyncClient.post", new=gemini_post):
                await plugin._process_summary_subscriptions(client)

            client.room_send.assert_not_called()
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)


def test_rss_summary_fallback_after_repeated_llm_failures():
    """After 3 failed LLM attempts the digest is delivered unsummarized instead of being lost."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = None
        try:
            async with get_db_session() as session:
                feed = RSSFeed(url=f"https://example.com/fallback_{unique_id}.xml", name="Fallback Feed")
                session.add(feed)
                await session.flush()
                feed_id = feed.id
                sub = RSSSubscription(
                    subscriber_id="#sauna:pramari.de", subscriber_type="room", feed_id=feed.id,
                    delivery_mode="summary"
                )
                session.add(sub)
                await session.flush()
                item = RSSQueueItem(
                    subscriber_id="#sauna:pramari.de", feed_id=feed.id,
                    entry_id=f"fallback-1-{unique_id}", title="Important News",
                    link="https://example.com/1", delivery_mode="summary", status="pending"
                )
                session.add(item)
                await session.commit()
                item_id = item.id

            plugin = _make_plugin()
            client = _make_mock_client()

            fail_resp = MagicMock()
            fail_resp.status_code = 500
            fail_resp.text = "internal error"
            with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=fail_resp)):
                # Attempt 1 and 2: no delivery, item stays pending
                await plugin._process_summary_subscriptions(client)
                await plugin._process_summary_subscriptions(client)
                client.room_send.assert_not_called()
                # Attempt 3: fallback digest delivered
                await plugin._process_summary_subscriptions(client)

            client.room_send.assert_called_once()
            content = client.room_send.call_args[1]["content"]
            assert "Important News" in content["body"]
            assert "Headlines:" in content["body"]

            async with get_db_session() as session:
                db_item = await session.get(RSSQueueItem, item_id)
                assert db_item.status == "delivered"
                subs = (await session.execute(
                    select(RSSSubscription).where(RSSSubscription.feed_id == feed_id)
                )).scalars().all()
                assert subs[0].last_summary_at is not None
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)
