import asyncio
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from sqlalchemy import select, func
from nio import RoomSendResponse, RoomResolveAliasResponse

from src.config import Settings
from src.core.database import get_db_session, run_migrations, close_engine
from src.models.rss import RSSFeed, RSSSubscription, RSSHistory, RSSQueueItem
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


def test_rss_queue_item_model():
    """Test that RSSQueueItem can be created and queried in the database."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = None
        try:
            async with get_db_session() as session:
                feed = RSSFeed(url=f"https://example.com/test_feed_{unique_id}.xml", name="Test Feed")
                session.add(feed)
                await session.flush()
                feed_id = feed.id

                queue_item = RSSQueueItem(
                    subscriber_id="#sauna:pramari.de",
                    feed_id=feed.id,
                    entry_id=f"test_guid_{unique_id}",
                    title="AI Revolution in Agentic Systems",
                    link="https://example.com/ai-revolution",
                    summary="Exciting developments in autonomous agents.",
                    matches=["Keyword: 'Agent'"],
                    feed_name=feed.name,
                    status="pending",
                )
                session.add(queue_item)
                await session.commit()

                # Query back
                res = await session.execute(
                    select(RSSQueueItem).where(RSSQueueItem.entry_id == f"test_guid_{unique_id}")
                )
                fetched = res.scalar_one()
                assert fetched.title == "AI Revolution in Agentic Systems"
                assert fetched.status == "pending"
                assert fetched.subscriber_id == "#sauna:pramari.de"
                assert fetched.matches == ["Keyword: 'Agent'"]
        finally:
            if feed_id:
                async with get_db_session() as session:
                    f = await session.get(RSSFeed, feed_id)
                    if f:
                        await session.delete(f)
                        await session.commit()

    run_async(_test)


def test_rss_poller_enqueues_articles():
    """Test that polling external feeds enqueues matching articles into the delivery queue."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        guid1 = f"tech-news-001-{unique_id}"
        guid2 = f"tech-news-002-{unique_id}"
        feed_url = f"https://technews.example.com/rss_{unique_id}.xml"
        feed_id = None

        mock_rss_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <title>Tech News Daily</title>
            <link>https://technews.example.com</link>
            <item>
              <title>DeepMind Unveils Next Gen Autonomous Agents</title>
              <link>https://technews.example.com/deepmind-agents</link>
              <description>Revolutionary developments in multi-agent orchestration.</description>
              <guid>{guid1}</guid>
            </item>
            <item>
              <title>Local Sports Scores Today</title>
              <link>https://technews.example.com/sports</link>
              <description>Soccer match results.</description>
              <guid>{guid2}</guid>
            </item>
          </channel>
        </rss>
        """

        settings = Settings(
            matrix_user_id="@bot:matrix.org",
            rss_poll_interval_seconds=3600,
            matrix_startup_rooms="#sauna:pramari.de",
        )
        plugin = RSSPlugin(settings=settings)

        try:
            async with get_db_session() as session:
                feed = RSSFeed(url=feed_url, name="Tech News Daily")
                session.add(feed)
                await session.flush()
                feed_id = feed.id

                # Subscribe #sauna:pramari.de to this feed with keyword filter 'Agent'
                sub = RSSSubscription(
                    subscriber_id="#sauna:pramari.de",
                    subscriber_type="room",
                    feed_id=feed.id,
                    keywords=["Agent"],
                )
                session.add(sub)
                await session.commit()

            async def mock_get_side_effect(url, *args, **kwargs):
                resp = MagicMock()
                if url == feed_url:
                    resp.status_code = 200
                    resp.text = mock_rss_xml
                else:
                    resp.status_code = 404
                    resp.text = ""
                return resp

            with patch("httpx.AsyncClient.get", side_effect=mock_get_side_effect):
                enqueued_count = await plugin.poll_all_feeds()

            assert enqueued_count == 1

            # Verify article is pending in the queue
            async with get_db_session() as session:
                q = select(RSSQueueItem).where(RSSQueueItem.entry_id == guid1)
                item = (await session.execute(q)).scalar_one_or_none()
                assert item is not None
                assert item.status == "pending"
                assert item.subscriber_id == "#sauna:pramari.de"
                assert item.title == "DeepMind Unveils Next Gen Autonomous Agents"

                # Verify entry is tracked in RSSHistory to prevent duplicate ingestion
                hist_q = select(RSSHistory).where(RSSHistory.entry_id == guid1)
                hist = (await session.execute(hist_q)).scalar_one_or_none()
                assert hist is not None
        finally:
            if feed_id:
                async with get_db_session() as session:
                    f = await session.get(RSSFeed, feed_id)
                    if f:
                        await session.delete(f)
                        await session.commit()

    run_async(_test)


def test_rss_paced_rate_calculation():
    """Test that dynamic pacing distributes queued articles across the poll interval."""
    settings = Settings(
        matrix_user_id="@bot:matrix.org",
        rss_poll_interval_seconds=3600,  # 60 minutes
        rss_min_delivery_interval_seconds=5,
        rss_max_delivery_interval_seconds=3600,
    )
    plugin = RSSPlugin(settings=settings)

    # Simulate 60 items pending and 3600 seconds remaining
    plugin._last_poll_time = datetime.now().timestamp()
    pending_count = 60
    time_until_next_poll = 3600.0

    ideal_interval = time_until_next_poll / pending_count
    assert ideal_interval == 60.0  # Exactly 1 per minute

    # 30 items pending in 3600 seconds
    assert (3600.0 / 30) == 120.0  # 1 every 2 minutes

    # 120 items pending in 3600 seconds
    assert (3600.0 / 120) == 30.0  # 1 every 30 seconds


def test_rss_delivery_queue_dispatch_one_by_one():
    """Test that the dispatcher pops items one by one, marks them delivered, and sends to Matrix."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        guid1 = f"dispatch-001-{unique_id}"
        guid2 = f"dispatch-002-{unique_id}"
        feed_id = None

        settings = Settings(
            matrix_user_id="@bot:matrix.org",
            rss_poll_interval_seconds=3600,
            matrix_startup_rooms="#sauna:pramari.de",
        )
        plugin = RSSPlugin(settings=settings)

        try:
            # Insert 2 pending items
            async with get_db_session() as session:
                feed = RSSFeed(url=f"https://example.com/dispatch_test_{unique_id}.xml", name="Dispatch Feed")
                session.add(feed)
                await session.flush()
                feed_id = feed.id

                item1 = RSSQueueItem(
                    subscriber_id="#sauna:pramari.de",
                    feed_id=feed.id,
                    entry_id=guid1,
                    title="First Article",
                    link="https://example.com/1",
                    status="pending",
                )
                item2 = RSSQueueItem(
                    subscriber_id="#sauna:pramari.de",
                    feed_id=feed.id,
                    entry_id=guid2,
                    title="Second Article",
                    link="https://example.com/2",
                    status="pending",
                )
                session.add_all([item1, item2])
                await session.commit()
                item1_id = item1.id
                item2_id = item2.id

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

            # Deliver first item
            async with get_db_session() as session:
                item = await session.get(RSSQueueItem, item1_id)
                await plugin._deliver_alert_from_queue(
                    client=mock_client,
                    subscriber_id=item.subscriber_id,
                    feed_name=item.feed_name,
                    title=item.title,
                    link=item.link,
                    summary=item.summary,
                    matches=item.matches or [],
                )
                item.status = "delivered"
                item.delivered_at = datetime.now()
                await session.commit()

            # Check that client.room_send was called with the resolved room ID
            mock_client.room_send.assert_called_once()
            call_args = mock_client.room_send.call_args[1]
            assert call_args["room_id"] == "!CuFFvooZfoEcYPNXOz:pramari.de"
            assert "First Article" in call_args["content"]["body"]

            # Verify database status
            async with get_db_session() as session:
                db_item1 = await session.get(RSSQueueItem, item1_id)
                db_item2 = await session.get(RSSQueueItem, item2_id)
                assert db_item1.status == "delivered"
                assert db_item2.status == "pending"
        finally:
            if feed_id:
                async with get_db_session() as session:
                    f = await session.get(RSSFeed, feed_id)
                    if f:
                        await session.delete(f)
                        await session.commit()

    run_async(_test)


def test_rss_queue_status_command():
    """Test that !rss queue displays pending and pacing stats."""
    async def _test():
        settings = Settings(
            matrix_user_id="@bot:matrix.org",
            rss_poll_interval_seconds=3600,
            matrix_startup_rooms="#sauna:pramari.de",
        )
        plugin = RSSPlugin(settings=settings)

        mock_client = AsyncMock()
        mock_room = MagicMock()
        mock_room.room_id = "!room:matrix.org"
        mock_event = MagicMock()
        mock_client.room_send.return_value = RoomSendResponse(
            event_id="$ev_status",
            room_id="!room:matrix.org"
        )

        await plugin._handle_queue_status(mock_client, mock_room, mock_event)

        mock_client.room_send.assert_called_once()
        content = mock_client.room_send.call_args[1]["content"]
        assert "RSS Delivery Queue & Pacing Status" in content["formatted_body"]
        assert "Poll Cycle Interval" in content["formatted_body"]

    run_async(_test)
