import asyncio
import os
import uuid

import pytest

# Isolate this module from the configured live database: run migrations and all
# plugin DB access against a dedicated SQLite file (data/ is gitignored).
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///data/test_rss_summary.db"

import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock, MagicMock

from nio import RoomSendResponse, UploadResponse

from src.config import Settings
from src.core.database import close_engine, get_db_session, run_migrations
from src.models.rss import RSSFeed, RSSSubscription
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
    ))


def _make_mock_client():
    mock_client = AsyncMock()
    mock_client.rooms = {}
    mock_client.room_send.return_value = RoomSendResponse(
        event_id="$event-opml",
        room_id="#sauna:pramari.de"
    )
    mock_client.upload.return_value = (
        UploadResponse(content_uri="mxc://example.org/opml123"),
        None,
    )
    return mock_client


def _make_room_event():
    room = MagicMock()
    room.room_id = "#sauna:pramari.de"
    room.canonical_alias = None
    room.name = "Sauna"
    room.users = ["a", "b", "c"]
    event = MagicMock()
    event.sender = "@alice:matrix.org"
    return room, event


async def _create_subscription(url: str, name: str, subscriber: str = "#sauna:pramari.de"):
    async with get_db_session() as session:
        feed = RSSFeed(url=url, name=name)
        session.add(feed)
        await session.flush()
        sub = RSSSubscription(
            subscriber_id=subscriber, subscriber_type="room", feed_id=feed.id
        )
        session.add(sub)
        await session.commit()
        return feed.id


async def _cleanup_feed(feed_id):
    if feed_id:
        async with get_db_session() as session:
            f = await session.get(RSSFeed, feed_id)
            if f:
                await session.delete(f)
                await session.commit()


def test_rss_opml_export_uploads_valid_opml_file():
    """!rss opml export uploads an OPML 2.0 file and attaches it as an m.file message."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = await _create_subscription(
            f"https://example.com/opml_feed_{unique_id}.xml", "OPML Test Feed"
        )
        try:
            plugin = _make_plugin()
            client = _make_mock_client()
            room, event = _make_room_event()

            await plugin._handle_opml(client, room, event, ["export"])

            # File uploaded once with the OPML payload
            client.upload.assert_called_once()
            upload_args, upload_kwargs = client.upload.call_args
            payload = upload_args[0].getvalue()
            assert upload_kwargs["content_type"] == "application/xml"
            assert upload_kwargs["filename"].endswith(".opml")

            # Payload is valid OPML 2.0 listing the subscribed feed
            root = ET.fromstring(payload)
            assert root.tag == "opml"
            assert root.get("version") == "2.0"
            outlines = root.findall("./body/outline")
            assert len(outlines) == 1
            assert outlines[0].get("type") == "rss"
            assert outlines[0].get("xmlUrl") == f"https://example.com/opml_feed_{unique_id}.xml"
            assert outlines[0].get("text") == "OPML Test Feed"

            # Attached to the room as an m.file message with the upload URI
            client.room_send.assert_called_once()
            call_kwargs = client.room_send.call_args[1]
            content = call_kwargs["content"]
            assert call_kwargs["room_id"] == "#sauna:pramari.de"
            assert content["msgtype"] == "m.file"
            assert content["url"] == "mxc://example.org/opml123"
            assert content["info"]["mimetype"] == "application/xml"
            assert content["info"]["size"] == len(payload)
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)


def test_rss_opml_export_escapes_special_characters():
    """Feed names containing XML-special characters survive the export intact."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = await _create_subscription(
            f"https://example.com/escape_{unique_id}.xml", "A & B <Test> Feed"
        )
        try:
            plugin = _make_plugin()
            client = _make_mock_client()
            room, event = _make_room_event()

            await plugin._handle_opml(client, room, event, [])

            payload = client.upload.call_args[0][0].getvalue()
            root = ET.fromstring(payload)
            outline = root.find("./body/outline")
            assert outline.get("text") == "A & B <Test> Feed"
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)


def test_rss_opml_export_empty_context_replies_text():
    """With no subscriptions in context, the bot replies with text and uploads nothing."""
    async def _test():
        plugin = _make_plugin()
        client = _make_mock_client()
        room, event = _make_room_event()
        room.room_id = "#empty-room:matrix.org"  # no subscriptions for this context

        await plugin._handle_opml(client, room, event, ["export"])

        client.upload.assert_not_called()
        client.room_send.assert_called_once()
        content = client.room_send.call_args[1]["content"]
        assert content["msgtype"] == "m.text"
        assert "No RSS subscriptions to export" in content["body"]

    run_async(_test)


def test_rss_opml_rejects_unknown_action():
    """Unknown opml sub-actions get a usage hint instead of an export."""
    async def _test():
        plugin = _make_plugin()
        client = _make_mock_client()
        room, event = _make_room_event()

        await plugin._handle_opml(client, room, event, ["import"])

        client.upload.assert_not_called()
        content = client.room_send.call_args[1]["content"]
        assert "Usage: !rss opml" in content["body"]

    run_async(_test)


def test_rss_opml_falls_back_to_inline_text_on_upload_failure():
    """If the media upload fails, the OPML document is delivered as inline text."""
    async def _test():
        unique_id = uuid.uuid4().hex[:8]
        feed_id = await _create_subscription(
            f"https://example.com/fallback_{unique_id}.xml", "Fallback Feed"
        )
        try:
            plugin = _make_plugin()
            client = _make_mock_client()
            client.upload.side_effect = RuntimeError("upload failed")
            room, event = _make_room_event()

            await plugin._handle_opml(client, room, event, ["export"])

            # Fallback message contains the full OPML document inline
            client.room_send.assert_called_once()
            content = client.room_send.call_args[1]["content"]
            assert f"https://example.com/fallback_{unique_id}.xml" in content["body"]
            assert "<opml" in content["body"]
        finally:
            await _cleanup_feed(feed_id)

    run_async(_test)
