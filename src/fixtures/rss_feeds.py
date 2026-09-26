import json
import os
from typing import List, Dict, Any, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from src.core.database import get_db_session
from src.models.rss import RSSFeed, RSSSubscription
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Complete list of curated major German news feeds
DEFAULT_GERMAN_NEWS_FEEDS: List[Dict[str, Any]] = [
    {
        "slug": "tagesschau",
        "name": "Tagesschau (ARD)",
        "url": "https://www.tagesschau.de/xml/rss2/",
        "category": "Allgemein & Eilmeldungen",
        "description": "Offizieller Newsfeed der ARD Tagesschau mit aktuellen Meldungen aus Politik, Wirtschaft und Weltgeschehen.",
        "language": "de",
        "default_keywords": ["Deutschland", "Bundesregierung", "Wirtschaft", "Europa"],
    },
    {
        "slug": "zdf",
        "name": "ZDFheute",
        "url": "https://www.zdf.de/rss/zdf/nachrichten",
        "category": "Allgemein & Eilmeldungen",
        "description": "Aktuelle Nachrichten, Reportagen und Eilmeldungen von heute.de (ZDF).",
        "language": "de",
        "default_keywords": ["Politik", "Nachrichten", "Gesellschaft"],
    },
    {
        "slug": "spiegel",
        "name": "Der Spiegel",
        "url": "https://www.spiegel.de/schlagzeilen/index.rss",
        "category": "Nachrichtenmagazin",
        "description": "Aktuelle Schlagzeilen, Recherchen und Hintergründe des SPIEGEL.",
        "language": "de",
        "default_keywords": ["Politik", "Wirtschaft", "Kultur", "Wissenschaft"],
    },
    {
        "slug": "zeit",
        "name": "Die Zeit",
        "url": "https://newsfeed.zeit.de/index",
        "category": "Wochenzeitung & Leitmedium",
        "description": "Nachrichten, Recherchen, Analysen und Kommentare von ZEIT ONLINE.",
        "language": "de",
        "default_keywords": ["Politik", "Wirtschaft", "Digital", "Kultur"],
    },
    {
        "slug": "sz",
        "name": "Süddeutsche Zeitung (SZ)",
        "url": "https://rss.sueddeutsche.de/alles",
        "category": "Überregionale Tageszeitung",
        "description": "Alle aktuellen Meldungen, Berichte und Leitartikel der Süddeutschen Zeitung.",
        "language": "de",
        "default_keywords": ["Bayern", "Deutschland", "Wirtschaft", "Kultur"],
    },
    {
        "slug": "faz",
        "name": "Frankfurter Allgemeine Zeitung (FAZ)",
        "url": "https://www.faz.net/rss/aktuell/",
        "category": "Überregionale Tageszeitung",
        "description": "Aktuelle Nachrichten aus Politik, Wirtschaft, Finanzen, Feuilleton und Gesellschaft.",
        "language": "de",
        "default_keywords": ["Finanzen", "Politik", "Unternehmen", "Wirtschaft"],
    },
    {
        "slug": "handelsblatt",
        "name": "Handelsblatt",
        "url": "https://www.handelsblatt.com/contentexport/feed/top-themen",
        "category": "Wirtschaft & Finanzen",
        "description": "Deutschlands führende Wirtschafts- und Finanzzeitung: Top-Themen, Märkte und Unternehmen.",
        "language": "de",
        "default_keywords": ["DAX", "Börse", "Finanzen", "Unternehmen", "EZB"],
    },
    {
        "slug": "welt",
        "name": "Die Welt",
        "url": "https://www.welt.de/feeds/latest.rss",
        "category": "Überregionale Tageszeitung",
        "description": "Aktuelle Nachrichten, Analysen und Kommentare aus Politik, Wirtschaft und Finanzen.",
        "language": "de",
        "default_keywords": ["Politik", "Wirtschaft", "Finanzen", "Debatte"],
    },
    {
        "slug": "ntv",
        "name": "n-tv Nachrichten",
        "url": "https://www.n-tv.de/rss",
        "category": "Nachrichtensender",
        "description": "24/7 Eilmeldungen, Politik, Wirtschaft und Börsenberichterstattung von n-tv.",
        "language": "de",
        "default_keywords": ["Eilmeldung", "Börse", "Wirtschaft", "Politik"],
    },
    {
        "slug": "focus",
        "name": "Focus Online",
        "url": "https://rss.focus.de/fol/XML/rss_folnews.xml",
        "category": "Nachrichtenportal",
        "description": "Eilmeldungen, Politik, Finanzen und Verbrauchernachrichten von Focus Online.",
        "language": "de",
        "default_keywords": ["Finanzen", "Politik", "Wissen", "Gesundheit"],
    },
    {
        "slug": "heise",
        "name": "Heise Online",
        "url": "https://www.heise.de/rss/heise-atom.xml",
        "category": "IT, Technologie & Digitalwirtschaft",
        "description": "Führendes deutsches Tech-Portal für IT-Nachrichten, Open Source, Security und Digitalpolitik.",
        "language": "de",
        "default_keywords": ["Security", "KI", "Linux", "Software", "Hardware", "Cloud"],
    },
    {
        "slug": "dw",
        "name": "Deutsche Welle (DW)",
        "url": "https://rss.dw.com/rdf/rss-de-all",
        "category": "International & Auslandssender",
        "description": "Unabhängiger Journalismus aus und über Deutschland für ein globales Publikum.",
        "language": "de",
        "default_keywords": ["Deutschland", "Europa", "Weltpolitik", "Menschenrechte"],
    },
    {
        "slug": "dlf",
        "name": "Deutschlandfunk (DLF)",
        "url": "https://www.deutschlandfunk.de/nachrichten-100.rss",
        "category": "Öffentlich-rechtlicher Hörfunk",
        "description": "Aktuelle Nachrichten und Hintergrundberichte aus der Nachrichtenredaktion des Deutschlandfunks.",
        "language": "de",
        "default_keywords": ["Nachrichten", "Politik", "Hintergrund"],
    },
    {
        "slug": "tagesspiegel",
        "name": "Der Tagesspiegel",
        "url": "https://www.tagesspiegel.de/contentexport/feed/home",
        "category": "Hauptstadt & National",
        "description": "Leitmedium aus Berlin mit Schwerpunkt auf Bundespolitik und Hauptstadtnachrichten.",
        "language": "de",
        "default_keywords": ["Berlin", "Bundestag", "Politik", "Hauptstadt"],
    },
    {
        "slug": "taz",
        "name": "taz (die tageszeitung)",
        "url": "https://taz.de/!p4608;rss/",
        "category": "Überregionale Tageszeitung",
        "description": "Unabhängige Berichterstattung zu Politik, Klima, Ökologie und Gesellschaft.",
        "language": "de",
        "default_keywords": ["Klima", "Umwelt", "Politik", "Gesellschaft"],
    },
]


def get_default_rss_fixtures() -> List[Dict[str, Any]]:
    """Loads default RSS fixtures from JSON file if available, otherwise returns embedded defaults."""
    fixture_paths = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/fixtures/rss_feeds.json")),
        os.path.abspath(os.path.join(os.getcwd(), "data/fixtures/rss_feeds.json")),
    ]
    for path in fixture_paths:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list) and len(data) > 0:
                        return data
            except Exception as e:
                logger.warning("Could not read fixtures JSON file, falling back to embedded defaults", path=path, error=str(e))
    return DEFAULT_GERMAN_NEWS_FEEDS


def find_fixture_by_slug_or_name(query: str) -> Optional[Dict[str, Any]]:
    """Finds a fixture feed by its slug alias, name substring, or direct URL."""
    q = query.strip().lower()
    fixtures = get_default_rss_fixtures()

    # Exact slug match first
    for f in fixtures:
        if f.get("slug", "").lower() == q:
            return f

    # Exact URL match
    for f in fixtures:
        if f.get("url", "").lower() == q:
            return f

    # Partial name match
    for f in fixtures:
        if q in f.get("name", "").lower():
            return f

    return None


async def seed_default_rss_feeds(
    session: Optional[AsyncSession] = None,
    auto_subscribe_targets: Optional[List[str]] = None,
) -> List[RSSFeed]:
    """Ensures default RSS fixtures exist in the database.

    Args:
        session: Optional external AsyncSession. If omitted, opens a new session.
        auto_subscribe_targets: Optional list of room aliases/IDs to auto-subscribe to all default feeds.

    Returns:
        List of RSSFeed models present or inserted.
    """
    fixtures = get_default_rss_fixtures()
    results: List[RSSFeed] = []

    async def _execute_seed(s: AsyncSession) -> List[RSSFeed]:
        seeded_count = 0
        updated_count = 0

        # Preload existing feeds by URL
        existing_res = await s.execute(select(RSSFeed))
        existing_feeds = {f.url: f for f in existing_res.scalars().all()}

        for item in fixtures:
            url = item["url"]
            name = item["name"]

            if url in existing_feeds:
                feed = existing_feeds[url]
                # Update name if previously empty or placeholder
                if not feed.name or feed.name == feed.url:
                    feed.name = name
                    updated_count += 1
                results.append(feed)
            else:
                feed = RSSFeed(url=url, name=name)
                s.add(feed)
                await s.flush()
                results.append(feed)
                seeded_count += 1

        # Auto-subscribe specified target rooms if requested
        if auto_subscribe_targets:
            for target in auto_subscribe_targets:
                if not target:
                    continue
                # Get existing subscriptions for this target
                sub_res = await s.execute(
                    select(RSSSubscription).where(RSSSubscription.subscriber_id == target)
                )
                existing_sub_feed_ids = {sub.feed_id for sub in sub_res.scalars().all()}

                for feed in results:
                    if feed.id not in existing_sub_feed_ids:
                        new_sub = RSSSubscription(
                            subscriber_id=target,
                            subscriber_type="room",
                            feed_id=feed.id,
                        )
                        s.add(new_sub)
                        logger.info("Auto-subscribed target to default feed", target=target, feed=feed.name)

        logger.info(
            "Default RSS fixtures synchronization complete",
            total_fixtures=len(fixtures),
            seeded_new=seeded_count,
            updated=updated_count,
        )
        return results

    if session is not None:
        return await _execute_seed(session)
    else:
        async with get_db_session() as new_session:
            return await _execute_seed(new_session)
