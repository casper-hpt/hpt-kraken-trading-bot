from unittest.mock import MagicMock, patch

from crypto_signal_service.fetchers.rss_fetcher import FeedItem, RSSFetcher


def _make_entry(link="http://example.com/1", title="Test headline", summary="Summary", published=None):
    e = MagicMock()
    e.link = link
    e.title = title
    e.summary = summary
    e.published = published
    return e


def test_fetch_feed_happy_path():
    mock_feed = MagicMock()
    mock_feed.entries = [_make_entry()]
    with patch("feedparser.parse", return_value=mock_feed):
        items = RSSFetcher().fetch_feed("http://example.com/feed")
    assert len(items) == 1
    assert items[0].title == "Test headline"
    assert len(items[0].signal_id) == 16


def test_fetch_feed_swallows_errors():
    with patch("feedparser.parse", side_effect=Exception("network error")):
        items = RSSFetcher().fetch_feed("http://bad.example.com/feed")
    assert items == []


def test_fetch_all_deduplication():
    entry = _make_entry(link="http://example.com/1", title="Same")
    mock_feed = MagicMock()
    mock_feed.entries = [entry]
    with patch("feedparser.parse", return_value=mock_feed):
        items = RSSFetcher().fetch_all(["http://feed1.com", "http://feed2.com"])
    assert len(items) == 1


def test_fetch_feed_skips_empty_title():
    mock_feed = MagicMock()
    mock_feed.entries = [_make_entry(title="")]
    with patch("feedparser.parse", return_value=mock_feed):
        items = RSSFetcher().fetch_feed("http://example.com/feed")
    assert items == []


def test_signal_id_deterministic():
    fetcher = RSSFetcher()
    id1 = fetcher._make_signal_id("http://x.com", "Hello")
    id2 = fetcher._make_signal_id("http://x.com", "Hello")
    assert id1 == id2
    assert len(id1) == 16


def test_signal_id_varies_by_input():
    fetcher = RSSFetcher()
    assert fetcher._make_signal_id("http://a.com", "A") != fetcher._make_signal_id("http://b.com", "B")
