from unittest.mock import MagicMock, patch

from crypto_signal_service.fetchers.gdelt_fetcher import GDELTFetcher, _classify_article

_SAMPLE_RESPONSE = {
    "articles": [
        {
            "url": "http://reuters.com/crypto-hack",
            "title": "Crypto exchange loses $200M in hack",
            "seendate": "20240417T120000Z",
        },
        {
            "url": "http://bloomberg.com/bitcoin-etf",
            "title": "Bitcoin ETF sees record inflows",
            "seendate": "20240417T110000Z",
        },
        {
            "url": "http://ft.com/fed-rate",
            "title": "Federal Reserve holds interest rate steady amid inflation fears",
            "seendate": "20240417T100000Z",
        },
    ]
}


def _mock_response(data: dict, status: int = 200):
    r = MagicMock()
    r.status_code = status
    r.text = "non-empty"
    r.json.return_value = data
    r.raise_for_status = MagicMock()
    return r


def test_fetch_happy_path():
    with patch("requests.get", return_value=_mock_response(_SAMPLE_RESPONSE)):
        items = GDELTFetcher().fetch()
    assert len(items) == 3
    assert items[0].title == "Crypto exchange loses $200M in hack"
    assert items[0].pub_ts == "20240417T120000Z"
    assert len(items[0].signal_id) == 16


def test_fetch_swallows_errors():
    with patch("requests.get", side_effect=Exception("timeout")):
        items = GDELTFetcher().fetch()
    assert items == []


def test_fetch_skips_empty_title():
    data = {"articles": [{"url": "http://x.com", "title": "", "seendate": None}]}
    with patch("requests.get", return_value=_mock_response(data)):
        items = GDELTFetcher().fetch()
    assert items == []


def test_fetch_empty_articles():
    with patch("requests.get", return_value=_mock_response({"articles": []})):
        items = GDELTFetcher().fetch()
    assert items == []


def test_signal_id_deterministic():
    fetcher = GDELTFetcher()
    id1 = fetcher._make_signal_id("http://x.com", "Hello")
    id2 = fetcher._make_signal_id("http://x.com", "Hello")
    assert id1 == id2
    assert len(id1) == 16


def test_classify_article_exchange():
    topics = _classify_article("Crypto exchange loses $200M in hack")
    assert "exchange_custody" in topics


def test_classify_article_macro():
    topics = _classify_article("Federal Reserve holds interest rate steady amid inflation fears")
    assert "macro_economics" in topics


def test_classify_article_bitcoin():
    topics = _classify_article("Bitcoin ETF sees record inflows")
    assert "bitcoin_ethereum" in topics


def test_classify_article_no_match():
    topics = _classify_article("Local weather forecast for Tuesday")
    assert topics == []


def test_unmatched_articles_excluded():
    data = {
        "articles": [
            {"url": "http://x.com/sports", "title": "Football match results today", "seendate": None},
        ]
    }
    with patch("requests.get", return_value=_mock_response(data)):
        items = GDELTFetcher().fetch()
    assert items == []


def test_topic_stored_in_summary():
    with patch("requests.get", return_value=_mock_response(_SAMPLE_RESPONSE)):
        items = GDELTFetcher().fetch()
    summaries = {item.summary for item in items}
    assert summaries <= {"exchange_custody", "bitcoin_ethereum", "macro_economics", "regulation",
                         "stablecoins", "defi_mining"}


def test_fetch_rate_limit_retries():
    limited = _mock_response({}, status=429)
    limited.text = ""
    ok = _mock_response(_SAMPLE_RESPONSE)
    with patch("requests.get", side_effect=[limited, ok]), patch("time.sleep"):
        items = GDELTFetcher().fetch()
    assert len(items) == 3
