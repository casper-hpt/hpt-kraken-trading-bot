import json
from unittest.mock import MagicMock

from crypto_signal_service.llm_classifier import LLMClassifier, CryptoSignal
from crypto_signal_service.fetchers.rss_fetcher import FeedItem

_VALID_RESPONSE = {
    "event_type": "hack",
    "asset_scope": "BTC",
    "affected_symbols": ["BTC"],
    "time_horizon": "1-7d",
    "direction": "bearish",
    "confidence": 0.90,
    "novelty": 0.80,
    "tradability": 0.85,
    "key_reason": "Major exchange hacked, BTC reserves at risk",
}


def _make_item(signal_id="abc1234567890123"):
    return FeedItem(
        signal_id=signal_id,
        title="Major crypto exchange hacked",
        summary="A major exchange lost $100M in BTC",
        pub_ts=None,
        source_url="http://example.com/article",
    )


def _make_classifier():
    return LLMClassifier(api_key="test", base_url="http://test", model="test-model")


def _mock_openai_response(data: dict):
    msg = MagicMock()
    msg.content = json.dumps(data)
    choice = MagicMock()
    choice.message = msg
    resp = MagicMock()
    resp.choices = [choice]
    return resp


def test_classify_valid():
    c = _make_classifier()
    c._client = MagicMock()
    c._client.chat.completions.create.return_value = _mock_openai_response(_VALID_RESPONSE)
    signal = c.classify(_make_item())
    assert signal is not None
    assert signal.direction == "bearish"
    assert signal.confidence == 0.90
    assert abs(signal.catalyst_score - 0.90 * 0.80 * 0.85) < 1e-9
    assert signal.affected_symbols == ["BTC"]


def test_classify_bad_json():
    c = _make_classifier()
    msg = MagicMock()
    msg.content = "not json at all"
    choice = MagicMock()
    choice.message = msg
    resp = MagicMock()
    resp.choices = [choice]
    c._client = MagicMock()
    c._client.chat.completions.create.return_value = resp
    assert c.classify(_make_item()) is None


def test_classify_missing_field():
    data = dict(_VALID_RESPONSE)
    del data["direction"]
    c = _make_classifier()
    c._client = MagicMock()
    c._client.chat.completions.create.return_value = _mock_openai_response(data)
    assert c.classify(_make_item()) is None


def test_classify_invalid_direction_enum():
    data = {**_VALID_RESPONSE, "direction": "sideways"}
    c = _make_classifier()
    c._client = MagicMock()
    c._client.chat.completions.create.return_value = _mock_openai_response(data)
    assert c.classify(_make_item()) is None


def test_classify_invalid_event_type():
    data = {**_VALID_RESPONSE, "event_type": "gossip"}
    c = _make_classifier()
    c._client = MagicMock()
    c._client.chat.completions.create.return_value = _mock_openai_response(data)
    assert c.classify(_make_item()) is None


def test_classify_llm_exception_returns_none():
    c = _make_classifier()
    c._client = MagicMock()
    c._client.chat.completions.create.side_effect = Exception("timeout")
    assert c.classify(_make_item()) is None


def test_classify_batch():
    c = _make_classifier()
    c._client = MagicMock()
    c._client.chat.completions.create.return_value = _mock_openai_response(_VALID_RESPONSE)
    signals = c.classify_batch([_make_item("id1"), _make_item("id2")])
    assert len(signals) == 2


def test_classify_batch_skips_failures():
    c = _make_classifier()
    c._client = MagicMock()
    # First call succeeds, second raises
    c._client.chat.completions.create.side_effect = [
        _mock_openai_response(_VALID_RESPONSE),
        Exception("timeout"),
    ]
    signals = c.classify_batch([_make_item("id1"), _make_item("id2")])
    assert len(signals) == 1
