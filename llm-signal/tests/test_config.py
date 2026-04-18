from crypto_signal_service.config import Config


def test_defaults():
    c = Config.from_env()
    assert c.llm_model == "gpt-4o-mini"
    assert c.prometheus_port == 9093
    assert c.signal_poll_interval_minutes == 60
    assert len(c.rss_feed_urls) > 0


def test_env_override(monkeypatch):
    monkeypatch.setenv("LLM_MODEL", "gpt-4o")
    monkeypatch.setenv("LLM_MAX_ITEMS_PER_CYCLE", "5")
    monkeypatch.setenv("PROMETHEUS_PORT", "9999")
    c = Config.from_env()
    assert c.llm_model == "gpt-4o"
    assert c.llm_max_items_per_cycle == 5
    assert c.prometheus_port == 9999


def test_rss_feed_override(monkeypatch):
    monkeypatch.setenv("RSS_FEED_URLS", "http://a.com/feed;http://b.com/feed")
    c = Config.from_env()
    assert list(c.rss_feed_urls) == ["http://a.com/feed", "http://b.com/feed"]


def test_questdb_urls():
    c = Config(questdb_host="myhost", questdb_http_port=9000)
    assert "myhost:9000" in c.questdb_ilp_conf
    assert "myhost:9000" in c.questdb_exec_url
