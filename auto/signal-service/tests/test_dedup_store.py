from crypto_signal_service.dedup_store import DedupStore


def test_new_id_not_seen():
    store = DedupStore()
    assert not store.is_seen("abc123")


def test_mark_then_seen():
    store = DedupStore()
    store.mark_seen("abc123")
    assert store.is_seen("abc123")


def test_filter_new():
    store = DedupStore()
    store.mark_seen("old1")
    store.mark_seen("old2")
    result = store.filter_new(["old1", "new1", "old2", "new2"])
    assert result == ["new1", "new2"]


def test_filter_new_all_seen():
    store = DedupStore()
    store.mark_seen("a")
    store.mark_seen("b")
    assert store.filter_new(["a", "b"]) == []


def test_filter_new_empty_input():
    store = DedupStore()
    assert store.filter_new([]) == []
