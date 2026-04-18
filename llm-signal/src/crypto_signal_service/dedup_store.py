from __future__ import annotations


class DedupStore:
    """In-process dedup set for signal IDs.

    QuestDB DEDUP UPSERT handles cross-restart idempotency;
    this class prevents redundant LLM calls within a single run.
    """

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def is_seen(self, signal_id: str) -> bool:
        return signal_id in self._seen

    def mark_seen(self, signal_id: str) -> None:
        self._seen.add(signal_id)

    def filter_new(self, signal_ids: list[str]) -> list[str]:
        return [sid for sid in signal_ids if sid not in self._seen]
