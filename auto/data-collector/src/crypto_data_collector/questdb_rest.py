from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import requests


LOG = logging.getLogger(__name__)


class QuestDBError(RuntimeError):
    pass


@dataclass(frozen=True)
class QuestDBRest:
    exec_url: str
    timeout_s: int = 30

    def exec(self, query: str) -> dict[str, Any]:
        """Execute SQL via QuestDB REST /exec endpoint."""
        try:
            r = requests.get(self.exec_url, params={"query": query}, timeout=self.timeout_s)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("error"):
                raise QuestDBError(str(data.get("error")))
            return data
        except requests.RequestException as e:
            raise QuestDBError(str(e)) from e

    def scalar(self, query: str, column: str) -> Any:
        """Execute a query and return a single scalar value.

        Args:
            query: SQL query to execute
            column: Name of the column to extract

        Returns:
            The value from the first row of the specified column, or None
        """
        data = self.exec(query)
        # /exec returns {"columns":[...], "dataset":[[...]], ...}
        ds = data.get("dataset") if isinstance(data, dict) else None
        cols = data.get("columns") if isinstance(data, dict) else None
        if not ds or not cols:
            return None
        col_names = [c.get("name") for c in cols]
        if column not in col_names:
            return None
        idx = col_names.index(column)
        return ds[0][idx] if ds and ds[0] else None
