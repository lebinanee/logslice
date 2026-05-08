"""Alert on log entries matching threshold conditions over a sliding window."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Iterable, Iterator

from logslice.query_parser import Query, match


@dataclass
class AlertRule:
    """Fires when *count* entries matching *query* arrive within *window_seconds*."""

    name: str
    query: Query
    threshold: int
    window_seconds: float
    on_alert: Callable[[str, list[dict]], None]


@dataclass
class _WindowCounter:
    window_seconds: float
    timestamps: deque = field(default_factory=deque)

    def add(self, ts: float) -> None:
        self.timestamps.append(ts)
        self._evict(ts)

    def _evict(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self.timestamps and self.timestamps[0] < cutoff:
            self.timestamps.popleft()

    def count(self) -> int:
        return len(self.timestamps)


def evaluate_rules(
    entries: Iterable[dict],
    rules: list[AlertRule],
    *,
    timestamp_key: str = "ts",
    clock: Callable[[], float] | None = None,
) -> Iterator[dict]:
    """Pass every entry through; call rule.on_alert when a threshold is crossed.

    Yields each entry unchanged so the function composes with other pipeline stages.
    """
    _clock = clock or time.time
    counters: dict[str, _WindowCounter] = {
        r.name: _WindowCounter(r.window_seconds) for r in rules
    }
    matching_entries: dict[str, list[dict]] = {r.name: [] for r in rules}
    fired: dict[str, bool] = {r.name: False for r in rules}

    for entry in entries:
        now = float(entry.get(timestamp_key, _clock()))
        for rule in rules:
            if match(entry, rule.query):
                counters[rule.name].add(now)
                matching_entries[rule.name].append(entry)
                if (
                    counters[rule.name].count() >= rule.threshold
                    and not fired[rule.name]
                ):
                    fired[rule.name] = True
                    rule.on_alert(rule.name, list(matching_entries[rule.name]))
        yield entry
