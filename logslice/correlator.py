"""Correlate log entries across streams by a shared field (e.g. request_id)."""
from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, Iterable, Iterator, List, Optional

Entry = Dict[str, object]


def correlate_by_field(
    entries: Iterable[Entry],
    field: str,
    *,
    min_count: int = 2,
    max_group_size: int = 1000,
) -> Iterator[List[Entry]]:
    """Collect entries that share the same *field* value and yield groups.

    Groups are emitted only when the source is exhausted (batch mode).
    Groups smaller than *min_count* are silently dropped.
    Groups larger than *max_group_size* are capped to avoid memory blow-up.
    """
    if min_count < 1:
        raise ValueError("min_count must be >= 1")
    if max_group_size < 1:
        raise ValueError("max_group_size must be >= 1")

    buckets: Dict[object, List[Entry]] = defaultdict(list)
    for entry in entries:
        key = entry.get(field)
        if key is None:
            continue
        bucket = buckets[key]
        if len(bucket) < max_group_size:
            bucket.append(entry)

    for group in buckets.values():
        if len(group) >= min_count:
            yield group


def correlate_with_timeout(
    entries: Iterable[Entry],
    field: str,
    *,
    ts_field: str = "timestamp",
    window_seconds: float = 60.0,
    min_count: int = 2,
) -> Iterator[List[Entry]]:
    """Like *correlate_by_field* but flushes a group once the gap between the
    first and latest entry in that group exceeds *window_seconds*."""
    if window_seconds <= 0:
        raise ValueError("window_seconds must be > 0")

    buckets: Dict[object, List[Entry]] = defaultdict(list)
    first_ts: Dict[object, float] = {}

    def _ts(e: Entry) -> Optional[float]:
        val = e.get(ts_field)
        try:
            return float(val)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    for entry in entries:
        key = entry.get(field)
        if key is None:
            continue
        now = _ts(entry)
        if key not in first_ts:
            if now is not None:
                first_ts[key] = now
        else:
            if now is not None and (now - first_ts[key]) > window_seconds:
                group = buckets.pop(key)
                first_ts.pop(key, None)
                if len(group) >= min_count:
                    yield group
                buckets[key] = [entry]
                if now is not None:
                    first_ts[key] = now
                continue
        buckets[key].append(entry)

    for key, group in buckets.items():
        if len(group) >= min_count:
            yield group


def flatten_groups(groups: Iterable[List[Entry]]) -> Iterator[Entry]:
    """Flatten correlated groups back into a single entry stream."""
    for group in groups:
        yield from group
