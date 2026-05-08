"""Merge multiple sorted log streams into a single time-ordered stream."""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Iterator
from typing import Any


_DEFAULT_TIMESTAMP_KEY = "timestamp"
_MISSING = object()


def _get_ts(entry: dict[str, Any], key: str) -> str:
    """Return the timestamp string, or empty string if absent."""
    val = entry.get(key, _MISSING)
    if val is _MISSING:
        return ""
    return str(val)


def merge_sorted(
    streams: Iterable[Iterable[dict[str, Any]]],
    timestamp_key: str = _DEFAULT_TIMESTAMP_KEY,
) -> Iterator[dict[str, Any]]:
    """Merge pre-sorted log streams into one stream ordered by *timestamp_key*.

    Each individual stream is assumed to already be sorted ascending by the
    given key.  Entries whose key is missing sort before those that have it.
    """
    iterators = [iter(s) for s in streams]

    # heap items: (timestamp_str, stream_index, entry)
    heap: list[tuple[str, int, dict[str, Any]]] = []
    for idx, it in enumerate(iterators):
        entry = next(it, None)
        if entry is not None:
            heapq.heappush(heap, (_get_ts(entry, timestamp_key), idx, entry))

    while heap:
        ts, idx, entry = heapq.heappop(heap)
        yield entry
        nxt = next(iterators[idx], None)
        if nxt is not None:
            heapq.heappush(heap, (_get_ts(nxt, timestamp_key), idx, nxt))


def merge_unsorted(
    streams: Iterable[Iterable[dict[str, Any]]],
    timestamp_key: str = _DEFAULT_TIMESTAMP_KEY,
) -> Iterator[dict[str, Any]]:
    """Collect all entries from every stream and yield them sorted by *timestamp_key*.

    Use this when individual streams are not guaranteed to be pre-sorted.
    For large datasets prefer :func:`merge_sorted` with pre-sorted inputs.
    """
    all_entries: list[dict[str, Any]] = []
    for stream in streams:
        all_entries.extend(stream)
    all_entries.sort(key=lambda e: _get_ts(e, timestamp_key))
    yield from all_entries
