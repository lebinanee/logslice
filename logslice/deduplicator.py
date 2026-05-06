"""Deduplication utilities for structured log entries."""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from typing import Iterable, Iterator, Optional


def _entry_fingerprint(entry: dict, fields: Optional[list[str]] = None) -> str:
    """Return a stable hash representing an entry or a subset of its fields."""
    if fields:
        subset = {k: entry[k] for k in fields if k in entry}
    else:
        subset = entry
    serialized = json.dumps(subset, sort_keys=True, default=str)
    return hashlib.md5(serialized.encode()).hexdigest()


def dedupe_exact(
    entries: Iterable[dict],
    fields: Optional[list[str]] = None,
) -> Iterator[dict]:
    """Yield entries, skipping exact duplicates.

    Args:
        entries: Iterable of log entry dicts.
        fields: If given, only these fields are used for comparison.
                Defaults to the full entry.
    """
    seen: set[str] = set()
    for entry in entries:
        fp = _entry_fingerprint(entry, fields)
        if fp not in seen:
            seen.add(fp)
            yield entry


def dedupe_window(
    entries: Iterable[dict],
    window_size: int = 100,
    fields: Optional[list[str]] = None,
) -> Iterator[dict]:
    """Yield entries, suppressing duplicates within a sliding window.

    Args:
        entries: Iterable of log entry dicts.
        window_size: Number of recent fingerprints to remember.
        fields: If given, only these fields are used for comparison.
    """
    if window_size <= 0:
        raise ValueError("window_size must be a positive integer")

    recent: OrderedDict[str, None] = OrderedDict()
    for entry in entries:
        fp = _entry_fingerprint(entry, fields)
        if fp in recent:
            # Move to end to refresh recency
            recent.move_to_end(fp)
            continue
        recent[fp] = None
        if len(recent) > window_size:
            recent.popitem(last=False)
        yield entry
