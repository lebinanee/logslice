"""Buffered log entry collection with flush strategies."""

from __future__ import annotations

from collections import deque
from typing import Callable, Iterable, Iterator


def flush_by_count(
    entries: Iterable[dict],
    size: int,
) -> Iterator[list[dict]]:
    """Yield batches of exactly *size* entries (final batch may be smaller)."""
    if size < 1:
        raise ValueError("size must be >= 1")
    buf: list[dict] = []
    for entry in entries:
        buf.append(entry)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def flush_by_field_change(
    entries: Iterable[dict],
    field: str,
    default: str = "",
) -> Iterator[list[dict]]:
    """Yield a batch whenever *field* changes value between consecutive entries."""
    buf: list[dict] = []
    current_value: object = object()  # sentinel — never matches a real value
    for entry in entries:
        value = entry.get(field, default)
        if buf and value != current_value:
            yield buf
            buf = []
        current_value = value
        buf.append(entry)
    if buf:
        yield buf


def flush_by_predicate(
    entries: Iterable[dict],
    predicate: Callable[[dict], bool],
) -> Iterator[list[dict]]:
    """Yield a batch each time *predicate* returns True for an entry.

    The triggering entry is included as the last item of its batch.
    """
    buf: list[dict] = []
    for entry in entries:
        buf.append(entry)
        if predicate(entry):
            yield buf
            buf = []
    if buf:
        yield buf


def sliding_window(
    entries: Iterable[dict],
    size: int,
) -> Iterator[list[dict]]:
    """Yield every consecutive sliding window of *size* entries."""
    if size < 1:
        raise ValueError("size must be >= 1")
    window: deque[dict] = deque(maxlen=size)
    for entry in entries:
        window.append(entry)
        if len(window) == size:
            yield list(window)
