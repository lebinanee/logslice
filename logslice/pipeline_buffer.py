"""Pipeline stages wrapping buffer flush strategies."""

from __future__ import annotations

from typing import Callable, Iterable, Iterator

from logslice.buffer import (
    flush_by_count,
    flush_by_field_change,
    flush_by_predicate,
    sliding_window,
)
from logslice.pipeline import stage


@stage
def count_buffer_stage(
    entries: Iterable[dict],
    *,
    size: int,
) -> Iterator[dict]:
    """Buffer entries and emit them in batches of *size*.

    Each entry is re-emitted individually; use this stage to group processing
    without changing the downstream entry shape.
    """
    for batch in flush_by_count(entries, size=size):
        yield from batch


@stage
def field_change_buffer_stage(
    entries: Iterable[dict],
    *,
    field: str,
    default: str = "",
) -> Iterator[dict]:
    """Emit entries grouped by consecutive runs of the same *field* value."""
    for batch in flush_by_field_change(entries, field=field, default=default):
        yield from batch


@stage
def predicate_buffer_stage(
    entries: Iterable[dict],
    *,
    predicate: Callable[[dict], bool],
) -> Iterator[dict]:
    """Emit entries in batches delimited by entries that satisfy *predicate*."""
    for batch in flush_by_predicate(entries, predicate=predicate):
        yield from batch


@stage
def sliding_window_stage(
    entries: Iterable[dict],
    *,
    size: int,
    merge: Callable[[list[dict]], dict] | None = None,
) -> Iterator[dict]:
    """Slide a window of *size* over entries, optionally merging each window.

    If *merge* is provided it is called with the window list and its return
    value is emitted; otherwise the last entry in each window is emitted.
    """
    for window in sliding_window(entries, size=size):
        if merge is not None:
            yield merge(window)
        else:
            yield window[-1]
