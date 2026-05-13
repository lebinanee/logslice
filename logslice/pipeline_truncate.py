"""Pipeline stages for field truncation."""
from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

from logslice.pipeline import stage
from logslice.truncator import (
    drop_long_fields,
    truncate_entries,
    truncate_fields,
)

Entry = Dict[str, Any]


def truncate_fields_stage(
    fields: List[str],
    max_length: int,
    suffix: str = "...",
) -> Callable[[Iterable[Entry]], Iterator[Entry]]:
    """Return a pipeline stage that truncates *fields* to *max_length* chars."""

    @stage
    def _truncate(entries: Iterable[Entry]) -> Iterator[Entry]:
        yield from truncate_entries(entries, fields, max_length, suffix)

    return _truncate


def drop_long_fields_stage(
    max_length: int,
    replace_with: Optional[str] = None,
) -> Callable[[Iterable[Entry]], Iterator[Entry]]:
    """Return a pipeline stage that drops (or replaces) fields longer than *max_length*."""

    @stage
    def _drop(entries: Iterable[Entry]) -> Iterator[Entry]:
        for entry in entries:
            yield drop_long_fields(entry, max_length, replace_with)

    return _drop
