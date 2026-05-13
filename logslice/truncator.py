"""Field-level and entry-level truncation for log entries."""
from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Optional


Entry = Dict[str, Any]


def truncate_field(entry: Entry, field: str, max_length: int, suffix: str = "...") -> Entry:
    """Return a copy of *entry* with *field* truncated to *max_length* chars.

    If the field value is not a string or is already short enough it is left
    unchanged.  The *suffix* is appended only when truncation actually occurs
    and counts toward *max_length*.
    """
    if max_length < 0:
        raise ValueError("max_length must be >= 0")
    value = entry.get(field)
    if not isinstance(value, str) or len(value) <= max_length:
        return dict(entry)
    cut = max(0, max_length - len(suffix))
    return {**entry, field: value[:cut] + suffix}


def truncate_fields(
    entry: Entry,
    fields: List[str],
    max_length: int,
    suffix: str = "...",
) -> Entry:
    """Apply :func:`truncate_field` for every field in *fields*."""
    result = dict(entry)
    for field in fields:
        result = truncate_field(result, field, max_length, suffix)
    return result


def drop_long_fields(entry: Entry, max_length: int, replace_with: Optional[str] = None) -> Entry:
    """Drop (or replace) any string field whose length exceeds *max_length*.

    If *replace_with* is given the field value is replaced with that string;
    otherwise the field is removed entirely.
    """
    if max_length < 0:
        raise ValueError("max_length must be >= 0")
    result: Entry = {}
    for key, value in entry.items():
        if isinstance(value, str) and len(value) > max_length:
            if replace_with is not None:
                result[key] = replace_with
        else:
            result[key] = value
    return result


def truncate_entries(
    entries: Iterable[Entry],
    fields: List[str],
    max_length: int,
    suffix: str = "...",
) -> Iterator[Entry]:
    """Lazily apply :func:`truncate_fields` over an iterable of entries."""
    for entry in entries:
        yield truncate_fields(entry, fields, max_length, suffix)
