"""Field transformation utilities for log entries."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional


Entry = Dict[str, Any]
TransformFn = Callable[[Any], Any]


def rename_fields(entry: Entry, mapping: Dict[str, str]) -> Entry:
    """Return a new entry with fields renamed according to *mapping*.

    Keys not present in *mapping* are left unchanged.
    """
    result = {}
    for key, value in entry.items():
        result[mapping.get(key, key)] = value
    return result


def drop_fields(entry: Entry, fields: List[str]) -> Entry:
    """Return a new entry with the specified *fields* removed."""
    return {k: v for k, v in entry.items() if k not in fields}


def keep_fields(entry: Entry, fields: List[str]) -> Entry:
    """Return a new entry containing only the specified *fields*."""
    return {k: entry[k] for k in fields if k in entry}


def apply_field(entry: Entry, field: str, fn: TransformFn) -> Entry:
    """Return a new entry with *fn* applied to *field*'s value.

    If *field* is absent the entry is returned unchanged.
    """
    if field not in entry:
        return dict(entry)
    result = dict(entry)
    result[field] = fn(entry[field])
    return result


def cast_field(entry: Entry, field: str, type_fn: Callable[[Any], Any]) -> Entry:
    """Attempt to cast *field* to *type_fn*; leave unchanged on failure."""
    if field not in entry:
        return dict(entry)
    result = dict(entry)
    try:
        result[field] = type_fn(entry[field])
    except (ValueError, TypeError):
        pass
    return result


def transform_entries(
    entries: Iterable[Entry],
    *transforms: Callable[[Entry], Entry],
) -> Iterator[Entry]:
    """Apply a sequence of *transforms* to each entry in *entries*."""
    for entry in entries:
        for fn in transforms:
            entry = fn(entry)
        yield entry
