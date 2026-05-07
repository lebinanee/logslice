"""Pipeline stages wrapping transformer utilities."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Iterator, List

from logslice.pipeline import stage
from logslice.transformer import (
    apply_field,
    cast_field,
    drop_fields,
    keep_fields,
    rename_fields,
)

Entry = Dict[str, Any]


@stage
def rename_fields_stage(
    entries: Iterable[Entry], mapping: Dict[str, str]
) -> Iterator[Entry]:
    """Pipeline stage: rename fields according to *mapping*."""
    for entry in entries:
        yield rename_fields(entry, mapping)


@stage
def drop_fields_stage(
    entries: Iterable[Entry], fields: List[str]
) -> Iterator[Entry]:
    """Pipeline stage: drop the listed *fields* from every entry."""
    for entry in entries:
        yield drop_fields(entry, fields)


@stage
def keep_fields_stage(
    entries: Iterable[Entry], fields: List[str]
) -> Iterator[Entry]:
    """Pipeline stage: keep only the listed *fields* in every entry."""
    for entry in entries:
        yield keep_fields(entry, fields)


@stage
def apply_field_stage(
    entries: Iterable[Entry],
    field: str,
    fn: Callable[[Any], Any],
) -> Iterator[Entry]:
    """Pipeline stage: apply *fn* to *field* in every entry."""
    for entry in entries:
        yield apply_field(entry, field, fn)


@stage
def cast_field_stage(
    entries: Iterable[Entry],
    field: str,
    type_fn: Callable[[Any], Any],
) -> Iterator[Entry]:
    """Pipeline stage: cast *field* using *type_fn* in every entry."""
    for entry in entries:
        yield cast_field(entry, field, type_fn)
