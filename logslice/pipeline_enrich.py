"""Pipeline stages that wrap enricher helpers."""

from __future__ import annotations

from typing import Any, Iterable, Iterator, List, Optional

from logslice.enricher import (
    LogEntry,
    add_entry_hash,
    add_ingestion_timestamp,
    add_static_field,
    enrich_entries,
    rename_field,
)
from logslice.pipeline import stage


@stage
def static_field_stage(
    entries: Iterable[LogEntry],
    key: str,
    value: Any,
) -> Iterator[LogEntry]:
    """Pipeline stage: inject a static field into every entry."""
    return enrich_entries(entries, [add_static_field(key, value)])


@stage
def ingestion_timestamp_stage(
    entries: Iterable[LogEntry],
    key: str = "_ingested_at",
) -> Iterator[LogEntry]:
    """Pipeline stage: add an ingestion timestamp to every entry."""
    return enrich_entries(entries, [add_ingestion_timestamp(key)])


@stage
def entry_hash_stage(
    entries: Iterable[LogEntry],
    fields: Optional[List[str]] = None,
    key: str = "_hash",
) -> Iterator[LogEntry]:
    """Pipeline stage: add a short content hash to every entry."""
    return enrich_entries(entries, [add_entry_hash(fields, key)])


@stage
def rename_field_stage(
    entries: Iterable[LogEntry],
    src: str,
    dst: str,
) -> Iterator[LogEntry]:
    """Pipeline stage: rename a field in every entry."""
    return enrich_entries(entries, [rename_field(src, dst)])
