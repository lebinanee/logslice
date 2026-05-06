"""Enrich log entries with derived or injected fields."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

LogEntry = Dict[str, Any]
Enricher = Callable[[LogEntry], LogEntry]


def add_static_field(key: str, value: Any) -> Enricher:
    """Return an enricher that adds a fixed key/value to every entry."""

    def _enrich(entry: LogEntry) -> LogEntry:
        out = dict(entry)
        out[key] = value
        return out

    return _enrich


def add_ingestion_timestamp(key: str = "_ingested_at") -> Enricher:
    """Return an enricher that stamps each entry with the current UTC time."""

    def _enrich(entry: LogEntry) -> LogEntry:
        out = dict(entry)
        out[key] = datetime.now(timezone.utc).isoformat()
        return out

    return _enrich


def add_entry_hash(fields: Optional[List[str]] = None, key: str = "_hash") -> Enricher:
    """Return an enricher that adds a short SHA-256 hash of selected fields.

    If *fields* is None the entire entry (sorted keys) is hashed.
    """

    def _enrich(entry: LogEntry) -> LogEntry:
        out = dict(entry)
        chosen = fields if fields is not None else sorted(entry.keys())
        raw = "|".join(str(entry.get(f, "")) for f in chosen)
        digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
        out[key] = digest
        return out

    return _enrich


def rename_field(src: str, dst: str) -> Enricher:
    """Return an enricher that renames *src* to *dst* (no-op if absent)."""

    def _enrich(entry: LogEntry) -> LogEntry:
        out = dict(entry)
        if src in out:
            out[dst] = out.pop(src)
        return out

    return _enrich


def enrich_entries(
    entries: Iterable[LogEntry],
    enrichers: List[Enricher],
) -> Iterator[LogEntry]:
    """Apply a sequence of enrichers to every entry in *entries*."""
    for entry in entries:
        for fn in enrichers:
            entry = fn(entry)
        yield entry
