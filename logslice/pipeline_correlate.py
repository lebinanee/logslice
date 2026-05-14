"""Pipeline stages for log correlation."""
from __future__ import annotations

from typing import Dict, Iterable, Iterator, List

from logslice.correlator import (
    correlate_by_field,
    correlate_with_timeout,
    flatten_groups,
)
from logslice.pipeline import stage

Entry = Dict[str, object]
Group = List[Entry]


@stage
def correlate_stage(
    entries: Iterable[Entry],
    *,
    field: str,
    min_count: int = 2,
    max_group_size: int = 1000,
    emit_flat: bool = False,
) -> Iterator[Entry]:
    """Correlate entries by *field*; yield each group as a synthetic summary
    entry (or flat entries when *emit_flat* is True)."""
    groups = correlate_by_field(
        entries,
        field,
        min_count=min_count,
        max_group_size=max_group_size,
    )
    if emit_flat:
        yield from flatten_groups(groups)
    else:
        for group in groups:
            yield {
                "_correlation_field": field,
                "_correlation_key": group[0].get(field),
                "_correlation_count": len(group),
                "entries": group,
            }


@stage
def correlate_window_stage(
    entries: Iterable[Entry],
    *,
    field: str,
    ts_field: str = "timestamp",
    window_seconds: float = 60.0,
    min_count: int = 2,
    emit_flat: bool = False,
) -> Iterator[Entry]:
    """Time-windowed correlation stage."""
    groups = correlate_with_timeout(
        entries,
        field,
        ts_field=ts_field,
        window_seconds=window_seconds,
        min_count=min_count,
    )
    if emit_flat:
        yield from flatten_groups(groups)
    else:
        for group in groups:
            yield {
                "_correlation_field": field,
                "_correlation_key": group[0].get(field),
                "_correlation_count": len(group),
                "_window_seconds": window_seconds,
                "entries": group,
            }
