"""Aggregation utilities for counting and grouping log entries."""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Iterable, Iterator


def count_by_field(
    entries: Iterable[dict],
    field: str,
) -> dict[str, int]:
    """Count log entries grouped by the value of *field*.

    Missing / None values are grouped under the key ``"<missing>"``.
    """
    counter: Counter[str] = Counter()
    for entry in entries:
        value = entry.get(field)
        key = str(value) if value is not None else "<missing>"
        counter[key] += 1
    return dict(counter.most_common())


def group_by_field(
    entries: Iterable[dict],
    field: str,
) -> dict[str, list[dict]]:
    """Group log entries by the value of *field*.

    Returns an ordered dict (most-frequent group first) where each key is a
    string representation of the field value and each value is the list of
    matching log entries.
    """
    groups: dict[str, list[dict]] = defaultdict(list)
    for entry in entries:
        value = entry.get(field)
        key = str(value) if value is not None else "<missing>"
        groups[key].append(entry)
    # Sort groups by descending size for ergonomic display
    return dict(sorted(groups.items(), key=lambda kv: -len(kv[1])))


def top_n(
    entries: Iterable[dict],
    field: str,
    n: int = 10,
) -> list[tuple[str, int]]:
    """Return the *n* most common values for *field* as (value, count) pairs."""
    counter: Counter[str] = Counter()
    for entry in entries:
        value = entry.get(field)
        key = str(value) if value is not None else "<missing>"
        counter[key] += 1
    return counter.most_common(n)


def iter_with_sequence(
    entries: Iterable[dict],
    start: int = 1,
) -> Iterator[tuple[int, dict]]:
    """Yield ``(sequence_number, entry)`` pairs, useful for numbered output."""
    for seq, entry in enumerate(entries, start=start):
        yield seq, entry
