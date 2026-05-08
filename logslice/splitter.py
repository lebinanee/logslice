"""Split a log stream into multiple named output streams based on field values."""

from __future__ import annotations

from collections import defaultdict
from typing import Callable, Dict, Generator, Iterable, List, Tuple


Entry = Dict[str, object]
Sink = List[Entry]


def split_by_field(
    entries: Iterable[Entry],
    field: str,
    default: str = "__other__",
) -> Dict[str, List[Entry]]:
    """Partition entries into buckets keyed by the value of *field*.

    Entries that are missing the field are placed in the *default* bucket.
    """
    buckets: Dict[str, List[Entry]] = defaultdict(list)
    for entry in entries:
        key = str(entry.get(field, default))
        buckets[key].append(entry)
    return dict(buckets)


def split_by_predicate(
    entries: Iterable[Entry],
    predicates: List[Tuple[str, Callable[[Entry], bool]]],
    default: str = "__other__",
) -> Dict[str, List[Entry]]:
    """Partition entries using a prioritised list of (name, predicate) pairs.

    The first matching predicate wins.  Entries that match no predicate are
    placed in the *default* bucket.
    """
    buckets: Dict[str, List[Entry]] = defaultdict(list)
    for entry in entries:
        matched = False
        for name, pred in predicates:
            if pred(entry):
                buckets[name].append(entry)
                matched = True
                break
        if not matched:
            buckets[default].append(entry)
    return dict(buckets)


def iter_split_by_field(
    entries: Iterable[Entry],
    field: str,
    default: str = "__other__",
) -> Generator[Tuple[str, Entry], None, None]:
    """Yield *(bucket_key, entry)* pairs without buffering all entries."""
    for entry in entries:
        key = str(entry.get(field, default))
        yield key, entry
