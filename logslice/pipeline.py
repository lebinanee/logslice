"""Composable pipeline builder for log processing stages.

This module extends the existing pipeline with redaction support.
"""

from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional

from logslice.log_filter import filter_logs
from logslice.deduplicator import dedupe_exact, dedupe_window
from logslice.sampler import sample_random, sample_every_nth
from logslice.redactor import redact_entries

Entry = Dict[str, Any]
Stage = Callable[[Iterable[Entry]], Iterator[Entry]]


def stage(fn: Callable[..., Iterator[Entry]], **kwargs) -> Stage:
    """Partially apply *kwargs* to *fn*, returning a single-arg Stage."""
    def _stage(entries: Iterable[Entry]) -> Iterator[Entry]:
        return fn(entries, **kwargs)
    _stage.__name__ = fn.__name__
    return _stage


def filter_stage(query: str) -> Stage:
    return stage(filter_logs, query=query)


def dedupe_exact_stage(fields: Optional[List[str]] = None) -> Stage:
    return stage(dedupe_exact, fields=fields)


def dedupe_window_stage(size: int, fields: Optional[List[str]] = None) -> Stage:
    return stage(dedupe_window, window_size=size, fields=fields)


def sample_random_stage(rate: float, seed: Optional[int] = None) -> Stage:
    return stage(sample_random, rate=rate, seed=seed)


def sample_nth_stage(n: int) -> Stage:
    return stage(sample_every_nth, n=n)


def redact_stage(
    fields: Optional[List[str]] = None,
    strategy: str = "mask",
    salt: str = "",
    visible: int = 4,
) -> Stage:
    """Pipeline stage that redacts sensitive fields from each entry."""
    return stage(
        redact_entries,
        fields=fields,
        strategy=strategy,
        salt=salt,
        visible=visible,
    )


def build_pipeline(stages: List[Stage]) -> Stage:
    """Compose a list of stages into a single stage."""
    def _pipeline(entries: Iterable[Entry]) -> Iterator[Entry]:
        stream: Iterable[Entry] = entries
        for s in stages:
            stream = s(stream)
        return iter(stream)
    return _pipeline


def pipeline(
    entries: Iterable[Entry],
    stages: List[Stage],
) -> Iterator[Entry]:
    """Run *entries* through each stage in order."""
    return build_pipeline(stages)(entries)


def iter_with_sequence(
    entries: Iterable[Entry],
    field: str = "_seq",
) -> Iterator[Entry]:
    for i, entry in enumerate(entries):
        yield {**entry, field: i}
