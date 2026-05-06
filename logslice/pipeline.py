"""Composable log processing pipeline."""

from __future__ import annotations

from typing import Callable, Iterable, Iterator, Optional

from logslice.deduplicator import dedupe_exact, dedupe_window
from logslice.log_filter import filter_logs
from logslice.sampler import sample_every_nth, sample_random


Stage = Callable[[Iterable[dict]], Iterator[dict]]


def build_pipeline(*stages: Stage) -> Stage:
    """Compose multiple pipeline stages into a single callable.

    Each stage is a function that accepts and returns an iterable of dicts.
    Stages are applied left-to-right.
    """
    def pipeline(entries: Iterable[dict]) -> Iterator[dict]:
        stream: Iterable[dict] = entries
        for stage in stages:
            stream = stage(stream)
        return iter(stream)

    return pipeline


def filter_stage(query: str) -> Stage:
    """Return a pipeline stage that filters entries by *query*."""
    def stage(entries: Iterable[dict]) -> Iterator[dict]:
        return filter_logs(entries, query)
    return stage


def dedupe_exact_stage(fields: Optional[list[str]] = None) -> Stage:
    """Return a pipeline stage that removes exact duplicate entries."""
    def stage(entries: Iterable[dict]) -> Iterator[dict]:
        return dedupe_exact(entries, fields=fields)
    return stage


def dedupe_window_stage(
    window_size: int = 100,
    fields: Optional[list[str]] = None,
) -> Stage:
    """Return a pipeline stage that suppresses duplicates within a window."""
    def stage(entries: Iterable[dict]) -> Iterator[dict]:
        return dedupe_window(entries, window_size=window_size, fields=fields)
    return stage


def sample_nth_stage(n: int) -> Stage:
    """Return a pipeline stage that keeps every *n*-th entry."""
    def stage(entries: Iterable[dict]) -> Iterator[dict]:
        return sample_every_nth(entries, n)
    return stage


def sample_rate_stage(rate: float, seed: Optional[int] = None) -> Stage:
    """Return a pipeline stage that randomly samples entries at *rate*."""
    def stage(entries: Iterable[dict]) -> Iterator[dict]:
        return sample_random(entries, rate, seed=seed)
    return stage


def limit_stage(n: int) -> Stage:
    """Return a pipeline stage that yields at most *n* entries."""
    def stage(entries: Iterable[dict]) -> Iterator[dict]:
        for i, entry in enumerate(entries):
            if i >= n:
                break
            yield entry
    return stage
