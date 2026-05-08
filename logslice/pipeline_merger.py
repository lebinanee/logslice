"""Pipeline stages for merging multiple log streams."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from logslice.merger import merge_sorted, merge_unsorted
from logslice.pipeline import stage


def merge_sorted_stage(
    *streams: Iterable[dict[str, Any]],
    timestamp_key: str = "timestamp",
) -> Iterator[dict[str, Any]]:
    """Pipeline stage: merge pre-sorted streams in timestamp order.

    Example::

        combined = merge_sorted_stage(stream_a, stream_b, timestamp_key="ts")
    """
    return merge_sorted(streams, timestamp_key=timestamp_key)


def merge_unsorted_stage(
    *streams: Iterable[dict[str, Any]],
    timestamp_key: str = "timestamp",
) -> Iterator[dict[str, Any]]:
    """Pipeline stage: merge unsorted streams, buffering all entries first.

    Example::

        combined = merge_unsorted_stage(stream_a, stream_b)
    """
    return merge_unsorted(streams, timestamp_key=timestamp_key)


@stage
def interleave_stage(
    entries: Iterable[dict[str, Any]],
    extra: Iterable[dict[str, Any]],
    timestamp_key: str = "timestamp",
) -> Iterator[dict[str, Any]]:
    """Pipeline stage: merge *entries* with one additional *extra* stream.

    Suitable for use inside a :func:`logslice.pipeline.stage`-based pipeline
    where a single primary stream is already flowing.
    """
    yield from merge_sorted([entries, extra], timestamp_key=timestamp_key)
