"""Pipeline stages for replaying log entries at controlled rates."""

from __future__ import annotations

import time
from typing import Callable, Iterable, Iterator

from logslice.pipeline import stage
from logslice.replayer import replay_realtime, replay_fixed_rate


@stage
def realtime_replay_stage(
    entries: Iterable[dict],
    ts_field: str = "timestamp",
    speed: float = 1.0,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Iterator[dict]:
    """Pipeline stage: replay entries mimicking original inter-arrival timing."""
    yield from replay_realtime(
        entries,
        ts_field=ts_field,
        speed=speed,
        sleep_fn=sleep_fn,
    )


@stage
def fixed_rate_replay_stage(
    entries: Iterable[dict],
    rate: float = 10.0,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Iterator[dict]:
    """Pipeline stage: replay entries at a fixed entries-per-second rate."""
    yield from replay_fixed_rate(
        entries,
        rate=rate,
        sleep_fn=sleep_fn,
    )
