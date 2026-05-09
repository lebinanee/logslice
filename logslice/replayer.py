"""Replay log entries at their original speed or a scaled rate."""

from __future__ import annotations

import time
from typing import Callable, Iterable, Iterator, Optional


def _get_timestamp(entry: dict, ts_field: str) -> Optional[float]:
    """Extract a numeric timestamp from an entry, returning None if absent/invalid."""
    raw = entry.get(ts_field)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def replay_realtime(
    entries: Iterable[dict],
    ts_field: str = "timestamp",
    speed: float = 1.0,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Iterator[dict]:
    """Yield entries with delays that mimic the original inter-arrival times.

    Args:
        entries: Source log entries.
        ts_field: Field that holds a numeric (Unix epoch) timestamp.
        speed: Playback multiplier. 2.0 = twice as fast, 0.5 = half speed.
        sleep_fn: Callable used to sleep; injectable for testing.
    """
    if speed <= 0:
        raise ValueError("speed must be positive")

    prev_ts: Optional[float] = None

    for entry in entries:
        ts = _get_timestamp(entry, ts_field)
        if ts is not None and prev_ts is not None:
            gap = (ts - prev_ts) / speed
            if gap > 0:
                sleep_fn(gap)
        if ts is not None:
            prev_ts = ts
        yield entry


def replay_fixed_rate(
    entries: Iterable[dict],
    rate: float,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Iterator[dict]:
    """Yield entries at a fixed rate (entries per second).

    Args:
        entries: Source log entries.
        rate: Desired throughput in entries per second.
        sleep_fn: Callable used to sleep; injectable for testing.
    """
    if rate <= 0:
        raise ValueError("rate must be positive")

    interval = 1.0 / rate
    for entry in entries:
        yield entry
        sleep_fn(interval)
