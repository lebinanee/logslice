"""Rate limiting utilities for log streams.

Provides time-window and token-bucket based rate limiters to cap
the number of log entries yielded per second (or custom interval).
"""

from __future__ import annotations

import time
from collections import deque
from typing import Iterable, Iterator


def rate_limit_window(
    entries: Iterable[dict],
    max_per_interval: int,
    interval_seconds: float = 1.0,
) -> Iterator[dict]:
    """Yield entries but drop those that exceed *max_per_interval* within
    a sliding *interval_seconds* window.

    Args:
        entries: Source iterable of log entry dicts.
        max_per_interval: Maximum entries allowed in the time window.
        interval_seconds: Length of the sliding window in seconds.

    Yields:
        Entries that fall within the rate limit.
    """
    if max_per_interval <= 0:
        return

    timestamps: deque[float] = deque()

    for entry in entries:
        now = time.monotonic()
        cutoff = now - interval_seconds
        while timestamps and timestamps[0] < cutoff:
            timestamps.popleft()
        if len(timestamps) < max_per_interval:
            timestamps.append(now)
            yield entry


def rate_limit_token_bucket(
    entries: Iterable[dict],
    rate: float,
    burst: int = 1,
) -> Iterator[dict]:
    """Yield entries using a token-bucket algorithm.

    Tokens accumulate at *rate* tokens/second up to *burst* capacity.
    Each yielded entry consumes one token; entries arriving when the
    bucket is empty are dropped.

    Args:
        entries: Source iterable of log entry dicts.
        rate: Token replenishment rate (tokens per second).
        burst: Maximum token capacity (burst size).

    Yields:
        Entries that can be served by available tokens.
    """
    if rate <= 0 or burst <= 0:
        return

    tokens: float = float(burst)
    last_check = time.monotonic()

    for entry in entries:
        now = time.monotonic()
        elapsed = now - last_check
        last_check = now
        tokens = min(float(burst), tokens + elapsed * rate)
        if tokens >= 1.0:
            tokens -= 1.0
            yield entry
