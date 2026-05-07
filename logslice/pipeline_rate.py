"""Pipeline stages for rate-limiting log entry streams.

Integrates :mod:`logslice.rate_limiter` with the pipeline decorator
pattern used throughout logslice.
"""

from __future__ import annotations

from typing import Iterable, Iterator

from logslice.pipeline import stage
from logslice.rate_limiter import rate_limit_token_bucket, rate_limit_window


@stage
def window_rate_limit_stage(
    entries: Iterable[dict],
    max_per_interval: int = 100,
    interval_seconds: float = 1.0,
) -> Iterator[dict]:
    """Pipeline stage: sliding-window rate limiter.

    Args:
        entries: Upstream log entries.
        max_per_interval: Max entries allowed per *interval_seconds*.
        interval_seconds: Window length in seconds.

    Yields:
        Entries within the allowed rate.
    """
    yield from rate_limit_window(
        entries,
        max_per_interval=max_per_interval,
        interval_seconds=interval_seconds,
    )


@stage
def token_bucket_rate_limit_stage(
    entries: Iterable[dict],
    rate: float = 50.0,
    burst: int = 10,
) -> Iterator[dict]:
    """Pipeline stage: token-bucket rate limiter.

    Args:
        entries: Upstream log entries.
        rate: Token replenishment rate (tokens/second).
        burst: Maximum burst capacity.

    Yields:
        Entries served by available tokens.
    """
    yield from rate_limit_token_bucket(entries, rate=rate, burst=burst)
