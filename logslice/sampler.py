"""Log sampling utilities for reducing high-volume log streams."""

from __future__ import annotations

import hashlib
import random
from typing import Iterable, Iterator


def sample_random(
    entries: Iterable[dict],
    rate: float,
    seed: int | None = None,
) -> Iterator[dict]:
    """Yield a random fraction of log entries.

    Args:
        entries: Iterable of parsed log dicts.
        rate: Fraction to keep, between 0.0 and 1.0 inclusive.
        seed: Optional RNG seed for reproducibility.

    Yields:
        Selected log entry dicts.
    """
    if not 0.0 <= rate <= 1.0:
        raise ValueError(f"rate must be between 0.0 and 1.0, got {rate}")
    rng = random.Random(seed)
    for entry in entries:
        if rng.random() < rate:
            yield entry


def sample_every_nth(entries: Iterable[dict], n: int) -> Iterator[dict]:
    """Yield every n-th log entry (1-based).

    Args:
        entries: Iterable of parsed log dicts.
        n: Step size; must be >= 1.

    Yields:
        Every n-th entry.
    """
    if n < 1:
        raise ValueError(f"n must be >= 1, got {n}")
    for i, entry in enumerate(entries):
        if i % n == 0:
            yield entry


def sample_by_field_hash(
    entries: Iterable[dict],
    field: str,
    rate: float,
) -> Iterator[dict]:
    """Deterministically sample entries based on the hash of a field value.

    Entries with the same field value are always included or always excluded,
    which is useful for consistent sampling by request_id, user_id, etc.

    Args:
        entries: Iterable of parsed log dicts.
        field: The field whose value is hashed.
        rate: Fraction to keep, between 0.0 and 1.0 inclusive.

    Yields:
        Selected log entry dicts.
    """
    if not 0.0 <= rate <= 1.0:
        raise ValueError(f"rate must be between 0.0 and 1.0, got {rate}")
    bucket_max = 2**32
    threshold = int(rate * bucket_max)
    for entry in entries:
        value = str(entry.get(field, ""))
        digest = hashlib.md5(value.encode(), usedforsecurity=False).digest()
        bucket = int.from_bytes(digest[:4], "little")
        if bucket < threshold:
            yield entry
