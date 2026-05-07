"""Tests for logslice.rate_limiter and logslice.pipeline_rate."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from logslice.rate_limiter import rate_limit_token_bucket, rate_limit_window
from logslice.pipeline_rate import token_bucket_rate_limit_stage, window_rate_limit_stage


def make_entries(n: int) -> list[dict]:
    return [{"id": i, "msg": f"entry-{i}"} for i in range(n)]


# ---------------------------------------------------------------------------
# rate_limit_window
# ---------------------------------------------------------------------------

def test_window_allows_up_to_limit():
    entries = make_entries(5)
    result = list(rate_limit_window(entries, max_per_interval=5, interval_seconds=60.0))
    assert result == entries


def test_window_drops_excess():
    entries = make_entries(10)
    result = list(rate_limit_window(entries, max_per_interval=3, interval_seconds=60.0))
    assert len(result) == 3
    assert result == entries[:3]


def test_window_zero_limit_yields_nothing():
    entries = make_entries(5)
    result = list(rate_limit_window(entries, max_per_interval=0))
    assert result == []


def test_window_negative_limit_yields_nothing():
    entries = make_entries(5)
    result = list(rate_limit_window(entries, max_per_interval=-1))
    assert result == []


def test_window_empty_input():
    result = list(rate_limit_window([], max_per_interval=10))
    assert result == []


def test_window_expired_slots_free_capacity():
    """Entries arriving after the window expires should be allowed again."""
    entries = make_entries(4)
    times = [0.0, 0.1, 0.2, 10.0]  # last entry is well outside 1-s window
    with patch("logslice.rate_limiter.time.monotonic", side_effect=times):
        result = list(rate_limit_window(entries, max_per_interval=2, interval_seconds=1.0))
    # entries 0,1 pass; entry 2 is dropped; entry 3 passes (window expired)
    assert len(result) == 3
    assert result[0]["id"] == 0
    assert result[1]["id"] == 1
    assert result[2]["id"] == 3


# ---------------------------------------------------------------------------
# rate_limit_token_bucket
# ---------------------------------------------------------------------------

def test_token_bucket_burst_allows_initial_entries():
    entries = make_entries(5)
    times = [0.0] * 10  # no time passes => no new tokens minted
    with patch("logslice.rate_limiter.time.monotonic", side_effect=times):
        result = list(rate_limit_token_bucket(entries, rate=1.0, burst=3))
    assert len(result) == 3


def test_token_bucket_zero_rate_yields_nothing():
    result = list(rate_limit_token_bucket(make_entries(5), rate=0, burst=5))
    assert result == []


def test_token_bucket_zero_burst_yields_nothing():
    result = list(rate_limit_token_bucket(make_entries(5), rate=10.0, burst=0))
    assert result == []


def test_token_bucket_empty_input():
    result = list(rate_limit_token_bucket([], rate=10.0, burst=5))
    assert result == []


def test_token_bucket_refills_over_time():
    entries = make_entries(4)
    # t=0 init, then each entry check advances time by 1 s (rate=1 => +1 token)
    times = [0.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    with patch("logslice.rate_limiter.time.monotonic", side_effect=times):
        result = list(rate_limit_token_bucket(entries, rate=1.0, burst=1))
    assert len(result) == 4


# ---------------------------------------------------------------------------
# pipeline stages
# ---------------------------------------------------------------------------

def test_window_rate_limit_stage_basic():
    entries = make_entries(6)
    result = list(window_rate_limit_stage(entries, max_per_interval=4, interval_seconds=60.0))
    assert len(result) == 4


def test_token_bucket_rate_limit_stage_basic():
    entries = make_entries(5)
    times = [0.0] * 12
    with patch("logslice.rate_limiter.time.monotonic", side_effect=times):
        result = list(token_bucket_rate_limit_stage(entries, rate=1.0, burst=2))
    assert len(result) == 2
