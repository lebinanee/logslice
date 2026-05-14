"""Tests for logslice.correlator."""
from __future__ import annotations

import pytest

from logslice.correlator import (
    correlate_by_field,
    correlate_with_timeout,
    flatten_groups,
)


def _e(req_id: str, level: str = "info", ts: float | None = None) -> dict:
    entry: dict = {"request_id": req_id, "level": level}
    if ts is not None:
        entry["timestamp"] = ts
    return entry


# ---------------------------------------------------------------------------
# correlate_by_field
# ---------------------------------------------------------------------------

def test_correlate_groups_shared_field():
    entries = [_e("a"), _e("b"), _e("a"), _e("a")]
    groups = list(correlate_by_field(entries, "request_id", min_count=2))
    assert len(groups) == 1
    assert len(groups[0]) == 3


def test_correlate_drops_singleton_groups():
    entries = [_e("x"), _e("y"), _e("x")]
    groups = list(correlate_by_field(entries, "request_id", min_count=2))
    keys = {g[0]["request_id"] for g in groups}
    assert "y" not in keys


def test_correlate_min_count_one_yields_all_groups():
    entries = [_e("a"), _e("b"), _e("a")]
    groups = list(correlate_by_field(entries, "request_id", min_count=1))
    assert len(groups) == 2


def test_correlate_missing_field_entries_ignored():
    entries = [{"level": "info"}, _e("a"), _e("a")]
    groups = list(correlate_by_field(entries, "request_id", min_count=2))
    assert len(groups) == 1
    assert all("request_id" in e for e in groups[0])


def test_correlate_max_group_size_caps_entries():
    entries = [_e("r") for _ in range(20)]
    groups = list(correlate_by_field(entries, "request_id", min_count=1, max_group_size=5))
    assert len(groups) == 1
    assert len(groups[0]) == 5


def test_correlate_empty_input_yields_nothing():
    groups = list(correlate_by_field([], "request_id"))
    assert groups == []


def test_correlate_invalid_min_count_raises():
    with pytest.raises(ValueError):
        list(correlate_by_field([], "request_id", min_count=0))


def test_correlate_invalid_max_group_size_raises():
    with pytest.raises(ValueError):
        list(correlate_by_field([], "request_id", max_group_size=0))


# ---------------------------------------------------------------------------
# correlate_with_timeout
# ---------------------------------------------------------------------------

def test_timeout_flushes_group_when_window_exceeded():
    entries = [
        _e("r1", ts=0.0),
        _e("r1", ts=30.0),
        _e("r1", ts=120.0),  # > 60 s from first => flush previous two
    ]
    groups = list(correlate_with_timeout(entries, "request_id", window_seconds=60.0, min_count=1))
    # first group (ts 0 & 30) flushed on seeing ts=120; then final group [ts=120]
    assert len(groups) == 2
    assert len(groups[0]) == 2
    assert len(groups[1]) == 1


def test_timeout_no_flush_within_window():
    entries = [_e("r1", ts=0.0), _e("r1", ts=10.0), _e("r1", ts=20.0)]
    groups = list(correlate_with_timeout(entries, "request_id", window_seconds=60.0, min_count=1))
    assert len(groups) == 1
    assert len(groups[0]) == 3


def test_timeout_invalid_window_raises():
    with pytest.raises(ValueError):
        list(correlate_with_timeout([], "request_id", window_seconds=0))


# ---------------------------------------------------------------------------
# flatten_groups
# ---------------------------------------------------------------------------

def test_flatten_groups_returns_all_entries():
    groups = [[_e("a"), _e("a")], [_e("b")]]
    flat = list(flatten_groups(groups))
    assert len(flat) == 3


def test_flatten_groups_empty_yields_nothing():
    assert list(flatten_groups([])) == []
