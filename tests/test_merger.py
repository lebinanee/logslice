"""Tests for logslice.merger and logslice.pipeline_merger."""

from __future__ import annotations

import pytest

from logslice.merger import merge_sorted, merge_unsorted
from logslice.pipeline_merger import merge_sorted_stage, merge_unsorted_stage


def _e(ts: str, msg: str = "x") -> dict:
    return {"timestamp": ts, "msg": msg}


# ---------------------------------------------------------------------------
# merge_sorted
# ---------------------------------------------------------------------------

def test_merge_sorted_two_streams_in_order():
    a = [_e("2024-01-01T00:00:01"), _e("2024-01-01T00:00:03")]
    b = [_e("2024-01-01T00:00:02"), _e("2024-01-01T00:00:04")]
    result = list(merge_sorted([a, b]))
    timestamps = [r["timestamp"] for r in result]
    assert timestamps == sorted(timestamps)
    assert len(result) == 4


def test_merge_sorted_single_stream_passthrough():
    a = [_e("2024-01-01T00:00:01"), _e("2024-01-01T00:00:02")]
    result = list(merge_sorted([a]))
    assert result == a


def test_merge_sorted_empty_streams_yield_nothing():
    result = list(merge_sorted([[], []]))
    assert result == []


def test_merge_sorted_one_empty_one_nonempty():
    a = [_e("2024-01-01T00:00:01")]
    result = list(merge_sorted([a, []]))
    assert result == a


def test_merge_sorted_missing_timestamp_sorts_first():
    a = [{"msg": "no-ts"}, _e("2024-01-01T00:00:01")]
    b = [_e("2024-01-01T00:00:00")]
    result = list(merge_sorted([a, b]))
    assert result[0]["msg"] == "no-ts"


def test_merge_sorted_custom_key():
    a = [{"ts": "10"}, {"ts": "30"}]
    b = [{"ts": "20"}, {"ts": "40"}]
    result = list(merge_sorted([a, b], timestamp_key="ts"))
    assert [r["ts"] for r in result] == ["10", "20", "30", "40"]


# ---------------------------------------------------------------------------
# merge_unsorted
# ---------------------------------------------------------------------------

def test_merge_unsorted_sorts_across_streams():
    a = [_e("2024-01-01T00:00:03"), _e("2024-01-01T00:00:01")]
    b = [_e("2024-01-01T00:00:04"), _e("2024-01-01T00:00:02")]
    result = list(merge_unsorted([a, b]))
    timestamps = [r["timestamp"] for r in result]
    assert timestamps == sorted(timestamps)
    assert len(result) == 4


def test_merge_unsorted_empty_yields_nothing():
    assert list(merge_unsorted([[], []])) == []


# ---------------------------------------------------------------------------
# pipeline stages
# ---------------------------------------------------------------------------

def test_merge_sorted_stage_returns_ordered():
    a = [_e("2024-01-01T00:00:01"), _e("2024-01-01T00:00:03")]
    b = [_e("2024-01-01T00:00:02"), _e("2024-01-01T00:00:04")]
    result = list(merge_sorted_stage(a, b))
    assert [r["timestamp"] for r in result] == sorted(r["timestamp"] for r in result)


def test_merge_unsorted_stage_returns_ordered():
    a = [_e("2024-01-01T00:00:03"), _e("2024-01-01T00:00:01")]
    b = [_e("2024-01-01T00:00:02")]
    result = list(merge_unsorted_stage(a, b))
    assert [r["timestamp"] for r in result] == sorted(r["timestamp"] for r in result)


def test_merge_sorted_stage_three_streams():
    streams = [
        [_e("T1"), _e("T4")],
        [_e("T2"), _e("T5")],
        [_e("T3"), _e("T6")],
    ]
    result = list(merge_sorted_stage(*streams))
    assert [r["timestamp"] for r in result] == ["T1", "T2", "T3", "T4", "T5", "T6"]
