"""Tests for logslice.pipeline_replay stages."""

from __future__ import annotations

import pytest

from logslice.pipeline_replay import realtime_replay_stage, fixed_rate_replay_stage


def _entries(timestamps):
    return [{"timestamp": float(ts), "msg": f"m{i}"} for i, ts in enumerate(timestamps)]


# ---------------------------------------------------------------------------
# realtime_replay_stage
# ---------------------------------------------------------------------------

def test_realtime_stage_yields_all():
    entries = _entries([0, 1, 2])
    sleeps = []
    result = list(realtime_replay_stage(entries, sleep_fn=sleeps.append))
    assert len(result) == 3


def test_realtime_stage_records_sleeps():
    entries = _entries([0.0, 1.0, 3.0])
    sleeps = []
    list(realtime_replay_stage(entries, sleep_fn=sleeps.append))
    assert sleeps == [1.0, 2.0]


def test_realtime_stage_speed_parameter():
    entries = _entries([0.0, 4.0])
    sleeps = []
    list(realtime_replay_stage(entries, speed=2.0, sleep_fn=sleeps.append))
    assert sleeps == [2.0]


def test_realtime_stage_preserves_entry_data():
    entries = [{"timestamp": 1.0, "level": "INFO", "msg": "hello"}]
    result = list(realtime_replay_stage(entries, sleep_fn=lambda _: None))
    assert result[0]["level"] == "INFO"
    assert result[0]["msg"] == "hello"


def test_realtime_stage_empty_input():
    result = list(realtime_replay_stage([], sleep_fn=lambda _: None))
    assert result == []


# ---------------------------------------------------------------------------
# fixed_rate_replay_stage
# ---------------------------------------------------------------------------

def test_fixed_rate_stage_yields_all():
    entries = [{"msg": str(i)} for i in range(4)]
    sleeps = []
    result = list(fixed_rate_replay_stage(entries, rate=5.0, sleep_fn=sleeps.append))
    assert len(result) == 4


def test_fixed_rate_stage_interval():
    entries = [{"msg": "x"}, {"msg": "y"}]
    sleeps = []
    list(fixed_rate_replay_stage(entries, rate=4.0, sleep_fn=sleeps.append))
    assert sleeps == [0.25, 0.25]


def test_fixed_rate_stage_empty_input():
    result = list(fixed_rate_replay_stage([], rate=1.0, sleep_fn=lambda _: None))
    assert result == []
