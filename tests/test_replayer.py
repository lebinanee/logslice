"""Tests for logslice.replayer."""

from __future__ import annotations

import pytest

from logslice.replayer import replay_realtime, replay_fixed_rate


def make_entries(timestamps):
    return [{"timestamp": ts, "msg": f"entry-{i}"} for i, ts in enumerate(timestamps)]


# ---------------------------------------------------------------------------
# replay_realtime
# ---------------------------------------------------------------------------

def test_realtime_yields_all_entries():
    entries = make_entries([1000.0, 1001.0, 1002.0])
    sleeps = []
    result = list(replay_realtime(entries, sleep_fn=sleeps.append))
    assert len(result) == 3


def test_realtime_correct_sleep_intervals():
    entries = make_entries([0.0, 1.0, 3.0])
    sleeps = []
    list(replay_realtime(entries, sleep_fn=sleeps.append))
    assert sleeps == [1.0, 2.0]


def test_realtime_speed_doubles_rate():
    entries = make_entries([0.0, 2.0, 4.0])
    sleeps = []
    list(replay_realtime(entries, speed=2.0, sleep_fn=sleeps.append))
    assert sleeps == [1.0, 1.0]


def test_realtime_speed_half_rate():
    entries = make_entries([0.0, 1.0])
    sleeps = []
    list(replay_realtime(entries, speed=0.5, sleep_fn=sleeps.append))
    assert sleeps == [2.0]


def test_realtime_no_sleep_for_first_entry():
    entries = make_entries([100.0, 101.0])
    sleeps = []
    list(replay_realtime(entries, sleep_fn=sleeps.append))
    assert len(sleeps) == 1


def test_realtime_skips_entries_without_timestamp():
    entries = [{"msg": "no-ts"}, {"timestamp": 1.0, "msg": "has-ts"}]
    sleeps = []
    result = list(replay_realtime(entries, sleep_fn=sleeps.append))
    assert len(result) == 2
    assert sleeps == []


def test_realtime_negative_gap_no_sleep():
    # Out-of-order timestamps should not cause negative sleeps.
    entries = make_entries([5.0, 3.0, 4.0])
    sleeps = []
    list(replay_realtime(entries, sleep_fn=sleeps.append))
    assert all(s >= 0 for s in sleeps)


def test_realtime_invalid_speed_raises():
    with pytest.raises(ValueError):
        list(replay_realtime([], speed=0.0))


def test_realtime_empty_input_yields_nothing():
    result = list(replay_realtime([], sleep_fn=lambda _: None))
    assert result == []


# ---------------------------------------------------------------------------
# replay_fixed_rate
# ---------------------------------------------------------------------------

def test_fixed_rate_yields_all_entries():
    entries = [{"msg": str(i)} for i in range(5)]
    sleeps = []
    result = list(replay_fixed_rate(entries, rate=10.0, sleep_fn=sleeps.append))
    assert len(result) == 5


def test_fixed_rate_correct_interval():
    entries = [{"msg": str(i)} for i in range(3)]
    sleeps = []
    list(replay_fixed_rate(entries, rate=2.0, sleep_fn=sleeps.append))
    assert sleeps == [0.5, 0.5, 0.5]


def test_fixed_rate_invalid_rate_raises():
    with pytest.raises(ValueError):
        list(replay_fixed_rate([], rate=0))


def test_fixed_rate_empty_input_yields_nothing():
    result = list(replay_fixed_rate([], rate=5.0, sleep_fn=lambda _: None))
    assert result == []
