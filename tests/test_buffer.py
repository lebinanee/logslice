"""Tests for logslice.buffer flush strategies."""

from __future__ import annotations

import pytest

from logslice.buffer import (
    flush_by_count,
    flush_by_field_change,
    flush_by_predicate,
    sliding_window,
)


def make_entries(n: int, **extra) -> list[dict]:
    return [{"id": i, **extra} for i in range(n)]


# ---------------------------------------------------------------------------
# flush_by_count
# ---------------------------------------------------------------------------

def test_flush_by_count_exact_batches():
    entries = make_entries(6)
    batches = list(flush_by_count(entries, size=2))
    assert len(batches) == 3
    assert all(len(b) == 2 for b in batches)


def test_flush_by_count_remainder_batch():
    entries = make_entries(5)
    batches = list(flush_by_count(entries, size=2))
    assert len(batches) == 3
    assert len(batches[-1]) == 1


def test_flush_by_count_empty_input():
    assert list(flush_by_count([], size=3)) == []


def test_flush_by_count_invalid_size():
    with pytest.raises(ValueError):
        list(flush_by_count(make_entries(3), size=0))


def test_flush_by_count_preserves_all_entries():
    entries = make_entries(7)
    flat = [e for batch in flush_by_count(entries, size=3) for e in batch]
    assert flat == entries


# ---------------------------------------------------------------------------
# flush_by_field_change
# ---------------------------------------------------------------------------

def _svc_entries():
    return [
        {"svc": "a", "n": 0},
        {"svc": "a", "n": 1},
        {"svc": "b", "n": 2},
        {"svc": "b", "n": 3},
        {"svc": "a", "n": 4},
    ]


def test_flush_by_field_change_batch_sizes():
    batches = list(flush_by_field_change(_svc_entries(), field="svc"))
    assert [len(b) for b in batches] == [2, 2, 1]


def test_flush_by_field_change_batch_values():
    batches = list(flush_by_field_change(_svc_entries(), field="svc"))
    assert all(e["svc"] == "a" for e in batches[0])
    assert all(e["svc"] == "b" for e in batches[1])


def test_flush_by_field_change_missing_field_uses_default():
    entries = [{"x": 1}, {"x": 1}, {"svc": "b"}]
    batches = list(flush_by_field_change(entries, field="svc"))
    assert len(batches) == 2


# ---------------------------------------------------------------------------
# flush_by_predicate
# ---------------------------------------------------------------------------

def test_flush_by_predicate_splits_on_trigger():
    entries = [
        {"level": "info"},
        {"level": "info"},
        {"level": "error"},
        {"level": "info"},
        {"level": "error"},
    ]
    batches = list(flush_by_predicate(entries, predicate=lambda e: e["level"] == "error"))
    assert len(batches) == 2
    assert batches[0][-1]["level"] == "error"


def test_flush_by_predicate_no_trigger_yields_single_batch():
    entries = make_entries(4)
    batches = list(flush_by_predicate(entries, predicate=lambda e: False))
    assert len(batches) == 1
    assert len(batches[0]) == 4


# ---------------------------------------------------------------------------
# sliding_window
# ---------------------------------------------------------------------------

def test_sliding_window_count():
    entries = make_entries(5)
    windows = list(sliding_window(entries, size=3))
    assert len(windows) == 3  # windows starting at index 0, 1, 2


def test_sliding_window_size():
    entries = make_entries(5)
    for w in sliding_window(entries, size=3):
        assert len(w) == 3


def test_sliding_window_invalid_size():
    with pytest.raises(ValueError):
        list(sliding_window(make_entries(3), size=0))


def test_sliding_window_smaller_than_window_yields_nothing():
    entries = make_entries(2)
    assert list(sliding_window(entries, size=5)) == []
