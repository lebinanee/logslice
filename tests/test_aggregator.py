"""Tests for logslice.aggregator."""
from __future__ import annotations

import pytest

from logslice.aggregator import (
    count_by_field,
    group_by_field,
    iter_with_sequence,
    top_n,
)


SAMPLE_ENTRIES = [
    {"level": "INFO",  "service": "api",    "msg": "started"},
    {"level": "ERROR", "service": "api",    "msg": "crash"},
    {"level": "INFO",  "service": "worker", "msg": "job done"},
    {"level": "WARN",  "service": "api",    "msg": "slow"},
    {"level": "INFO",  "service": "worker", "msg": "job done"},
    {"service": "db",  "msg": "connected"},   # no 'level'
]


def test_count_by_field_basic():
    result = count_by_field(SAMPLE_ENTRIES, "level")
    assert result["INFO"] == 3
    assert result["ERROR"] == 1
    assert result["WARN"] == 1
    assert result["<missing>"] == 1


def test_count_by_field_empty():
    assert count_by_field([], "level") == {}


def test_count_by_field_ordered_most_common_first():
    result = count_by_field(SAMPLE_ENTRIES, "level")
    counts = list(result.values())
    assert counts == sorted(counts, reverse=True)


def test_group_by_field_keys():
    result = group_by_field(SAMPLE_ENTRIES, "service")
    assert set(result.keys()) == {"api", "worker", "db"}


def test_group_by_field_sizes():
    result = group_by_field(SAMPLE_ENTRIES, "service")
    assert len(result["api"]) == 3
    assert len(result["worker"]) == 2
    assert len(result["db"]) == 1


def test_group_by_field_sorted_descending():
    result = group_by_field(SAMPLE_ENTRIES, "service")
    sizes = [len(v) for v in result.values()]
    assert sizes == sorted(sizes, reverse=True)


def test_group_by_field_missing_value():
    entries = [{"level": "INFO"}, {"msg": "no level"}]
    result = group_by_field(entries, "level")
    assert "<missing>" in result
    assert len(result["<missing>"]) == 1


def test_top_n_default():
    result = top_n(SAMPLE_ENTRIES, "level")
    assert result[0] == ("INFO", 3)


def test_top_n_limit():
    result = top_n(SAMPLE_ENTRIES, "level", n=2)
    assert len(result) == 2


def test_top_n_empty():
    assert top_n([], "level") == []


def test_iter_with_sequence_default_start():
    pairs = list(iter_with_sequence(SAMPLE_ENTRIES))
    assert pairs[0][0] == 1
    assert pairs[-1][0] == len(SAMPLE_ENTRIES)


def test_iter_with_sequence_custom_start():
    pairs = list(iter_with_sequence(SAMPLE_ENTRIES, start=0))
    assert pairs[0][0] == 0


def test_iter_with_sequence_entries_unchanged():
    pairs = list(iter_with_sequence(SAMPLE_ENTRIES))
    for _, entry in pairs:
        assert isinstance(entry, dict)
