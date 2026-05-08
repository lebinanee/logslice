"""Tests for logslice.splitter."""

from __future__ import annotations

import pytest

from logslice.splitter import (
    iter_split_by_field,
    split_by_field,
    split_by_predicate,
)


DEFAULT = "__other__"


def make_entries():
    return [
        {"level": "info", "msg": "started"},
        {"level": "error", "msg": "boom"},
        {"level": "info", "msg": "ok"},
        {"level": "warn", "msg": "slow"},
        {"msg": "no level"},
    ]


# ---------------------------------------------------------------------------
# split_by_field
# ---------------------------------------------------------------------------

def test_split_by_field_creates_correct_buckets():
    result = split_by_field(make_entries(), "level")
    assert set(result.keys()) == {"info", "error", "warn", DEFAULT}


def test_split_by_field_bucket_sizes():
    result = split_by_field(make_entries(), "level")
    assert len(result["info"]) == 2
    assert len(result["error"]) == 1
    assert len(result["warn"]) == 1


def test_split_by_field_missing_goes_to_default():
    result = split_by_field(make_entries(), "level")
    assert len(result[DEFAULT]) == 1
    assert result[DEFAULT][0]["msg"] == "no level"


def test_split_by_field_custom_default_key():
    result = split_by_field(make_entries(), "level", default="unknown")
    assert "unknown" in result
    assert DEFAULT not in result


def test_split_by_field_empty_input():
    result = split_by_field([], "level")
    assert result == {}


def test_split_by_field_all_same_value():
    entries = [{"level": "info"}, {"level": "info"}]
    result = split_by_field(entries, "level")
    assert list(result.keys()) == ["info"]
    assert len(result["info"]) == 2


# ---------------------------------------------------------------------------
# split_by_predicate
# ---------------------------------------------------------------------------

def test_split_by_predicate_routes_correctly():
    preds = [
        ("errors", lambda e: e.get("level") == "error"),
        ("warnings", lambda e: e.get("level") == "warn"),
    ]
    result = split_by_predicate(make_entries(), preds)
    assert len(result["errors"]) == 1
    assert len(result["warnings"]) == 1


def test_split_by_predicate_unmatched_in_default():
    preds = [("errors", lambda e: e.get("level") == "error")]
    result = split_by_predicate(make_entries(), preds)
    # info x2, warn x1, missing-level x1 → 4 in default
    assert len(result[DEFAULT]) == 4


def test_split_by_predicate_first_match_wins():
    entries = [{"level": "error", "critical": True}]
    preds = [
        ("critical", lambda e: e.get("critical")),
        ("errors", lambda e: e.get("level") == "error"),
    ]
    result = split_by_predicate(entries, preds)
    assert "critical" in result
    assert "errors" not in result


# ---------------------------------------------------------------------------
# iter_split_by_field
# ---------------------------------------------------------------------------

def test_iter_split_by_field_yields_tuples():
    pairs = list(iter_split_by_field(make_entries(), "level"))
    assert all(isinstance(k, str) and isinstance(e, dict) for k, e in pairs)


def test_iter_split_by_field_correct_keys():
    pairs = list(iter_split_by_field(make_entries(), "level"))
    keys = [k for k, _ in pairs]
    assert keys == ["info", "error", "info", "warn", DEFAULT]


def test_iter_split_by_field_preserves_entry_identity():
    entries = make_entries()
    pairs = list(iter_split_by_field(entries, "level"))
    yielded_entries = [e for _, e in pairs]
    assert yielded_entries == entries
