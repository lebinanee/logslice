"""Tests for logslice.deduplicator."""

import pytest

from logslice.deduplicator import dedupe_exact, dedupe_window, _entry_fingerprint


def make_entries(*msgs: str) -> list[dict]:
    return [{"level": "INFO", "message": m} for m in msgs]


# --- _entry_fingerprint ---

def test_fingerprint_same_entry_stable():
    e = {"level": "ERROR", "msg": "boom"}
    assert _entry_fingerprint(e) == _entry_fingerprint(e)


def test_fingerprint_different_entries_differ():
    a = {"msg": "hello"}
    b = {"msg": "world"}
    assert _entry_fingerprint(a) != _entry_fingerprint(b)


def test_fingerprint_field_subset():
    a = {"msg": "hello", "ts": "2024-01-01"}
    b = {"msg": "hello", "ts": "2024-01-02"}
    assert _entry_fingerprint(a, ["msg"]) == _entry_fingerprint(b, ["msg"])


# --- dedupe_exact ---

def test_dedupe_exact_no_duplicates():
    entries = make_entries("a", "b", "c")
    result = list(dedupe_exact(entries))
    assert result == entries


def test_dedupe_exact_removes_duplicates():
    entries = make_entries("a", "a", "b", "a")
    result = list(dedupe_exact(entries))
    assert len(result) == 2
    assert result[0]["message"] == "a"
    assert result[1]["message"] == "b"


def test_dedupe_exact_empty():
    assert list(dedupe_exact([])) == []


def test_dedupe_exact_field_subset_dedupes_on_fields_only():
    entries = [
        {"msg": "hello", "ts": "T1"},
        {"msg": "hello", "ts": "T2"},
        {"msg": "world", "ts": "T1"},
    ]
    result = list(dedupe_exact(entries, fields=["msg"]))
    assert len(result) == 2
    assert result[0]["msg"] == "hello"
    assert result[1]["msg"] == "world"


# --- dedupe_window ---

def test_dedupe_window_basic():
    entries = make_entries("a", "a", "b")
    result = list(dedupe_window(entries, window_size=10))
    assert len(result) == 2


def test_dedupe_window_outside_window_reappears():
    # After window is full, a re-seen entry should pass through.
    entries = make_entries("a", "b", "c", "a")  # window=2 forgets "a" after "b","c"
    result = list(dedupe_window(entries, window_size=2))
    messages = [e["message"] for e in result]
    assert messages == ["a", "b", "c", "a"]


def test_dedupe_window_invalid_size():
    with pytest.raises(ValueError):
        list(dedupe_window([], window_size=0))


def test_dedupe_window_empty():
    assert list(dedupe_window([], window_size=5)) == []


def test_dedupe_window_all_unique():
    entries = make_entries("x", "y", "z")
    result = list(dedupe_window(entries, window_size=10))
    assert result == entries
