"""Tests for logslice.redactor."""

import pytest
from logslice.redactor import (
    redact_entry,
    redact_entries,
    _mask_value,
    _hash_value,
    _partial_mask,
    _MASK,
)


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------

def test_mask_value_returns_redacted_string():
    assert _mask_value("supersecret") == _MASK


def test_hash_value_is_deterministic():
    assert _hash_value("abc") == _hash_value("abc")


def test_hash_value_differs_for_different_inputs():
    assert _hash_value("abc") != _hash_value("xyz")


def test_hash_value_includes_salt():
    assert _hash_value("abc", salt="s1") != _hash_value("abc", salt="s2")


def test_partial_mask_hides_most_chars():
    result = _partial_mask("1234567890", visible=4)
    assert result.endswith("7890")
    assert result.startswith("******")


def test_partial_mask_short_value_fully_masked():
    result = _partial_mask("abc", visible=4)
    assert result == "***"


# ---------------------------------------------------------------------------
# redact_entry
# ---------------------------------------------------------------------------

def test_redact_entry_masks_default_fields():
    entry = {"user": "alice", "password": "hunter2", "level": "info"}
    result = redact_entry(entry)
    assert result["password"] == _MASK
    assert result["user"] == "alice"
    assert result["level"] == "info"


def test_redact_entry_custom_fields():
    entry = {"email": "a@b.com", "msg": "hello"}
    result = redact_entry(entry, fields=["email"])
    assert result["email"] == _MASK
    assert result["msg"] == "hello"


def test_redact_entry_hash_strategy():
    entry = {"token": "abc123"}
    result = redact_entry(entry, strategy="hash")
    assert result["token"].startswith("sha256:")
    assert result["token"] != "abc123"


def test_redact_entry_partial_strategy():
    entry = {"secret": "ABCDEFGHIJ"}
    result = redact_entry(entry, strategy="partial", visible=4)
    assert result["secret"].endswith("GHIJ")


def test_redact_entry_does_not_mutate_original():
    entry = {"password": "pass"}
    original = dict(entry)
    redact_entry(entry)
    assert entry == original


def test_redact_entry_case_insensitive_field_matching():
    entry = {"PASSWORD": "pass", "Token": "tok"}
    result = redact_entry(entry)
    assert result["PASSWORD"] == _MASK
    assert result["Token"] == _MASK


# ---------------------------------------------------------------------------
# redact_entries
# ---------------------------------------------------------------------------

def test_redact_entries_lazy_iterator():
    entries = [{"password": "p1"}, {"password": "p2"}]
    gen = redact_entries(entries)
    first = next(gen)
    assert first["password"] == _MASK


def test_redact_entries_all_processed():
    entries = [{"token": str(i)} for i in range(5)]
    results = list(redact_entries(entries))
    assert all(r["token"] == _MASK for r in results)
