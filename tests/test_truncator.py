"""Tests for logslice.truncator."""
import pytest

from logslice.truncator import (
    drop_long_fields,
    truncate_entries,
    truncate_field,
    truncate_fields,
)


# ---------------------------------------------------------------------------
# truncate_field
# ---------------------------------------------------------------------------

def test_truncate_field_shortens_long_value():
    entry = {"msg": "hello world", "level": "info"}
    result = truncate_field(entry, "msg", 8)
    assert result["msg"] == "hello..."


def test_truncate_field_keeps_short_value():
    entry = {"msg": "hi", "level": "info"}
    result = truncate_field(entry, "msg", 10)
    assert result["msg"] == "hi"


def test_truncate_field_exact_length_unchanged():
    entry = {"msg": "exact"}
    result = truncate_field(entry, "msg", 5)
    assert result["msg"] == "exact"


def test_truncate_field_custom_suffix():
    entry = {"msg": "abcdefgh"}
    result = truncate_field(entry, "msg", 5, suffix="!")
    assert result["msg"] == "abcd!"


def test_truncate_field_non_string_unchanged():
    entry = {"count": 12345}
    result = truncate_field(entry, "count", 3)
    assert result["count"] == 12345


def test_truncate_field_missing_field_unchanged():
    entry = {"level": "info"}
    result = truncate_field(entry, "msg", 5)
    assert result == {"level": "info"}


def test_truncate_field_does_not_mutate_original():
    entry = {"msg": "hello world"}
    truncate_field(entry, "msg", 5)
    assert entry["msg"] == "hello world"


def test_truncate_field_negative_max_length_raises():
    with pytest.raises(ValueError):
        truncate_field({"msg": "hi"}, "msg", -1)


def test_truncate_field_zero_max_length():
    entry = {"msg": "hello"}
    result = truncate_field(entry, "msg", 0)
    assert result["msg"] == ""


# ---------------------------------------------------------------------------
# truncate_fields
# ---------------------------------------------------------------------------

def test_truncate_fields_multiple_fields():
    entry = {"msg": "hello world", "detail": "some long detail text", "level": "error"}
    result = truncate_fields(entry, ["msg", "detail"], 8)
    assert result["msg"] == "hello..."
    assert result["detail"] == "some ..."
    assert result["level"] == "error"


def test_truncate_fields_empty_list_unchanged():
    entry = {"msg": "hello world"}
    result = truncate_fields(entry, [], 5)
    assert result == entry


# ---------------------------------------------------------------------------
# drop_long_fields
# ---------------------------------------------------------------------------

def test_drop_long_fields_removes_field():
    entry = {"msg": "a" * 200, "level": "info"}
    result = drop_long_fields(entry, 100)
    assert "msg" not in result
    assert result["level"] == "info"


def test_drop_long_fields_replace_with():
    entry = {"msg": "a" * 200, "level": "info"}
    result = drop_long_fields(entry, 100, replace_with="[TRUNCATED]")
    assert result["msg"] == "[TRUNCATED]"


def test_drop_long_fields_keeps_short_strings():
    entry = {"msg": "short", "level": "debug"}
    result = drop_long_fields(entry, 10)
    assert result == entry


def test_drop_long_fields_non_string_always_kept():
    entry = {"count": 999999}
    result = drop_long_fields(entry, 3)
    assert result["count"] == 999999


def test_drop_long_fields_negative_max_raises():
    with pytest.raises(ValueError):
        drop_long_fields({"msg": "hi"}, -5)


# ---------------------------------------------------------------------------
# truncate_entries
# ---------------------------------------------------------------------------

def test_truncate_entries_lazy_iterator():
    entries = [
        {"msg": "hello world", "level": "info"},
        {"msg": "short", "level": "debug"},
    ]
    results = list(truncate_entries(entries, ["msg"], 8))
    assert results[0]["msg"] == "hello..."
    assert results[1]["msg"] == "short"


def test_truncate_entries_empty_input():
    results = list(truncate_entries([], ["msg"], 10))
    assert results == []
