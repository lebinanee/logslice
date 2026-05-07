"""Tests for logslice.transformer and logslice.pipeline_transform."""

from __future__ import annotations

import pytest

from logslice.transformer import (
    apply_field,
    cast_field,
    drop_fields,
    keep_fields,
    rename_fields,
    transform_entries,
)
from logslice.pipeline_transform import (
    apply_field_stage,
    cast_field_stage,
    drop_fields_stage,
    keep_fields_stage,
    rename_fields_stage,
)


# ---------------------------------------------------------------------------
# rename_fields
# ---------------------------------------------------------------------------

def test_rename_fields_basic():
    entry = {"level": "info", "msg": "hello"}
    result = rename_fields(entry, {"msg": "message"})
    assert result == {"level": "info", "message": "hello"}


def test_rename_fields_does_not_mutate():
    entry = {"a": 1, "b": 2}
    rename_fields(entry, {"a": "z"})
    assert "a" in entry


def test_rename_fields_unknown_key_ignored():
    entry = {"x": 1}
    result = rename_fields(entry, {"y": "z"})
    assert result == {"x": 1}


# ---------------------------------------------------------------------------
# drop_fields
# ---------------------------------------------------------------------------

def test_drop_fields_removes_listed():
    entry = {"a": 1, "b": 2, "c": 3}
    assert drop_fields(entry, ["b", "c"]) == {"a": 1}


def test_drop_fields_missing_field_ignored():
    entry = {"a": 1}
    assert drop_fields(entry, ["z"]) == {"a": 1}


# ---------------------------------------------------------------------------
# keep_fields
# ---------------------------------------------------------------------------

def test_keep_fields_retains_only_listed():
    entry = {"a": 1, "b": 2, "c": 3}
    assert keep_fields(entry, ["a", "c"]) == {"a": 1, "c": 3}


def test_keep_fields_missing_field_skipped():
    entry = {"a": 1}
    assert keep_fields(entry, ["a", "z"]) == {"a": 1}


# ---------------------------------------------------------------------------
# apply_field
# ---------------------------------------------------------------------------

def test_apply_field_transforms_value():
    entry = {"level": "info"}
    result = apply_field(entry, "level", str.upper)
    assert result["level"] == "INFO"


def test_apply_field_absent_key_unchanged():
    entry = {"a": 1}
    result = apply_field(entry, "missing", str.upper)
    assert result == {"a": 1}


# ---------------------------------------------------------------------------
# cast_field
# ---------------------------------------------------------------------------

def test_cast_field_converts_string_to_int():
    entry = {"code": "42"}
    result = cast_field(entry, "code", int)
    assert result["code"] == 42


def test_cast_field_invalid_value_left_unchanged():
    entry = {"code": "not-a-number"}
    result = cast_field(entry, "code", int)
    assert result["code"] == "not-a-number"


# ---------------------------------------------------------------------------
# transform_entries
# ---------------------------------------------------------------------------

def test_transform_entries_applies_all_stages():
    entries = [{"msg": "hi", "tmp": True}]
    result = list(
        transform_entries(
            entries,
            lambda e: rename_fields(e, {"msg": "message"}),
            lambda e: drop_fields(e, ["tmp"]),
        )
    )
    assert result == [{"message": "hi"}]


# ---------------------------------------------------------------------------
# pipeline stages
# ---------------------------------------------------------------------------

def test_rename_fields_stage():
    entries = [{"a": 1}]
    out = list(rename_fields_stage(entries, mapping={"a": "b"}))
    assert out == [{"b": 1}]


def test_drop_fields_stage():
    entries = [{"a": 1, "b": 2}]
    out = list(drop_fields_stage(entries, fields=["b"]))
    assert out == [{"a": 1}]


def test_keep_fields_stage():
    entries = [{"a": 1, "b": 2, "c": 3}]
    out = list(keep_fields_stage(entries, fields=["a"]))
    assert out == [{"a": 1}]


def test_apply_field_stage():
    entries = [{"level": "warn"}]
    out = list(apply_field_stage(entries, field="level", fn=str.upper))
    assert out[0]["level"] == "WARN"


def test_cast_field_stage():
    entries = [{"status": "200"}]
    out = list(cast_field_stage(entries, field="status", type_fn=int))
    assert out[0]["status"] == 200
