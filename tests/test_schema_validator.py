"""Tests for logslice.schema_validator."""
from __future__ import annotations

import pytest
from logslice.schema_validator import FieldSpec, Schema, SchemaViolation, validate_entries


def _entry(**kwargs):
    return dict(kwargs)


# --- FieldSpec / Schema.validate ---

def test_valid_entry_no_violations():
    schema = Schema()
    schema.add_field(FieldSpec("level", required=True, expected_type=str))
    assert schema.validate(_entry(level="info")) == []


def test_missing_required_field_is_violation():
    schema = Schema()
    schema.add_field(FieldSpec("level", required=True))
    violations = schema.validate(_entry(msg="hello"))
    assert len(violations) == 1
    assert violations[0].field == "level"
    assert "missing" in violations[0].reason


def test_optional_missing_field_no_violation():
    schema = Schema()
    schema.add_field(FieldSpec("trace_id", required=False))
    assert schema.validate(_entry(level="info")) == []


def test_wrong_type_is_violation():
    schema = Schema()
    schema.add_field(FieldSpec("status", expected_type=int))
    violations = schema.validate(_entry(status="200"))
    assert len(violations) == 1
    assert "expected int" in violations[0].reason


def test_correct_type_no_violation():
    schema = Schema()
    schema.add_field(FieldSpec("status", expected_type=int))
    assert schema.validate(_entry(status=200)) == []


def test_allowed_values_violation():
    schema = Schema()
    schema.add_field(FieldSpec("level", allowed_values=["info", "warn", "error"]))
    violations = schema.validate(_entry(level="debug"))
    assert len(violations) == 1
    assert "not in allowed set" in violations[0].reason


def test_allowed_values_no_violation():
    schema = Schema()
    schema.add_field(FieldSpec("level", allowed_values=["info", "warn", "error"]))
    assert schema.validate(_entry(level="warn")) == []


def test_multiple_violations_reported():
    schema = Schema()
    schema.add_field(FieldSpec("level", required=True, allowed_values=["info", "error"]))
    schema.add_field(FieldSpec("status", required=True, expected_type=int))
    violations = schema.validate(_entry(level="debug", status="bad"))
    assert len(violations) == 2


def test_violation_str_format():
    v = SchemaViolation(entry={}, field="level", reason="required field is missing")
    assert str(v) == "[level] required field is missing"


# --- validate_entries ---

def test_validate_entries_drop_removes_invalid():
    schema = Schema()
    schema.add_field(FieldSpec("level", required=True))
    entries = [_entry(level="info"), _entry(msg="no level"), _entry(level="error")]
    result = list(validate_entries(entries, schema, on_violation="drop"))
    assert len(result) == 2
    assert all("level" in e for e in result)


def test_validate_entries_tag_adds_violations_key():
    schema = Schema()
    schema.add_field(FieldSpec("level", required=True))
    entries = [_entry(msg="no level")]
    result = list(validate_entries(entries, schema, on_violation="tag"))
    assert len(result) == 1
    assert "_violations" in result[0]
    assert len(result[0]["_violations"]) == 1


def test_validate_entries_passthrough_yields_all():
    schema = Schema()
    schema.add_field(FieldSpec("level", required=True))
    entries = [_entry(level="info"), _entry(msg="no level")]
    result = list(validate_entries(entries, schema, on_violation="passthrough"))
    assert len(result) == 2


def test_validate_entries_valid_entry_not_tagged():
    schema = Schema()
    schema.add_field(FieldSpec("level", required=True))
    entries = [_entry(level="info")]
    result = list(validate_entries(entries, schema, on_violation="tag"))
    assert "_violations" not in result[0]


def test_validate_entries_empty_input():
    schema = Schema()
    schema.add_field(FieldSpec("level", required=True))
    assert list(validate_entries([], schema)) == []
