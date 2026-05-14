"""Tests for logslice.pipeline_schema pipeline stages."""
from __future__ import annotations

import pytest
from logslice.pipeline_schema import tag_invalid_stage, validate_stage, reject_invalid_stage


def _e(**kwargs):
    return dict(kwargs)


_ENTRIES = [
    _e(level="info", status=200, msg="ok"),
    _e(level="debug", status=404, msg="not found"),
    _e(msg="missing level and status"),
    _e(level="error", status="bad_type", msg="type error"),
]


# --- validate_stage (drop mode) ---

def test_validate_stage_drop_removes_invalid():
    result = list(
        validate_stage(
            _ENTRIES,
            required_fields=["level", "status"],
            on_violation="drop",
        )
    )
    assert all("level" in e and "status" in e for e in result)


def test_validate_stage_drop_keeps_valid():
    result = list(
        validate_stage(
            _ENTRIES,
            required_fields=["level", "status"],
            on_violation="drop",
        )
    )
    assert len(result) == 3  # entry without level/status is dropped


def test_validate_stage_typed_field_drops_wrong_type():
    result = list(
        validate_stage(
            _ENTRIES,
            typed_fields={"status": int},
            on_violation="drop",
        )
    )
    # entry with status="bad_type" should be dropped
    assert all(isinstance(e["status"], int) for e in result if "status" in e)


def test_validate_stage_enum_field_drops_disallowed():
    result = list(
        validate_stage(
            _ENTRIES,
            enum_fields={"level": ["info", "error"]},
            on_violation="drop",
        )
    )
    levels = [e["level"] for e in result if "level" in e]
    assert "debug" not in levels


# --- validate_stage (tag mode) ---

def test_validate_stage_tag_preserves_all_entries():
    result = list(
        validate_stage(
            _ENTRIES,
            required_fields=["level"],
            on_violation="tag",
        )
    )
    assert len(result) == len(_ENTRIES)


def test_validate_stage_tag_adds_violations_key():
    result = list(
        validate_stage(
            [_e(msg="no level")],
            required_fields=["level"],
            on_violation="tag",
        )
    )
    assert "_violations" in result[0]


# --- reject_invalid_stage ---

def test_reject_invalid_stage_drops_bad_entries():
    result = list(
        reject_invalid_stage(_ENTRIES, required_fields=["level", "status"])
    )
    assert all("level" in e and "status" in e for e in result)


# --- tag_invalid_stage ---

def test_tag_invalid_stage_yields_all():
    result = list(tag_invalid_stage(_ENTRIES, required_fields=["level", "status"]))
    assert len(result) == len(_ENTRIES)


def test_tag_invalid_stage_does_not_tag_valid():
    result = list(
        tag_invalid_stage([_e(level="info", status=200)], required_fields=["level", "status"])
    )
    assert "_violations" not in result[0]


def test_validate_stage_passthrough_yields_all():
    result = list(
        validate_stage(
            _ENTRIES,
            required_fields=["level"],
            on_violation="passthrough",
        )
    )
    assert len(result) == len(_ENTRIES)
