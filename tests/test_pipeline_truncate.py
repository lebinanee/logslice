"""Tests for logslice.pipeline_truncate stages."""
from typing import Any, Dict, List

from logslice.pipeline_truncate import drop_long_fields_stage, truncate_fields_stage

Entry = Dict[str, Any]


def _entries() -> List[Entry]:
    return [
        {"msg": "hello world, this is a long message", "level": "info", "count": 3},
        {"msg": "short", "level": "debug", "count": 1},
        {"msg": "another lengthy string that exceeds limit", "level": "error", "count": 7},
    ]


# ---------------------------------------------------------------------------
# truncate_fields_stage
# ---------------------------------------------------------------------------

def test_truncate_stage_shortens_target_field():
    stage = truncate_fields_stage(["msg"], max_length=10)
    results = list(stage(_entries()))
    assert results[0]["msg"] == "hello w..."
    assert results[1]["msg"] == "short"  # unchanged


def test_truncate_stage_preserves_other_fields():
    stage = truncate_fields_stage(["msg"], max_length=10)
    results = list(stage(_entries()))
    assert results[0]["level"] == "info"
    assert results[0]["count"] == 3


def test_truncate_stage_custom_suffix():
    stage = truncate_fields_stage(["msg"], max_length=8, suffix="!")
    results = list(stage(_entries()))
    assert results[0]["msg"].endswith("!")
    assert len(results[0]["msg"]) == 8


def test_truncate_stage_multiple_fields():
    entries = [{"msg": "hello world", "detail": "extra long detail info", "level": "warn"}]
    stage = truncate_fields_stage(["msg", "detail"], max_length=8)
    results = list(stage(entries))
    assert len(results[0]["msg"]) == 8
    assert len(results[0]["detail"]) == 8


def test_truncate_stage_empty_input():
    stage = truncate_fields_stage(["msg"], max_length=10)
    assert list(stage([])) == []


# ---------------------------------------------------------------------------
# drop_long_fields_stage
# ---------------------------------------------------------------------------

def test_drop_stage_removes_long_field():
    stage = drop_long_fields_stage(max_length=10)
    results = list(stage(_entries()))
    assert "msg" not in results[0]
    assert "msg" in results[1]  # short enough to keep


def test_drop_stage_replace_with():
    stage = drop_long_fields_stage(max_length=10, replace_with="[DROPPED]")
    results = list(stage(_entries()))
    assert results[0]["msg"] == "[DROPPED]"
    assert results[1]["msg"] == "short"


def test_drop_stage_preserves_non_string_fields():
    stage = drop_long_fields_stage(max_length=1)
    results = list(stage(_entries()))
    for result in results:
        assert "count" in result


def test_drop_stage_empty_input():
    stage = drop_long_fields_stage(max_length=10)
    assert list(stage([])) == []
