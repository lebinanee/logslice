"""Tests for logslice.pipeline_correlate stages."""
from __future__ import annotations

from logslice.pipeline_correlate import correlate_stage, correlate_window_stage


def _e(req_id: str, ts: float | None = None) -> dict:
    entry: dict = {"request_id": req_id, "level": "info"}
    if ts is not None:
        entry["timestamp"] = ts
    return entry


def _entries():
    return [
        _e("abc"),
        _e("xyz"),
        _e("abc"),
        _e("abc"),
    ]


# ---------------------------------------------------------------------------
# correlate_stage
# ---------------------------------------------------------------------------

def test_correlate_stage_yields_summary_entry():
    results = list(correlate_stage(_entries(), field="request_id"))
    assert len(results) == 1
    summary = results[0]
    assert summary["_correlation_key"] == "abc"
    assert summary["_correlation_count"] == 3
    assert "entries" in summary


def test_correlate_stage_emit_flat_yields_raw_entries():
    results = list(correlate_stage(_entries(), field="request_id", emit_flat=True))
    assert all("request_id" in r for r in results)
    assert len(results) == 3  # only the correlated group (min_count=2)


def test_correlate_stage_min_count_respected():
    results = list(correlate_stage(_entries(), field="request_id", min_count=1))
    # both 'abc' (3) and 'xyz' (1) should appear
    keys = {r["_correlation_key"] for r in results}
    assert "abc" in keys
    assert "xyz" in keys


def test_correlate_stage_summary_contains_field_metadata():
    results = list(correlate_stage(_entries(), field="request_id"))
    assert results[0]["_correlation_field"] == "request_id"


def test_correlate_stage_empty_input_yields_nothing():
    results = list(correlate_stage([], field="request_id"))
    assert results == []


# ---------------------------------------------------------------------------
# correlate_window_stage
# ---------------------------------------------------------------------------

def test_window_stage_yields_summary_with_window_field():
    entries = [_e("r", ts=0.0), _e("r", ts=10.0)]
    results = list(correlate_window_stage(entries, field="request_id", window_seconds=60.0))
    assert len(results) == 1
    assert results[0]["_window_seconds"] == 60.0


def test_window_stage_emit_flat_yields_raw():
    entries = [_e("r", ts=0.0), _e("r", ts=10.0)]
    results = list(
        correlate_window_stage(entries, field="request_id", window_seconds=60.0, emit_flat=True)
    )
    assert len(results) == 2
    assert all("request_id" in r for r in results)


def test_window_stage_empty_input_yields_nothing():
    results = list(correlate_window_stage([], field="request_id"))
    assert results == []
