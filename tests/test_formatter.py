"""Tests for logslice.formatter module."""

import pytest
import json

from logslice.formatter import format_entry, format_entries, OutputFormat


SAMPLE = {
    "timestamp": "2024-01-15T10:00:00Z",
    "level": "INFO",
    "service": "api",
    "message": "Request received",
    "status": 200,
}


def test_format_json_roundtrip():
    result = format_entry(SAMPLE, fmt=OutputFormat.JSON)
    parsed = json.loads(result)
    assert parsed == SAMPLE


def test_format_compact_default_fields():
    result = format_entry(SAMPLE, fmt=OutputFormat.COMPACT)
    assert "2024-01-15T10:00:00Z" in result
    assert "INFO" in result
    assert "api" in result
    assert "Request received" in result
    assert " | " in result


def test_format_compact_custom_fields():
    result = format_entry(SAMPLE, fmt=OutputFormat.COMPACT, fields=["service", "message"])
    assert result == "api | Request received"


def test_format_compact_missing_field_skipped():
    result = format_entry(SAMPLE, fmt=OutputFormat.COMPACT, fields=["service", "nonexistent"])
    assert result == "api"


def test_format_pretty_contains_separator():
    result = format_entry(SAMPLE, fmt=OutputFormat.PRETTY)
    assert result.startswith("---")
    assert "level: INFO" in result
    assert "service: api" in result


def test_format_pretty_extra_fields_included():
    result = format_entry(SAMPLE, fmt=OutputFormat.PRETTY)
    assert "status: 200" in result


def test_format_color_compact_level():
    result = format_entry(SAMPLE, fmt=OutputFormat.COMPACT, color=True)
    # ANSI escape code for green should be present for INFO
    assert "\033[32m" in result
    assert "\033[0m" in result


def test_format_color_pretty_level():
    result = format_entry(SAMPLE, fmt=OutputFormat.PRETTY, color=True)
    assert "\033[32m" in result


def test_format_no_color_by_default():
    result = format_entry(SAMPLE, fmt=OutputFormat.COMPACT)
    assert "\033[" not in result


def test_format_entries_returns_list():
    entries = [SAMPLE, {**SAMPLE, "level": "ERROR", "message": "Failure"}]
    results = format_entries(entries, fmt=OutputFormat.COMPACT)
    assert len(results) == 2
    assert "ERROR" in results[1]


def test_format_entries_empty():
    assert format_entries([], fmt=OutputFormat.JSON) == []


def test_format_unknown_level_no_color_crash():
    entry = {**SAMPLE, "level": "TRACE"}
    result = format_entry(entry, fmt=OutputFormat.COMPACT, color=True)
    assert "TRACE" in result
