"""Tests for query_parser and log_filter modules."""

import io
import json
import pytest

from logslice.query_parser import Condition, LogicalOp, Operator, Query, parse_query
from logslice.log_filter import filter_logs, iter_json_lines, run_filter


# ---------------------------------------------------------------------------
# parse_query tests
# ---------------------------------------------------------------------------

def test_parse_empty_query():
    q = parse_query("")
    assert q.conditions == []
    assert q.match({"level": "error"}) is True


def test_parse_single_eq_condition():
    q = parse_query("level=error")
    assert len(q.conditions) == 1
    assert q.conditions[0].field == "level"
    assert q.conditions[0].operator == Operator.EQ
    assert q.conditions[0].value == "error"


def test_parse_and_query():
    q = parse_query("level=error AND service=api")
    assert len(q.conditions) == 2
    assert q.operators == [LogicalOp.AND]


def test_parse_or_query():
    q = parse_query("level=warn OR level=error")
    assert q.operators == [LogicalOp.OR]


def test_parse_contains_operator():
    q = parse_query('message~=timeout')
    assert q.conditions[0].operator == Operator.CONTAINS


def test_parse_invalid_token_raises():
    with pytest.raises(ValueError, match="Invalid query token"):
        parse_query("level")


# ---------------------------------------------------------------------------
# Condition.match tests
# ---------------------------------------------------------------------------

def test_condition_eq_match():
    c = Condition("level", Operator.EQ, "error")
    assert c.match({"level": "error"}) is True
    assert c.match({"level": "warn"}) is False


def test_condition_contains_match():
    c = Condition("message", Operator.CONTAINS, "timeout")
    assert c.match({"message": "Connection timeout occurred"}) is True
    assert c.match({"message": "All good"}) is False


def test_condition_missing_field():
    c = Condition("service", Operator.EQ, "api")
    assert c.match({"level": "error"}) is False


# ---------------------------------------------------------------------------
# filter_logs tests
# ---------------------------------------------------------------------------

LOGS = [
    {"level": "error", "service": "api", "message": "timeout"},
    {"level": "warn", "service": "worker", "message": "slow query"},
    {"level": "error", "service": "worker", "message": "crash"},
    {"level": "info", "service": "api", "message": "started"},
]


def make_stream(records):
    return io.StringIO("\n".join(json.dumps(r) for r in records))


def test_filter_no_query_returns_all():
    results = list(filter_logs(make_stream(LOGS)))
    assert len(results) == 4


def test_filter_eq_query():
    q = parse_query("level=error")
    results = list(filter_logs(make_stream(LOGS), query=q))
    assert len(results) == 2
    assert all(r["level"] == "error" for r in results)


def test_filter_and_query():
    q = parse_query("level=error AND service=api")
    results = list(filter_logs(make_stream(LOGS), query=q))
    assert len(results) == 1
    assert results[0]["message"] == "timeout"


def test_filter_with_limit():
    results = list(filter_logs(make_stream(LOGS), limit=2))
    assert len(results) == 2


def test_filter_with_fields_projection():
    q = parse_query("level=error")
    results = list(filter_logs(make_stream(LOGS), query=q, fields=["level", "service"]))
    assert all("message" not in r for r in results)
    assert all("level" in r and "service" in r for r in results)


def test_iter_json_lines_skips_invalid(capsys):
    stream = io.StringIO("not-json\n{\"ok\": true}\n")
    records = list(iter_json_lines(stream))
    assert records == [{"ok": True}]
    captured = capsys.readouterr()
    assert "Warning" in captured.err


def test_run_filter_writes_output():
    out = io.StringIO()
    count = run_filter(
        query_str="level=error",
        input_stream=make_stream(LOGS),
        output_stream=out,
    )
    assert count == 2
    out.seek(0)
    lines = [json.loads(l) for l in out if l.strip()]
    assert all(l["level"] == "error" for l in lines)
