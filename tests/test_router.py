"""Tests for logslice.router."""

import pytest
from logslice.router import (
    Route,
    collect_sink,
    field_contains,
    field_equals,
    field_in,
    route_entries,
)


def make_entries():
    return [
        {"level": "error", "service": "auth", "msg": "login failed"},
        {"level": "info", "service": "auth", "msg": "user created"},
        {"level": "error", "service": "payments", "msg": "charge failed"},
        {"level": "warn", "service": "payments", "msg": "slow response"},
        {"level": "info", "service": "gateway", "msg": "request received"},
    ]


def test_field_equals_matches_correctly():
    pred = field_equals("level", "error")
    assert pred({"level": "error"}) is True
    assert pred({"level": "info"}) is False
    assert pred({}) is False


def test_field_contains_matches_substring():
    pred = field_contains("msg", "failed")
    assert pred({"msg": "login failed"}) is True
    assert pred({"msg": "success"}) is False
    assert pred({}) is False


def test_field_in_matches_any_value():
    pred = field_in("service", ["auth", "payments"])
    assert pred({"service": "auth"}) is True
    assert pred({"service": "payments"}) is True
    assert pred({"service": "gateway"}) is False


def test_route_entries_sends_to_matching_sink():
    errors = []
    routes = [Route("errors", field_equals("level", "error"), collect_sink(errors))]
    result = list(route_entries(make_entries(), routes))
    assert len(result) == 5  # all entries still yielded
    assert len(errors) == 2
    assert all(e["level"] == "error" for e in errors)


def test_route_entries_fanout_multiple_routes():
    errors = []
    auth = []
    routes = [
        Route("errors", field_equals("level", "error"), collect_sink(errors)),
        Route("auth", field_equals("service", "auth"), collect_sink(auth)),
    ]
    list(route_entries(make_entries(), routes))
    assert len(errors) == 2
    assert len(auth) == 2


def test_route_entries_fallback_sink_receives_unmatched():
    fallback = []
    routes = [Route("errors", field_equals("level", "error"), collect_sink([]))]
    list(route_entries(make_entries(), routes, fallback_sink=collect_sink(fallback)))
    # 3 entries are not errors
    assert len(fallback) == 3


def test_route_entries_no_fallback_unmatched_still_yielded():
    routes = [Route("errors", field_equals("level", "error"), collect_sink([]))]
    result = list(route_entries(make_entries(), routes))
    assert len(result) == 5


def test_route_entries_empty_input_yields_nothing():
    result = list(route_entries([], []))
    assert result == []


def test_collect_sink_appends_entry():
    bucket = []
    sink = collect_sink(bucket)
    entry = {"level": "info"}
    sink(entry)
    assert bucket == [entry]
