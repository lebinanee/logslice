"""Tests for logslice.alerter."""

from __future__ import annotations

import pytest

from logslice.alerter import AlertRule, evaluate_rules
from logslice.query_parser import Query, Condition, Operator


def _eq_query(field: str, value: str) -> Query:
    return Query(conditions=[Condition(field=field, op=Operator.EQ, value=value)])


def _entries(*levels: str, base_ts: float = 1_000.0, gap: float = 1.0) -> list[dict]:
    return [{"level": lvl, "msg": "x", "ts": base_ts + i * gap} for i, lvl in enumerate(levels)]


def test_evaluate_rules_yields_all_entries():
    entries = _entries("info", "warn", "error")
    fired = []
    rule = AlertRule(
        name="err",
        query=_eq_query("level", "error"),
        threshold=5,
        window_seconds=60.0,
        on_alert=lambda name, _: fired.append(name),
    )
    result = list(evaluate_rules(entries, [rule]))
    assert result == entries


def test_alert_fires_when_threshold_reached():
    entries = _entries("error", "error", "error")
    fired = []
    rule = AlertRule(
        name="too_many_errors",
        query=_eq_query("level", "error"),
        threshold=3,
        window_seconds=60.0,
        on_alert=lambda name, _: fired.append(name),
    )
    list(evaluate_rules(entries, [rule]))
    assert fired == ["too_many_errors"]


def test_alert_does_not_fire_below_threshold():
    entries = _entries("error", "error")
    fired = []
    rule = AlertRule(
        name="r",
        query=_eq_query("level", "error"),
        threshold=3,
        window_seconds=60.0,
        on_alert=lambda name, _: fired.append(name),
    )
    list(evaluate_rules(entries, [rule]))
    assert fired == []


def test_alert_fires_only_once():
    entries = _entries("error", "error", "error", "error", "error")
    fired = []
    rule = AlertRule(
        name="r",
        query=_eq_query("level", "error"),
        threshold=2,
        window_seconds=60.0,
        on_alert=lambda name, _: fired.append(name),
    )
    list(evaluate_rules(entries, [rule]))
    assert len(fired) == 1


def test_alert_respects_window_eviction():
    # entries spaced 30 s apart; window is 20 s — each entry expires before the next
    entries = _entries("error", "error", "error", gap=30.0)
    fired = []
    rule = AlertRule(
        name="r",
        query=_eq_query("level", "error"),
        threshold=2,
        window_seconds=20.0,
        on_alert=lambda name, _: fired.append(name),
    )
    list(evaluate_rules(entries, [rule]))
    assert fired == []


def test_on_alert_receives_matching_entries():
    entries = _entries("info", "error", "error")
    received: list[list[dict]] = []
    rule = AlertRule(
        name="r",
        query=_eq_query("level", "error"),
        threshold=2,
        window_seconds=60.0,
        on_alert=lambda _, matched: received.append(matched),
    )
    list(evaluate_rules(entries, [rule]))
    assert len(received) == 1
    assert all(e["level"] == "error" for e in received[0])


def test_multiple_rules_independent():
    entries = _entries("error", "warn", "warn", "warn")
    fired = []
    rules = [
        AlertRule(
            name="errors",
            query=_eq_query("level", "error"),
            threshold=1,
            window_seconds=60.0,
            on_alert=lambda name, _: fired.append(name),
        ),
        AlertRule(
            name="warns",
            query=_eq_query("level", "warn"),
            threshold=3,
            window_seconds=60.0,
            on_alert=lambda name, _: fired.append(name),
        ),
    ]
    list(evaluate_rules(entries, rules))
    assert "errors" in fired
    assert "warns" in fired
