"""Integration tests for the redact_stage inside the pipeline."""

from logslice.pipeline import (
    build_pipeline,
    filter_stage,
    redact_stage,
    pipeline,
)
from logslice.redactor import _MASK


def _entries():
    return [
        {"level": "info",  "msg": "login",  "user": "alice", "password": "s3cr3t"},
        {"level": "error", "msg": "failed", "user": "bob",   "token": "tok-xyz"},
        {"level": "info",  "msg": "ping",   "user": "carol"},
    ]


def test_redact_stage_masks_password():
    results = list(pipeline(_entries(), [redact_stage()]))
    assert all(r["password"] == _MASK for r in results if "password" in r)


def test_redact_stage_masks_token():
    results = list(pipeline(_entries(), [redact_stage()]))
    assert all(r["token"] == _MASK for r in results if "token" in r)


def test_redact_stage_preserves_non_sensitive_fields():
    results = list(pipeline(_entries(), [redact_stage()]))
    users = {r["user"] for r in results}
    assert users == {"alice", "bob", "carol"}


def test_redact_stage_custom_fields():
    entries = [{"user": "alice", "email": "a@b.com", "msg": "hi"}]
    results = list(pipeline(entries, [redact_stage(fields=["email"])]))
    assert results[0]["email"] == _MASK
    assert results[0]["user"] == "alice"


def test_redact_stage_hash_strategy():
    entries = [{"token": "abc"}]
    results = list(pipeline(entries, [redact_stage(strategy="hash")]))
    assert results[0]["token"].startswith("sha256:")


def test_redact_stage_partial_strategy():
    entries = [{"secret": "ABCDEFGH"}]
    results = list(pipeline(entries, [redact_stage(strategy="partial", visible=4)]))
    assert results[0]["secret"].endswith("EFGH")


def test_pipeline_filter_then_redact():
    stages = [
        filter_stage("level=error"),
        redact_stage(),
    ]
    results = list(pipeline(_entries(), stages))
    assert len(results) == 1
    assert results[0]["user"] == "bob"
    assert results[0]["token"] == _MASK


def test_pipeline_redact_then_filter_preserves_non_sensitive():
    stages = [
        redact_stage(),
        filter_stage("level=info"),
    ]
    results = list(pipeline(_entries(), stages))
    assert all(r["level"] == "info" for r in results)
    assert len(results) == 2


def test_build_pipeline_returns_callable():
    p = build_pipeline([redact_stage()])
    results = list(p(_entries()))
    assert len(results) == len(_entries())
