"""Tests for logslice.enricher and logslice.pipeline_enrich."""

from __future__ import annotations

import re
from typing import List

import pytest

from logslice.enricher import (
    add_entry_hash,
    add_ingestion_timestamp,
    add_static_field,
    enrich_entries,
    rename_field,
)
from logslice.pipeline_enrich import (
    entry_hash_stage,
    ingestion_timestamp_stage,
    rename_field_stage,
    static_field_stage,
)


def _entries(n: int = 3) -> List[dict]:
    return [{"level": "info", "msg": f"message {i}", "idx": i} for i in range(n)]


# ---------------------------------------------------------------------------
# add_static_field
# ---------------------------------------------------------------------------

def test_add_static_field_injects_value():
    fn = add_static_field("env", "production")
    result = fn({"msg": "hi"})
    assert result["env"] == "production"


def test_add_static_field_does_not_mutate_original():
    original = {"msg": "hi"}
    fn = add_static_field("env", "prod")
    fn(original)
    assert "env" not in original


# ---------------------------------------------------------------------------
# add_ingestion_timestamp
# ---------------------------------------------------------------------------

def test_add_ingestion_timestamp_adds_key():
    fn = add_ingestion_timestamp()
    result = fn({"msg": "hi"})
    assert "_ingested_at" in result


def test_add_ingestion_timestamp_custom_key():
    fn = add_ingestion_timestamp(key="received")
    result = fn({"msg": "hi"})
    assert "received" in result


def test_add_ingestion_timestamp_is_iso_format():
    fn = add_ingestion_timestamp()
    result = fn({"msg": "hi"})
    # Basic ISO-8601 check
    assert re.match(r"\d{4}-\d{2}-\d{2}T", result["_ingested_at"])


# ---------------------------------------------------------------------------
# add_entry_hash
# ---------------------------------------------------------------------------

def test_add_entry_hash_adds_key():
    fn = add_entry_hash()
    result = fn({"msg": "hello"})
    assert "_hash" in result


def test_add_entry_hash_is_16_chars():
    fn = add_entry_hash()
    result = fn({"msg": "hello"})
    assert len(result["_hash"]) == 16


def test_add_entry_hash_deterministic():
    fn = add_entry_hash(fields=["msg"])
    h1 = fn({"msg": "hello"})["_hash"]
    h2 = fn({"msg": "hello"})["_hash"]
    assert h1 == h2


def test_add_entry_hash_differs_for_different_content():
    fn = add_entry_hash(fields=["msg"])
    h1 = fn({"msg": "hello"})["_hash"]
    h2 = fn({"msg": "world"})["_hash"]
    assert h1 != h2


# ---------------------------------------------------------------------------
# rename_field
# ---------------------------------------------------------------------------

def test_rename_field_renames_key():
    fn = rename_field("message", "msg")
    result = fn({"message": "hi"})
    assert result["msg"] == "hi"
    assert "message" not in result


def test_rename_field_noop_if_absent():
    fn = rename_field("missing", "dst")
    result = fn({"msg": "hi"})
    assert result == {"msg": "hi"}


# ---------------------------------------------------------------------------
# enrich_entries
# ---------------------------------------------------------------------------

def test_enrich_entries_applies_all_enrichers():
    enrichers = [add_static_field("env", "test"), add_static_field("version", "1")]
    results = list(enrich_entries(_entries(2), enrichers))
    for r in results:
        assert r["env"] == "test"
        assert r["version"] == "1"


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def test_static_field_stage():
    out = list(static_field_stage(_entries(2), key="src", value="svc-a"))
    assert all(e["src"] == "svc-a" for e in out)


def test_ingestion_timestamp_stage_adds_field():
    out = list(ingestion_timestamp_stage(_entries(2)))
    assert all("_ingested_at" in e for e in out)


def test_entry_hash_stage_adds_field():
    out = list(entry_hash_stage(_entries(2)))
    assert all("_hash" in e for e in out)


def test_rename_field_stage_renames():
    entries = [{"message": "hi", "level": "info"}]
    out = list(rename_field_stage(entries, src="message", dst="msg"))
    assert out[0]["msg"] == "hi"
    assert "message" not in out[0]
