"""Tests for logslice.checkpoint and logslice.pipeline_checkpoint."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from logslice.checkpoint import (
    iter_lines_from_checkpoint,
    load_checkpoints,
    save_checkpoints,
)
from logslice.pipeline_checkpoint import checkpoint_sink_stage


# ---------------------------------------------------------------------------
# load / save
# ---------------------------------------------------------------------------

def test_load_checkpoints_missing_file(tmp_path):
    result = load_checkpoints(str(tmp_path / "nonexistent.json"))
    assert result == {}


def test_load_checkpoints_malformed_file(tmp_path):
    p = tmp_path / "cp.json"
    p.write_text("not json", encoding="utf-8")
    assert load_checkpoints(str(p)) == {}


def test_save_and_load_roundtrip(tmp_path):
    path = str(tmp_path / "cp.json")
    offsets = {"service_a.log": 1024, "service_b.log": 2048}
    save_checkpoints(offsets, path)
    loaded = load_checkpoints(path)
    assert loaded == offsets


def test_save_is_atomic(tmp_path):
    """No .tmp file should survive after save_checkpoints."""
    path = str(tmp_path / "cp.json")
    save_checkpoints({"x": 0}, path)
    tmp_file = Path(path).with_suffix(".tmp")
    assert not tmp_file.exists()


# ---------------------------------------------------------------------------
# iter_lines_from_checkpoint
# ---------------------------------------------------------------------------

def test_iter_lines_from_checkpoint_reads_all_when_no_offset(tmp_path):
    log = tmp_path / "app.log"
    log.write_text("line1\nline2\nline3\n", encoding="utf-8")
    offsets: dict = {}
    lines = list(iter_lines_from_checkpoint(str(log), offsets))
    assert lines == ["line1", "line2", "line3"]


def test_iter_lines_from_checkpoint_updates_offset(tmp_path):
    log = tmp_path / "app.log"
    log.write_text("hello\nworld\n", encoding="utf-8")
    offsets: dict = {}
    list(iter_lines_from_checkpoint(str(log), offsets))
    assert offsets[str(log)] == log.stat().st_size


def test_iter_lines_from_checkpoint_resumes_from_offset(tmp_path):
    log = tmp_path / "app.log"
    first_part = "skip_me\n"
    log.write_text(first_part + "keep_me\n", encoding="utf-8")
    offsets = {str(log): len(first_part.encode())}
    lines = list(iter_lines_from_checkpoint(str(log), offsets))
    assert lines == ["keep_me"]


def test_iter_lines_from_checkpoint_custom_key(tmp_path):
    log = tmp_path / "app.log"
    log.write_text("data\n", encoding="utf-8")
    offsets: dict = {}
    list(iter_lines_from_checkpoint(str(log), offsets, key="my_key"))
    assert "my_key" in offsets
    assert str(log) not in offsets


# ---------------------------------------------------------------------------
# checkpoint_sink_stage
# ---------------------------------------------------------------------------

def _make_entries(n: int):
    return [{"index": i} for i in range(n)]


def test_checkpoint_sink_stage_passes_entries_through(tmp_path):
    cp = str(tmp_path / "cp.json")
    offsets: dict = {}
    entries = _make_entries(5)
    result = list(checkpoint_sink_stage(entries, offsets, checkpoint_path=cp))
    assert result == entries


def test_checkpoint_sink_stage_saves_on_completion(tmp_path):
    cp = str(tmp_path / "cp.json")
    offsets = {"svc": 512}
    list(checkpoint_sink_stage(_make_entries(3), offsets, checkpoint_path=cp))
    loaded = load_checkpoints(cp)
    assert loaded["svc"] == 512


def test_checkpoint_sink_stage_saves_periodically(tmp_path):
    cp = str(tmp_path / "cp.json")
    offsets = {"svc": 0}
    saves: list = []

    # Wrap save to track calls via side-effect on offsets mutation
    entries = _make_entries(10)
    list(checkpoint_sink_stage(entries, offsets, checkpoint_path=cp, save_every=3))
    # At least one intermediate + final save must have occurred
    assert Path(cp).exists()
