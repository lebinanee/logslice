"""Tests for logslice.source — line-source resolution helpers."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from logslice.source import (
    iter_lines_from_file,
    iter_lines_from_dir,
    resolve_sources,
)


def write_file(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content), encoding="utf-8")


def test_iter_lines_from_file(tmp_path):
    f = tmp_path / "a.log"
    write_file(f, """\
        {"level":"info"}
        {"level":"error"}
    """)
    lines = list(iter_lines_from_file(str(f)))
    assert lines == ['{"level":"info"}', '{"level":"error"}']


def test_iter_lines_from_file_strips_newline(tmp_path):
    f = tmp_path / "b.log"
    f.write_text("line1\nline2\n", encoding="utf-8")
    lines = list(iter_lines_from_file(str(f)))
    assert all("\n" not in ln for ln in lines)


def test_iter_lines_from_dir_sorted(tmp_path):
    write_file(tmp_path / "b.log", "{\"n\":2}\n")
    write_file(tmp_path / "a.log", "{\"n\":1}\n")
    lines = list(iter_lines_from_dir(str(tmp_path)))
    assert lines == ['{"n":1}', '{"n":2}']


def test_iter_lines_from_dir_pattern(tmp_path):
    write_file(tmp_path / "app.log", "log_line\n")
    write_file(tmp_path / "readme.txt", "ignored\n")
    lines = list(iter_lines_from_dir(str(tmp_path), pattern="*.log"))
    assert lines == ["log_line"]


def test_resolve_sources_explicit_files(tmp_path):
    f1 = tmp_path / "1.log"
    f2 = tmp_path / "2.log"
    write_file(f1, "line_a\n")
    write_file(f2, "line_b\n")
    lines = list(resolve_sources(paths=[str(f1), str(f2)]))
    assert lines == ["line_a", "line_b"]


def test_resolve_sources_directory(tmp_path):
    write_file(tmp_path / "x.log", "from_dir\n")
    lines = list(resolve_sources(paths=None, directory=str(tmp_path)))
    assert lines == ["from_dir"]


def test_resolve_sources_files_take_priority_over_dir(tmp_path):
    f = tmp_path / "explicit.log"
    write_file(f, "explicit_line\n")
    other = tmp_path / "sub"
    other.mkdir()
    write_file(other / "other.log", "other_line\n")
    lines = list(resolve_sources(paths=[str(f)], directory=str(other)))
    assert lines == ["explicit_line"]
