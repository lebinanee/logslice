"""Integration tests for the logslice CLI entry point."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from logslice.cli import main


def write_log(path: Path, records: list) -> None:
    path.write_text(
        "\n".join(json.dumps(r) for r in records) + "\n",
        encoding="utf-8",
    )


@pytest.fixture()
def log_file(tmp_path):
    f = tmp_path / "app.log"
    write_log(f, [
        {"level": "info", "msg": "started", "service": "api"},
        {"level": "error", "msg": "boom", "service": "api"},
        {"level": "info", "msg": "ping", "service": "worker"},
    ])
    return f


def test_cli_no_filter_returns_all(log_file, capsys):
    rc = main([str(log_file), "--no-color"])
    assert rc == 0
    out = capsys.readouterr().out
    assert out.count("\n") == 3


def test_cli_query_filters_records(log_file, capsys):
    rc = main([str(log_file), "-q", "level=error", "--no-color"])
    assert rc == 0
    out = capsys.readouterr().out
    lines = [l for l in out.splitlines() if l.strip()]
    assert len(lines) == 1
    assert "boom" in lines[0]


def test_cli_json_format(log_file, capsys):
    rc = main([str(log_file), "-f", "json", "--no-color"])
    assert rc == 0
    out = capsys.readouterr().out
    for line in out.splitlines():
        parsed = json.loads(line)
        assert "level" in parsed


def test_cli_dir_source(tmp_path, capsys):
    write_log(tmp_path / "svc.log", [{"level": "warn", "msg": "low disk"}])
    rc = main(["--dir", str(tmp_path), "--no-color"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "low disk" in out


def test_cli_custom_fields(log_file, capsys):
    rc = main([str(log_file), "-f", "compact", "--fields", "level", "--no-color"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "info" in out
    # 'service' field should not appear when not requested
    assert "api" not in out
