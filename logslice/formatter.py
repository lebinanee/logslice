"""Output formatting for filtered log entries."""

from __future__ import annotations

import json
from enum import Enum
from typing import Any


class OutputFormat(str, Enum):
    JSON = "json"
    PRETTY = "pretty"
    COMPACT = "compact"


DEFAULT_PRETTY_FIELDS = ["timestamp", "level", "service", "message"]

ANSI_COLORS = {
    "DEBUG": "\033[36m",    # cyan
    "INFO": "\033[32m",     # green
    "WARNING": "\033[33m",  # yellow
    "WARN": "\033[33m",
    "ERROR": "\033[31m",    # red
    "CRITICAL": "\033[35m", # magenta
    "RESET": "\033[0m",
}


def _colorize_level(level: str) -> str:
    color = ANSI_COLORS.get(level.upper(), "")
    reset = ANSI_COLORS["RESET"] if color else ""
    return f"{color}{level}{reset}"


def format_entry(
    entry: dict[str, Any],
    fmt: OutputFormat = OutputFormat.JSON,
    color: bool = False,
    fields: list[str] | None = None,
) -> str:
    """Format a single log entry according to the chosen output format."""
    if fmt == OutputFormat.JSON:
        return json.dumps(entry, ensure_ascii=False)

    if fmt == OutputFormat.COMPACT:
        selected = fields or DEFAULT_PRETTY_FIELDS
        parts = []
        for key in selected:
            if key in entry:
                value = str(entry[key])
                if color and key == "level":
                    value = _colorize_level(value)
                parts.append(value)
        return " | ".join(parts)

    if fmt == OutputFormat.PRETTY:
        selected_keys = fields or DEFAULT_PRETTY_FIELDS
        extra_keys = [k for k in entry if k not in selected_keys]
        lines = []
        for key in selected_keys + extra_keys:
            if key not in entry:
                continue
            value = str(entry[key])
            if color and key == "level":
                value = _colorize_level(value)
            lines.append(f"  {key}: {value}")
        return "---\n" + "\n".join(lines)

    return json.dumps(entry, ensure_ascii=False)


def format_entries(
    entries: list[dict[str, Any]],
    fmt: OutputFormat = OutputFormat.JSON,
    color: bool = False,
    fields: list[str] | None = None,
) -> list[str]:
    """Format multiple log entries."""
    return [format_entry(e, fmt=fmt, color=color, fields=fields) for e in entries]
