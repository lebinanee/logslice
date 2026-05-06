"""Log source abstractions for reading from files, stdin, and directories."""

from __future__ import annotations

import sys
import os
from pathlib import Path
from typing import Iterator, List, Optional


def iter_lines_from_file(path: str) -> Iterator[str]:
    """Yield lines from a single file."""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            yield line.rstrip("\n")


def iter_lines_from_stdin() -> Iterator[str]:
    """Yield lines from standard input."""
    for line in sys.stdin:
        yield line.rstrip("\n")


def iter_lines_from_dir(directory: str, pattern: str = "*.log") -> Iterator[str]:
    """Yield lines from all matching files in a directory (sorted by name)."""
    dir_path = Path(directory)
    for file_path in sorted(dir_path.glob(pattern)):
        if file_path.is_file():
            yield from iter_lines_from_file(str(file_path))


def resolve_sources(
    paths: Optional[List[str]],
    directory: Optional[str] = None,
    dir_pattern: str = "*.log",
) -> Iterator[str]:
    """Resolve one or more source specifications into a unified line iterator.

    Priority:
      1. Explicit file paths (iterated in order).
      2. A directory scan if *directory* is given.
      3. Standard input as a fallback.
    """
    if paths:
        for p in paths:
            yield from iter_lines_from_file(p)
        return

    if directory:
        yield from iter_lines_from_dir(directory, dir_pattern)
        return

    yield from iter_lines_from_stdin()
