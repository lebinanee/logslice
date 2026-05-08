"""Checkpoint support: persist and resume log stream positions."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Optional


_DEFAULT_CHECKPOINT_FILE = ".logslice_checkpoint.json"


def load_checkpoints(path: str = _DEFAULT_CHECKPOINT_FILE) -> Dict[str, int]:
    """Load checkpoint offsets from *path*.

    Returns a mapping of source-key -> byte-offset.
    Returns an empty dict if the file does not exist or is malformed.
    """
    p = Path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return {str(k): int(v) for k, v in data.items()}
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return {}


def save_checkpoints(
    offsets: Dict[str, int],
    path: str = _DEFAULT_CHECKPOINT_FILE,
) -> None:
    """Persist *offsets* to *path* atomically."""
    p = Path(path)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(offsets, indent=2), encoding="utf-8")
    os.replace(tmp, p)


def iter_lines_from_checkpoint(
    file_path: str,
    offsets: Dict[str, int],
    key: Optional[str] = None,
):
    """Yield lines from *file_path* starting at the stored byte offset.

    *key* defaults to *file_path* when not provided.  The *offsets* dict is
    updated in-place so the caller can persist it after processing.
    """
    source_key = key or file_path
    start_offset = offsets.get(source_key, 0)

    with open(file_path, "rb") as fh:
        fh.seek(start_offset)
        for raw in fh:
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            yield line
        offsets[source_key] = fh.tell()
