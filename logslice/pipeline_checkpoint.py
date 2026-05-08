"""Pipeline stage that wraps an iterable with checkpoint tracking."""

from __future__ import annotations

from typing import Dict, Iterable, Iterator, Optional

from logslice.checkpoint import save_checkpoints
from logslice.pipeline import stage


@stage
def checkpoint_sink_stage(
    entries: Iterable[dict],
    offsets: Dict[str, int],
    checkpoint_path: str = ".logslice_checkpoint.json",
    save_every: int = 100,
) -> Iterator[dict]:
    """Pass entries through unchanged, periodically persisting *offsets*.

    After every *save_every* entries the current *offsets* snapshot is written
    to *checkpoint_path*.  A final save is performed when the stream ends.
    """
    count = 0
    for entry in entries:
        yield entry
        count += 1
        if count % save_every == 0:
            save_checkpoints(offsets, checkpoint_path)
    save_checkpoints(offsets, checkpoint_path)
