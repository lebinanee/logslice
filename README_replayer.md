# logslice — Replayer Module

The **replayer** module lets you re-emit a stream of structured log entries at a
controlled pace, useful for load-testing downstream consumers or visually
reviewing logs at human-readable speed.

## Functions

### `replay_realtime(entries, ts_field, speed, sleep_fn)`

Replays entries with delays proportional to the gap between successive
timestamps found in `ts_field` (default `"timestamp"`).  
Entries that lack a valid numeric timestamp are emitted immediately without
affecting the timing state.

| Parameter  | Default       | Description |
|------------|---------------|-------------|
| `ts_field` | `"timestamp"` | Entry key holding a Unix-epoch float |
| `speed`    | `1.0`         | Playback multiplier (2 = 2× faster) |
| `sleep_fn` | `time.sleep`  | Callable for sleeping (injectable for tests) |

### `replay_fixed_rate(entries, rate, sleep_fn)`

Emits entries at a steady **rate** (entries per second), sleeping
`1 / rate` seconds after each entry regardless of timestamps.

## Pipeline Stages

```python
from logslice.pipeline_replay import realtime_replay_stage, fixed_rate_replay_stage

# Replay at original speed
pipeline = realtime_replay_stage(speed=1.0)

# Replay at 100 entries/sec
pipeline = fixed_rate_replay_stage(rate=100.0)
```

## Example

```python
from logslice.replayer import replay_realtime
import json, sys

entries = (json.loads(line) for line in sys.stdin)
for entry in replay_realtime(entries, speed=2.0):
    print(json.dumps(entry))
```
