# Log Correlator

The **correlator** module groups log entries that share a common field value
(e.g. `request_id`, `trace_id`) so you can analyse the full lifecycle of a
request across multiple services.

## Core functions

| Function | Description |
|---|---|
| `correlate_by_field` | Batch-mode grouping; emits groups after the stream ends |
| `correlate_with_timeout` | Stream-friendly grouping; flushes a group once the time gap exceeds a threshold |
| `flatten_groups` | Converts groups back to a flat entry stream |

## Pipeline stages

```python
from logslice.pipeline_correlate import correlate_stage, correlate_window_stage
```

### `correlate_stage`

```python
results = list(correlate_stage(entries, field="request_id", min_count=2))
# Each result is a summary entry:
# {
#   "_correlation_field": "request_id",
#   "_correlation_key": "abc-123",
#   "_correlation_count": 4,
#   "entries": [...]
# }
```

Set `emit_flat=True` to yield the raw correlated entries instead of summaries.

### `correlate_window_stage`

Like `correlate_stage` but flushes groups whose time span exceeds
`window_seconds` (requires a numeric `timestamp` field).

```python
results = list(
    correlate_window_stage(
        entries,
        field="request_id",
        ts_field="timestamp",
        window_seconds=30.0,
    )
)
```

## Parameters

| Parameter | Default | Description |
|---|---|---|
| `field` | required | Entry field to correlate on |
| `min_count` | `2` | Minimum group size to emit |
| `max_group_size` | `1000` | Cap on entries per group |
| `ts_field` | `"timestamp"` | Numeric timestamp field (window mode) |
| `window_seconds` | `60.0` | Flush threshold in seconds |
| `emit_flat` | `False` | Yield raw entries instead of summary entries |
