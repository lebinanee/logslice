"""Log filter that applies parsed queries to JSON log streams."""

import json
import sys
from typing import IO, Iterator, Optional

from logslice.query_parser import Query, parse_query


def iter_json_lines(stream: IO[str]) -> Iterator[dict]:
    """Yield parsed JSON objects from a line-delimited JSON stream."""
    for line_num, line in enumerate(stream, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError as exc:
            print(
                f"[logslice] Warning: skipping invalid JSON on line {line_num}: {exc}",
                file=sys.stderr,
            )


def filter_logs(
    stream: IO[str],
    query: Optional[Query] = None,
    fields: Optional[list] = None,
    limit: Optional[int] = None,
) -> Iterator[dict]:
    """Filter JSON log records from a stream using an optional query.

    Args:
        stream: Input stream of newline-delimited JSON.
        query: Parsed Query object; if None, all records pass.
        fields: If provided, only include these fields in output.
        limit: Maximum number of matching records to yield.

    Yields:
        Matching log record dicts (optionally projected to `fields`).
    """
    count = 0
    for record in iter_json_lines(stream):
        if query is not None and not query.match(record):
            continue
        if fields:
            record = {k: record[k] for k in fields if k in record}
        yield record
        count += 1
        if limit is not None and count >= limit:
            break


def run_filter(
    query_str: str = "",
    input_stream: IO[str] = sys.stdin,
    output_stream: IO[str] = sys.stdout,
    fields: Optional[list] = None,
    limit: Optional[int] = None,
) -> int:
    """Parse query and run the filter, writing results as JSON lines.

    Returns the number of records written.
    """
    query = parse_query(query_str)
    count = 0
    for record in filter_logs(input_stream, query=query, fields=fields, limit=limit):
        output_stream.write(json.dumps(record) + "\n")
        count += 1
    return count
