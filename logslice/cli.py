"""Command-line interface for logslice."""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from logslice.source import resolve_sources
from logslice.log_filter import iter_json_lines, filter_logs
from logslice.formatter import OutputFormat, format_entries
from logslice.query_parser import Query


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="logslice",
        description="Stream and filter structured JSON logs.",
    )
    p.add_argument(
        "files",
        nargs="*",
        metavar="FILE",
        help="Log files to read (default: stdin).",
    )
    p.add_argument(
        "-d", "--dir",
        metavar="DIR",
        help="Directory to scan for log files.",
    )
    p.add_argument(
        "--dir-pattern",
        default="*.log",
        metavar="GLOB",
        help="Glob pattern for --dir (default: *.log).",
    )
    p.add_argument(
        "-q", "--query",
        default="",
        metavar="EXPR",
        help="Filter expression, e.g. 'level=error AND service=api'.",
    )
    p.add_argument(
        "-f", "--format",
        choices=[f.value for f in OutputFormat],
        default=OutputFormat.COMPACT.value,
        help="Output format (default: compact).",
    )
    p.add_argument(
        "--fields",
        nargs="+",
        metavar="FIELD",
        help="Fields to include in compact output.",
    )
    p.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colour output.",
    )
    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    lines = resolve_sources(
        paths=args.files or None,
        directory=args.dir,
        dir_pattern=args.dir_pattern,
    )

    records = iter_json_lines(lines)
    query = Query(args.query)
    matched = filter_logs(records, query)

    fmt = OutputFormat(args.format)
    for text in format_entries(
        matched,
        fmt=fmt,
        fields=args.fields,
        colorize=not args.no_color,
    ):
        print(text)

    return 0


if __name__ == "__main__":
    sys.exit(main())
