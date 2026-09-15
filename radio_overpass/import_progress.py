"""Stream an OSM XML file while reporting import progress."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import BinaryIO, TextIO


OBJECT_START = re.compile(rb"<(?:node|way|relation)(?:\s|>)")


def stream_with_progress(
    source_path: Path,
    output: BinaryIO,
    log: TextIO = sys.stderr,
    report_every: int = 5000,
    chunk_size: int = 1024 * 1024,
) -> int:
    if report_every < 1:
        raise ValueError("report_every must be positive")
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")

    processed = 0
    next_report = report_every
    pending = b""
    with source_path.open("rb") as source:
        for chunk in iter(lambda: source.read(chunk_size), b""):
            output.write(chunk)
            data = pending + chunk
            safe_length = max(0, len(data) - 32)
            processed += sum(
                1 for match in OBJECT_START.finditer(data) if match.start() < safe_length
            )
            pending = data[safe_length:]
            while processed >= next_report:
                print(
                    "initial database import: "
                    f"{next_report:,} node/way/relation entries streamed",
                    file=log,
                    flush=True,
                )
                next_report += report_every
    processed += len(OBJECT_START.findall(pending))
    output.flush()
    print(
        "initial database import: "
        f"completed streaming {processed:,} node/way/relation entries",
        file=log,
        flush=True,
    )
    return processed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument(
        "--report-every",
        type=int,
        default=5000,
        help="report after this many OSM objects (default: 5000)",
    )
    args = parser.parse_args()
    stream_with_progress(args.source, sys.stdout.buffer, report_every=args.report_every)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
