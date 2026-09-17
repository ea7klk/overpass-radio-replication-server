"""Import an XML file into a fresh Overpass database with progress logging."""

from __future__ import annotations

import argparse
from pathlib import Path

from .replicator import import_xml_with_progress


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-dir", required=True, type=Path)
    parser.add_argument("--xml", required=True, type=Path)
    parser.add_argument("--update-binary", default="/opt/overpass/bin/update_database")
    parser.add_argument("--every", default=5000, type=int)
    args = parser.parse_args()
    import_xml_with_progress(
        [args.update_binary, f"--db-dir={args.db_dir}", "--meta=no"],
        args.xml,
        args.every,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
