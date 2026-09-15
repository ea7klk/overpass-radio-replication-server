#!/usr/bin/env python3
"""Prepare catalog metadata from an initial OSM XML snapshot."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from lxml import etree as ET


OBJECT_TYPES = ("node", "way", "relation")


def nested(data: dict[str, Any], *keys: str) -> Any:
    value: Any = data
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def fileinfo(pbf_path: Path, *, extended: bool = False) -> dict[str, Any]:
    command = ["osmium", "fileinfo", "--json"]
    if extended:
        command.append("--extended")
    command.extend(("--no-crc", str(pbf_path)))
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def snapshot_timestamp(pbf_path: Path) -> tuple[str, str, int | None]:
    info = fileinfo(pbf_path)
    timestamp = nested(info, "header", "option", "osmosis_replication_timestamp")
    source = "header.option.osmosis_replication_timestamp"
    sequence = nested(info, "header", "option", "osmosis_replication_sequence_number")
    if timestamp is None:
        info = fileinfo(pbf_path, extended=True)
        timestamp = nested(info, "data", "timestamp", "last")
        source = "data.timestamp.last"
    if not isinstance(timestamp, str) or not timestamp:
        raise RuntimeError(
            f"{pbf_path} has no usable replication timestamp in its PBF header "
            "and no object timestamp could be inferred"
        )
    try:
        sequence_value = int(sequence) if sequence is not None else None
    except (TypeError, ValueError):
        sequence_value = None
    return timestamp, source, sequence_value


def element_key(element: ET._Element) -> str:
    return f"{element.tag}:{element.get('id')}"


def references(element: ET._Element) -> list[str]:
    if element.tag == "way":
        return [
            f"node:{child.get('ref')}"
            for child in element
            if child.tag == "nd" and child.get("ref")
        ]
    if element.tag == "relation":
        return [
            f"{child.get('type')}:{child.get('ref')}"
            for child in element
            if child.tag == "member"
            and child.get("type") in OBJECT_TYPES
            and child.get("ref")
        ]
    return []


def has_matching_tag(element: ET._Element, prefix: str) -> bool:
    return any(
        child.tag == "tag" and (child.get("k") or "").startswith(prefix)
        for child in element
    )


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def build_catalog(xml_path: Path, output_path: Path, prefix: str) -> tuple[int, int]:
    roots: set[str] = set()
    objects: set[str] = set()
    refs: dict[str, list[str]] = {}
    with xml_path.open("rb") as source:
        stream = ET.iterparse(
            source,
            events=("end",),
            tag=OBJECT_TYPES,
            huge_tree=True,
            resolve_entities=False,
            load_dtd=False,
            no_network=True,
        )
        for _, element in stream:
            key = element_key(element)
            objects.add(key)
            object_refs = references(element)
            if object_refs:
                refs[key] = sorted(set(object_refs))
            if has_matching_tag(element, prefix):
                roots.add(key)
            element.clear()

    catalog = {
        "roots": sorted(roots),
        "dependencies": sorted(objects - roots),
        "refs": {key: refs[key] for key in sorted(refs) if key in objects},
    }
    atomic_json(output_path, catalog)
    return len(roots), len(objects - roots)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pbf", required=True, type=Path)
    parser.add_argument("--xml", required=True, type=Path)
    parser.add_argument("--catalog", required=True, type=Path)
    parser.add_argument("--metadata", required=True, type=Path)
    parser.add_argument("--prefix", default="communication:amateur_radio")
    args = parser.parse_args()

    timestamp, timestamp_source, sequence = snapshot_timestamp(args.pbf)
    roots, dependencies = build_catalog(args.xml, args.catalog, args.prefix)
    atomic_json(
        args.metadata,
        {
            "source_file": args.pbf.name,
            "timestamp": timestamp,
            "timestamp_source": timestamp_source,
            "replication_sequence": sequence,
            "roots": roots,
            "dependencies": dependencies,
        },
    )
    print(
        f"prepared initial catalog with {roots} root(s) and {dependencies} "
        f"dependency object(s) at snapshot {timestamp}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
