#!/usr/bin/env python3
"""Filter an OsmChange XML stream by tag-key prefix.

The filter keeps all tags on matching objects. It also handles the important
case where a modify loses the matching tag: if that object is known to be in
the target set, a delete is emitted.

Input is gzip-compressed OSC XML on stdin. Output is an OSC XML batch on
stdout. A JSON-lines delta is written to --delta so membership is committed
only after Overpass accepts the batch.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.sax.saxutils import quoteattr


def load_membership(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    return set(data)


def osm_key(element: ET.Element) -> str:
    return f"{element.tag}:{element.attrib['id']}"


def matches(element: ET.Element, prefix: str) -> bool:
    return any(
        child.tag == "tag"
        and child.attrib.get("k", "").startswith(prefix)
        for child in element
    )


def xml_start(tag: str, attributes: dict[str, str]) -> bytes:
    attrs = "".join(f" {key}={quoteattr(value)}" for key, value in attributes.items())
    return f"<{tag}>\n".encode("utf-8").replace(b">\n", (attrs + ">\n").encode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--membership", required=True, type=Path)
    parser.add_argument("--delta", required=True, type=Path)
    parser.add_argument("--prefix", default="communication:amateur_radio")
    args = parser.parse_args()

    membership = load_membership(args.membership)
    args.delta.parent.mkdir(parents=True, exist_ok=True)
    delta_handle = args.delta.open("w", encoding="utf-8")

    def record(item: dict[str, str]) -> None:
        delta_handle.write(json.dumps(item, separators=(",", ":")) + "\n")

    output = sys.stdout.buffer
    action: str | None = None
    object_element: ET.Element | None = None
    root_seen = False
    groups_open = False

    # iterparse keeps only the current object in memory. This matters for the
    # daily stream, whose unfiltered changes can be very large.
    with gzip.GzipFile(fileobj=sys.stdin.buffer, mode="rb") as source:
        for event, element in ET.iterparse(source, events=("start", "end")):
            if event == "start":
                if not root_seen:
                    root_seen = True
                    output.write(b"<?xml version='1.0' encoding='UTF-8'?>\n")
                    output.write(xml_start(element.tag, element.attrib))
                    for group in ("create", "modify", "delete"):
                        output.write(f"<{group}>\n".encode("ascii"))
                    groups_open = True
                elif element.tag in {"create", "modify", "delete"}:
                    action = element.tag
                elif element.tag in {"node", "way", "relation"}:
                    object_element = element
                continue

            if element.tag in {"node", "way", "relation"} and object_element is element:
                key = osm_key(element)
                is_match = matches(element, args.prefix)
                known = key in membership
                selected_action = action

                if selected_action == "delete":
                    if known:
                        output.write(ET.tostring(element, encoding="utf-8"))
                        output.write(b"\n")
                        record({"id": key, "op": "remove"})
                elif is_match:
                    output.write(ET.tostring(element, encoding="utf-8"))
                    output.write(b"\n")
                    record({"id": key, "op": "add"})
                elif known:
                    output.write(ET.tostring(element, encoding="utf-8"))
                    output.write(b"\n")
                    record({"id": key, "op": "remove"})

                element.clear()
                object_element = None
            elif element.tag in {"create", "modify", "delete"}:
                action = None

    delta_handle.close()

    if groups_open:
        output.write(b"</create>\n</modify>\n</delete>\n</osmChange>\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
