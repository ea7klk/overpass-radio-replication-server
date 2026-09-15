#!/usr/bin/env python3
"""Filter an OsmChange XML stream and retain complete references.

Matching objects are roots. Their referenced nodes, ways, and relations are
promoted to dependencies so Overpass can resolve geometry and members. The
membership state is written to a delta file and committed only after the
filtered change has been accepted by Overpass.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import tempfile
from lxml import etree as ET
from pathlib import Path
from xml.sax.saxutils import quoteattr


OBJECT_TYPES = {"node", "way", "relation"}
GROUPS = ("create", "modify", "delete")


def empty_state() -> dict[str, object]:
    return {"roots": [], "dependencies": [], "refs": {}}


def load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return empty_state()
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    # Accept the old project format, which was just a list of matching IDs.
    if isinstance(data, list):
        return {"roots": sorted(set(data)), "dependencies": [], "refs": {}}
    return {
        "roots": sorted(set(data.get("roots", []))),
        "dependencies": sorted(set(data.get("dependencies", []))),
        "refs": {
            str(key): sorted(set(value))
            for key, value in data.get("refs", {}).items()
        },
    }


def osm_key(element: ET.Element) -> str:
    return f"{element.tag}:{element.attrib['id']}"


def matches(element: ET.Element, prefix: str) -> bool:
    return any(
        child.tag == "tag"
        and child.attrib.get("k", "").startswith(prefix)
        for child in element
    )


def references(element: ET.Element) -> list[str]:
    if element.tag == "way":
        return [f"node:{child.attrib['ref']}" for child in element if child.tag == "nd"]
    if element.tag == "relation":
        return [
            f"{child.attrib['type']}:{child.attrib['ref']}"
            for child in element
            if child.tag == "member" and child.attrib.get("type") in OBJECT_TYPES
        ]
    return []


def xml_start(tag: str, attributes: dict[str, str]) -> bytes:
    attrs = "".join(f" {key}={quoteattr(value)}" for key, value in attributes.items())
    return f"<{tag}{attrs}>\n".encode("utf-8")


def deletion_element(element: ET.Element) -> ET.Element:
    """Create a valid minimal delete element for a tag-loss removal."""
    attrs = {key: value for key, value in element.attrib.items() if key != "visible"}
    attrs["visible"] = "false"
    return ET.Element(element.tag, attrs)


def state_sets(state: dict[str, object]) -> tuple[set[str], set[str], dict[str, set[str]]]:
    roots = set(state["roots"])
    dependencies = set(state["dependencies"])
    refs = {key: set(value) for key, value in state["refs"].items()}
    return roots, dependencies, refs


def serialized_state(
    roots: set[str], dependencies: set[str], refs: dict[str, set[str]]
) -> dict[str, object]:
    retained = roots | dependencies
    return {
        "roots": sorted(roots),
        "dependencies": sorted(dependencies),
        "refs": {
            key: sorted(refs.get(key, set()))
            for key in sorted(retained)
            if refs.get(key)
        },
    }


def promote(key: str, dependencies: set[str], refs: dict[str, set[str]]) -> None:
    """Promote a known object's complete, already-known reference subtree."""
    pending = [key]
    seen: set[str] = set()
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        for child in refs.get(current, set()):
            if child not in dependencies:
                dependencies.add(child)
            pending.append(child)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--membership", required=True, type=Path)
    parser.add_argument("--delta", required=True, type=Path)
    parser.add_argument("--prefix", default="communication:amateur_radio")
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="dependency ID to seed for a replay pass, such as node:123",
    )
    args = parser.parse_args()

    roots, dependencies, refs = state_sets(load_state(args.membership))
    dependencies.update(args.include)
    args.delta.parent.mkdir(parents=True, exist_ok=True)

    events: list[dict[str, object]] = []
    output_files = {group: tempfile.TemporaryFile(mode="w+b") for group in GROUPS}
    root_tag: str | None = None
    root_attributes: dict[str, str] = {}

    def emit(group: str, element: ET.Element) -> None:
        output_files[group].write(ET.tostring(element, encoding="utf-8"))
        output_files[group].write(b"\n")

    def event(op: str, key: str, root: bool = False) -> None:
        events.append({"op": op, "id": key, "root": root})

    # lxml parses the XML in optimized C code. Restricting events to the
    # elements that matter avoids a Python callback for the document root,
    # action wrappers, and every <tag>, <nd>, and <member> child in a
    # planet-scale change stream. The three short-lived group files let us
    # route a tag-loss removal into <delete> while the upstream stream remains
    # create/modify/delete ordered.
    try:
        with gzip.GzipFile(fileobj=sys.stdin.buffer, mode="rb") as source:
            for _, element in ET.iterparse(
                source,
                events=("end",),
                tag=tuple(OBJECT_TYPES),
                huge_tree=True,
                resolve_entities=False,
                load_dtd=False,
                no_network=True,
            ):
                parent = element.getparent()
                if root_tag is None and parent is not None and parent.getparent() is not None:
                    root = parent.getparent()
                    root_tag = root.tag
                    root_attributes = dict(root.attrib)
                action = parent.tag if parent is not None and parent.tag in GROUPS else "modify"
                key = osm_key(element)
                is_root = matches(element, args.prefix)
                known_root = key in roots
                known_dependency = key in dependencies
                object_refs = set(references(element))

                if action == "delete":
                    if known_root or known_dependency:
                        emit("delete", element)
                        event("remove", key, known_root)
                    roots.discard(key)
                    dependencies.discard(key)
                    refs.pop(key, None)
                elif is_root:
                    emit(action, element)
                    event("add", key, True)
                    roots.add(key)
                    refs[key] = object_refs
                    promote(key, dependencies, refs)
                elif known_root or known_dependency:
                    # A matching tag was removed from a root. Keep the
                    # object if another root uses it as a dependency.
                    roots.discard(key)
                    if known_dependency:
                        emit(action, element)
                        event("add", key, False)
                        refs[key] = object_refs
                        promote(key, dependencies, refs)
                    else:
                        refs.pop(key, None)
                        emit("delete", deletion_element(element))
                        event("remove", key, True)

                element.clear()
                # Remove already processed siblings from the lxml tree.
                # Without this, the root group retains every object until
                # the whole file has been parsed even though iterparse is
                # otherwise incremental.
                while parent is not None and element.getprevious() is not None:
                    del parent[0]

        # Persist the new membership graph only after the caller has applied
        # all emitted OSM changes successfully.
        with args.delta.open("w", encoding="utf-8") as delta_handle:
            for item in events:
                delta_handle.write(json.dumps(item, separators=(",", ":")) + "\n")
            if events:
                delta_handle.write(
                    json.dumps(
                        {"op": "state", "state": serialized_state(roots, dependencies, refs)},
                        separators=(",", ":"),
                    )
                    + "\n"
                )

        output = sys.stdout.buffer
        output.write(b"<?xml version='1.0' encoding='UTF-8'?>\n")
        output.write(xml_start(root_tag or "osmChange", root_attributes))
        for group in GROUPS:
            output.write(f"<{group}>\n".encode("ascii"))
            output_files[group].seek(0)
            while chunk := output_files[group].read(1024 * 1024):
                output.write(chunk)
            output.write(f"</{group}>\n".encode("ascii"))
        output.write(b"</osmChange>\n")
    finally:
        for handle in output_files.values():
            handle.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
