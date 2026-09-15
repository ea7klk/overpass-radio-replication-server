#!/usr/bin/env python3
"""Resumable daily-to-minute filtered Overpass replication."""

from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
import re
import sys
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from lxml import etree as ET
from pathlib import Path
from typing import Any
from html.parser import HTMLParser


LOG = logging.getLogger("radio-overpass")
PREFETCH_WINDOW = 10
PREFETCH_WORKERS = 2
ANSI_RED = "\033[91m"
ANSI_LIGHT_GREEN = "\033[92m"
ANSI_LIGHT_BLUE = "\033[94m"
ANSI_RESET = "\033[0m"
OBJECT_TYPES = {"node", "way", "relation"}
ROOT_KEY = re.compile(r"^(node|way|relation):([1-9][0-9]*)$")


@dataclass
class DownloadedChange:
    path: Path
    state: dict[str, Any]
    seconds: float


@dataclass
class RemoteRefresh:
    path: Path
    objects: dict[str, bytes]
    refs: dict[str, set[str]]
    names: dict[str, str]
    tags: dict[str, list[dict[str, str]]]
    seconds: float


def red_console(text: str, *, is_tty: bool | None = None) -> str:
    """Color selected interactive log messages red without adding a dependency."""
    if os.environ.get("NO_COLOR") is not None:
        return text
    if is_tty is None:
        is_tty = sys.stderr.isatty()
    if not is_tty and os.environ.get("FORCE_COLOR") != "1":
        return text
    return f"{ANSI_RED}{text}{ANSI_RESET}"


def light_green_console(text: str, *, is_tty: bool | None = None) -> str:
    if os.environ.get("NO_COLOR") is not None:
        return text
    if is_tty is None:
        is_tty = sys.stderr.isatty()
    if not is_tty and os.environ.get("FORCE_COLOR") != "1":
        return text
    return f"{ANSI_LIGHT_GREEN}{text}{ANSI_RESET}"


def light_blue_console(text: str, *, is_tty: bool | None = None) -> str:
    if os.environ.get("NO_COLOR") is not None:
        return text
    if is_tty is None:
        is_tty = sys.stderr.isatty()
    if not is_tty and os.environ.get("FORCE_COLOR") != "1":
        return text
    return f"{ANSI_LIGHT_BLUE}{text}{ANSI_RESET}"


def object_log_message(
    verb: str,
    kind: str,
    object_id: str,
    cadence: str,
    sequence: int,
    name: object = None,
    tags: object = None,
) -> str:
    message = f"{verb} {kind} {object_id} at replication {cadence}/{sequence}"
    if isinstance(name, str) and name:
        message += f" name={name!r}"
    if isinstance(tags, list):
        rendered_tags = []
        for tag in tags:
            if isinstance(tag, dict) and isinstance(tag.get("key"), str):
                rendered_tags.append(f"{tag['key']}={tag.get('value', '')!r}")
        if rendered_tags:
            message += " tags=" + ", ".join(rendered_tags)
    return message


def catalog_path(config: dict[str, Any]) -> Path:
    configured = config.get("catalog_file")
    if configured:
        return Path(configured)
    return Path(config["membership_file"]).with_name("catalog.json")


def root_key_parts(key: str) -> tuple[str, str]:
    match = ROOT_KEY.fullmatch(key)
    if match is None:
        raise ValueError(f"invalid OSM object key: {key}")
    return match.group(1), match.group(2)


def element_key(element: ET._Element) -> str:
    return f"{element.tag}:{element.get('id')}"


def element_references(element: ET._Element) -> list[str]:
    if element.tag == "way":
        return [f"node:{child.get('ref')}" for child in element if child.tag == "nd"]
    if element.tag == "relation":
        return [
            f"{child.get('type')}:{child.get('ref')}"
            for child in element
            if child.tag == "member" and child.get("type") in OBJECT_TYPES
        ]
    return []


def overpass_query(root_keys: set[str], timeout: int) -> bytes:
    grouped: dict[str, list[str]] = {kind: [] for kind in OBJECT_TYPES}
    for key in sorted(root_keys):
        kind, object_id = root_key_parts(key)
        grouped[kind].append(object_id)
    selectors = "\n".join(
        f"  {kind}(id:{','.join(ids)});"
        for kind, ids in grouped.items()
        if ids
    )
    query = f"""[out:xml][timeout:{timeout}];
(
{selectors}
);
(._;>;);
(._;<<;);
out body;
"""
    return query.encode("utf-8")


def element_name(element: ET._Element) -> str | None:
    for child in element:
        if child.tag == "tag" and child.get("k") == "name":
            return child.get("v") or None
    return None


def element_matching_tags(element: ET._Element, prefix: str) -> list[dict[str, str]]:
    return [
        {"key": child.get("k", ""), "value": child.get("v", "")}
        for child in element
        if child.tag == "tag" and child.get("k", "").startswith(prefix)
    ]


def remote_osc(
    output_path: Path,
    objects: dict[str, bytes],
    known_keys: set[str],
) -> None:
    groups: dict[str, list[bytes]] = {"create": [], "modify": []}
    for key in sorted(objects):
        groups["modify" if key in known_keys else "create"].append(objects[key])
    with output_path.open("wb") as output:
        output.write(b"<?xml version='1.0' encoding='UTF-8'?>\n")
        output.write(b'<osmChange version="0.6" generator="radio-overpass">\n')
        for group in ("create", "modify"):
            output.write(f"<{group}>\n".encode("ascii"))
            for object_xml in groups[group]:
                output.write(object_xml)
                output.write(b"\n")
            output.write(f"</{group}>\n".encode("ascii"))
        output.write(b"<delete>\n</delete>\n</osmChange>\n")


def query_overpass(
    config: dict[str, Any],
    root_keys: set[str],
    known_keys: set[str],
    output_path: Path,
) -> RemoteRefresh:
    if not root_keys:
        raise ValueError("at least one root is required for an Overpass query")
    endpoint = config.get(
        "overpass_query_url",
        "https://overpass.private.coffee/api/interpreter",
    )
    timeout = int(config.get("overpass_query_timeout", 180))
    retries = int(config.get("overpass_query_retries", 3))
    retry_wait = int(config["retry_initial_seconds"])
    max_wait = int(config["retry_max_seconds"])
    query = overpass_query(root_keys, timeout)
    last_error: Exception | None = None

    for attempt in range(retries + 1):
        started = time.monotonic()
        objects: dict[str, bytes] = {}
        refs: dict[str, set[str]] = {}
        names: dict[str, str] = {}
        tags: dict[str, list[dict[str, str]]] = {}
        remarks: list[str] = []
        try:
            request = urllib.request.Request(
                endpoint,
                data=query,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
                    "User-Agent": "overpass-radio-replication-server/1.0",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=timeout + 30) as response:
                status = getattr(response, "status", None)
                if status is None:
                    status = response.getcode()
                if status < 200 or status >= 300:
                    raise RuntimeError(f"Overpass HTTP status {status}")
                for _, element in ET.iterparse(
                    response,
                    events=("end",),
                    tag=("node", "way", "relation", "remark", "error"),
                    huge_tree=True,
                    resolve_entities=False,
                    load_dtd=False,
                    no_network=True,
                ):
                    if element.tag in {"remark", "error"}:
                        text = " ".join("".join(element.itertext()).split())
                        if text:
                            remarks.append(text)
                    elif element.tag in OBJECT_TYPES:
                        key = element_key(element)
                        objects[key] = ET.tostring(element, encoding="utf-8")
                        refs[key] = set(element_references(element))
                        name = element_name(element)
                        if name:
                            names[key] = name
                        matching = element_matching_tags(
                            element, config["tag_key_prefix"]
                        )
                        if matching:
                            tags[key] = matching
                    element.clear()
        except (OSError, urllib.error.HTTPError, ET.XMLSyntaxError, RuntimeError) as exc:
            last_error = exc
        else:
            missing = sorted(root_keys - objects.keys())
            if remarks:
                last_error = RuntimeError("; ".join(remarks))
            elif missing:
                last_error = RuntimeError(
                    "Overpass response omitted requested root(s): " + ", ".join(missing)
                )
            else:
                remote_osc(output_path, objects, known_keys)
                seconds = time.monotonic() - started
                LOG.info(
                    "%s",
                    light_green_console(
                        f"Overpass query succeeded: {endpoint}; retrieved "
                        f"{len(objects)} object(s), including dependencies and dependents, "
                        f"for {len(root_keys)} root(s) in {seconds:.1f}s"
                    ),
                )
                return RemoteRefresh(output_path, objects, refs, names, tags, seconds)

        if attempt < retries:
            LOG.warning(
                "%s",
                red_console(
                    f"Overpass query failed for {len(root_keys)} root(s): "
                    f"{last_error}; retrying in {retry_wait}s"
                ),
            )
            time.sleep(retry_wait)
            retry_wait = min(max_wait, max(retry_wait * 2, 1))

    message = f"Overpass query unsuccessful after {retries + 1} attempt(s): {last_error}"
    LOG.error("%s", red_console(message))
    raise RuntimeError(message)


def merge_remote_state(
    state: dict[str, Any],
    root_keys: set[str],
    refresh: RemoteRefresh,
) -> dict[str, Any]:
    roots = set(state.get("roots", [])) | root_keys
    dependencies = set(state.get("dependencies", [])) | (set(refresh.objects) - root_keys)
    refs = {key: set(value) for key, value in state.get("refs", {}).items()}
    refs.update(refresh.refs)
    return {
        "roots": sorted(roots),
        "dependencies": sorted(dependencies - roots),
        "refs": {key: sorted(value) for key, value in sorted(refs.items()) if key in roots | dependencies},
    }


def pending_entities(path: Path) -> list[str]:
    value = load_json(path, None)
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, dict):
        # Dependencies are derived from roots. Legacy membership files can be
        # replayed by processing their roots without querying every geometry
        # member independently.
        return [str(item) for item in value.get("roots", [])]
    return []


def enqueue_pending(path: Path, key: str) -> None:
    pending = pending_entities(path)
    if key not in pending:
        pending.append(key)
        atomic_json(path, pending)


def acknowledge_pending(path: Path, key: str) -> None:
    remaining = [item for item in pending_entities(path) if item != key]
    if remaining:
        atomic_json(path, remaining)
    else:
        path.unlink(missing_ok=True)


def log_remote_objects(
    refresh: RemoteRefresh,
    root_keys: set[str],
    context: str,
) -> None:
    for key in sorted(refresh.objects):
        kind, _ = root_key_parts(key)
        role = "root" if key in root_keys else "dependency/dependent"
        message = f"retrieved {role} {kind} {key} from public Overpass ({context})"
        name = refresh.names.get(key)
        if name:
            message += f" name={name!r}"
        matching = refresh.tags.get(key)
        if matching:
            message += " tags=" + ", ".join(
                f"{item['key']}={item['value']!r}" for item in matching
            )
        LOG.info("%s", light_green_console(message))


def refresh_roots(
    config: dict[str, Any],
    root_keys: set[str],
    state: dict[str, Any],
    output_path: Path,
    timestamp: str,
    context: str,
) -> tuple[RemoteRefresh, dict[str, Any]]:
    known_keys = set(state.get("roots", [])) | set(state.get("dependencies", []))
    refresh = query_overpass(config, root_keys, known_keys, output_path)
    update_database(
        config,
        refresh.path,
        timestamp,
        description=f"public Overpass dependencies/dependents ({context})",
    )
    merged = merge_remote_state(state, root_keys, refresh)
    atomic_json(catalog_path(config), merged)
    log_remote_objects(refresh, root_keys, context)
    return refresh, merged


def process_pending_membership(config: dict[str, Any]) -> None:
    pending_path = Path(config["membership_file"])
    if not pending_path.exists():
        return
    pending_value = load_json(pending_path, None)
    if not isinstance(pending_value, (list, dict)):
        raise ValueError(f"membership file must contain a JSON list or object: {pending_path}")
    pending = pending_entities(pending_path)
    if not pending:
        pending_path.unlink(missing_ok=True)
        return

    catalog = load_json(catalog_path(config), {})
    if not isinstance(catalog, dict):
        catalog = {}
    if isinstance(pending_value, dict) and not catalog:
        # Migrate the previous persistent membership format before replacing
        # membership.json with the startup queue semantics.
        catalog = pending_value
        atomic_json(catalog_path(config), catalog)
    work_dir = Path(config["work_dir"])
    work_dir.mkdir(parents=True, exist_ok=True)
    batch_size = max(1, int(config.get("overpass_query_batch_size", 50)))
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with tempfile.TemporaryDirectory(prefix="membership-", dir=work_dir) as directory:
        temp_dir = Path(directory)
        while pending:
            batch = set(pending[:batch_size])
            refresh, catalog = refresh_roots(
                config,
                batch,
                catalog,
                temp_dir / "membership.osc",
                timestamp,
                "startup membership",
            )
            del refresh
            pending = pending[batch_size:]
            if pending:
                atomic_json(pending_path, pending)
            else:
                pending_path.unlink(missing_ok=True)


def sequence_path(sequence: int) -> str:
    padded = f"{sequence:09d}"
    return "/".join((padded[:3], padded[3:6], padded[6:9]))


def urls(base: str, sequence: int) -> tuple[str, str]:
    stem = sequence_path(sequence)
    base = base.rstrip("/")
    return f"{base}/{stem}.state.txt", f"{base}/{stem}.osc.gz"


def parse_state(payload: str) -> dict[str, Any]:
    values: dict[str, str] = {}
    for line in payload.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().replace("\\:", ":")
    return {
        "sequence": int(values["sequenceNumber"]),
        "timestamp": values["timestamp"],
    }


def fetch(url: str, retries: int, max_wait: int) -> bytes:
    wait = retries
    while True:
        try:
            LOG.debug("fetch %s", url)
            with urllib.request.urlopen(url, timeout=120) as response:
                return response.read()
        except (OSError, urllib.error.HTTPError) as exc:
            LOG.warning("download failed for %s: %s; retrying in %ss", url, exc, wait)
            time.sleep(wait)
            wait = min(max_wait, max(wait * 2, 1))


class DirectoryIndex(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        for key, value in attrs:
            if key == "href" and value:
                self.links.append(value)


def indexed_sequences(base: str, retry: int, max_wait: int) -> list[int]:
    """Discover sequence numbers from the public 3/3/3 directory index."""
    base = base.rstrip("/") + "/"
    discovered: set[int] = set()

    def walk(url: str, depth: int) -> None:
        if depth == 3:
            return
        parser = DirectoryIndex()
        parser.feed(fetch(url, retry, max_wait).decode("utf-8", "replace"))
        for link in parser.links:
            if link.startswith("../") or link.startswith("/"):
                continue
            name = link.rstrip("/")
            if depth < 2 and name.isdigit() and len(name) == 3:
                walk(url + name + "/", depth + 1)
            elif depth == 2 and name.endswith(".state.txt"):
                stem = name[:-10]
                if stem.isdigit() and len(stem) == 3:
                    prefix = url.removeprefix(base).split("/")
                    if len(prefix) >= 2 and all(part.isdigit() for part in prefix[:2]):
                        discovered.add(int("".join(prefix[:2]) + stem))

    walk(base, 0)
    return sorted(discovered)


def fetch_state(base: str, sequence: int, retry: int, max_wait: int) -> dict[str, Any]:
    state_url, _ = urls(base, sequence)
    return parse_state(fetch(state_url, retry, max_wait).decode("utf-8"))


def latest(base: str, retry: int, max_wait: int) -> dict[str, Any]:
    return parse_state(fetch(f"{base.rstrip('/')}/state.txt", retry, max_wait).decode("utf-8"))


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".new")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def update_database(
    config: dict[str, Any],
    osc_path: Path,
    timestamp: str,
    description: str = "filtered change",
) -> None:
    command = [
        config["overpass_update_from_dir"],
        f"--db-dir={config['db_dir']}",
        f"--osc-dir={osc_path.parent}",
        f"--version={timestamp}",
    ]
    if config.get("meta_mode"):
        command.append(config["meta_mode"])
    LOG.info("%s", light_blue_console(f"writing {description} to Overpass DB at {timestamp}"))
    subprocess.run(command, check=True)


def commit_delta(membership_path: Path, delta_path: Path) -> None:
    final_state: dict[str, Any] | None = None
    with delta_path.open(encoding="utf-8") as handle:
        for line in handle:
            item = json.loads(line)
            if item["op"] == "state":
                final_state = item["state"]
    if final_state is not None:
        atomic_json(membership_path, final_state)


def download_change(
    config: dict[str, Any],
    cadence: str,
    sequence: int,
    destination: Path,
) -> DownloadedChange:
    base_key = "daily" if cadence == "day" else cadence
    base = config[f"{base_key}_base_url"]
    state = fetch_state(
        base,
        sequence,
        int(config["retry_initial_seconds"]),
        int(config["retry_max_seconds"]),
    )
    _, change_url = urls(base, sequence)
    partial = destination.with_suffix(destination.suffix + ".part")
    wait = int(config["retry_initial_seconds"])
    max_wait = int(config["retry_max_seconds"])
    while True:
        started = time.monotonic()
        try:
            partial.unlink(missing_ok=True)
            subprocess.run(
                [
                    "curl",
                    "--fail",
                    "--location",
                    "--silent",
                    "--show-error",
                    "--retry",
                    "3",
                    "--retry-delay",
                    "5",
                    "--connect-timeout",
                    "120",
                    "--output",
                    str(partial),
                    change_url,
                ],
                check=True,
            )
            subprocess.run(["gzip", "-t", str(partial)], check=True)
            os.replace(partial, destination)
            seconds = time.monotonic() - started
            LOG.info(
                "downloaded %s replication file %s in %.1fs",
                cadence,
                change_url,
                seconds,
            )
            return DownloadedChange(destination, state, seconds)
        except (OSError, urllib.error.HTTPError, subprocess.CalledProcessError) as exc:
            LOG.warning("download failed for %s: %s; retrying in %ss", change_url, exc, wait)
            partial.unlink(missing_ok=True)
            destination.unlink(missing_ok=True)
            time.sleep(wait)
            wait = min(max_wait, max(wait * 2, 1))


def gzip_contains(
    source_path: Path,
    needle: str | None = None,
    pattern_path: Path | None = None,
) -> bool:
    """Search a gzip file without constructing an XML tree.

    grep and gzip do the scan in native code and stop at the first match. A
    non-match still reads the file once, but avoids the much more expensive XML
    parser and Python object handling.
    """
    if (needle is None) == (pattern_path is None):
        raise ValueError("provide exactly one quick-check pattern")
    matcher_command = ["grep", "-a", "-m", "1", "-F"]
    if pattern_path is not None:
        matcher_command.extend(("-f", str(pattern_path)))
    else:
        matcher_command.append(needle or "")
    matcher_command.append("-")

    decompressor = subprocess.Popen(
        ["gzip", "--decompress", "--stdout", str(source_path)],
        stdout=subprocess.PIPE,
    )
    assert decompressor.stdout is not None
    matcher = subprocess.Popen(
        matcher_command,
        stdin=decompressor.stdout,
        stdout=subprocess.DEVNULL,
    )
    decompressor.stdout.close()
    matcher_status = matcher.wait()
    gzip_status = decompressor.wait()

    if matcher_status not in (0, 1):
        raise subprocess.CalledProcessError(matcher_status, matcher_command)
    # gzip is expected to receive SIGPIPE when grep finds a match early.
    if matcher_status == 1 and gzip_status != 0:
        raise subprocess.CalledProcessError(
            gzip_status, ["gzip", "--decompress", "--stdout", str(source_path)]
        )
    return matcher_status == 0


def write_id_patterns(pattern_path: Path, object_keys: set[str]) -> bool:
    object_ids = sorted({key.split(":", 1)[1] for key in object_keys if ":" in key})
    if not object_ids:
        return False
    pattern_path.write_text(
        "".join(f'id="{object_id}"\n' for object_id in object_ids),
        encoding="ascii",
    )
    return True


def quick_check(
    source_path: Path,
    prefix: str,
    retained: set[str],
    pattern_path: Path,
) -> str | None:
    """Return why a source needs parsing, or None if it can be discarded."""
    if gzip_contains(source_path, needle=prefix):
        return "tag prefix"
    if not write_id_patterns(pattern_path, retained):
        return None
    if gzip_contains(source_path, pattern_path=pattern_path):
        return "retained object"
    return None


def filter_file(
    config: dict[str, Any],
    source_path: Path,
    membership: Path,
    filtered_path: Path,
    delta_path: Path,
    include: set[str],
) -> float:
    command = [
        sys.executable,
        "-m",
        "radio_overpass.osc_filter",
        "--membership",
        str(membership),
        "--delta",
        str(delta_path),
        "--prefix",
        config["tag_key_prefix"],
    ]
    for dependency_id in sorted(include):
        command.extend(("--include", dependency_id))

    environment = os.environ.copy()
    package_root = str(Path(__file__).parent.parent)
    environment["PYTHONPATH"] = package_root + os.pathsep + environment.get("PYTHONPATH", "")
    started = time.monotonic()
    with source_path.open("rb") as raw:
        with filtered_path.open("wb") as filtered:
            subprocess.run(command, stdin=raw, stdout=filtered, check=True, env=environment)
    return time.monotonic() - started


def delta_items(delta_path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in delta_path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def delta_state(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    for item in reversed(items):
        if item["op"] == "state":
            return item["state"]
    return None


def process_one(
    config: dict[str, Any],
    cadence: str,
    downloaded: DownloadedChange,
    *,
    apply_database: bool = True,
) -> dict[str, Any]:
    base_key = "daily" if cadence == "day" else cadence
    base = config[f"{base_key}_base_url"]
    state = downloaded.state
    sequence = int(state["sequence"])
    _, change_url = urls(base, sequence)
    LOG.info("processing %s replication file %s", cadence, change_url)

    work_dir = Path(config["work_dir"])
    work_dir.mkdir(parents=True, exist_ok=True)
    catalog = catalog_path(config)
    old_state = load_json(catalog, {})
    if isinstance(old_state, dict):
        retained = set(old_state.get("roots", [])) | set(old_state.get("dependencies", []))
        old_dependencies = set(old_state.get("dependencies", []))
    else:
        retained = set()
        old_dependencies = set()

    try:
        with tempfile.TemporaryDirectory(prefix=f"{cadence}-{sequence}-", dir=work_dir) as temp:
            temp_path = Path(temp)
            filtered_path = temp_path / "filtered-0.osc"
            delta_path = temp_path / "delta-0.jsonl"
            file_started = time.monotonic()
            quick_check_seconds = 0.0
            quick_check_started = time.monotonic()
            first_check = quick_check(
                downloaded.path,
                config["tag_key_prefix"],
                retained,
                temp_path / "quick-check.ids",
            )
            first_quick_check_seconds = time.monotonic() - quick_check_started
            quick_check_seconds += first_quick_check_seconds
            LOG.info(
                "quick-check %s replication file %s pass=1 result=%s in %.1fs",
                cadence,
                change_url,
                first_check or "discarded",
                first_quick_check_seconds,
            )
            if first_check is None:
                LOG.info(
                    "discarded %s replication file %s after quick-check",
                    cadence,
                    change_url,
                )
                return state
            include: set[str] = set()

            # A root can reference an object that appears earlier in the same
            # OSC file (for example, a way's nodes). Re-read the local file when
            # a pass discovers new dependencies, seeding the next pass with
            # those IDs. The source file is downloaded only once.
            pass_number = 0
            filter_seconds = 0.0
            events: list[dict[str, Any]] = []
            while True:
                if pass_number:
                    next_filtered_path = temp_path / f"filtered-{pass_number}.osc"
                    next_delta_path = temp_path / f"delta-{pass_number}.jsonl"
                    quick_check_path = temp_path / "quick-check.ids"
                    quick_check_started = time.monotonic()
                    dependent_present = write_id_patterns(quick_check_path, include) and gzip_contains(
                        downloaded.path, pattern_path=quick_check_path
                    )
                    dependency_quick_check_seconds = time.monotonic() - quick_check_started
                    quick_check_seconds += dependency_quick_check_seconds
                    LOG.info(
                        "quick-check %s replication file %s pass=%d result=%s in %.1fs",
                        cadence,
                        change_url,
                        pass_number + 1,
                        "dependent object" if dependent_present else "discarded",
                        dependency_quick_check_seconds,
                    )
                    if not dependent_present:
                        LOG.info(
                            "skipping filter replay pass %d for %s: no newly "
                            "discovered dependent object is present",
                            pass_number + 1,
                            change_url,
                        )
                        break
                    # Do not replace the paths from the last completed pass
                    # until this replay is known to be needed. A skipped
                    # replay has no delta or filtered OSC file to commit/apply.
                    filtered_path = next_filtered_path
                    delta_path = next_delta_path
                filter_seconds += filter_file(
                    config, downloaded.path, catalog, filtered_path, delta_path, include
                )
                events = delta_items(delta_path)
                candidate_state = delta_state(events)
                if candidate_state is None:
                    break
                new_dependencies = set(candidate_state.get("dependencies", [])) - (old_dependencies | include)
                if not new_dependencies:
                    break
                include.update(new_dependencies)
                pass_number += 1

            root_keys = {
                str(item["id"])
                for item in events
                if item["op"] == "add" and item.get("root")
            }
            remote_refresh: RemoteRefresh | None = None
            if root_keys and apply_database:
                pending_path = Path(config["membership_file"])
                for root_key in root_keys:
                    enqueue_pending(pending_path, root_key)
                remote_dir = temp_path / "remote"
                remote_dir.mkdir()
                remote_refresh, _ = refresh_roots(
                    config,
                    root_keys,
                    old_state,
                    remote_dir / "remote.osc",
                    state["timestamp"],
                    f"{cadence}/{sequence}",
                )
                for root_key in root_keys:
                    acknowledge_pending(pending_path, root_key)

            has_database_changes = any(item["op"] in {"add", "remove"} for item in events)
            apply_started = time.monotonic()
            if remote_refresh is None and has_database_changes and apply_database:
                update_database(
                    config,
                    filtered_path,
                    state["timestamp"],
                    description=f"filtered replication change ({cadence}/{sequence})",
                )
            apply_seconds = time.monotonic() - apply_started
            if events and remote_refresh is None:
                commit_delta(catalog, delta_path)
            else:
                if not events:
                    LOG.info("sequence %s/%s contained no matching objects", cadence, sequence)

            for item in events:
                if item["op"] == "add":
                    if remote_refresh is not None and item["id"] in remote_refresh.objects:
                        continue
                    kind = "root" if item.get("root") else "dependency"
                    verb = "applied" if apply_database else "discovered"
                    message = object_log_message(
                        verb,
                        kind,
                        str(item["id"]),
                        cadence,
                        sequence,
                        item.get("name"),
                        item.get("tags"),
                    )
                    if not apply_database and item.get("root") and item["id"].startswith("node:"):
                        message = red_console(message)
                    LOG.info("%s", message)
                elif item["op"] == "remove":
                    kind = "root" if item.get("root") else "dependency"
                    verb = "removed" if apply_database else "discovered removal of"
                    LOG.info("%s %s at replication %s/%s", verb, kind, cadence, sequence)

            process_seconds = time.monotonic() - file_started
            LOG.info(
                "completed %s replication file %s: passes=%d download=%.1fs "
                "quick-check=%.1fs filter=%.1fs apply=%.1fs process=%.1fs",
                cadence,
                change_url,
                pass_number + 1,
                downloaded.seconds,
                quick_check_seconds,
                filter_seconds,
                apply_seconds,
                process_seconds,
            )
    finally:
        downloaded.path.unlink(missing_ok=True)

    return state


class Prefetcher:
    def __init__(self, config: dict[str, Any], cadence: str, directory: Path) -> None:
        self.config = config
        self.cadence = cadence
        self.directory = directory
        self.executor = ThreadPoolExecutor(max_workers=PREFETCH_WORKERS)
        self.futures: dict[int, Future[DownloadedChange]] = {}

    def fill(self, first_sequence: int, target_sequence: int) -> None:
        # Keep the current file plus the next ten files queued. Two workers
        # download concurrently; the larger queue keeps the network busy while
        # the current file is filtered and applied without overloading storage.
        last_sequence = min(target_sequence, first_sequence + PREFETCH_WINDOW - 1)
        for sequence in range(first_sequence, last_sequence + 1):
            if sequence in self.futures:
                continue
            destination = self.directory / f"download-{self.cadence}-{sequence}.osc.gz"
            LOG.info(
                "queueing %s replication file %s for download (prefetch window)",
                self.cadence,
                sequence,
            )
            self.futures[sequence] = self.executor.submit(
                download_change,
                self.config,
                self.cadence,
                sequence,
                destination,
            )

    def take(self, sequence: int) -> DownloadedChange:
        return self.futures.pop(sequence).result()

    def close(self) -> None:
        self.executor.shutdown(wait=True)


def resolve_minute_start(config: dict[str, Any], timestamp: str) -> int:
    """Find the first minute sequence after a daily checkpoint timestamp.

    This deliberately uses a linear probe from the beginning only when no
    persisted minute sequence exists. Operators can set minute_start_sequence
    after the first migration to avoid a long search.
    """
    explicit = config.get("minute_start_sequence")
    if explicit is not None:
        return int(explicit)

    upper = latest(
        config["minute_base_url"],
        int(config["retry_initial_seconds"]),
        int(config["retry_max_seconds"]),
    )["sequence"]

    # Do not enumerate the minute index: it has millions of leaves. Sequence
    # URLs are directly addressable, so binary-search their state files.
    lo, hi = 1, upper + 1
    while lo < hi:
        mid = (lo + hi) // 2
        candidate = fetch_state(
            config["minute_base_url"],
            mid,
            int(config["retry_initial_seconds"]),
            int(config["retry_max_seconds"]),
        )
        if candidate["timestamp"] <= timestamp:
            lo = mid + 1
        else:
            hi = mid
    return lo


def catch_up(
    config: dict[str, Any],
    checkpoint: dict[str, Any],
    cadence: str,
    *,
    apply_database: bool = True,
) -> dict[str, Any]:
    base_key = "daily" if cadence == "day" else cadence
    base = config[f"{base_key}_base_url"]
    work_dir = Path(config["work_dir"])
    work_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f"prefetch-{cadence}-", dir=work_dir) as directory:
        prefetcher = Prefetcher(config, cadence, Path(directory))
        try:
            while checkpoint["cadence"] == cadence:
                target = latest(
                    base,
                    int(config["retry_initial_seconds"]),
                    int(config["retry_max_seconds"]),
                )
                next_sequence = checkpoint["sequence"] + 1
                if next_sequence > target["sequence"]:
                    break
                prefetcher.fill(next_sequence, target["sequence"])
                downloaded = prefetcher.take(next_sequence)
                result = process_one(
                    config,
                    cadence,
                    downloaded,
                    apply_database=apply_database,
                )
                checkpoint = {
                    "phase": checkpoint.get("phase", "apply"),
                    "cadence": cadence,
                    "sequence": result["sequence"],
                    "timestamp": result["timestamp"],
                }
                atomic_json(Path(config["state_file"]), checkpoint)
        finally:
            prefetcher.close()
    return checkpoint


def run(config: dict[str, Any]) -> None:
    process_pending_membership(config)
    state_path = Path(config["state_file"])
    checkpoint = load_json(state_path, None)
    if checkpoint is None:
        daily_start = config.get("daily_start_sequence")
        if daily_start is None:
            sequences = indexed_sequences(
                config["daily_base_url"],
                int(config["retry_initial_seconds"]),
                int(config["retry_max_seconds"]),
            )
            if not sequences:
                raise RuntimeError("daily replication index contains no sequences")
            daily_start = sequences[0]
        dependency_start = config.get("daily_dependency_start_sequence")
        if dependency_start is not None and int(dependency_start) > int(daily_start):
            raise ValueError("daily_dependency_start_sequence must not exceed daily_start_sequence")
        checkpoint = {
            "phase": "discovery" if dependency_start is not None else "apply",
            "cadence": "day",
            "sequence": int(daily_start) - 1,
            "timestamp": "",
        }

    discovery = checkpoint.get("phase") == "discovery"
    checkpoint = catch_up(config, checkpoint, "day", apply_database=not discovery)

    if discovery and checkpoint["cadence"] == "day":
        dependency_start = config.get("daily_dependency_start_sequence")
        if dependency_start is None:
            raise RuntimeError("discovery checkpoint requires daily_dependency_start_sequence")
        checkpoint = {
            "phase": "replay",
            "cadence": "day",
            "sequence": int(dependency_start) - 1,
            "timestamp": checkpoint["timestamp"],
        }
        atomic_json(state_path, checkpoint)
        checkpoint = catch_up(config, checkpoint, "day", apply_database=True)

    if checkpoint["cadence"] == "day":
        minute_sequence = resolve_minute_start(config, checkpoint["timestamp"])
        checkpoint = {
            "phase": "apply",
            "cadence": "minute",
            "sequence": minute_sequence - 1,
            "timestamp": checkpoint["timestamp"],
        }
        atomic_json(state_path, checkpoint)

    while True:
        checkpoint = catch_up(config, checkpoint, "minute")
        time.sleep(int(config["poll_seconds"]))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SystemExit(f"configuration file not found: {args.config}")
    except json.JSONDecodeError as exc:
        raise SystemExit(
            f"invalid JSON in {args.config}: line {exc.lineno}, "
            f"column {exc.colno}: {exc.msg}"
        )
    run(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
