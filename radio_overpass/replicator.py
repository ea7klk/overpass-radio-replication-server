#!/usr/bin/env python3
"""Resumable daily-to-minute filtered Overpass replication."""

from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import json
import logging
import os
import re
import shutil
import sys
import subprocess
import tempfile
import threading
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
OSC_INSPECTION_RETENTION_SECONDS = 24 * 60 * 60
ANSI_RED = "\033[91m"
ANSI_LIGHT_GREEN = "\033[92m"
ANSI_LIGHT_BLUE = "\033[94m"
ANSI_LIGHT_TURQUOISE = "\033[96m"
ANSI_RESET = "\033[0m"
OBJECT_TYPES = {"node", "way", "relation"}
ROOT_KEY = re.compile(r"^(node|way|relation):([1-9][0-9]*)$")
MAINTENANCE_STATE = threading.local()


class OverpassRootNotFoundError(RuntimeError):
    """The public API answered successfully but did not return the root."""


class DispatcherMaintenance:
    """Coordinate database writers with the dispatcher through a shared file."""

    def __init__(self, config: dict[str, Any]) -> None:
        configured_db_dir = config.get("db_dir")
        if configured_db_dir:
            db_dir = Path(configured_db_dir)
        else:
            db_dir = Path(config["membership_file"]).parent / "db"
        self.path = Path(
            config.get(
                "maintenance_lock",
                str(db_dir.parent / ".maintenance.lock"),
            )
        )
        self.dispatcher_marker = db_dir / "osm3s_osm_base"
        self.timeout_seconds = max(
            1,
            int(config.get("maintenance_lock_timeout_seconds", 300)),
        )
        self.stale_seconds = max(
            self.timeout_seconds,
            int(config.get("maintenance_lock_stale_seconds", 1800)),
        )
        self.heartbeat_seconds = max(
            1,
            int(config.get("maintenance_lock_heartbeat_seconds", 10)),
        )
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

    def _write_lock(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                fd = os.open(
                    self.path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o644,
                )
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(
                        {
                            "pid": os.getpid(),
                            "started_at": datetime.now(timezone.utc).isoformat(),
                        },
                        handle,
                    )
                return
            except FileExistsError:
                try:
                    age = time.time() - self.path.stat().st_mtime
                except FileNotFoundError:
                    continue
                if age > self.stale_seconds:
                    LOG.warning("removing stale dispatcher maintenance lock %s", self.path)
                    self.path.unlink(missing_ok=True)
                    continue
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"timed out waiting for dispatcher maintenance lock {self.path}"
                    )
                time.sleep(1)

    def _heartbeat(self) -> None:
        while not self._heartbeat_stop.wait(self.heartbeat_seconds):
            try:
                self.path.touch(exist_ok=True)
            except OSError:
                return

    def __enter__(self) -> "DispatcherMaintenance":
        depth = getattr(MAINTENANCE_STATE, "depth", 0)
        if depth == 0:
            self._write_lock()
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat,
                name="dispatcher-maintenance-heartbeat",
                daemon=True,
            )
            self._heartbeat_thread.start()
            deadline = time.monotonic() + self.timeout_seconds
            while self.dispatcher_marker.exists():
                if time.monotonic() >= deadline:
                    self.__exit__(None, None, None)
                    raise TimeoutError(
                        "timed out waiting for the Overpass dispatcher to stop"
                    )
                time.sleep(0.5)
        MAINTENANCE_STATE.depth = depth + 1
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        depth = getattr(MAINTENANCE_STATE, "depth", 0)
        if depth <= 1:
            MAINTENANCE_STATE.depth = 0
            self._heartbeat_stop.set()
            if self._heartbeat_thread is not None:
                self._heartbeat_thread.join(timeout=self.heartbeat_seconds + 1)
            self.path.unlink(missing_ok=True)
        else:
            MAINTENANCE_STATE.depth = depth - 1


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
    osm_path: Path | None = None


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


def light_turquoise_console(text: str, *, is_tty: bool | None = None) -> str:
    """Color discovery entity messages light turquoise in interactive logs."""
    if os.environ.get("NO_COLOR") is not None:
        return text
    if is_tty is None:
        is_tty = sys.stderr.isatty()
    if not is_tty and os.environ.get("FORCE_COLOR") != "1":
        return text
    return f"{ANSI_LIGHT_TURQUOISE}{text}{ANSI_RESET}"


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


def overpass_query(root_key: str, timeout: int) -> bytes:
    kind, object_id = root_key_parts(root_key)
    query = f"""[out:xml][timeout:{timeout}];
(
  {kind}(id:{object_id});
);
(._;>>;);
(._;<<;);
(._;>>;);
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


def remote_osm(output_path: Path, objects: dict[str, bytes]) -> None:
    with output_path.open("wb") as output:
        output.write(b"<?xml version='1.0' encoding='UTF-8'?>\n")
        output.write(b'<osm version="0.6" generator="radio-overpass">\n')
        for key in sorted(objects):
            output.write(objects[key])
            output.write(b"\n")
        output.write(b"</osm>\n")


def osc_inspection_directory(config: dict[str, Any], fallback: Path) -> Path:
    configured = config.get("osc_inspection_dir")
    if configured:
        return Path(configured)
    if config.get("work_dir"):
        return Path(config["work_dir"]).parent / "osc-inspection"
    return fallback.parent / "osc-inspection"


def prune_osc_inspection_dir(
    directory: Path,
    *,
    now: float | None = None,
) -> int:
    """Remove retained OSC artifacts older than the 24-hour inspection window."""
    if not directory.exists():
        return 0
    cutoff = (time.time() if now is None else now) - OSC_INSPECTION_RETENTION_SECONDS
    removed = 0
    for pattern in ("*.osc", "*.osc.gz"):
        for path in directory.rglob(pattern):
            try:
                if path.is_file() and path.stat().st_mtime < cutoff:
                    path.unlink(missing_ok=True)
                    removed += 1
            except OSError as exc:
                LOG.warning("could not prune expired OSC inspection artifact %s: %s", path, exc)
    return removed


def retain_osc_artifact(config: dict[str, Any], source: Path) -> Path:
    """Atomically copy a generated OSC file to persistent inspection storage."""
    directory = osc_inspection_directory(config, source)
    directory.mkdir(parents=True, exist_ok=True)
    prune_osc_inspection_dir(directory)

    source_context = re.sub(r"[^A-Za-z0-9_.-]+", "-", source.parent.name).strip(".-")
    source_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", source.stem).strip(".-")
    filename = f"{source_context or 'generated'}-{source_name or 'change'}-{time.time_ns()}.osc"
    destination = directory / filename
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix=".osc-inspection-", suffix=".tmp", dir=directory, delete=False
        ) as handle:
            temporary = Path(handle.name)
            with source.open("rb") as generated:
                shutil.copyfileobj(generated, handle)
        os.chmod(temporary, 0o644)
        os.replace(temporary, destination)
    except OSError as exc:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise RuntimeError(f"could not retain generated OSC file {source}: {exc}") from exc
    LOG.info("retained generated OSC for inspection: %s", destination)
    return destination


def query_overpass(
    config: dict[str, Any],
    root_key: str,
    known_keys: set[str],
    output_path: Path,
) -> RemoteRefresh:
    endpoint = config.get(
        "overpass_query_url",
        "https://overpass.private.coffee/api/interpreter",
    )
    timeout = int(config.get("overpass_query_timeout", 180))
    retries = int(config.get("overpass_query_retries", 3))
    retry_wait = int(config["retry_initial_seconds"])
    max_wait = int(config["retry_max_seconds"])
    query = overpass_query(root_key, timeout)
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
            missing = root_key not in objects
            if remarks:
                last_error = RuntimeError("; ".join(remarks))
            elif missing:
                last_error = OverpassRootNotFoundError(
                    f"Overpass response omitted requested root: {root_key}"
                )
                LOG.warning(
                    "%s",
                    red_console(
                        f"Overpass response omitted requested root {root_key}; "
                        "assuming the entity no longer exists"
                    ),
                )
                break
            else:
                remote_osc(output_path, objects, known_keys)
                retain_osc_artifact(config, output_path)
                osm_path = output_path.with_suffix(".osm")
                remote_osm(osm_path, objects)
                seconds = time.monotonic() - started
                LOG.info(
                    "%s",
                    light_green_console(
                        f"Overpass query succeeded: {endpoint}; retrieved "
                        f"{len(objects)} object(s), including dependencies and dependents, "
                        f"for root {root_key} in {seconds:.1f}s"
                    ),
                )
                return RemoteRefresh(output_path, objects, refs, names, tags, seconds, osm_path)

        if attempt < retries:
            LOG.warning(
                "%s",
                red_console(
                    f"Overpass query failed for root {root_key}: {last_error}; "
                    f"retrying in {retry_wait}s"
                ),
            )
            time.sleep(retry_wait)
            retry_wait = min(max_wait, max(retry_wait * 2, 1))

    if isinstance(last_error, OverpassRootNotFoundError):
        raise last_error
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


def refresh_root(
    config: dict[str, Any],
    root_key: str,
    state: dict[str, Any],
    output_directory: Path,
    timestamp: str,
    context: str,
) -> tuple[RemoteRefresh, dict[str, Any]]:
    known_keys = set(state.get("roots", [])) | set(state.get("dependencies", []))
    kind, object_id = root_key_parts(root_key)
    root_directory = output_directory / f"{kind}-{object_id}"
    root_directory.mkdir(parents=True, exist_ok=True)
    refresh = query_overpass(
        config,
        root_key,
        known_keys,
        root_directory / "remote.osc",
    )
    merged = apply_remote_refresh(
        config,
        root_key,
        refresh,
        state,
        timestamp,
        context,
    )
    return refresh, merged


def apply_remote_refresh(
    config: dict[str, Any],
    root_key: str,
    refresh: RemoteRefresh,
    state: dict[str, Any],
    timestamp: str,
    context: str,
) -> dict[str, Any]:
    """Write one completed public query result in catalog/database order."""
    update_database(
        config,
        refresh.osm_path or refresh.path,
        timestamp,
        description=f"public Overpass dependencies/dependents ({context})",
    )
    merged = merge_remote_state(state, {root_key}, refresh)
    atomic_json(catalog_path(config), merged)
    log_remote_objects(refresh, {root_key}, context)
    return merged


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
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with DispatcherMaintenance(config):
        with tempfile.TemporaryDirectory(prefix="membership-", dir=work_dir) as directory:
            temp_dir = Path(directory)
            while pending:
                root_key = pending[0]
                try:
                    refresh, catalog = refresh_root(
                        config,
                        root_key,
                        catalog,
                        temp_dir,
                        timestamp,
                        "startup membership",
                    )
                except OverpassRootNotFoundError:
                    # A successful empty result is terminal: remove only this
                    # queue entry and continue. Network, HTTP, XML, and database
                    # failures still propagate and preserve the current entry.
                    pending = pending[1:]
                    if pending:
                        atomic_json(pending_path, pending)
                    else:
                        pending_path.unlink(missing_ok=True)
                    continue
                del refresh
                pending = pending[1:]
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


def atomic_text(path: Path, value: str) -> None:
    """Replace a small coordination file without exposing a partial value."""
    temporary = path.with_suffix(path.suffix + ".new")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


MISSING_GEOMETRY_RE = re.compile(
    r"compute_geometry:\s+(Node|Way)\s+(\d+)\s+used in\s+"
    r"(node|way|relation)\s+(\d+)\s+not found\.?$"
)


def overpass_update_diagnostics(stderr: str | None) -> str:
    """Return a compact summary of diagnostics emitted by an Overpass update."""
    lines = [line.strip() for line in (stderr or "").splitlines() if line.strip()]
    if not lines:
        return ""

    missing = [match for line in lines if (match := MISSING_GEOMETRY_RE.fullmatch(line))]
    parts: list[str] = []
    if missing:
        parents = {(match.group(3), match.group(4)) for match in missing}
        examples = ", ".join(
            f"{match.group(1).lower()} {match.group(2)} -> "
            f"{match.group(3)} {match.group(4)}"
            for match in missing[:3]
        )
        parts.append(
            f"{len(missing)} missing geometry reference(s) across "
            f"{len(parents)} object(s); examples: {examples}"
        )

    other = [line for line in lines if not MISSING_GEOMETRY_RE.fullmatch(line)]
    if other:
        parts.append("diagnostic: " + " | ".join(other[-3:]))
    return ": " + "; ".join(parts)


def log_overpass_update_diagnostics(stderr: str | None) -> None:
    summary = overpass_update_diagnostics(stderr)
    if summary:
        LOG.warning(
            "%s",
            red_console(f"Overpass DB update completed with diagnostics{summary}"),
        )


def update_database(
    config: dict[str, Any],
    osc_path: Path,
    timestamp: str,
    description: str = "filtered change",
) -> None:
    if config.get("official_replica_dir"):
        apply_with_official_helper(config, osc_path, timestamp, description)
        return
    with DispatcherMaintenance(config):
        _update_database(config, osc_path, timestamp, description)


def empty_osc(path: Path) -> None:
    path.write_bytes(
        b"<?xml version='1.0' encoding='UTF-8'?>\n"
        b'<osmChange version="0.6" generator="radio-overpass">\n'
        b"<create></create><modify></modify><delete></delete>\n"
        b"</osmChange>\n"
    )


def _gzip_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp-{os.getpid()}")
    try:
        with source.open("rb") as raw, gzip.open(temporary, "wb", compresslevel=6) as zipped:
            shutil.copyfileobj(raw, zipped, length=1024 * 1024)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def apply_with_official_helper(
    config: dict[str, Any],
    osc_path: Path,
    timestamp: str,
    description: str,
) -> None:
    """Publish exactly one change and wait for apply_osc_to_db.sh to commit it.

    The rebuild worker stages one official sequence at a time. This removes
    worker-side batching while retaining Overpass's supported permanent
    apply_osc_to_db.sh consumer and its durable replicate_id cursor.
    """
    sequence_value = config.get("_official_sequence")
    if sequence_value is None:
        raise RuntimeError("official helper mode requires _official_sequence")
    sequence = int(sequence_value)
    replica_dir = Path(config["official_replica_dir"])
    db_dir = Path(config["db_dir"])
    cursor_path = db_dir / "replicate_id"
    current = None
    try:
        current = int(cursor_path.read_text(encoding="ascii").strip())
    except (FileNotFoundError, ValueError):
        pass
    if current is not None and current >= sequence:
        LOG.info(
            "official applier already reached %s; acknowledging %s for %s",
            current,
            sequence,
            description,
        )
        return
    if current is not None and current != sequence - 1:
        raise RuntimeError(
            f"official applier cursor {current} cannot accept sequence {sequence}; "
            "cadence boundary was not initialized correctly"
        )

    relative = Path(sequence_path(sequence))
    target = replica_dir / relative.parent
    target.mkdir(parents=True, exist_ok=True)
    staged = target / f"{relative.name}.osc.gz"
    _gzip_copy(osc_path, staged)
    atomic_text(
        target / f"{relative.name}.state.txt",
        f"sequenceNumber={sequence}\ntimestamp={timestamp}\n",
    )
    atomic_text(replica_dir / "replicate_id", str(sequence) + "\n")
    LOG.info(
        "published %s as official replication sequence %s; waiting for "
        "apply_osc_to_db.sh",
        description,
        sequence,
    )
    timeout = max(60, int(config.get("official_apply_timeout_seconds", 3600)))
    deadline = time.monotonic() + timeout
    while True:
        try:
            applied = int(cursor_path.read_text(encoding="ascii").strip())
        except (FileNotFoundError, ValueError):
            applied = None
        if applied is not None and applied >= sequence:
            LOG.info(
                "apply_osc_to_db.sh committed official sequence %s for %s",
                sequence,
                description,
            )
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"apply_osc_to_db.sh did not commit sequence {sequence} within {timeout}s"
            )
        time.sleep(2)


def _update_database(
    config: dict[str, Any],
    osc_path: Path,
    timestamp: str,
    description: str = "filtered change",
) -> None:
    db_dir = Path(config["db_dir"])
    if not (db_dir / "nodes.map").exists():
        db_dir.mkdir(parents=True, exist_ok=True)
        first_entry = next(db_dir.iterdir(), None)
        if first_entry is not None:
            message = (
                f"Overpass DB directory is partially initialized: {db_dir}/nodes.map "
                f"is missing while {first_entry.name!r} exists; leaving the database "
                "untouched"
            )
            LOG.error("%s", red_console(message))
            raise RuntimeError(
                message
                + "; stop the dispatcher and repair or restore it explicitly"
            )
        update_binary = config.get("overpass_update_database")
        if not update_binary:
            update_binary = str(Path(config["overpass_update_from_dir"]).with_name("update_database"))
        command = [update_binary, f"--db-dir={db_dir}"]
        if config.get("meta_mode"):
            command.append(config["meta_mode"])
        LOG.info(
            "%s",
            light_blue_console(
                f"initializing Overpass DB from {osc_path} at {timestamp}"
            ),
        )
        try:
            with osc_path.open("rb") as source:
                result = subprocess.run(
                    command,
                    stdin=source,
                    check=True,
                    stderr=subprocess.PIPE,
                    text=True,
                )
        except subprocess.CalledProcessError as exc:
            diagnostics = overpass_update_diagnostics(exc.stderr)
            LOG.error(
                "%s",
                red_console(
                    f"Overpass DB initialization failed for {osc_path} "
                    f"(exit {exc.returncode}){diagnostics}"
                ),
            )
            raise
        log_overpass_update_diagnostics(result.stderr)
        return

    command = [
        config["overpass_update_from_dir"],
        f"--db-dir={config['db_dir']}",
        f"--osc-dir={osc_path.parent}",
        f"--version={timestamp}",
    ]
    if config.get("meta_mode"):
        command.append(config["meta_mode"])
    LOG.info("%s", light_blue_console(f"writing {description} to Overpass DB at {timestamp}"))
    try:
        result = subprocess.run(
            command,
            check=True,
            stderr=subprocess.PIPE,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        diagnostics = overpass_update_diagnostics(exc.stderr)
        LOG.error(
            "%s",
            red_console(
                f"Overpass DB update failed for {description} (exit {exc.returncode})"
                f"{diagnostics}"
            ),
        )
        raise
    log_overpass_update_diagnostics(result.stderr)


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
    destination.parent.mkdir(parents=True, exist_ok=True)
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
    regex: bool = False,
) -> bool:
    """Search a gzip file without constructing an XML tree.

    grep and gzip do the scan in native code and stop at the first match. A
    non-match still reads the file once, but avoids the much more expensive XML
    parser and Python object handling.
    """
    if (needle is None) == (pattern_path is None):
        raise ValueError("provide exactly one quick-check pattern")
    matcher_command = ["grep", "-a", "-m", "1", "-E" if regex else "-F"]
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


def gzip_contains_tag_prefix(source_path: Path, prefix: str) -> bool:
    """Return whether a gzipped OSM change file contains a matching tag key.

    This is deliberately a conservative, grep-like candidate check. It only
    decides whether the expensive XML filter should run; the XML parser still
    performs the authoritative object and reference handling.
    """
    pattern = rf'<tag[^>]*[[:space:]]k="{re.escape(prefix)}[^"]*"'
    return gzip_contains(source_path, needle=pattern, regex=True)


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
    *,
    tag_only: bool = False,
) -> str | None:
    """Return why a source needs parsing, or None if it can be discarded."""
    if tag_only:
        return "tag key prefix" if gzip_contains_tag_prefix(source_path, prefix) else None
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
                tag_only=bool(config.get("tag_only_prefilter", False)),
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
                if apply_database and config.get("official_replica_dir"):
                    empty_path = temp_path / "empty.osc"
                    empty_osc(empty_path)
                    config["_official_sequence"] = sequence
                    update_database(
                        config,
                        empty_path,
                        state["timestamp"],
                        description=f"empty filtered replication change ({cadence}/{sequence})",
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
                retain_osc_artifact(config, filtered_path)
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
                remote_dir = temp_path / "remote"
                remote_dir.mkdir()
                remote_objects: dict[str, bytes] = {}
                remote_refs: dict[str, set[str]] = {}
                remote_names: dict[str, str] = {}
                remote_tags: dict[str, list[dict[str, str]]] = {}
                remote_seconds = 0.0
                current_state = old_state
                last_remote_path = remote_dir / "remote.osc"
                known_keys = set(current_state.get("roots", [])) | set(
                    current_state.get("dependencies", [])
                )
                query_workers = max(
                    1,
                    min(
                        len(root_keys),
                        int(config.get("overpass_query_workers", 4)),
                    ),
                )
                LOG.info(
                    "starting %d public Overpass query worker(s) for %d root(s); "
                    "replication prefetch/download continues concurrently",
                    query_workers,
                    len(root_keys),
                )
                query_futures: dict[str, Future[RemoteRefresh]] = {}
                with ThreadPoolExecutor(max_workers=query_workers) as query_executor:
                    for root_key in sorted(root_keys):
                        kind, object_id = root_key_parts(root_key)
                        root_directory = remote_dir / f"{kind}-{object_id}"
                        root_directory.mkdir(parents=True, exist_ok=True)
                        query_futures[root_key] = query_executor.submit(
                            query_overpass,
                            config,
                            root_key,
                            known_keys,
                            root_directory / "remote.osc",
                        )

                    for root_key in sorted(root_keys):
                        refresh = query_futures[root_key].result()
                        # Queries run concurrently, but local writes remain
                        # ordered. Reclassify objects against the state after
                        # each preceding root so overlapping results are safe.
                        known_remote_keys = set(current_state.get("roots", [])) | set(
                            current_state.get("dependencies", [])
                        )
                        remote_osc(refresh.path, refresh.objects, known_remote_keys)
                        if config.get("official_replica_dir"):
                            # All roots discovered in one source file must be
                            # committed as one official sequence. Applying a
                            # second helper update with the same sequence
                            # would make the official cursor ambiguous.
                            current_state = merge_remote_state(
                                current_state, {root_key}, refresh
                            )
                        else:
                            current_state = apply_remote_refresh(
                                config,
                                root_key,
                                refresh,
                                current_state,
                                state["timestamp"],
                                f"{cadence}/{sequence}",
                            )
                        remote_objects.update(refresh.objects)
                        remote_refs.update(refresh.refs)
                        remote_names.update(refresh.names)
                        remote_tags.update(refresh.tags)
                        remote_seconds += refresh.seconds
                        last_remote_path = refresh.path
                remote_refresh = RemoteRefresh(
                    last_remote_path,
                    remote_objects,
                    remote_refs,
                    remote_names,
                    remote_tags,
                    remote_seconds,
                )

                if config.get("official_replica_dir"):
                    combined_remote = remote_dir / "combined.osc"
                    remote_osc(combined_remote, remote_objects, known_keys)
                    config["_official_sequence"] = sequence
                    update_database(
                        config,
                        combined_remote,
                        state["timestamp"],
                        description=f"public Overpass dependencies/dependents ({cadence}/{sequence})",
                    )
                    atomic_json(catalog_path(config), current_state)
                    for root_key in sorted(root_keys):
                        LOG.info(
                            "applied external Overpass dependencies immediately for %s at %s/%s",
                            root_key,
                            cadence,
                            sequence,
                        )

            has_database_changes = any(item["op"] in {"add", "remove"} for item in events)
            apply_started = time.monotonic()
            if remote_refresh is None and apply_database and (
                has_database_changes or config.get("official_replica_dir")
            ):
                config["_official_sequence"] = sequence
                update_path = filtered_path
                if not has_database_changes and config.get("official_replica_dir"):
                    update_path = temp_path / "empty-after-filter.osc"
                    empty_osc(update_path)
                update_database(
                    config,
                    update_path,
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
                    if not apply_database:
                        message = light_turquoise_console(message)
                    LOG.info("%s", message)
                elif item["op"] == "remove":
                    kind = "root" if item.get("root") else "dependency"
                    verb = "removed" if apply_database else "discovered removal of"
                    message = f"{verb} {kind} {item['id']} at replication {cadence}/{sequence}"
                    if not apply_database:
                        message = light_turquoise_console(message)
                    LOG.info("%s", message)

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


class CatalogQueryPool:
    """Continuously query catalog roots while replication catches up."""

    def __init__(self, config: dict[str, Any], directory: Path) -> None:
        self.config = config
        self.directory = directory
        self.interval_seconds = max(
            1,
            int(config.get("catalog_query_interval_seconds", 3600)),
        )
        self.executor = ThreadPoolExecutor(
            max_workers=max(1, int(config.get("catalog_query_workers", 4)))
        )
        self.futures: dict[str, Future[RemoteRefresh]] = {}
        self.roots: set[str] = set()
        self.next_due: dict[str, float] = {}

    def _refresh_catalog_roots(self, *, initial_delay_seconds: int = 0) -> None:
        catalog = load_json(catalog_path(self.config), {})
        if not isinstance(catalog, dict):
            return
        roots = {str(root) for root in catalog.get("roots", [])}
        now = time.monotonic()
        for root_key in roots - self.roots:
            self.next_due[root_key] = now + max(0, initial_delay_seconds)
        for root_key in self.roots - roots:
            self.next_due.pop(root_key, None)
        self.roots = roots

    def _schedule_due(self) -> None:
        if not self.roots:
            return
        catalog = load_json(catalog_path(self.config), {})
        if not isinstance(catalog, dict):
            return
        known_keys = set(catalog.get("roots", [])) | set(catalog.get("dependencies", []))
        self.directory.mkdir(parents=True, exist_ok=True)
        now = time.monotonic()
        scheduled = 0
        for root_key in sorted(self.roots):
            if root_key in self.futures or self.next_due.get(root_key, now) > now:
                continue
            kind, object_id = root_key_parts(root_key)
            root_directory = self.directory / f"{kind}-{object_id}"
            root_directory.mkdir(parents=True, exist_ok=True)
            self.futures[root_key] = self.executor.submit(
                query_overpass,
                self.config,
                root_key,
                known_keys,
                root_directory / "remote.osc",
            )
            self.next_due[root_key] = now + self.interval_seconds
            scheduled += 1
        if scheduled:
            LOG.info(
                "scheduled %d catalog root query(ies); replication downloads and "
                "filtering continue concurrently",
                scheduled,
            )

    def start(self) -> None:
        self._refresh_catalog_roots(
            initial_delay_seconds=max(
                0,
                int(self.config.get("catalog_query_initial_delay_seconds", 0)),
            )
        )
        self._schedule_due()

    def has_ready(self) -> bool:
        return any(future.done() for future in self.futures.values())

    def drain_ready(
        self,
        *,
        wait: bool = False,
        reschedule: bool = True,
        max_items: int | None = None,
    ) -> None:
        """Apply completed results in catalog order; never write from workers."""
        if reschedule:
            self._refresh_catalog_roots()
            self._schedule_due()
        processed = 0
        while self.futures:
            if max_items is not None and processed >= max_items:
                return
            root_key, future = next(iter(self.futures.items()))
            if not wait and not future.done():
                return
            del self.futures[root_key]
            processed += 1
            try:
                refresh = future.result()
            except OverpassRootNotFoundError:
                catalog = load_json(catalog_path(self.config), {})
                if isinstance(catalog, dict):
                    catalog["roots"] = sorted(
                        set(catalog.get("roots", [])) - {root_key}
                    )
                    atomic_json(catalog_path(self.config), catalog)
                LOG.warning(
                    "%s",
                    red_console(
                        f"removed vanished catalog root {root_key}; "
                        "continuing catalog query lane"
                    ),
                )
                if reschedule:
                    self.next_due[root_key] = time.monotonic() + max(
                        1,
                        int(self.config.get("retry_initial_seconds", 15)),
                    )
                continue
            except Exception as exc:
                # A transient catalog refresh must not stop the replication
                # lane. The root remains in catalog.json and will be retried
                # on the next process startup.
                LOG.error(
                    "%s",
                    red_console(
                        f"background catalog query failed for {root_key}: {exc}; "
                        "replication continues"
                    ),
                )
                if reschedule:
                    self.next_due[root_key] = time.monotonic() + max(
                        1,
                        int(self.config.get("retry_initial_seconds", 15)),
                    )
                continue

            state = load_json(catalog_path(self.config), {})
            if not isinstance(state, dict):
                state = {}
            try:
                remote_osc(
                    refresh.path,
                    refresh.objects,
                    set(state.get("roots", [])) | set(state.get("dependencies", [])),
                )
                apply_remote_refresh(
                    self.config,
                    root_key,
                    refresh,
                    state,
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "catalog background refresh",
                )
            except Exception as exc:
                LOG.error(
                    "%s",
                    red_console(
                        f"catalog database update failed for {root_key}: {exc}; "
                        "replication continues and the root will be retried"
                    ),
                )
                if reschedule:
                    self.next_due[root_key] = time.monotonic() + max(
                        1,
                        int(self.config.get("retry_initial_seconds", 15)),
                    )
                continue
            if reschedule:
                self.next_due[root_key] = time.monotonic() + self.interval_seconds

        if reschedule:
            self._refresh_catalog_roots()
            self._schedule_due()

    def close(self) -> None:
        with DispatcherMaintenance(self.config):
            self.drain_ready(wait=True, reschedule=False)
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


def snapshot_checkpoint(config: dict[str, Any]) -> dict[str, Any] | None:
    metadata_path = config.get("snapshot_metadata_file")
    if not metadata_path:
        return None
    metadata = load_json(Path(metadata_path), None)
    if not isinstance(metadata, dict):
        return None
    timestamp = metadata.get("timestamp")
    if not isinstance(timestamp, str) or not timestamp:
        raise ValueError(f"initial snapshot metadata has no timestamp: {metadata_path}")
    minute_sequence = resolve_minute_start(config, timestamp)
    LOG.info(
        "initial snapshot timestamp is %s; starting minute replication at sequence %s",
        timestamp,
        minute_sequence,
    )
    return {
        "phase": "apply",
        "cadence": "minute",
        "sequence": minute_sequence - 1,
        "timestamp": timestamp,
    }


def catch_up(
    config: dict[str, Any],
    checkpoint: dict[str, Any],
    cadence: str,
    *,
    apply_database: bool = True,
    catalog_queries: CatalogQueryPool | None = None,
) -> dict[str, Any]:
    base_key = "daily" if cadence == "day" else cadence
    base = config[f"{base_key}_base_url"]
    work_dir = Path(config["work_dir"])
    work_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f"prefetch-{cadence}-", dir=work_dir) as directory:
        prefetcher = Prefetcher(config, cadence, Path(directory))
        catalog_query_batch_size = max(
            1,
            int(config.get("catalog_query_batch_size", 20)),
        )
        try:
            while checkpoint["cadence"] == cadence:
                if (
                    catalog_queries is not None
                    and (
                        not apply_database
                        or catalog_queries.has_ready()
                    )
                ):
                    catalog_queries.drain_ready(
                        max_items=catalog_query_batch_size
                    )
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
                if catalog_queries is not None:
                    catalog_queries.drain_ready(
                        max_items=catalog_query_batch_size
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
    inspection_dir = osc_inspection_directory(config, Path(config["work_dir"]))
    removed = prune_osc_inspection_dir(inspection_dir)
    if removed:
        LOG.info("pruned %d expired OSC inspection artifact(s)", removed)
    process_pending_membership(config)
    state_path = Path(config["state_file"])
    work_dir = Path(config["work_dir"])
    work_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="catalog-queries-", dir=work_dir) as directory:
        catalog_queries = CatalogQueryPool(config, Path(directory))
        catalog_queries.start()
        try:
            checkpoint = load_json(state_path, None)
            if checkpoint is None:
                checkpoint = snapshot_checkpoint(config)
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
                atomic_json(state_path, checkpoint)

            discovery = checkpoint.get("phase") == "discovery"
            checkpoint = catch_up(
                config,
                checkpoint,
                "day",
                apply_database=not discovery,
                catalog_queries=catalog_queries,
            )

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
                checkpoint = catch_up(
                    config,
                    checkpoint,
                    "day",
                    apply_database=True,
                    catalog_queries=catalog_queries,
                )

            if checkpoint["cadence"] == "day":
                if config.get("minute_start_at_current", False):
                    current_minute = latest(
                        config["minute_base_url"],
                        int(config["retry_initial_seconds"]),
                        int(config["retry_max_seconds"]),
                    )
                    minute_sequence = int(current_minute["sequence"]) + 1
                    minute_timestamp = current_minute["timestamp"]
                    LOG.info(
                        "starting minute replication at current sequence %s; "
                        "skipping minute history before %s",
                        minute_sequence,
                        minute_timestamp,
                    )
                else:
                    minute_sequence = resolve_minute_start(config, checkpoint["timestamp"])
                    minute_timestamp = checkpoint["timestamp"]
                checkpoint = {
                    "phase": "apply",
                    "cadence": "minute",
                    "sequence": minute_sequence - 1,
                    "timestamp": minute_timestamp,
                }
                atomic_json(state_path, checkpoint)

            while True:
                removed = prune_osc_inspection_dir(inspection_dir)
                if removed:
                    LOG.info("pruned %d expired OSC inspection artifact(s)", removed)
                checkpoint = catch_up(
                    config,
                    checkpoint,
                    "minute",
                    catalog_queries=catalog_queries,
                )
                time.sleep(int(config["poll_seconds"]))
        finally:
            catalog_queries.close()


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
