#!/usr/bin/env python3
"""Resumable daily-to-minute filtered Overpass replication."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from html.parser import HTMLParser


LOG = logging.getLogger("radio-overpass")


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


def update_database(config: dict[str, Any], osc_path: Path, timestamp: str) -> None:
    command = [
        config["overpass_update_from_dir"],
        f"--db-dir={config['db_dir']}",
        f"--osc-dir={osc_path.parent}",
        f"--version={timestamp}",
    ]
    if config.get("meta_mode"):
        command.append(config["meta_mode"])
    LOG.info("applying filtered change at %s", timestamp)
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


def filter_stream(
    config: dict[str, Any],
    change_url: str,
    membership: Path,
    filtered_path: Path,
    delta_path: Path,
    include: set[str],
) -> None:
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
    curl_command = [
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
        change_url,
    ]
    wait = int(config["retry_initial_seconds"])
    max_wait = int(config["retry_max_seconds"])
    while True:
        curl_process: subprocess.Popen[bytes] | None = None
        try:
            curl_process = subprocess.Popen(curl_command, stdout=subprocess.PIPE)
            assert curl_process.stdout is not None
            with curl_process.stdout as raw:
                with filtered_path.open("wb") as filtered:
                    subprocess.run(command, stdin=raw, stdout=filtered, check=True, env=environment)
            curl_returncode = curl_process.wait()
            if curl_returncode != 0:
                raise subprocess.CalledProcessError(curl_returncode, curl_command)
            return
        except (OSError, urllib.error.HTTPError, subprocess.CalledProcessError) as exc:
            LOG.warning("streaming download/filter failed for %s: %s; retrying in %ss", change_url, exc, wait)
            filtered_path.unlink(missing_ok=True)
            delta_path.unlink(missing_ok=True)
            if curl_process is not None and curl_process.poll() is None:
                curl_process.kill()
                curl_process.wait()
            time.sleep(wait)
            wait = min(max_wait, max(wait * 2, 1))


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


def process_one(config: dict[str, Any], cadence: str, sequence: int) -> dict[str, Any]:
    base_key = "daily" if cadence == "day" else cadence
    base = config[f"{base_key}_base_url"]
    retry = int(config["retry_initial_seconds"])
    max_wait = int(config["retry_max_seconds"])
    state = fetch_state(base, sequence, retry, max_wait)
    _, change_url = urls(base, sequence)
    LOG.info("processing %s replication file %s", cadence, change_url)

    work_dir = Path(config["work_dir"])
    work_dir.mkdir(parents=True, exist_ok=True)
    membership = Path(config["membership_file"])

    with tempfile.TemporaryDirectory(prefix=f"{cadence}-{sequence}-", dir=work_dir) as temp:
        temp_path = Path(temp)
        filtered_path = temp_path / "filtered-0.osc"
        delta_path = temp_path / "delta-0.jsonl"
        old_state = load_json(membership, {})
        old_dependencies = set(old_state.get("dependencies", [])) if isinstance(old_state, dict) else set()
        include: set[str] = set()

        # A root can reference an object that appears earlier in the same OSC
        # file (for example, a way's nodes). Re-stream the file when a pass
        # discovers new dependencies, seeding the next pass with those IDs.
        # This keeps source files off disk while making same-file references
        # available to Overpass. Most minute files need only one pass.
        pass_number = 0
        while True:
            if pass_number:
                filtered_path = temp_path / f"filtered-{pass_number}.osc"
                delta_path = temp_path / f"delta-{pass_number}.jsonl"
            filter_stream(config, change_url, membership, filtered_path, delta_path, include)
            events = delta_items(delta_path)
            candidate_state = delta_state(events)
            if candidate_state is None:
                break
            new_dependencies = set(candidate_state.get("dependencies", [])) - (old_dependencies | include)
            if not new_dependencies:
                break
            include.update(new_dependencies)
            pass_number += 1

        has_database_changes = any(item["op"] in {"add", "remove"} for item in events)
        if has_database_changes:
            update_database(config, filtered_path, state["timestamp"])
        if events:
            commit_delta(membership, delta_path)
        else:
            LOG.info("sequence %s/%s contained no matching objects", cadence, sequence)

        for item in events:
            if item["op"] == "add":
                kind = "root" if item.get("root") else "dependency"
                LOG.info("applied %s %s at replication %s/%s", kind, item["id"], cadence, sequence)
            elif item["op"] == "remove":
                kind = "root" if item.get("root") else "dependency"
                LOG.info("removed %s %s at replication %s/%s", kind, item["id"], cadence, sequence)

    return state


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


def run(config: dict[str, Any]) -> None:
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
        checkpoint = {
            "cadence": "day",
            "sequence": int(daily_start) - 1,
        }

    while checkpoint["cadence"] == "day":
        target = latest(config["daily_base_url"], int(config["retry_initial_seconds"]), int(config["retry_max_seconds"]))
        next_sequence = checkpoint["sequence"] + 1
        if next_sequence > target["sequence"]:
            break
        result = process_one(config, "day", next_sequence)
        checkpoint = {"cadence": "day", "sequence": result["sequence"], "timestamp": result["timestamp"]}
        atomic_json(state_path, checkpoint)

    if checkpoint["cadence"] == "day":
        minute_sequence = resolve_minute_start(config, checkpoint["timestamp"])
        checkpoint = {"cadence": "minute", "sequence": minute_sequence - 1, "timestamp": checkpoint["timestamp"]}
        atomic_json(state_path, checkpoint)

    while True:
        target = latest(config["minute_base_url"], int(config["retry_initial_seconds"]), int(config["retry_max_seconds"]))
        next_sequence = checkpoint["sequence"] + 1
        if next_sequence <= target["sequence"]:
            result = process_one(config, "minute", next_sequence)
            checkpoint = {"cadence": "minute", "sequence": result["sequence"], "timestamp": result["timestamp"]}
            atomic_json(state_path, checkpoint)
            continue
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
