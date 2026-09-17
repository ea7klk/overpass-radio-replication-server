#!/usr/bin/env python3
"""Maintain a full Planet PBF and rebuild the filtered Overpass database.

The full PBF is the source of truth.  Replication OSC files are applied to it
with osmium, then a fresh filtered PBF is produced only when the change file
contains a communication:amateur_radio* tag.  The filtered PBF is imported
into an isolated staging database and cut over by the Overpass process.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
import logging
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
from typing import Any

from . import replicator as common


LOG = logging.getLogger("radio-overpass.full-pbf")
CADENCES = ("day", "hour", "minute")


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".new")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def config_path(config: dict[str, Any], key: str) -> Path:
    return Path(str(config[key]))


def cadence_base(config: dict[str, Any], cadence: str) -> str:
    return str(config["daily_base_url" if cadence == "day" else f"{cadence}_base_url"])


def retry_values(config: dict[str, Any]) -> tuple[int, int]:
    return int(config["retry_initial_seconds"]), int(config["retry_max_seconds"])


def first_sequence_after(config: dict[str, Any], cadence: str, timestamp: str) -> int:
    explicit = config.get(f"{cadence}_start_sequence")
    if explicit is not None:
        return int(explicit)
    retry, maximum = retry_values(config)
    base = cadence_base(config, cadence)
    latest = int(common.latest(base, retry, maximum)["sequence"])
    lo, hi = 1, latest + 1
    while lo < hi:
        mid = (lo + hi) // 2
        if common.fetch_state(base, mid, retry, maximum)["timestamp"] <= timestamp:
            lo = mid + 1
        else:
            hi = mid
    LOG.info("%s boundary after %s is sequence %s", cadence, timestamp, lo)
    return lo


def contains_radio_tag(path: Path, prefix: str) -> bool:
    pattern = re.compile(
        rb'<tag\b[^>]*\bk=["\']' + re.escape(prefix.encode()) + rb'[^"\']*["\']'
    )
    with gzip.open(path, "rb") as source:
        overlap = b""
        while chunk := source.read(1024 * 1024):
            data = overlap + chunk
            if pattern.search(data):
                return True
            overlap = data[-512:]
    return False


def read_marker(config: dict[str, Any], key: str) -> str:
    path = config.get(key)
    if not path:
        return ""
    try:
        return Path(str(path)).read_text(encoding="ascii").strip()
    except FileNotFoundError:
        return ""


def write_marker(config: dict[str, Any], key: str, value: str) -> None:
    path_value = config.get(key)
    if not path_value:
        return
    path = Path(str(path_value))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{time.monotonic_ns()}")
    temporary.write_text(value, encoding="ascii")
    temporary.replace(path)


def record_current_planet_size(config: dict[str, Any], payload: Path) -> None:
    metadata_path = config.get("planet_metadata_file")
    if not metadata_path:
        return
    metadata = load_json(Path(str(metadata_path)), {})
    if not isinstance(metadata, dict):
        metadata = {}
    metadata["current_size_bytes"] = payload.stat().st_size
    write_json(Path(str(metadata_path)), metadata)


def inactive_slot(config: dict[str, Any]) -> str:
    active = read_marker(config, "active_slot_file")
    if active not in {"blue", "green"}:
        return "blue"
    return "green" if active == "blue" else "blue"


def slot_dir(config: dict[str, Any], slot: str) -> Path:
    return config_path(config, "db_root") / slot


def apply_full_changes(
    config: dict[str, Any], changes: list[Path], cadence: str, first: int, last: int
) -> None:
    current = config_path(config, "planet_pbf")
    payload = current.resolve() if current.is_symlink() else current
    work_dir = config_path(config, "work_dir")
    work_dir.mkdir(parents=True, exist_ok=True)
    merged = work_dir / f"{cadence}-{first:09d}-{last:09d}.osc.gz"
    merged.unlink(missing_ok=True)
    merge_started = time.monotonic()
    LOG.info(
        "merging %s %s replication files (%s through %s)",
        len(changes), cadence, first, last,
    )
    subprocess.run(
        [
            "osmium", "merge-changes", "--simplify",
            *[str(path) for path in changes],
            "-o", str(merged), "--overwrite", "--progress",
        ],
        check=True,
    )
    LOG.info(
        "merged %s %s files in %.1fs: %s",
        len(changes), cadence, time.monotonic() - merge_started, merged,
    )
    next_pbf = payload.with_name(f".{payload.name}.{cadence}-{last}.osm.pbf")
    next_pbf.unlink(missing_ok=True)
    started = time.monotonic()
    LOG.info(
        "starting full-PBF apply for %s batch %s through %s: %s",
        cadence, first, last, merged,
    )
    subprocess.run(
        [
            "osmium",
            "apply-changes",
            str(payload),
            str(merged),
            "-o",
            str(next_pbf),
            "--overwrite",
            "--progress",
        ],
        check=True,
    )
    # Keep planet-latest.osm.pbf as a stable symlink. Replacing the payload
    # itself removes the previous version instead of leaving a second full
    # Planet file beside the updated one.
    next_pbf.replace(payload)
    record_current_planet_size(config, payload)
    LOG.info(
        "full Planet PBF advanced through %s/%s in %.1fs",
        cadence, last,
        time.monotonic() - started,
    )
    merged.unlink(missing_ok=True)


def import_filtered_pbf(config: dict[str, Any], filtered: Path, slot: str) -> None:
    staging = slot_dir(config, slot)
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="filtered-xml-", dir=str(config_path(config, "filtered_dir"))
    ) as directory:
        xml = Path(directory) / "filtered.osm"
        extraction_started = time.monotonic()
        subprocess.run(
            ["osmium", "cat", str(filtered), "-o", str(xml), "--overwrite", "--progress"],
            check=True,
        )
        LOG.info(
            "extracted filtered PBF %s to XML in %.1fs",
            filtered,
            time.monotonic() - extraction_started,
        )
        import_started = time.monotonic()
        LOG.info("starting staging Overpass import for %s", filtered)
        common.update_database(
            {**config, "db_dir": str(staging)},
            xml,
            str(config.get("last_change_timestamp", "")),
            description=f"filtered PBF {filtered.name}",
        )
        LOG.info(
            "staging Overpass import for %s completed in %.1fs",
            filtered,
            time.monotonic() - import_started,
        )
    write_marker(config, "ready_slot_file", slot)
    LOG.info("replacement database is ready in slot %s", slot)


def rebuild_filtered(
    config: dict[str, Any], cadence: str, first: int, last: int, timestamp: str
) -> None:
    filtered_dir = config_path(config, "filtered_dir")
    filtered_dir.mkdir(parents=True, exist_ok=True)
    filtered = filtered_dir / f"filtered-{cadence}-{first:09d}-{last:09d}.osm.pbf"
    prefix = str(config["tag_key_prefix"])
    LOG.info(
        "radio tag found in %s batch %s through %s; generating %s",
        cadence, first, last, filtered.name,
    )
    filter_started = time.monotonic()
    subprocess.run(
        [
            "osmium",
            "tags-filter",
            str(config_path(config, "planet_pbf")),
            f"{prefix}*",
            "-o",
            str(filtered),
            "--progress",
            "--overwrite",
            "--verbose",
        ],
        check=True,
    )
    LOG.info(
        "radio tags-filter for %s/%s through %s completed in %.1fs",
        cadence, first, last,
        time.monotonic() - filter_started,
    )
    config["last_change_timestamp"] = timestamp
    import_filtered_pbf(config, filtered, inactive_slot(config))


def download(config: dict[str, Any], cadence: str, sequence: int) -> tuple[Path, dict[str, Any]]:
    base = cadence_base(config, cadence)
    retry, maximum = retry_values(config)
    state = common.fetch_state(base, sequence, retry, maximum)
    _, url = common.urls(base, sequence)
    raw_dir = config_path(config, "raw_dir") / cadence
    raw_dir.mkdir(parents=True, exist_ok=True)
    destination = raw_dir / f"{sequence:09d}.osc.gz"
    partial = destination.with_suffix(destination.suffix + ".part")
    if destination.exists():
        return destination, state
    wait = retry
    while True:
        try:
            partial.unlink(missing_ok=True)
            started = time.monotonic()
            subprocess.run(
                [
                    "curl", "--fail", "--location", "--silent", "--show-error",
                    "--retry", "3", "--retry-delay", "5", "--connect-timeout", "120",
                    "--output", str(partial), url,
                ],
                check=True,
            )
            subprocess.run(["gzip", "-t", str(partial)], check=True)
            partial.replace(destination)
            LOG.info(
                "downloaded %s replication file %s in %.1fs",
                cadence,
                url,
                time.monotonic() - started,
            )
            return destination, state
        except (OSError, subprocess.CalledProcessError) as exc:
            LOG.warning("download failed for %s: %s; retrying in %ss", url, exc, wait)
            partial.unlink(missing_ok=True)
            time.sleep(wait)
            wait = min(maximum, max(wait * 2, 1))


def prefilter(config: dict[str, Any], cadence: str, sequence: int, change: Path) -> bool:
    has_tag = contains_radio_tag(change, str(config["tag_key_prefix"]))
    LOG.info(
        "%s %s %s tag prefilter: %s",
        cadence,
        sequence,
        "accepted" if has_tag else "discarded",
        "matching communication:amateur_radio*" if has_tag else "no matching tag",
    )
    if has_tag:
        marker = config.get("accepted_prefilter_marker_file")
        if marker:
            marker_path = Path(str(marker))
            marker_path.parent.mkdir(parents=True, exist_ok=True)
            marker_path.write_text(
                f"{cadence}/{sequence} accepted tag prefilter: "
                "matching communication:amateur_radio*\n",
                encoding="utf-8",
            )
            LOG.info("accepted prefilter marker written: %s", marker_path)
    return has_tag


def apply_batch(
    config: dict[str, Any], cadence: str, first: int, batch: list[tuple[Path, dict[str, Any], bool]]
) -> dict[str, Any]:
    last = first + len(batch) - 1
    changes = [item[0] for item in batch]
    apply_full_changes(config, changes, cadence, first, last)
    write_json(
        config_path(config, "full_pbf_state_file"),
        {"cadence": cadence, "sequence": last, "timestamp": batch[-1][1]["timestamp"]},
    )
    if any(item[2] for item in batch):
        rebuild_filtered(config, cadence, first, last, str(batch[-1][1]["timestamp"]))
    for change, _, _ in batch:
        change.unlink(missing_ok=True)
    return batch[-1][1]


def download_until_caught_up(
    config: dict[str, Any], cadence: str, first: int, retry: int, maximum: int,
    due_at: float,
) -> list[tuple[Path, dict[str, Any], bool]]:
    """Prefetch one batch and wait for the hourly apply gate if necessary.

    The full Planet PBF is intentionally advanced in bounded batches. While
    an apply is cooling down, the next ten files can already be downloaded and
    prefiltered, but no second full-PBF rewrite is started.
    """
    sequence = first
    target = int(common.latest(cadence_base(config, cadence), retry, maximum)["sequence"])
    if sequence > target:
        return []
    prefetch = max(1, int(config.get("prefetch_files", 10)))
    window_end = min(sequence + prefetch - 1, target)
    window = list(range(sequence, window_end + 1))
    LOG.info(
        "prefetching %s %s update files (%s through %s)",
        len(window), cadence, sequence, window_end,
    )
    with ThreadPoolExecutor(
        max_workers=len(window), thread_name_prefix="download"
    ) as executor:
        downloaded = list(
            executor.map(lambda value: download(config, cadence, value), window)
        )
    batch = [
        (change, state, prefilter(config, cadence, value, change))
        for value, (change, state) in zip(window, downloaded)
    ]
    if time.time() < due_at:
        LOG.info(
            "prefetch complete through %s; full-PBF apply is gated until %s",
            window_end,
            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(due_at)),
        )
    while time.time() < due_at:
        time.sleep(min(30, max(0.1, due_at - time.time())))
    LOG.info(
        "%s batch window reached at %s; applying %s file(s), backlog target was %s",
        cadence,
        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        len(batch),
        target,
    )
    return batch


def run(config: dict[str, Any]) -> None:
    metadata = load_json(config_path(config, "snapshot_metadata_file"), None)
    if not isinstance(metadata, dict) or not metadata.get("timestamp"):
        raise RuntimeError("fresh Planet PBF metadata is missing")
    state_path = config_path(config, "state_file")
    state = load_json(state_path, None)
    if not isinstance(state, dict):
        timestamp = str(metadata["timestamp"])
        state = {
            "cadence": "day",
            "sequence": first_sequence_after(config, "day", timestamp) - 1,
            "timestamp": timestamp,
        }
        write_json(state_path, state)
        LOG.info("starting raw global replication after fresh PBF boundary %s", timestamp)
    while True:
        cadence = str(state["cadence"])
        retry, maximum = retry_values(config)
        target = int(common.latest(cadence_base(config, cadence), retry, maximum)["sequence"])
        next_sequence = int(state["sequence"]) + 1
        if next_sequence <= target:
            last_applied = float(state.get("last_batch_applied_at", 0))
            due_at = max(
                time.time(),
                last_applied + float(config.get("batch_interval_seconds", 3600)),
            )
            batch = download_until_caught_up(
                config, cadence, next_sequence, retry, maximum, due_at
            )
            state = apply_batch(config, cadence, next_sequence, batch)
            state["cadence"] = cadence
            state["last_batch_applied_at"] = time.time()
            write_json(state_path, state)
            continue
        index = CADENCES.index(cadence)
        if index < len(CADENCES) - 1:
            next_cadence = CADENCES[index + 1]
            next_sequence = first_sequence_after(config, next_cadence, str(state["timestamp"]))
            state = {
                "cadence": next_cadence,
                "sequence": next_sequence - 1,
                "timestamp": state["timestamp"],
            }
            write_json(state_path, state)
            LOG.info("caught up %s; switching to %s", cadence, next_cadence)
            continue
        time.sleep(max(1, int(config.get("poll_seconds", 60))))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(json.loads(args.config.read_text(encoding="utf-8")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
