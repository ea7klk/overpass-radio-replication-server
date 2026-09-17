#!/usr/bin/env python3
"""Maintain a full Planet PBF and rebuild the filtered Overpass database.

The full PBF is the source of truth.  Replication OSC files are applied to it
with osmium, then a fresh filtered PBF is produced only when the change file
contains a communication:amateur_radio* tag.  The filtered PBF is imported
into an isolated staging database and cut over by the Overpass process.
"""

from __future__ import annotations

import argparse
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


def apply_full_change(config: dict[str, Any], change: Path, sequence: int) -> None:
    current = config_path(config, "planet_pbf")
    next_pbf = current.with_name(f".planet-{sequence}.osm.pbf")
    next_pbf.unlink(missing_ok=True)
    started = time.monotonic()
    LOG.info("starting full-PBF apply for sequence %s: %s", sequence, change)
    subprocess.run(
        [
            "osmium",
            "apply-changes",
            str(current),
            str(change),
            "-o",
            str(next_pbf),
            "--overwrite",
            "--progress",
        ],
        check=True,
    )
    next_pbf.replace(current)
    LOG.info(
        "full Planet PBF advanced through sequence %s in %.1fs",
        sequence,
        time.monotonic() - started,
    )


def import_filtered_pbf(config: dict[str, Any], filtered: Path) -> None:
    staging = config_path(config, "staging_db_dir")
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


def rebuild_filtered(config: dict[str, Any], change: Path, timestamp: str, sequence: int) -> None:
    filtered_dir = config_path(config, "filtered_dir")
    filtered_dir.mkdir(parents=True, exist_ok=True)
    filtered = filtered_dir / f"filtered-{sequence:09d}.osm.pbf"
    prefix = str(config["tag_key_prefix"])
    LOG.info("radio tag found in %s; generating %s", change.name, filtered.name)
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
        "radio tags-filter for sequence %s completed in %.1fs",
        sequence,
        time.monotonic() - filter_started,
    )
    config["last_change_timestamp"] = timestamp
    import_filtered_pbf(config, filtered)
    marker = config_path(config, "staging_ready_file")
    marker.write_text(f"{sequence}\n", encoding="ascii")
    LOG.info("staging database ready through %s", sequence)


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


def apply_one(config: dict[str, Any], cadence: str, sequence: int) -> dict[str, Any]:
    change, state = download(config, cadence, sequence)
    has_tag = contains_radio_tag(change, str(config["tag_key_prefix"]))
    LOG.info(
        "%s %s %s tag prefilter: %s",
        cadence,
        sequence,
        "accepted" if has_tag else "discarded",
        "matching communication:amateur_radio*" if has_tag else "no matching tag",
    )
    if has_tag:
        marker_path = config.get("accepted_prefilter_marker_file")
        if marker_path:
            marker = Path(str(marker_path))
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(
                f"{cadence}/{sequence} accepted tag prefilter: "
                "matching communication:amateur_radio*\n",
                encoding="utf-8",
            )
            LOG.info("accepted prefilter marker written: %s", marker)
    full_pbf_state = load_json(config_path(config, "full_pbf_state_file"), {})
    already_applied = (
        isinstance(full_pbf_state, dict)
        and full_pbf_state.get("cadence") == cadence
        and int(full_pbf_state.get("sequence", -1)) == sequence
    )
    if already_applied:
        LOG.info(
            "full Planet PBF already contains %s/%s; resuming filtering/import",
            cadence,
            sequence,
        )
    else:
        apply_full_change(config, change, sequence)
        write_json(
            config_path(config, "full_pbf_state_file"),
            {"cadence": cadence, "sequence": sequence, "timestamp": state["timestamp"]},
        )
    if has_tag:
        rebuild_filtered(config, change, str(state["timestamp"]), sequence)
        cutover = config_path(config, "cutover_requested_file")
        cutover.parent.mkdir(parents=True, exist_ok=True)
        Path(config["cutover_complete_file"]).unlink(missing_ok=True)
        cutover.write_text(
            f"filtered staging ready for {cadence}/{sequence}\n", encoding="utf-8"
        )
    change.unlink(missing_ok=True)
    return state


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
        cutover = config_path(config, "cutover_requested_file")
        if cutover.exists():
            LOG.info("waiting for Overpass server to complete staged database cutover")
            while cutover.exists():
                time.sleep(2)
            LOG.info("staged database cutover completed; replication resumed")
        cadence = str(state["cadence"])
        retry, maximum = retry_values(config)
        target = int(common.latest(cadence_base(config, cadence), retry, maximum)["sequence"])
        next_sequence = int(state["sequence"]) + 1
        if next_sequence <= target:
            state = apply_one(config, cadence, next_sequence)
            state["cadence"] = cadence
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
