#!/usr/bin/env python3
"""Refresh the full Planet PBF and rebuild the filtered Overpass database.

This process is run by a Kubernetes CronJob. The full Planet PBF is the only
source of truth: pyosmium catches it up directly through the minutely
replication service, then osmium creates the current radio-tagged extract.
The extract is imported into the inactive blue/green database slot and the
Overpass pods perform the existing readiness-gated cutover.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
from pathlib import Path
import subprocess
import time
from typing import Any

from . import full_pbf_worker as common


LOG = logging.getLogger("radio-overpass.scheduled-rebuild")


def load_config(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"configuration is not an object: {path}")
    return value


def path(config: dict[str, Any], key: str) -> Path:
    return Path(str(config[key]))


def load_metadata(config: dict[str, Any]) -> dict[str, Any]:
    metadata = common.load_json(path(config, "planet_metadata_file"), None)
    if not isinstance(metadata, dict) or not metadata.get("timestamp"):
        raise RuntimeError("planet-download.json has no usable timestamp")
    return metadata


def update_planet_metadata(config: dict[str, Any]) -> dict[str, Any]:
    """Refresh age and size metadata after pyosmium changes the PBF."""
    planet = path(config, "planet_pbf")
    info = json.loads(
        subprocess.check_output(
            ["osmium", "fileinfo", "--json", "--no-crc", str(planet)],
            text=True,
        )
    )
    options = info.get("header", {}).get("option", {})
    timestamp = options.get("osmosis_replication_timestamp")
    if not timestamp:
        raise RuntimeError(f"updated Planet PBF has no replication timestamp: {planet}")
    sequence = options.get("osmosis_replication_sequence_number")
    metadata = common.load_json(path(config, "planet_metadata_file"), {})
    if not isinstance(metadata, dict):
        metadata = {}
    metadata.update(
        {
            "local_file": planet.name,
            "current_size_bytes": planet.stat().st_size,
            "timestamp": timestamp,
            "replication_sequence": int(sequence) if sequence is not None else None,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    )
    common.write_json(path(config, "planet_metadata_file"), metadata)
    common.write_json(
        path(config, "snapshot_metadata_file"),
        {
            "source_file": metadata.get("source_file", planet.name),
            "local_file": planet.name,
            "timestamp": timestamp,
            "replication_sequence": metadata.get("replication_sequence"),
        },
    )
    LOG.info(
        "Planet metadata updated: timestamp=%s sequence=%s size=%s",
        timestamp,
        sequence,
        metadata["current_size_bytes"],
    )
    return metadata


def planet_replication_source(config: dict[str, Any]) -> str:
    """Read the small replication URL header without scanning Planet objects."""
    planet = path(config, "planet_pbf")
    info = json.loads(
        subprocess.check_output(
            ["osmium", "fileinfo", "--json", "--no-crc", str(planet)],
            text=True,
        )
    )
    options = info.get("header", {}).get("option", {})
    return str(options.get("osmosis_replication_base_url", "")).rstrip("/")


def run_until_current(
    config: dict[str, Any], server: str, *, ignore_osmosis_headers: bool = False
) -> None:
    """Run the requested command until pyosmium reports no remaining data."""
    planet = path(config, "planet_pbf")
    while True:
        LOG.info("running pyosmium-up-to-date against %s", server)
        command = [
            "pyosmium-up-to-date",
            "-vvv",
            "--size",
            "10000",
            "--server",
            server,
        ]
        if ignore_osmosis_headers:
            command.append("--ignore-osmosis-headers")
        command.append(str(planet))
        result = subprocess.run(
            command,
            check=False,
        )
        if result.returncode == 0:
            LOG.info("pyosmium-up-to-date caught %s up to the current server state", planet)
            return
        if result.returncode == 1:
            LOG.info(
                "pyosmium-up-to-date applied a partial batch for %s; continuing until current",
                server,
            )
            time.sleep(int(config.get("retry_seconds", 15)))
            continue
        raise subprocess.CalledProcessError(result.returncode, result.args)


def extract_filtered(config: dict[str, Any]) -> Path:
    planet = path(config, "planet_pbf")
    filtered = path(config, "filtered_pbf")
    filtered.parent.mkdir(parents=True, exist_ok=True)
    # Keep .pbf as the final suffix so osmium selects the PBF writer.
    temporary = filtered.with_name(f".{filtered.stem}.part{filtered.suffix}")
    temporary.unlink(missing_ok=True)
    started = time.monotonic()
    LOG.info(
        "creating current filtered extract with osmium tags-filter: %s",
        filtered,
    )
    subprocess.run(
        [
            "osmium",
            "tags-filter",
            str(planet),
            f"{config['tag_key_prefix']}*",
            "-o",
            str(temporary),
            "--progress",
            "--overwrite",
            "--verbose",
        ],
        check=True,
    )
    temporary.replace(filtered)
    LOG.info(
        "filtered extract completed in %.1fs: %s (%s bytes)",
        time.monotonic() - started,
        filtered,
        filtered.stat().st_size,
    )
    return filtered


def active_slot(config: dict[str, Any]) -> str:
    value = common.read_marker(config, "active_slot_file")
    if value not in {"blue", "green"}:
        raise RuntimeError("active-slot marker is missing or invalid")
    return value


def wait_for_cutover(config: dict[str, Any], slot: str) -> None:
    ready = path(config, "ready_slot_file")
    active = path(config, "active_slot_file")
    deadline = time.monotonic() + int(config.get("cutover_timeout_seconds", 1800))
    while time.monotonic() < deadline:
        if common.read_marker(config, "active_slot_file") == slot:
            LOG.info("blue/green cutover completed; active slot is %s", slot)
            return
        LOG.info(
            "replacement database is ready in slot %s; waiting for Overpass cutover "
            "(active=%s, ready=%s)",
            slot,
            active.read_text(encoding="ascii").strip() if active.exists() else "",
            ready.read_text(encoding="ascii").strip() if ready.exists() else "",
        )
        time.sleep(5)
    raise TimeoutError(f"Overpass did not activate replacement slot {slot}")


def run(config: dict[str, Any]) -> None:
    planet = path(config, "planet_pbf")
    if not planet.is_file() or planet.stat().st_size == 0:
        raise RuntimeError(f"Planet PBF is missing: {planet}")

    # The full Planet PBF is the starting snapshot. Update it directly from
    # the minute service; the snapshot header may name the hourly service, so
    # explicitly allow the intended minute-service handoff.
    minute_server = str(config["minute_base_url"])
    embedded_source = planet_replication_source(config)
    ignore_headers = embedded_source != minute_server.rstrip("/")
    if ignore_headers:
        LOG.info(
            "Planet PBF header points to %s; switching to minute replication "
            "with --ignore-osmosis-headers for this initial handoff",
            embedded_source or "no replication service",
        )
    run_until_current(
        config,
        minute_server,
        ignore_osmosis_headers=ignore_headers,
    )
    metadata = update_planet_metadata(config)
    filtered = extract_filtered(config)

    current = active_slot(config)
    replacement = "green" if current == "blue" else "blue"
    config["last_change_timestamp"] = str(metadata["timestamp"])
    LOG.info(
        "importing %s into inactive Overpass slot %s while slot %s remains active",
        filtered,
        replacement,
        current,
    )
    common.import_filtered_pbf(config, filtered, replacement)
    wait_for_cutover(config, replacement)


def run_locked(config: dict[str, Any]) -> None:
    """Prevent a manually-triggered run from racing the CronJob."""
    state_dir = path(config, "state_dir")
    state_dir.mkdir(parents=True, exist_ok=True)
    lock_path = state_dir / "scheduled-rebuild.lock"
    with lock_path.open("w", encoding="ascii") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"another scheduled rebuild is already running: {lock_path}") from exc
        run(config)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    run_locked(load_config(args.config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
