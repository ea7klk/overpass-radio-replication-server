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
FILTERED_ARTIFACT = re.compile(
    r"^filtered-(day|hour|minute)-([0-9]{9})-([0-9]{9})\.osm\.pbf$"
)


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


def clear_marker(config: dict[str, Any], key: str) -> None:
    path_value = config.get(key)
    if not path_value:
        return
    Path(str(path_value)).unlink(missing_ok=True)


def full_pbf_state(config: dict[str, Any]) -> dict[str, Any] | None:
    path = config.get("full_pbf_state_file")
    if not path:
        return None
    value = load_json(Path(str(path)), None)
    return value if isinstance(value, dict) else None


def full_pbf_contains_batch(
    config: dict[str, Any], cadence: str, last: int
) -> bool:
    state = full_pbf_state(config)
    return bool(
        state
        and state.get("cadence") == cadence
        and int(state.get("sequence", -1)) >= last
    )


def resume_full_pbf_apply(
    config: dict[str, Any],
    cadence: str,
    first: int,
    last: int,
    timestamp: str,
    payload: Path,
    next_pbf: Path,
) -> bool:
    journal_path = config.get("full_pbf_apply_journal_file")
    if not journal_path:
        return False
    path = Path(str(journal_path))
    journal = load_json(path, None)
    if not isinstance(journal, dict):
        return False
    if (
        journal.get("cadence") != cadence
        or int(journal.get("first", -1)) != first
        or int(journal.get("last", -1)) != last
    ):
        return False
    phase = journal.get("phase")
    if phase == "applying":
        next_pbf.unlink(missing_ok=True)
        path.unlink(missing_ok=True)
        LOG.info(
            "discarding incomplete full-PBF apply checkpoint for %s through %s; "
            "the source PBF remains unchanged",
            first,
            last,
        )
        return False
    if phase not in {"built", "switched"}:
        return False
    target_size = int(journal.get("target_size_bytes", -1))
    if phase == "built":
        if next_pbf.exists():
            next_pbf.replace(payload)
        elif target_size < 0 or payload.stat().st_size != target_size:
            return False
    record_current_planet_size(config, payload)
    write_json(
        config_path(config, "full_pbf_state_file"),
        {"cadence": cadence, "sequence": last, "timestamp": timestamp},
    )
    path.unlink(missing_ok=True)
    LOG.info(
        "recovered full Planet PBF apply for %s batch %s through %s without "
        "reapplying OSC files",
        cadence,
        first,
        last,
    )
    return True


def record_current_planet_size(config: dict[str, Any], payload: Path) -> None:
    metadata_path = config.get("planet_metadata_file")
    if not metadata_path:
        return
    metadata = load_json(Path(str(metadata_path)), {})
    if not isinstance(metadata, dict):
        metadata = {}
    metadata["current_size_bytes"] = payload.stat().st_size
    write_json(Path(str(metadata_path)), metadata)


def planet_download_metadata(config: dict[str, Any]) -> dict[str, Any] | None:
    path = config.get("planet_metadata_file")
    if not path:
        return None
    value = load_json(Path(str(path)), None)
    return value if isinstance(value, dict) else None


def new_planet_snapshot(config: dict[str, Any]) -> dict[str, Any] | None:
    """Return completed Planet metadata when it is newer than the imported snapshot."""
    planet = planet_download_metadata(config)
    snapshot_path = config.get("snapshot_metadata_file")
    snapshot = (
        load_json(Path(str(snapshot_path)), None)
        if snapshot_path
        else None
    )
    if not isinstance(planet, dict) or not isinstance(snapshot, dict):
        return None
    planet_source = str(planet.get("source_file", ""))
    snapshot_source = str(snapshot.get("source_file", ""))
    planet_timestamp = str(planet.get("timestamp", ""))
    snapshot_timestamp = str(snapshot.get("timestamp", ""))
    timestamp_changed = bool(
        planet_timestamp and snapshot_timestamp and
        planet_timestamp != snapshot_timestamp
    )
    source_changed = bool(planet_source and snapshot_source and
                          planet_source != snapshot_source)
    # Older releases recorded the stable symlink rather than its payload name.
    # Do not treat that legacy spelling as a new snapshot unless the bootstrap
    # metadata also proves that the completed PBF timestamp changed.
    legacy_name = Path(str(config["planet_pbf"])).name
    if snapshot_source == legacy_name and not planet_timestamp:
        source_changed = False
    if not (source_changed or timestamp_changed):
        return None
    if not planet_timestamp:
        raise RuntimeError(
            "completed Planet metadata changed but has no snapshot timestamp; "
            "the PBF must be re-indexed before replication can resume"
        )
    return planet


def clear_replication_inputs(config: dict[str, Any], keep_filtered: Path | None = None) -> None:
    for cadence in CADENCES:
        directory = config_path(config, "raw_dir") / cadence
        if not directory.exists():
            continue
        for path in directory.iterdir():
            if path.is_file() and (path.name.endswith(".osc.gz") or path.name.endswith(".part")):
                path.unlink(missing_ok=True)
    work_dir = config_path(config, "work_dir")
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    filtered_dir = config_path(config, "filtered_dir")
    filtered_dir.mkdir(parents=True, exist_ok=True)
    for path in filtered_dir.glob("*.osm.pbf"):
        if keep_filtered is None or path != keep_filtered:
            path.unlink(missing_ok=True)
    for key in (
        "accepted_prefilter_marker_file",
        "ready_slot_file",
        "building_slot_file",
        "full_pbf_apply_journal_file",
        "filtered_import_state_file",
    ):
        clear_marker(config, key)


def reset_from_new_planet_snapshot(
    config: dict[str, Any], planet: dict[str, Any]
) -> dict[str, Any]:
    """Build and publish a filtered DB from a newly completed Planet PBF."""
    source = Path(str(planet["source_file"])).name
    timestamp = str(planet["timestamp"])
    filtered_dir = config_path(config, "filtered_dir")
    filtered_dir.mkdir(parents=True, exist_ok=True)
    filtered = filtered_dir / f"initial-{source}"
    if filtered.suffix != ".pbf":
        filtered = filtered.with_suffix(filtered.suffix + ".osm.pbf")
    reset_path = config.get("planet_reset_state_file")
    checkpoint = load_json(Path(str(reset_path)), None) if reset_path else None
    checkpoint_matches = (
        isinstance(checkpoint, dict) and
        checkpoint.get("source_file") == source and
        checkpoint.get("timestamp") == timestamp
    )
    if not checkpoint_matches:
        clear_replication_inputs(config, filtered if filtered.exists() else None)
        checkpoint = {
            "phase": "filtering",
            "source_file": source,
            "timestamp": timestamp,
        }
        if reset_path:
            write_json(Path(str(reset_path)), checkpoint)
    slot = str(checkpoint.get("slot", "")) if isinstance(checkpoint, dict) else ""
    if slot not in {"blue", "green"}:
        slot = inactive_slot(config)
    if not filtered.exists():
        temporary = filtered.with_name(f".{filtered.stem}.part{filtered.suffix}")
        temporary.unlink(missing_ok=True)
        filtered.with_suffix(filtered.suffix + ".part").unlink(missing_ok=True)
        started = time.monotonic()
        LOG.info(
            "new Planet payload %s detected; generating filtered initial PBF %s",
            source,
            filtered,
        )
        subprocess.run(
            [
                "osmium", "tags-filter", str(config_path(config, "planet_pbf")),
                f"{config['tag_key_prefix']}*", "-o", str(temporary),
                "--progress", "--overwrite", "--verbose",
            ],
            check=True,
        )
        temporary.replace(filtered)
        LOG.info(
            "new Planet filtering completed in %.1fs: %s",
            time.monotonic() - started,
            filtered,
        )
        checkpoint["phase"] = "filtered"
        if reset_path:
            write_json(Path(str(reset_path)), checkpoint)
    else:
        LOG.info("reusing completed filtered PBF for new Planet payload: %s", filtered)
    if checkpoint.get("phase") != "imported" or not (slot_dir(config, slot) / "nodes.bin").exists():
        config["last_change_timestamp"] = timestamp
        checkpoint["phase"] = "importing"
        checkpoint["slot"] = slot
        if reset_path:
            write_json(Path(str(reset_path)), checkpoint)
        import_filtered_pbf(config, filtered, slot)
        checkpoint["phase"] = "imported"
        if reset_path:
            write_json(Path(str(reset_path)), checkpoint)
    else:
        LOG.info("new Planet filtered database import was already complete in %s", slot)
    boundary = first_sequence_after(config, "day", timestamp) - 1
    snapshot = {
        "source_file": source,
        "timestamp": timestamp,
        "replication_sequence": planet.get("replication_sequence"),
    }
    write_json(config_path(config, "snapshot_metadata_file"), snapshot)
    state = {
        "cadence": "day",
        "sequence": boundary,
        "timestamp": timestamp,
        "last_batch_applied_at": 0,
    }
    write_json(config_path(config, "full_pbf_state_file"), state)
    write_json(config_path(config, "state_file"), state)
    if reset_path:
        Path(str(reset_path)).unlink(missing_ok=True)
    LOG.info(
        "new Planet snapshot %s is active through %s; daily replication resumes at %s",
        source,
        timestamp,
        boundary + 1,
    )
    return state


def inactive_slot(config: dict[str, Any]) -> str:
    active = read_marker(config, "active_slot_file")
    if active not in {"blue", "green"}:
        return "blue"
    return "green" if active == "blue" else "blue"


def slot_dir(config: dict[str, Any], slot: str) -> Path:
    return config_path(config, "db_root") / slot


def pending_filtered_artifact(
    config: dict[str, Any], cadence: str, first: int, full_state: dict[str, Any]
) -> tuple[Path, int, str] | None:
    if full_state.get("cadence") != cadence:
        return None
    full_sequence = int(full_state.get("sequence", -1))
    if full_sequence < first:
        return None
    filtered_dir = config_path(config, "filtered_dir")
    candidates: list[tuple[int, Path]] = []
    for path in filtered_dir.glob(f"filtered-{cadence}-{first:09d}-*.osm.pbf"):
        match = FILTERED_ARTIFACT.match(path.name)
        if not match or int(match.group(2)) != first:
            continue
        last = int(match.group(3))
        if last == full_sequence:
            candidates.append((last, path))
    if not candidates:
        return None
    last, artifact = max(candidates)
    timestamp = str(full_state.get("timestamp", ""))
    if not timestamp:
        return None
    return artifact, last, timestamp


def recover_filtered_artifact(
    config: dict[str, Any], state: dict[str, Any]
) -> dict[str, Any] | None:
    cadence = str(state.get("cadence", ""))
    if cadence not in CADENCES:
        return None
    first = int(state.get("sequence", -1)) + 1
    full_state_path = config.get("full_pbf_state_file")
    if not full_state_path:
        return None
    full_state = load_json(Path(str(full_state_path)), None)
    if not isinstance(full_state, dict):
        return None
    recovery = pending_filtered_artifact(config, cadence, first, full_state)
    if recovery is None:
        return None
    artifact, last, timestamp = recovery
    completion_path = config.get("filtered_import_state_file")
    completion = (
        load_json(Path(str(completion_path)), None)
        if completion_path
        else None
    )
    completion_matches = (
        isinstance(completion, dict)
        and completion.get("cadence") == cadence
        and int(completion.get("first", -1)) == first
        and int(completion.get("last", -1)) == last
        and completion.get("slot") in {"blue", "green"}
        and (slot_dir(config, str(completion["slot"])) / "nodes.bin").exists()
    )
    LOG.info(
        "recovering existing filtered PBF %s for %s batch %s through %s; "
        "skipping full-PBF merge/apply",
        artifact,
        cadence,
        first,
        last,
    )
    config["last_change_timestamp"] = timestamp
    if completion_matches:
        slot = str(completion["slot"])
        write_marker(config, "ready_slot_file", slot)
        LOG.info(
            "filtered import for %s through %s was already complete in %s; "
            "skipping re-import",
            first,
            last,
            slot,
        )
    else:
        import_filtered_pbf(config, artifact, inactive_slot(config), cadence, first, last)
    for sequence in range(first, last + 1):
        (config_path(config, "raw_dir") / cadence / f"{sequence:09d}.osc.gz").unlink(
            missing_ok=True
        )
    recovered = {
        "cadence": cadence,
        "sequence": last,
        "timestamp": timestamp,
        "last_batch_applied_at": time.time(),
    }
    write_json(config_path(config, "state_file"), recovered)
    LOG.info(
        "recovered filtered database batch %s through %s; replication state advanced",
        first,
        last,
    )
    return recovered


def apply_full_changes(
    config: dict[str, Any], changes: list[Path], cadence: str, first: int, last: int,
    timestamp: str,
) -> None:
    current = config_path(config, "planet_pbf")
    payload = current.resolve() if current.is_symlink() else current
    next_pbf = payload.with_name(f".{payload.name}.{cadence}-{last}.osm.pbf")
    if resume_full_pbf_apply(
        config, cadence, first, last, timestamp, payload, next_pbf
    ):
        return
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
    next_pbf.unlink(missing_ok=True)
    journal_path = config.get("full_pbf_apply_journal_file")
    journal = {
        "phase": "applying",
        "cadence": cadence,
        "first": first,
        "last": last,
        "timestamp": timestamp,
        "payload": str(payload),
        "next_pbf": str(next_pbf),
    }
    if journal_path:
        write_json(Path(str(journal_path)), journal)
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
    journal["phase"] = "built"
    journal["target_size_bytes"] = next_pbf.stat().st_size
    if journal_path:
        write_json(Path(str(journal_path)), journal)
    # Keep planet-latest.osm.pbf as a stable symlink. Replacing the payload
    # itself removes the previous version instead of leaving a second full
    # Planet file beside the updated one.
    next_pbf.replace(payload)
    record_current_planet_size(config, payload)
    write_json(
        config_path(config, "full_pbf_state_file"),
        {"cadence": cadence, "sequence": last, "timestamp": timestamp},
    )
    journal["phase"] = "switched"
    if journal_path:
        write_json(Path(str(journal_path)), journal)
        Path(str(journal_path)).unlink(missing_ok=True)
    LOG.info(
        "full Planet PBF advanced through %s/%s in %.1fs",
        cadence, last,
        time.monotonic() - started,
    )
    merged.unlink(missing_ok=True)


def import_filtered_pbf(
    config: dict[str, Any],
    filtered: Path,
    slot: str,
    cadence: str | None = None,
    first: int | None = None,
    last: int | None = None,
) -> None:
    staging = slot_dir(config, slot)
    # The standby API pod shares this PVC. Mark the slot before touching it so
    # its inactive-slot retirement loop cannot remove the directory while
    # update_database is still creating the database files.
    write_marker(config, "building_slot_file", slot)
    try:
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
        if cadence is not None and first is not None and last is not None:
            write_json(
                config_path(config, "filtered_import_state_file"),
                {
                    "cadence": cadence,
                    "first": first,
                    "last": last,
                    "timestamp": str(config.get("last_change_timestamp", "")),
                    "slot": slot,
                },
            )
        write_marker(config, "ready_slot_file", slot)
        LOG.info("replacement database is ready in slot %s", slot)
    finally:
        clear_marker(config, "building_slot_file")


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
    if (
        filtered.exists()
        and full_pbf_state(config)
        and full_pbf_state(config).get("cadence") == cadence
        and int(full_pbf_state(config).get("sequence", -1)) >= last
    ):
        LOG.info(
            "reusing existing filtered PBF %s; full Planet PBF already contains "
            "batch through %s",
            filtered,
            last,
        )
    else:
        filtered.unlink(missing_ok=True)
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
    import_filtered_pbf(config, filtered, inactive_slot(config), cadence, first, last)


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
    timestamp = str(batch[-1][1]["timestamp"])
    if full_pbf_contains_batch(config, cadence, last):
        LOG.info(
            "full Planet PBF already contains %s batch %s through %s; "
            "skipping duplicate apply",
            cadence,
            first,
            last,
        )
    else:
        apply_full_changes(config, changes, cadence, first, last, timestamp)
    if any(item[2] for item in batch):
        rebuild_filtered(config, cadence, first, last, str(batch[-1][1]["timestamp"]))
    for change, _, _ in batch:
        change.unlink(missing_ok=True)
    return batch[-1][1]


def prefetch_replication_window(
    config: dict[str, Any], cadence: str, first: int, retry: int, maximum: int,
) -> tuple[list[tuple[Path, dict[str, Any], bool]], int]:
    target = int(common.latest(cadence_base(config, cadence), retry, maximum)["sequence"])
    if first > target:
        return [], target
    prefetch = max(1, int(config.get("prefetch_files", 10)))
    window_end = min(first + prefetch - 1, target)
    window = list(range(first, window_end + 1))
    LOG.info(
        "prefetching %s %s update files (%s through %s)",
        len(window), cadence, first, window_end,
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
    return batch, target


def prefetch_minute_while_hourly_apply_is_gated(
    config: dict[str, Any],
    hourly_batch: list[tuple[Path, dict[str, Any], bool]],
    retry: int,
    maximum: int,
    due_at: float,
) -> None:
    """Keep the minute queue warm while the hourly full-PBF apply is gated.

    Applying a minute file out of order would make the full Planet PBF
    inconsistent, but downloading and prefiltering it is safe. Polling must
    therefore continue during the gate instead of waiting after one initial
    prefetch window. ``next_sequence`` advances only after a complete window
    has been downloaded and prefiltered, so a restart can safely rediscover
    any files that were not completed.
    """
    if not hourly_batch:
        return
    minute_first = first_sequence_after(
        config, "minute", str(hourly_batch[-1][1]["timestamp"])
    )
    next_sequence = minute_first
    poll_seconds = max(1, int(config.get("poll_seconds", 60)))
    while time.time() < due_at:
        LOG.info(
            "polling minute replication while hourly apply is gated; "
            "next sequence %s",
            next_sequence,
        )
        minute_batch, minute_target = prefetch_replication_window(
            config, "minute", next_sequence, retry, maximum
        )
        if minute_batch:
            last = next_sequence + len(minute_batch) - 1
            LOG.info(
                "hourly apply is gated; downloaded and prefiltered minute files "
                "through %s (upstream target %s) for later ordered application",
                last,
                minute_target,
            )
            next_sequence = last + 1
            continue
        remaining = due_at - time.time()
        if remaining <= 0:
            break
        LOG.info(
            "minute replication is caught up through %s (upstream target %s); "
            "next remote scan in %ss",
            next_sequence - 1,
            minute_target,
            min(poll_seconds, int(remaining)),
        )
        time.sleep(min(poll_seconds, max(0.1, remaining)))


def download_until_caught_up(
    config: dict[str, Any], cadence: str, first: int, retry: int, maximum: int,
    due_at: float,
) -> list[tuple[Path, dict[str, Any], bool]]:
    """Prefetch one batch and wait for the hourly apply gate if necessary.

    The full Planet PBF is intentionally advanced in bounded batches. While
    an apply is cooling down, the next cadence's minute files can already be
    downloaded and prefiltered, but no out-of-order full-PBF rewrite starts.
    """
    batch, target = prefetch_replication_window(
        config, cadence, first, retry, maximum
    )
    if not batch:
        return []
    if cadence == "hour" and time.time() < due_at:
        prefetch_minute_while_hourly_apply_is_gated(
            config, batch, retry, maximum, due_at
        )
    if time.time() < due_at:
        LOG.info(
            "prefetch complete through %s; full-PBF apply is gated until %s",
            first + len(batch) - 1,
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
    new_snapshot = new_planet_snapshot(config)
    if new_snapshot is not None:
        state = reset_from_new_planet_snapshot(config, new_snapshot)
    elif not isinstance(state, dict):
        timestamp = str(metadata["timestamp"])
        state = {
            "cadence": "day",
            "sequence": first_sequence_after(config, "day", timestamp) - 1,
            "timestamp": timestamp,
        }
        write_json(state_path, state)
        LOG.info("starting raw global replication after fresh PBF boundary %s", timestamp)
    while True:
        recovered = recover_filtered_artifact(config, state)
        if recovered is not None:
            state = recovered
            continue
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
