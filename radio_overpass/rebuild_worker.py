#!/usr/bin/env python3
"""Single-process daily -> hourly -> minute rebuild and replication lane.

The initial snapshot is imported by the init container into an isolated
database directory. This worker then advances one official change at a time,
using the permanent Overpass apply_osc_to_db.sh consumer. A source file that
does not contain a communication:amateur_radio* tag key is rejected by a
native gzip/grep scan before the XML parser is started.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import logging
from pathlib import Path
import time
from typing import Any

from . import replicator as common


LOG = logging.getLogger("radio-overpass.rebuild")
CADENCES = ("day", "hour", "minute")


def base_for(config: dict[str, Any], cadence: str) -> str:
    key = "daily_base_url" if cadence == "day" else f"{cadence}_base_url"
    return str(config[key])


def retry_values(config: dict[str, Any]) -> tuple[int, int]:
    return (
        int(config.get("retry_initial_seconds", 15)),
        int(config.get("retry_max_seconds", 900)),
    )


def first_sequence_after(
    config: dict[str, Any], cadence: str, timestamp: str
) -> int:
    explicit = config.get(f"{cadence}_start_sequence")
    if explicit is not None:
        return int(explicit)
    retry, max_wait = retry_values(config)
    base = base_for(config, cadence)
    upper = int(common.latest(base, retry, max_wait)["sequence"])
    lo, hi = 1, upper + 1
    while lo < hi:
        mid = (lo + hi) // 2
        state = common.fetch_state(base, mid, retry, max_wait)
        if state["timestamp"] <= timestamp:
            lo = mid + 1
        else:
            hi = mid
    LOG.info(
        "%s boundary after %s is sequence %s (previous=%s)",
        cadence,
        timestamp,
        lo,
        lo - 1,
    )
    return lo


def active_db(config: dict[str, Any]) -> Path:
    complete = Path(config["cutover_complete_file"])
    if complete.exists():
        return Path(config["db_dir"])
    return Path(config["rebuild_db_dir"])


def write_checkpoint(config: dict[str, Any], checkpoint: dict[str, Any]) -> None:
    common.atomic_json(Path(config["state_file"]), checkpoint)


def initial_checkpoint(config: dict[str, Any]) -> dict[str, Any]:
    metadata = common.load_json(Path(config["snapshot_metadata_file"]), None)
    if not isinstance(metadata, dict) or not isinstance(metadata.get("timestamp"), str):
        raise RuntimeError("initial PBF metadata is missing or has no timestamp")
    timestamp = str(metadata["timestamp"])
    sequence = first_sequence_after(config, "day", timestamp)
    return {
        "phase": "day",
        "cadence": "day",
        "sequence": sequence - 1,
        "timestamp": timestamp,
        "snapshot": metadata.get("source_file", "planet-260907.osm.pbf"),
    }


def advance_snapshot_boundary(config: dict[str, Any], checkpoint: dict[str, Any]) -> None:
    """Advance the private official cursor across the PBF boundary.

    The PBF already contains the tagged objects and the references retained by
    osmium. Reverse/dependent closure queries begin only for relevant objects
    found in later replication changes. An empty official change keeps
    apply_osc_to_db.sh aligned without replaying the snapshot boundary.
    """
    boundary = int(checkpoint["sequence"])
    cursor_path = Path(config["db_dir"]) / "replicate_id"
    try:
        current = int(cursor_path.read_text(encoding="ascii").strip())
    except (FileNotFoundError, ValueError) as exc:
        raise RuntimeError(f"private database has no valid replicate_id: {cursor_path}") from exc
    if current >= boundary:
        LOG.info("private official cursor already at snapshot boundary %s", current)
        return
    if current != boundary - 1:
        raise RuntimeError(
            f"private official cursor {current} cannot advance to snapshot boundary {boundary}"
        )
    empty_path = Path(config["work_dir"]) / "empty-snapshot-boundary.osc"
    common.empty_osc(empty_path)
    config["_official_sequence"] = boundary
    common.update_database(
        config,
        empty_path,
        str(checkpoint["timestamp"]),
        description="empty snapshot-boundary change",
    )
    LOG.info("advanced private official cursor to snapshot boundary %s", boundary)


def transition(
    config: dict[str, Any], checkpoint: dict[str, Any], cadence: str
) -> dict[str, Any]:
    index = CADENCES.index(cadence)
    if index + 1 >= len(CADENCES):
        return checkpoint
    next_cadence = CADENCES[index + 1]
    sequence = first_sequence_after(config, next_cadence, checkpoint["timestamp"])
    reset_file = Path(config["phase_reset_requested_file"])
    reset_complete = Path(config["phase_reset_complete_file"])
    reset_complete.unlink(missing_ok=True)
    reset_file.write_text(f"{sequence - 1}\n", encoding="ascii")
    LOG.info(
        "pausing the private official applier to reset its cursor to %s for %s",
        sequence - 1,
        next_cadence,
    )
    deadline = time.monotonic() + int(config.get("official_apply_timeout_seconds", 3600))
    while not reset_complete.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"private official applier did not reset to {sequence - 1}"
            )
        time.sleep(2)
    next_checkpoint = {
        "phase": next_cadence,
        "cadence": next_cadence,
        "sequence": sequence - 1,
        "timestamp": checkpoint["timestamp"],
    }
    LOG.info(
        "caught up %s through %s; switching to %s at sequence %s",
        cadence,
        checkpoint["sequence"],
        next_cadence,
        sequence,
    )
    write_checkpoint(config, next_checkpoint)
    return next_checkpoint


def catch_up_one_phase(
    config: dict[str, Any], checkpoint: dict[str, Any]
) -> tuple[dict[str, Any], bool]:
    cadence = str(checkpoint["cadence"])
    base = base_for(config, cadence)
    retry, max_wait = retry_values(config)
    target = int(common.latest(base, retry, max_wait)["sequence"])
    next_sequence = int(checkpoint["sequence"]) + 1
    if next_sequence > target:
        return checkpoint, False

    LOG.info(
        "processing exactly one %s replication file %s (target=%s); no update batching",
        cadence,
        next_sequence,
        target,
    )
    work_dir = Path(config["work_dir"])
    work_dir.mkdir(parents=True, exist_ok=True)
    download_path = work_dir / "downloads" / f"{cadence}-{next_sequence:09d}.osc.gz"
    downloaded = common.download_change(config, cadence, next_sequence, download_path)
    config["_official_sequence"] = next_sequence
    try:
        result = common.process_one(config, cadence, downloaded)
    except Exception:
        # process_one removes the downloaded source in its finally block. The
        # checkpoint remains unchanged, so the complete file is downloaded and
        # retried on the next loop.
        raise
    next_checkpoint = {
        "phase": cadence,
        "cadence": cadence,
        "sequence": int(result["sequence"]),
        "timestamp": str(result["timestamp"]),
    }
    write_checkpoint(config, next_checkpoint)
    return next_checkpoint, True


def request_cutover(config: dict[str, Any]) -> None:
    requested = Path(config["cutover_requested_file"])
    if requested.exists() or Path(config["cutover_complete_file"]).exists():
        return
    requested.parent.mkdir(parents=True, exist_ok=True)
    requested.write_text("replacement database caught up\n", encoding="utf-8")
    LOG.info(
        "replacement database is caught up through minute/%s; requested public cutover",
        common.load_json(Path(config["state_file"]), {}).get("sequence"),
    )


def run(config: dict[str, Any]) -> None:
    state_path = Path(config["state_file"])
    Path(config["work_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["official_replica_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["osc_inspection_dir"]).mkdir(parents=True, exist_ok=True)

    checkpoint = common.load_json(state_path, None)
    if not isinstance(checkpoint, dict):
        config["db_dir"] = str(active_db(config))
        checkpoint = initial_checkpoint(config)
        advance_snapshot_boundary(config, checkpoint)
        write_checkpoint(config, checkpoint)
        LOG.info(
            "starting rebuild from %s in %s; PBF source is not copied or removed; "
            "external dependency/dependent queries begin only for later relevant changes",
            checkpoint["snapshot"],
            config["rebuild_db_dir"],
        )

    last_idle = 0.0
    while True:
        config["db_dir"] = str(active_db(config))
        try:
            checkpoint, advanced = catch_up_one_phase(config, checkpoint)
            if advanced:
                last_idle = 0.0
                continue

            cadence = str(checkpoint["cadence"])
            if cadence != "minute":
                checkpoint = transition(config, checkpoint, cadence)
                continue

            if not Path(config["cutover_complete_file"]).exists():
                request_cutover(config)
                now = time.monotonic()
                if now >= last_idle:
                    LOG.info(
                        "minute stream caught up; waiting for the public Overpass cutover handshake"
                    )
                    last_idle = now + 60
                time.sleep(5)
                continue

            retry, _ = retry_values(config)
            time.sleep(max(1, int(config.get("poll_seconds", retry))))
        except Exception:
            LOG.exception(
                "replication failed in %s at sequence %s; retaining checkpoint and retrying",
                checkpoint.get("cadence"),
                checkpoint.get("sequence"),
            )
            time.sleep(max(1, int(config.get("retry_initial_seconds", 15))))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    run(json.loads(args.config.read_text(encoding="utf-8")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
