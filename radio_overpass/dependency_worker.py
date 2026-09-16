"""Refresh newly discovered radio roots from public Overpass, separately."""

from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
import gzip
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from typing import Any

from . import replicator as common
from .minute_worker import queue_path, read_sequence, writer_lock


LOG = logging.getLogger("radio-overpass.dependencies")


def apply_osc_without_stopping_dispatcher(
    config: dict[str, Any], osc_path: Path, version: str
) -> None:
    """Apply one clean OSC directory while the Overpass dispatcher stays up."""
    update_binary = config["overpass_update_from_dir"]
    db_dir = Path(config["db_dir"])
    with tempfile.TemporaryDirectory(
        prefix="dependency-apply-", dir=config["work_dir"]
    ) as temporary:
        osc_dir = Path(temporary)
        shutil.copyfile(osc_path, osc_dir / "000000001.osc")
        command = [
            update_binary,
            f"--db-dir={db_dir}",
            f"--osc-dir={osc_dir}",
            f"--version={version}",
            "--flush-size=0",
        ]
        if config.get("meta_mode"):
            command.append(config["meta_mode"])
        LOG.info("applying dependency OSC through update_from_dir (dispatcher remains running)")
        try:
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            detail = common.overpass_update_diagnostics(exc.stderr)
            LOG.error("dependency update failed (exit %s)%s", exc.returncode, detail)
            raise
        common.log_overpass_update_diagnostics(result.stderr)


def save_compressed_inspection(config: dict[str, Any], osc_path: Path) -> Path:
    """Keep a compressed query-produced OSC beside the normal 24-hour artifact."""
    inspection = common.osc_inspection_directory(config, Path(config["work_dir"]))
    inspection.mkdir(parents=True, exist_ok=True)
    common.prune_osc_inspection_dir(inspection)
    filename = f"dependency-{time.time_ns()}-{osc_path.stem}.osc.gz"
    destination = inspection / filename
    temporary = destination.with_name(f".{destination.name}.tmp-{os.getpid()}")
    try:
        with osc_path.open("rb") as source, gzip.open(temporary, "wb", compresslevel=6) as zipped:
            shutil.copyfileobj(source, zipped, length=1024 * 1024)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    LOG.info("retained compressed dependency OSC for inspection: %s", destination)
    return destination


def load_task(path: Path) -> tuple[str, int]:
    value = json.loads(path.read_text(encoding="utf-8"))
    root = value.get("root")
    sequence = value.get("sequence")
    if not isinstance(root, str) or not isinstance(sequence, int):
        raise ValueError(f"invalid dependency queue entry: {path}")
    common.root_key_parts(root)
    return root, sequence


def cursors_caught_up(
    config: dict[str, Any], replica_dir: Path, sequence: int
) -> bool:
    db_dir = Path(config["db_dir"])
    db_sequence = read_sequence(db_dir / "replicate_id")
    published = read_sequence(replica_dir / "replicate_id", db_sequence)
    if db_sequence is None or published is None or db_sequence < sequence:
        return False
    if published != db_sequence:
        return False
    current = common.latest(
        config["minute_base_url"],
        int(config["retry_initial_seconds"]),
        int(config["retry_max_seconds"]),
    )
    return db_sequence >= int(current["sequence"])


def refresh_batch(
    config: dict[str, Any], roots: list[str], output_dir: Path
) -> set[str]:
    catalog_path = Path(config["catalog_file"])
    state = common.load_json(catalog_path, {})
    if not isinstance(state, dict):
        raise ValueError(f"catalog must contain an object: {catalog_path}")
    known = set(state.get("roots", [])) | set(state.get("dependencies", []))
    output_dir.mkdir(parents=True, exist_ok=True)
    workers = max(1, min(len(roots), int(config.get("dependency_query_workers", 4))))
    futures: dict[str, Future[common.RemoteRefresh]] = {}
    refreshes: dict[str, common.RemoteRefresh] = {}

    LOG.info("querying %d newly tagged roots with %s public-query worker(s)", len(roots), workers)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for root_key in roots:
            kind, object_id = common.root_key_parts(root_key)
            root_dir = output_dir / f"{kind}-{object_id}"
            root_dir.mkdir(parents=True, exist_ok=True)
            futures[root_key] = executor.submit(
                common.query_overpass,
                config,
                root_key,
                known,
                root_dir / "remote.osc",
            )
        for root_key in roots:
            try:
                refreshes[root_key] = futures[root_key].result()
            except common.OverpassRootNotFoundError as exc:
                LOG.warning("root %s is not visible in public Overpass yet: %s", root_key, exc)
            except Exception:
                LOG.exception("public dependency query failed for %s", root_key)

    if not refreshes:
        return set()

    objects: dict[str, bytes] = {}
    for root_key, refresh in refreshes.items():
        save_compressed_inspection(config, refresh.path)
        objects.update(refresh.objects)
        state = common.merge_remote_state(state, {root_key}, refresh)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with tempfile.TemporaryDirectory(prefix="dependency-batch-", dir=output_dir) as temporary:
        combined_osc = Path(temporary) / "combined.osc"
        common.remote_osc(combined_osc, objects, known)
        save_compressed_inspection(config, combined_osc)
        apply_osc_without_stopping_dispatcher(config, combined_osc, timestamp)
    common.atomic_json(catalog_path, state)
    for root_key, refresh in refreshes.items():
        common.log_remote_objects(refresh, {root_key}, "dependency worker batch")
    LOG.info("applied dependency batch for %d root(s)", len(refreshes))
    return set(refreshes)


def run(config: dict[str, Any]) -> None:
    work_dir = Path(config["work_dir"])
    queue_dir = Path(config.get("dependency_queue_dir", str(work_dir / "dependency-queue")))
    replica_dir = Path(config.get("filtered_replica_dir", str(work_dir / "filtered-replica")))
    lock_path = Path(config.get("writer_lock", Path(config["db_dir"]).parent / ".database-writer.lock"))
    query_dir = work_dir / "dependency-queries"
    work_dir.mkdir(parents=True, exist_ok=True)
    query_dir.mkdir(parents=True, exist_ok=True)
    retry_wait = max(1, int(config.get("retry_initial_seconds", 15)))
    retry_max = max(retry_wait, int(config.get("retry_max_seconds", 900)))
    poll = max(1, int(config.get("dependency_poll_seconds", 30)))
    batch_size = max(1, int(config.get("dependency_query_batch_size", 20)))

    while True:
        tasks = sorted(queue_dir.glob("*.json")) if queue_dir.exists() else []
        if not tasks:
            time.sleep(poll)
            continue
        try:
            catalog = common.load_json(Path(config["catalog_file"]), {})
            active_roots = set(catalog.get("roots", [])) if isinstance(catalog, dict) else set()
            batch: list[tuple[Path, str, int]] = []
            for task_path in tasks:
                root_key, sequence = load_task(task_path)
                if root_key not in active_roots:
                    LOG.info("discarding stale dependency task for removed root %s", root_key)
                    task_path.unlink(missing_ok=True)
                    continue
                batch.append((task_path, root_key, sequence))
                if len(batch) >= batch_size:
                    break
            if not batch:
                continue
            max_sequence = max(sequence for _, _, sequence in batch)
            if not cursors_caught_up(config, replica_dir, max_sequence):
                LOG.info(
                    "%d queued root(s) through minute/%s; waiting for minute applier to catch up",
                    len(batch),
                    max_sequence,
                )
                time.sleep(poll)
                continue
            with writer_lock(lock_path):
                if not cursors_caught_up(config, replica_dir, max_sequence):
                    continue
                completed = refresh_batch(
                    config,
                    [root_key for _, root_key, _ in batch],
                    query_dir,
                )
                for task_path, root_key, _ in batch:
                    if root_key in completed:
                        task_path.unlink(missing_ok=True)
                if completed:
                    retry_wait = max(1, int(config.get("retry_initial_seconds", 15)))
            if len(completed) < len(batch):
                time.sleep(retry_wait)
        except Exception:
            LOG.exception("dependency task %s failed; retrying in %ss", task_path, retry_wait)
            time.sleep(retry_wait)
            retry_wait = min(retry_max, max(retry_wait * 2, 1))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = json.loads(args.config.read_text(encoding="utf-8"))
    run(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
