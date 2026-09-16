"""Filter minute diffs into an official Overpass replica directory.

The companion apply_osc_to_db.sh process is the only consumer of this
directory. The database's replicate_id is its durable commit cursor; filter
membership is committed only after that cursor reaches a published batch.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import gzip
import json
import logging
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any, Iterator

from . import replicator as common


LOG = logging.getLogger("radio-overpass.minute-filter")
EMPTY_OSC = (
    b"<?xml version='1.0' encoding='UTF-8'?>\n"
    b'<osmChange version="0.6" generator="radio-overpass">\n'
    b"<create></create><modify></modify><delete></delete>\n"
    b"</osmChange>\n"
)


def sequence_text(sequence: int, timestamp: str) -> str:
    return f"sequenceNumber={sequence}\ntimestamp={timestamp}\n"


def read_sequence(path: Path, default: int | None = None) -> int | None:
    try:
        value = int(path.read_text(encoding="ascii").strip())
    except FileNotFoundError:
        return default
    if value < 0:
        raise ValueError(f"invalid replication cursor in {path}: {value}")
    return value


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def atomic_sequence(path: Path, sequence: int) -> None:
    atomic_text(path, f"{sequence}\n")


@contextmanager
def writer_lock(path: Path) -> Iterator[None]:
    """Exclusive lock shared by batch publication and dependency updates."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def replica_path(directory: Path, sequence: int, suffix: str) -> Path:
    return directory / f"{common.sequence_path(sequence)}{suffix}"


def delta_path(work_dir: Path, sequence: int) -> Path:
    return work_dir / "pending-deltas" / f"{sequence:09d}.jsonl"


def queue_path(queue_dir: Path, root_key: str) -> Path:
    return queue_dir / (root_key.replace(":", "-") + ".json")


def ensure_empty_osc(path: Path) -> None:
    path.write_bytes(EMPTY_OSC)


def gzip_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp-{os.getpid()}")
    try:
        with source.open("rb") as raw, gzip.open(temporary, "wb", compresslevel=6) as zipped:
            shutil.copyfileobj(raw, zipped, length=1024 * 1024)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def stage_change(
    config: dict[str, Any],
    sequence: int,
    membership: Path,
    replica_dir: Path,
    work_dir: Path,
) -> dict[str, Any]:
    """Download/filter one minute and atomically stage it for the official applier."""
    base = config["minute_base_url"]
    retry = int(config["retry_initial_seconds"])
    max_wait = int(config["retry_max_seconds"])
    state = common.fetch_state(base, sequence, retry, max_wait)
    if int(state["sequence"]) != sequence:
        raise RuntimeError(f"upstream returned state for unexpected sequence {sequence}")

    change_url = common.urls(base, sequence)[1]
    LOG.info("downloading minute/%s from %s", sequence, change_url)
    download_path = work_dir / "downloads" / f"minute-{sequence:09d}.osc.gz"
    download_path.parent.mkdir(parents=True, exist_ok=True)
    downloaded = common.download_change(config, "minute", sequence, download_path)
    temporary = Path(tempfile.mkdtemp(prefix=f"minute-{sequence}-", dir=work_dir))
    try:
        old_state = common.load_json(membership, {})
        if not isinstance(old_state, dict):
            raise ValueError(f"membership state is not an object: {membership}")
        retained = set(old_state.get("roots", [])) | set(old_state.get("dependencies", []))
        filtered_path = temporary / "filtered.osc"
        events_path = temporary / "delta.jsonl"
        reason = common.quick_check(
            downloaded.path,
            config["tag_key_prefix"],
            retained,
            temporary / "quick-check.ids",
        )
        if reason is None:
            ensure_empty_osc(filtered_path)
            events_path.write_text("", encoding="utf-8")
            LOG.info("minute/%s has no retained or matching objects", sequence)
        else:
            include: set[str] = set()
            old_dependencies = set(old_state.get("dependencies", []))
            pass_number = 0
            while True:
                if pass_number:
                    ids_path = temporary / "quick-check.ids"
                    if not common.write_id_patterns(ids_path, include) or not common.gzip_contains(
                        downloaded.path, pattern_path=ids_path
                    ):
                        LOG.info(
                            "minute/%s dependency replay pass %s has no new referenced objects",
                            sequence,
                            pass_number + 1,
                        )
                        break
                common.filter_file(
                    config,
                    downloaded.path,
                    membership,
                    filtered_path,
                    events_path,
                    include,
                )
                candidate_items = common.delta_items(events_path)
                candidate_state = common.delta_state(candidate_items)
                if candidate_state is None:
                    raise RuntimeError(f"filter produced no final membership state for {sequence}")
                discovered = set(candidate_state.get("dependencies", [])) - (
                    old_dependencies | include
                )
                if not discovered:
                    break
                include.update(discovered)
                pass_number += 1
            if not filtered_path.is_file() or filtered_path.stat().st_size == 0:
                ensure_empty_osc(filtered_path)

        items = common.delta_items(events_path)
        next_state = common.delta_state(items) or old_state
        common.retain_osc_artifact(config, filtered_path)

        staged_osc = replica_path(replica_dir, sequence, ".osc.gz")
        gzip_atomic(filtered_path, staged_osc)
        state_file = replica_path(replica_dir, sequence, ".state.txt")
        atomic_text(state_file, sequence_text(sequence, str(state["timestamp"])))
        staged_delta = delta_path(work_dir, sequence)
        staged_delta.parent.mkdir(parents=True, exist_ok=True)
        temporary_delta = staged_delta.with_name(f".{staged_delta.name}.tmp-{os.getpid()}")
        shutil.copyfile(events_path, temporary_delta)
        os.replace(temporary_delta, staged_delta)
        common.atomic_json(membership, next_state)
        LOG.info(
            "staged minute/%s (%s); filtered=%s, membership roots=%s dependencies=%s",
            sequence,
            state["timestamp"],
            reason or "empty",
            len(next_state.get("roots", [])),
            len(next_state.get("dependencies", [])),
        )
        return state
    finally:
        downloaded.path.unlink(missing_ok=True)
        shutil.rmtree(temporary, ignore_errors=True)


def commit_applied_state(
    config: dict[str, Any],
    sequence: int,
    work_dir: Path,
    queue_dir: Path,
) -> None:
    membership = Path(config["catalog_file"])
    membership_cursor = Path(
        config.get("membership_cursor_file", str(work_dir / "membership-replicate-id"))
    )
    old_cursor = read_sequence(membership_cursor, sequence)
    if old_cursor is None:
        old_cursor = sequence
    if old_cursor > sequence:
        raise RuntimeError(
            f"membership cursor {old_cursor} is ahead of database cursor {sequence}"
        )
    previous = common.load_json(membership, {})
    known_roots = set(previous.get("roots", [])) if isinstance(previous, dict) else set()
    final_state: dict[str, Any] | None = None
    newly_seen: dict[str, int] = {}
    for current in range(old_cursor + 1, sequence + 1):
        current_delta = delta_path(work_dir, current)
        if not current_delta.exists():
            continue
        items = common.delta_items(current_delta)
        for item in items:
            if item.get("op") == "add" and item.get("root"):
                key = str(item["id"])
                if key not in known_roots:
                    newly_seen[key] = current
        state = common.delta_state(items)
        if state is not None:
            final_state = state
            known_roots = set(state.get("roots", []))

    # Queue before advancing the membership cursor so a restart can safely
    # repeat these idempotent entries if it is interrupted during commit.
    queue_dir.mkdir(parents=True, exist_ok=True)
    for root_key, discovered_at in sorted(newly_seen.items()):
        common.atomic_json(
            queue_path(queue_dir, root_key),
            {"root": root_key, "sequence": discovered_at},
        )
    if final_state is not None:
        common.atomic_json(membership, final_state)
    atomic_sequence(membership_cursor, sequence)
    for path in (work_dir / "pending-deltas").glob("*.jsonl"):
        try:
            if int(path.stem) <= sequence:
                path.unlink(missing_ok=True)
        except ValueError:
            continue
    if newly_seen:
        LOG.info("queued %d newly tagged root(s) for dependency refresh", len(newly_seen))
    LOG.info("committed filtered membership through minute/%s", sequence)


def wait_for_applier(db_dir: Path, published: int, poll_seconds: int) -> int:
    marker = db_dir / "replicate_id"
    last_logged: int | None = None
    while True:
        current = read_sequence(marker)
        if current is None:
            raise RuntimeError(f"database replicate_id disappeared: {marker}")
        if current >= published:
            return current
        if current != last_logged:
            LOG.info("official applier at %s; waiting for published batch through %s", current, published)
            last_logged = current
        time.sleep(max(1, poll_seconds))


def run(config: dict[str, Any]) -> None:
    db_dir = Path(config["db_dir"])
    work_dir = Path(config["work_dir"])
    replica_dir = Path(config.get("filtered_replica_dir", str(work_dir / "filtered-replica")))
    queue_dir = Path(config.get("dependency_queue_dir", str(work_dir / "dependency-queue")))
    membership_cursor = Path(
        config.get("membership_cursor_file", str(work_dir / "membership-replicate-id"))
    )
    lock_path = Path(config.get("writer_lock", str(db_dir.parent / ".database-writer.lock")))
    poll = max(1, int(config.get("poll_seconds", 60)))
    batch_size = max(1, int(config.get("minute_update_batch_size", 100)))
    work_dir.mkdir(parents=True, exist_ok=True)
    replica_dir.mkdir(parents=True, exist_ok=True)
    common.osc_inspection_directory(config, work_dir).mkdir(parents=True, exist_ok=True)

    while True:
        has_batch = False
        with writer_lock(lock_path):
            db_sequence = read_sequence(db_dir / "replicate_id")
            if db_sequence is None:
                raise RuntimeError(f"database has no initialized replicate_id: {db_dir}")
            published = read_sequence(replica_dir / "replicate_id", db_sequence)
            if published is None:
                published = db_sequence

            if published > db_sequence:
                pass
            else:
                if not membership_cursor.exists():
                    atomic_sequence(membership_cursor, db_sequence)
                if published < db_sequence:
                    atomic_sequence(replica_dir / "replicate_id", db_sequence)
                    published = db_sequence
                if published == db_sequence:
                    current = common.latest(
                        config["minute_base_url"],
                        int(config["retry_initial_seconds"]),
                        int(config["retry_max_seconds"]),
                    )
                    first = db_sequence + 1
                    last = min(current["sequence"], db_sequence + batch_size)
                    if first <= last:
                        working_membership = work_dir / "working-membership.json"
                        initial_state = common.load_json(Path(config["catalog_file"]), {})
                        if not isinstance(initial_state, dict):
                            raise ValueError("catalog must contain a JSON object")
                        common.atomic_json(working_membership, initial_state)
                        LOG.info(
                            "filtering minute updates %s..%s into one official-applier batch",
                            first,
                            last,
                        )
                        staged_last = db_sequence
                        for sequence in range(first, last + 1):
                            result = stage_change(
                                config,
                                sequence,
                                working_membership,
                                replica_dir,
                                work_dir,
                            )
                            staged_last = int(result["sequence"])
                        atomic_sequence(replica_dir / "replicate_id", staged_last)
                        working_membership.unlink(missing_ok=True)
                        published = staged_last
                        has_batch = True
                        LOG.info(
                            "published filtered replica batch through %s; apply_osc_to_db.sh will consume it",
                            staged_last,
                        )

        if published > db_sequence:
            LOG.info(
                "replica batch %s is still applying; database cursor is %s",
                published,
                db_sequence,
            )
            reached = wait_for_applier(db_dir, published, poll)
            with writer_lock(lock_path):
                commit_applied_state(config, reached, work_dir, queue_dir)
            continue

        if not has_batch:
            time.sleep(poll)


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
