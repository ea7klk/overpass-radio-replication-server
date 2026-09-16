from __future__ import annotations

import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from radio_overpass import minute_worker


class MinuteWorkerTest(unittest.TestCase):
    def test_prefetch_window_returns_to_one_file_near_upstream_tip(self) -> None:
        self.assertEqual(minute_worker.next_batch_end(100, 150, 20), 120)
        self.assertEqual(minute_worker.next_batch_end(100, 120, 20), 101)
        self.assertEqual(minute_worker.next_batch_end(100, 100, 20), 100)

    def test_sequence_state_and_standard_replica_path(self) -> None:
        self.assertEqual(
            minute_worker.sequence_text(7275790, "2026-09-07T00:00:21Z"),
            "sequenceNumber=7275790\ntimestamp=2026-09-07T00:00:21Z\n",
        )
        self.assertEqual(
            minute_worker.replica_path(Path("/mirror"), 7275790, ".osc.gz"),
            Path("/mirror/007/275/790.osc.gz"),
        )

    def test_staged_empty_diff_is_a_valid_gzip_osc_and_state_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            download = root / "downloads" / "source.osc.gz"
            download.parent.mkdir()
            with gzip.open(download, "wb") as zipped:
                zipped.write(b"unused by mocked quick check")
            membership = root / "catalog.json"
            membership.write_text(
                json.dumps({"roots": ["node:1"], "dependencies": [], "refs": {}}),
                encoding="utf-8",
            )
            (root / "work").mkdir()
            config = {
                "minute_base_url": "https://replication.invalid/minute/",
                "retry_initial_seconds": 1,
                "retry_max_seconds": 2,
                "tag_key_prefix": "communication:amateur_radio",
                "catalog_file": str(membership),
                "work_dir": str(root / "work"),
                "osc_inspection_dir": str(root / "inspection"),
            }
            state = {"sequence": 7275790, "timestamp": "2026-09-07T00:00:21Z"}
            with (
                patch.object(minute_worker.common, "fetch_state", return_value=state),
                patch.object(
                    minute_worker.common,
                    "download_change",
                    return_value=minute_worker.common.DownloadedChange(download, state, 0.1),
                ),
                patch.object(minute_worker.common, "quick_check", return_value=None),
                patch.object(minute_worker.common, "retain_osc_artifact"),
            ):
                result = minute_worker.stage_change(
                    config,
                    7275790,
                    membership,
                    root / "replica",
                    root / "work",
                )

            self.assertEqual(result, state)
            osc_path = minute_worker.replica_path(root / "replica", 7275790, ".osc.gz")
            with gzip.open(osc_path, "rb") as zipped:
                self.assertIn(b"<osmChange", zipped.read())
            state_path = minute_worker.replica_path(root / "replica", 7275790, ".state.txt")
            self.assertEqual(
                state_path.read_text(encoding="utf-8"),
                "sequenceNumber=7275790\ntimestamp=2026-09-07T00:00:21Z\n",
            )

    def test_type_collision_in_quick_check_stages_empty_minute(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            download = root / "downloads" / "source.osc.gz"
            download.parent.mkdir()
            source_osc = b'''<?xml version="1.0"?>
<osmChange version="0.6"><modify>
  <way id="1" version="2"><nd ref="10"/><nd ref="11"/></way>
</modify></osmChange>'''
            with gzip.open(download, "wb") as zipped:
                zipped.write(source_osc)

            membership = root / "catalog.json"
            old_state = {"roots": ["node:1"], "dependencies": [], "refs": {}}
            membership.write_text(json.dumps(old_state), encoding="utf-8")
            work = root / "work"
            work.mkdir()
            config = {
                "minute_base_url": "https://replication.invalid/minute/",
                "retry_initial_seconds": 1,
                "retry_max_seconds": 2,
                "tag_key_prefix": "communication:amateur_radio",
                "catalog_file": str(membership),
                "work_dir": str(work),
                "osc_inspection_dir": str(root / "inspection"),
            }
            state = {"sequence": 7275823, "timestamp": "2026-09-07T00:34:01Z"}
            with (
                patch.object(minute_worker.common, "fetch_state", return_value=state),
                patch.object(
                    minute_worker.common,
                    "download_change",
                    return_value=minute_worker.common.DownloadedChange(download, state, 0.1),
                ),
                patch.object(minute_worker.common, "retain_osc_artifact"),
            ):
                result = minute_worker.stage_change(
                    config,
                    7275823,
                    membership,
                    root / "replica",
                    work,
                )

            self.assertEqual(result, state)
            self.assertEqual(json.loads(membership.read_text(encoding="utf-8")), old_state)
            self.assertEqual(
                minute_worker.delta_path(work, 7275823).read_text(encoding="utf-8"),
                "",
            )
            osc_path = minute_worker.replica_path(root / "replica", 7275823, ".osc.gz")
            with gzip.open(osc_path, "rb") as zipped:
                filtered = zipped.read()
            self.assertIn(b"<osmChange", filtered)
            self.assertNotIn(b"<way", filtered)

    def test_applied_batch_commits_last_membership_and_queues_new_roots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            work = root / "work"
            queue = root / "queue"
            catalog = root / "catalog.json"
            catalog.write_text(
                json.dumps({"roots": ["node:1"], "dependencies": [], "refs": {}}),
                encoding="utf-8",
            )
            cursor = work / "membership-replicate-id"
            cursor.parent.mkdir(parents=True)
            cursor.write_text("10\n", encoding="ascii")
            first_state = {
                "roots": ["node:1", "way:20"],
                "dependencies": ["node:2"],
                "refs": {"way:20": ["node:2"]},
            }
            first_delta = [
                {"op": "add", "id": "way:20", "root": True},
                {"op": "state", "state": first_state},
            ]
            minute_worker.delta_path(work, 11).parent.mkdir(parents=True)
            minute_worker.delta_path(work, 11).write_text(
                "".join(json.dumps(item) + "\n" for item in first_delta),
                encoding="utf-8",
            )
            minute_worker.delta_path(work, 12).write_text("", encoding="utf-8")
            config = {
                "catalog_file": str(catalog),
                "membership_cursor_file": str(cursor),
            }

            minute_worker.commit_applied_state(config, 12, work, queue)

            self.assertEqual(json.loads(catalog.read_text(encoding="utf-8")), first_state)
            self.assertEqual(cursor.read_text(encoding="ascii"), "12\n")
            task = json.loads((queue / "way-20.json").read_text(encoding="utf-8"))
            self.assertEqual(task, {"root": "way:20", "sequence": 11})
            self.assertFalse(minute_worker.delta_path(work, 11).exists())
            self.assertFalse(minute_worker.delta_path(work, 12).exists())


if __name__ == "__main__":
    unittest.main()
