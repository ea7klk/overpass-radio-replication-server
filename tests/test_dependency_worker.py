from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from radio_overpass import dependency_worker
from radio_overpass.replicator import RemoteRefresh


class DependencyWorkerTest(unittest.TestCase):
    def test_idle_logging_defaults_to_five_minutes(self) -> None:
        self.assertEqual(
            dependency_worker.idle_log_interval_seconds({"dependency_poll_seconds": 30}),
            300,
        )
        self.assertEqual(
            dependency_worker.idle_log_interval_seconds(
                {"dependency_poll_seconds": 30, "dependency_idle_log_interval_seconds": 90}
            ),
            90,
        )

    def test_apply_uses_an_isolated_osc_directory_without_stopping_dispatcher(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            work = root / "work"
            work.mkdir()
            source_dir = work / "query"
            source_dir.mkdir()
            osc = source_dir / "remote.osc"
            osc.write_text("<osmChange></osmChange>", encoding="utf-8")
            observed: dict[str, object] = {}

            def fake_run(command, **kwargs):
                osc_dir = Path(next(arg.split("=", 1)[1] for arg in command if arg.startswith("--osc-dir=")))
                observed["inputs"] = sorted(path.name for path in osc_dir.iterdir())
                observed["command"] = command
                return SimpleNamespace(stderr="")

            with patch.object(dependency_worker.subprocess, "run", side_effect=fake_run):
                dependency_worker.apply_osc_without_stopping_dispatcher(
                    {
                        "db_dir": str(root / "db"),
                        "work_dir": str(work),
                        "overpass_update_from_dir": "/opt/overpass/bin/update_from_dir",
                        "meta_mode": "--meta=no",
                    },
                    osc,
                    "2026-09-16T08:00:00Z",
                )

            self.assertEqual(observed["inputs"], ["000000001.osc"])
            self.assertIn("--flush-size=0", observed["command"])
            self.assertIn("--meta=no", observed["command"])

    def test_multiple_roots_are_queried_and_applied_in_one_batch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            catalog = root / "catalog.json"
            catalog.write_text(
                '{"roots":[],"dependencies":[],"refs":{}}', encoding="utf-8"
            )
            roots = ["node:1", "way:2"]

            def query(_config, root_key, _known, output):
                kind, object_id = root_key.split(":", 1)
                object_key = root_key
                return RemoteRefresh(
                    Path(output),
                    {object_key: f'<{kind} id="{object_id}"/>'.encode()},
                    {object_key: set()},
                    {},
                    {},
                    0.1,
                )

            observed: list[bytes] = []

            def apply(_config, osc_path, _version):
                observed.append(osc_path.read_bytes())

            config = {
                "catalog_file": str(catalog),
                "work_dir": str(root / "work"),
                "osc_inspection_dir": str(root / "inspection"),
            }
            (root / "work").mkdir()
            with (
                patch.object(dependency_worker.common, "query_overpass", side_effect=query),
                patch.object(dependency_worker, "save_compressed_inspection"),
                patch.object(
                    dependency_worker,
                    "apply_osc_without_stopping_dispatcher",
                    side_effect=apply,
                ),
                patch.object(dependency_worker.common, "log_remote_objects"),
            ):
                completed = dependency_worker.refresh_batch(config, roots, root / "queries")

            self.assertEqual(completed, set(roots))
            self.assertEqual(len(observed), 1)
            self.assertIn(b'<node id="1"/>', observed[0])
            self.assertIn(b'<way id="2"/>', observed[0])
            saved = json.loads(catalog.read_text(encoding="utf-8"))
            self.assertEqual(saved["roots"], roots)


if __name__ == "__main__":
    unittest.main()
