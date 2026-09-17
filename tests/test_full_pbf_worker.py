import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from radio_overpass.full_pbf_worker import (
    import_filtered_pbf,
    recover_filtered_artifact,
)


class StagingSlotCoordinationTest(unittest.TestCase):
    def test_building_marker_survives_import_and_is_cleared_after_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            filtered_dir = root / "filtered"
            filtered_dir.mkdir()
            filtered = filtered_dir / "filtered.osm.pbf"
            filtered.write_bytes(b"placeholder")
            building = root / "state" / "building-slot"
            ready = root / "state" / "ready-slot"
            config = {
                "db_root": str(root / "db"),
                "filtered_dir": str(filtered_dir),
                "building_slot_file": str(building),
                "ready_slot_file": str(ready),
            }

            def fake_update(*_args, **_kwargs):
                self.assertEqual(building.read_text(encoding="ascii"), "green")

            with patch("radio_overpass.full_pbf_worker.subprocess.run"), patch(
                "radio_overpass.full_pbf_worker.common.update_database",
                side_effect=fake_update,
            ):
                import_filtered_pbf(config, filtered, "green")

            self.assertFalse(building.exists())
            self.assertEqual(ready.read_text(encoding="ascii"), "green")

    def test_recovers_existing_filtered_batch_without_rebuilding_full_pbf(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            filtered_dir = root / "filtered"
            raw_dir = root / "raw" / "day"
            filtered_dir.mkdir(parents=True)
            raw_dir.mkdir(parents=True)
            artifact = filtered_dir / "filtered-day-000005109-000005118.osm.pbf"
            artifact.write_bytes(b"already generated")
            for sequence in range(5109, 5119):
                (raw_dir / f"{sequence:09d}.osc.gz").write_bytes(b"raw")
            full_state = root / "state" / "full-pbf-state.json"
            state_file = root / "state" / "replication-state.json"
            full_state.parent.mkdir()
            full_state.write_text(
                '{"cadence":"day","sequence":5118,'
                '"timestamp":"2026-09-17T00:00:00Z"}',
                encoding="utf-8",
            )
            state_file.write_text(
                '{"cadence":"day","sequence":5108,'
                '"timestamp":"2026-09-07T00:00:04Z"}',
                encoding="utf-8",
            )
            config = {
                "filtered_dir": str(filtered_dir),
                "raw_dir": str(root / "raw"),
                "full_pbf_state_file": str(full_state),
                "state_file": str(state_file),
                "db_root": str(root / "db"),
                "ready_slot_file": str(root / "state" / "ready-slot"),
                "building_slot_file": str(root / "state" / "building-slot"),
                "active_slot_file": str(root / "state" / "active-slot"),
            }
            with patch(
                "radio_overpass.full_pbf_worker.import_filtered_pbf"
            ) as import_pbf, patch(
                "radio_overpass.full_pbf_worker.inactive_slot",
                return_value="green",
            ):
                recovered = recover_filtered_artifact(
                    config, json.loads(state_file.read_text(encoding="utf-8"))
                )

            self.assertEqual(recovered["sequence"], 5118)
            import_pbf.assert_called_once_with(config, artifact, "green")
            self.assertEqual(
                json.loads(state_file.read_text(encoding="utf-8"))["sequence"],
                5118,
            )
            self.assertFalse(any(raw_dir.iterdir()))

    def test_building_marker_is_cleared_after_failed_import(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            filtered_dir = root / "filtered"
            filtered_dir.mkdir()
            filtered = filtered_dir / "filtered.osm.pbf"
            filtered.write_bytes(b"placeholder")
            building = root / "state" / "building-slot"
            config = {
                "db_root": str(root / "db"),
                "filtered_dir": str(filtered_dir),
                "building_slot_file": str(building),
            }

            with patch(
                "radio_overpass.full_pbf_worker.subprocess.run",
                side_effect=RuntimeError("import failed"),
            ):
                with self.assertRaises(RuntimeError):
                    import_filtered_pbf(config, filtered, "green")

            self.assertFalse(building.exists())
