import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from radio_overpass.full_pbf_worker import import_filtered_pbf


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

