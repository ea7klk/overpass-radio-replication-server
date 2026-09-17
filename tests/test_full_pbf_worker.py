import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from radio_overpass.full_pbf_worker import (
    apply_batch,
    import_filtered_pbf,
    new_planet_snapshot,
    prefetch_minute_while_hourly_apply_is_gated,
    recover_filtered_artifact,
)


class StagingSlotCoordinationTest(unittest.TestCase):
    def test_detects_completed_planet_payload_with_new_timestamp(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            planet_metadata = root / "planet-download.json"
            snapshot_metadata = root / "snapshot-metadata.json"
            planet_metadata.write_text(
                json.dumps({
                    "source_file": "planet-260914.osm.pbf",
                    "timestamp": "2026-09-14T00:00:00Z",
                }),
                encoding="utf-8",
            )
            snapshot_metadata.write_text(
                json.dumps({
                    "source_file": "planet-latest.osm.pbf",
                    "timestamp": "2026-09-07T00:00:04Z",
                }),
                encoding="utf-8",
            )

            result = new_planet_snapshot({
                "planet_metadata_file": str(planet_metadata),
                "snapshot_metadata_file": str(snapshot_metadata),
                "planet_pbf": str(root / "planet-latest.osm.pbf"),
            })

            self.assertEqual(result["source_file"], "planet-260914.osm.pbf")

    def test_ignores_legacy_symlink_name_without_new_timestamp(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            planet_metadata = root / "planet-download.json"
            snapshot_metadata = root / "snapshot-metadata.json"
            planet_metadata.write_text(
                '{"source_file":"planet-260907.osm.pbf"}', encoding="utf-8"
            )
            snapshot_metadata.write_text(
                '{"source_file":"planet-latest.osm.pbf",'
                '"timestamp":"2026-09-07T00:00:04Z"}', encoding="utf-8"
            )

            self.assertIsNone(new_planet_snapshot({
                "planet_metadata_file": str(planet_metadata),
                "snapshot_metadata_file": str(snapshot_metadata),
                "planet_pbf": str(root / "planet-latest.osm.pbf"),
            }))

    def test_prefetches_minutes_while_hourly_apply_is_gated(self):
        config = {"prefetch_files": 10}
        hourly_batch = [
            (Path("hour.osc.gz"), {"timestamp": "2026-09-17T20:00:00Z"}, False)
        ]
        with patch(
            "radio_overpass.full_pbf_worker.first_sequence_after",
            return_value=7000000,
        ), patch(
            "radio_overpass.full_pbf_worker.prefetch_replication_window",
            return_value=([("minute.osc.gz", {}, True)], 7000000),
        ) as prefetch, patch(
            "radio_overpass.full_pbf_worker.time.time",
            side_effect=[0, 2],
        ):
            prefetch_minute_while_hourly_apply_is_gated(
                config, hourly_batch, 1, 9, 1
            )

        prefetch.assert_called_once_with(config, "minute", 7000000, 1, 9)

    def test_keeps_polling_minute_replication_until_hourly_gate_expires(self):
        config = {"poll_seconds": 60}
        hourly_batch = [
            (Path("hour.osc.gz"), {"timestamp": "2026-09-17T20:00:00Z"}, False)
        ]
        with patch(
            "radio_overpass.full_pbf_worker.first_sequence_after",
            return_value=7000000,
        ), patch(
            "radio_overpass.full_pbf_worker.prefetch_replication_window",
            side_effect=[
                ([(Path("minute-1.osc.gz"), {}, True)], 7000000),
                ([], 7000000),
            ],
        ) as prefetch, patch(
            "radio_overpass.full_pbf_worker.time.time",
            side_effect=[0, 1, 2, 5],
        ), patch(
            "radio_overpass.full_pbf_worker.time.sleep"
        ) as sleep:
            prefetch_minute_while_hourly_apply_is_gated(
                config, hourly_batch, 1, 9, 5
            )

        self.assertEqual(prefetch.call_count, 2)
        prefetch.assert_has_calls([
            unittest.mock.call(config, "minute", 7000000, 1, 9),
            unittest.mock.call(config, "minute", 7000001, 1, 9),
        ])
        sleep.assert_called_once()

    def test_apply_batch_skips_full_pbf_when_checkpoint_already_covers_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / "raw" / "day"
            raw.mkdir(parents=True)
            change = raw / "000005109.osc.gz"
            change.write_bytes(b"raw")
            full_state = root / "state" / "full-pbf-state.json"
            full_state.parent.mkdir()
            full_state.write_text(
                '{"cadence":"day","sequence":5109,'
                '"timestamp":"2026-09-17T00:00:00Z"}',
                encoding="utf-8",
            )
            config = {
                "raw_dir": str(root / "raw"),
                "full_pbf_state_file": str(full_state),
            }
            batch = [
                (
                    change,
                    {"sequence": 5109, "timestamp": "2026-09-17T00:00:00Z"},
                    False,
                )
            ]
            with patch(
                "radio_overpass.full_pbf_worker.apply_full_changes"
            ) as apply:
                result = apply_batch(config, "day", 5109, batch)

            apply.assert_not_called()
            self.assertEqual(result["sequence"], 5109)
            self.assertFalse(change.exists())

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
            import_pbf.assert_called_once_with(config, artifact, "green", "day", 5109, 5118)
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
