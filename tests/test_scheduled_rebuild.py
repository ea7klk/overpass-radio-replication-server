import tempfile
import unittest
from pathlib import Path
import subprocess
from unittest.mock import patch

from radio_overpass.scheduled_rebuild import run_until_current


class ScheduledRebuildTest(unittest.TestCase):
    def test_retries_partial_pyosmium_update_until_current(self):
        with tempfile.TemporaryDirectory() as directory:
            planet = Path(directory) / "planet.osm.pbf"
            planet.write_bytes(b"pbf")
            results = [1, 1, 0]
            calls = []

            def fake_run(command, check=False):
                calls.append(command)
                return type("Result", (), {
                    "returncode": results.pop(0),
                    "args": command,
                })()

            with patch("radio_overpass.scheduled_rebuild.subprocess.run", side_effect=fake_run), patch(
                "radio_overpass.scheduled_rebuild.time.sleep"
            ) as sleep:
                run_until_current(
                    {"planet_pbf": str(planet), "retry_seconds": 15},
                    "https://planet.osm.org/replication/hour",
                )

            self.assertEqual(len(calls), 3)
            self.assertEqual(calls[0][-1], str(planet))
            self.assertEqual(sleep.call_count, 2)

    def test_fails_on_unrecoverable_pyosmium_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            planet = Path(directory) / "planet.osm.pbf"
            planet.write_bytes(b"pbf")

            class Result:
                returncode = 2
                args = ["pyosmium-up-to-date"]

            with patch(
                "radio_overpass.scheduled_rebuild.subprocess.run",
                return_value=Result(),
            ):
                with self.assertRaises(subprocess.CalledProcessError):
                    run_until_current({"planet_pbf": str(planet)}, "https://example.test")


if __name__ == "__main__":
    unittest.main()
