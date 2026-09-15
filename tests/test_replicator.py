import gzip
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from radio_overpass.replicator import (
    DownloadedChange,
    PREFETCH_WINDOW,
    process_one,
    quick_check,
    red_console,
)


class QuickCheckTest(unittest.TestCase):
    def run_check(self, xml: bytes, retained: set[str]) -> str | None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "change.osc.gz"
            patterns = root / "ids"
            source.write_bytes(gzip.compress(xml))
            return quick_check(source, "communication:amateur_radio", retained, patterns)

    def test_tag_prefix_is_detected(self):
        self.assertEqual(
            self.run_check(
                b'<osmChange><create><node id="1"><tag k="communication:amateur_radio:band" v="2m"/></node></create></osmChange>',
                set(),
            ),
            "tag prefix",
        )

    def test_retained_object_is_detected_without_tag(self):
        self.assertEqual(
            self.run_check(
                b'<osmChange><modify><node id="42"/></modify></osmChange>',
                {"node:42"},
            ),
            "retained object",
        )

    def test_unrelated_file_is_discarded(self):
        self.assertIsNone(
            self.run_check(
                b'<osmChange><modify><node id="42"/></modify></osmChange>',
                {"node:99"},
            )
        )

    def test_prefetch_window_is_ten(self):
        self.assertEqual(PREFETCH_WINDOW, 10)

    def test_discovered_root_node_can_be_colored_red(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                red_console("discovered root node:10", is_tty=True),
                "\033[31mdiscovered root node:10\033[0m",
            )
            self.assertEqual(
                red_console("discovered root node:10", is_tty=False),
                "discovered root node:10",
            )

    def test_skipped_dependency_replay_keeps_previous_delta(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "change.osc.gz"
            membership = root / "membership.json"
            source.write_bytes(gzip.compress(b"<osmChange/>"))
            config = {
                "daily_base_url": "https://example.test/day",
                "work_dir": str(root / "work"),
                "membership_file": str(membership),
                "tag_key_prefix": "communication:amateur_radio",
                "overpass_update_from_dir": "/bin/true",
                "db_dir": str(root / "db"),
            }
            downloaded = DownloadedChange(
                source,
                {"sequence": 1816, "timestamp": "2026-09-15T12:00:00Z"},
                0.1,
            )

            def fake_filter(_config, _source, _membership, filtered, delta, _include):
                filtered.write_text("<osmChange/>", encoding="utf-8")
                delta.write_text(
                    json.dumps(
                        {
                            "op": "state",
                            "state": {
                                "roots": ["node:10"],
                                "dependencies": ["node:1"],
                            },
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return 0.1

            with (
                patch(
                    "radio_overpass.replicator.quick_check",
                    return_value="tag prefix",
                ),
                patch(
                    "radio_overpass.replicator.gzip_contains",
                    return_value=False,
                ),
                patch(
                    "radio_overpass.replicator.filter_file",
                    side_effect=fake_filter,
                ),
                patch("radio_overpass.replicator.update_database"),
            ):
                result = process_one(config, "day", downloaded)

            self.assertEqual(result["sequence"], 1816)
            self.assertFalse(source.exists())
            self.assertEqual(json.loads(membership.read_text())["dependencies"], ["node:1"])


if __name__ == "__main__":
    unittest.main()
