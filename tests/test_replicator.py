import gzip
import tempfile
import unittest
from pathlib import Path

from radio_overpass.replicator import PREFETCH_WINDOW, quick_check


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


if __name__ == "__main__":
    unittest.main()
