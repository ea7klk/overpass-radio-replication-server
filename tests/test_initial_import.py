import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from radio_overpass.initial_import import build_catalog, snapshot_timestamp


class InitialImportTest(unittest.TestCase):
    def test_build_catalog_keeps_roots_dependencies_and_references(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            xml = root / "snapshot.osm"
            catalog = root / "catalog.json"
            xml.write_text(
                """<osm version=\"0.6\">
                  <node id=\"1\" lat=\"1\" lon=\"2\">
                    <tag k=\"communication:amateur_radio\" v=\"repeater\"/>
                  </node>
                  <way id=\"2\">
                    <nd ref=\"1\"/>
                  </way>
                </osm>""",
                encoding="utf-8",
            )

            roots, dependencies = build_catalog(
                xml, catalog, "communication:amateur_radio"
            )

            self.assertEqual((roots, dependencies), (1, 1))
            self.assertEqual(
                json.loads(catalog.read_text(encoding="utf-8")),
                {
                    "roots": ["node:1"],
                    "dependencies": ["way:2"],
                    "refs": {"way:2": ["node:1"]},
                },
            )

    def test_snapshot_timestamp_prefers_replication_header(self):
        with patch(
            "radio_overpass.initial_import.fileinfo",
            return_value={
                "header": {
                    "option": {
                        "osmosis_replication_timestamp": "2026-09-07T00:00:00Z",
                        "osmosis_replication_sequence_number": "123",
                    }
                }
            },
        ):
            timestamp, source, sequence = snapshot_timestamp(Path("planet.osm.pbf"))

        self.assertEqual(timestamp, "2026-09-07T00:00:00Z")
        self.assertEqual(source, "header.option.osmosis_replication_timestamp")
        self.assertEqual(sequence, 123)
