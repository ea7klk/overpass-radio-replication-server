import io
import tempfile
import unittest
from pathlib import Path

from radio_overpass.import_progress import stream_with_progress


class ImportProgressTest(unittest.TestCase):
    def test_stream_preserves_xml_and_reports_every_threshold(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "snapshot.osm"
            source.write_bytes(
                b"<osm>"
                b"<node id='1'/><way id='2'/><relation id='3'/>"
                b"<node id='4'/><way id='5'/></osm>"
            )
            output = io.BytesIO()
            log = io.StringIO()

            count = stream_with_progress(source, output, log=log, report_every=2)

            self.assertEqual(count, 5)
            self.assertEqual(output.getvalue(), source.read_bytes())
            self.assertIn("2 node/way/relation entries streamed", log.getvalue())
            self.assertIn("4 node/way/relation entries streamed", log.getvalue())
            self.assertIn("completed streaming 5", log.getvalue())

    def test_stream_counts_object_tag_split_across_chunks(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "snapshot.osm"
            source.write_bytes(b"<node id='1'/>" + b"<way id='2'/>")
            output = io.BytesIO()

            count = stream_with_progress(
                source, output, log=io.StringIO(), chunk_size=8
            )

            self.assertEqual(count, 2)
