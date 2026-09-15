import gzip
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class OscFilterTest(unittest.TestCase):
    def run_filter(
        self,
        osc: bytes,
        state: dict[str, object] | None = None,
        include: list[str] | None = None,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            membership = root / "membership.json"
            delta = root / "delta.jsonl"
            if state is not None:
                membership.write_text(json.dumps(state), encoding="utf-8")
            command = [
                    sys.executable,
                    "-m",
                    "radio_overpass.osc_filter",
                    "--membership",
                    str(membership),
                    "--delta",
                    str(delta),
                ]
            for dependency_id in include or []:
                command.extend(("--include", dependency_id))
            result = subprocess.run(
                command,
                input=gzip.compress(osc),
                stdout=subprocess.PIPE,
                check=True,
            )
            return result.stdout.decode("utf-8"), delta.read_text(encoding="utf-8")

    def test_root_promotes_way_nodes(self):
        osc = b'''<?xml version="1.0"?><osmChange version="0.6"><create>
          <way id="2" version="1"><nd ref="1"/><tag k="communication:amateur_radio" v="repeater"/><tag k="name" v="Local Repeater"/></way>
        </create></osmChange>'''
        output, delta = self.run_filter(osc)
        self.assertIn("<way id=\"2\"", output)
        self.assertIn('"dependencies":["node:1"]', delta)
        self.assertIn('"name":"Local Repeater"', delta)
        self.assertIn('"tags":[{"key":"communication:amateur_radio","value":"repeater"}]', delta)

    def test_dependency_update_is_imported(self):
        osc = b'''<?xml version="1.0"?><osmChange version="0.6"><modify>
          <node id="1" lat="40.1" lon="-3.1" version="2"/>
        </modify></osmChange>'''
        state = {"roots": ["way:2"], "dependencies": ["node:1"], "refs": {"way:2": ["node:1"]}}
        output, delta = self.run_filter(osc, state)
        self.assertIn("<node id=\"1\"", output)
        self.assertIn('"op":"add","id":"node:1","root":false', delta)

    def test_seeded_dependency_can_be_replayed(self):
        osc = b'''<?xml version="1.0"?><osmChange version="0.6"><create>
          <node id="1" lat="40" lon="-3" version="1"/>
        </create></osmChange>'''
        output, delta = self.run_filter(osc, include=["node:1"])
        self.assertIn("<node id=\"1\"", output)
        self.assertIn('"id":"node:1"', delta)


if __name__ == "__main__":
    unittest.main()
