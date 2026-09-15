import gzip
from io import BytesIO
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from radio_overpass.replicator import (
    DownloadedChange,
    CatalogQueryPool,
    PREFETCH_WINDOW,
    object_log_message,
    process_pending_membership,
    process_one,
    query_overpass,
    quick_check,
    update_database,
    light_blue_console,
    light_green_console,
    light_turquoise_console,
    overpass_query,
    overpass_update_diagnostics,
    red_console,
    RemoteRefresh,
    OverpassRootNotFoundError,
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

    def test_discovered_entities_can_be_colored_light_turquoise(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                light_turquoise_console("discovered root node:10", is_tty=True),
                "\033[96mdiscovered root node:10\033[0m",
            )
            self.assertEqual(
                light_turquoise_console("discovered dependency way:20", is_tty=False),
                "discovered dependency way:20",
            )
            self.assertEqual(
                light_turquoise_console("discovered dependency relation:30", is_tty=True),
                "\033[96mdiscovered dependency relation:30\033[0m",
            )
            self.assertEqual(red_console("query failed", is_tty=True), "\033[91mquery failed\033[0m")
            self.assertEqual(light_green_console("query succeeded", is_tty=True), "\033[92mquery succeeded\033[0m")
            self.assertEqual(light_blue_console("database write", is_tty=True), "\033[94mdatabase write\033[0m")

    def test_object_log_message_includes_name(self):
        self.assertEqual(
            object_log_message(
                "discovered",
                "root",
                "node:10",
                "day",
                1816,
                "Local Repeater",
                [{"key": "communication:amateur_radio", "value": "repeater"}],
            ),
            "discovered root node:10 at replication day/1816 name='Local Repeater' "
            "tags=communication:amateur_radio='repeater'",
        )

    def test_skipped_dependency_replay_keeps_previous_delta(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "change.osc.gz"
            catalog = root / "catalog.json"
            source.write_bytes(gzip.compress(b"<osmChange/>"))
            config = {
                "daily_base_url": "https://example.test/day",
                "work_dir": str(root / "work"),
                "membership_file": str(root / "membership.json"),
                "catalog_file": str(catalog),
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
            self.assertEqual(json.loads(catalog.read_text())["dependencies"], ["node:1"])

    def test_public_root_queries_run_in_parallel_before_ordered_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "change.osc.gz"
            catalog = root / "catalog.json"
            source.write_bytes(gzip.compress(b"<osmChange/>"))
            config = {
                "daily_base_url": "https://example.test/day",
                "work_dir": str(root / "work"),
                "membership_file": str(root / "membership.json"),
                "catalog_file": str(catalog),
                "tag_key_prefix": "communication:amateur_radio",
                "overpass_update_from_dir": "/bin/true",
                "db_dir": str(root / "db"),
                "overpass_query_workers": 2,
            }
            downloaded = DownloadedChange(
                source,
                {"sequence": 1816, "timestamp": "2026-09-15T12:00:00Z"},
                0.1,
            )
            active = 0
            maximum_active = 0
            active_lock = threading.Lock()
            write_order: list[str] = []

            def fake_filter(_config, _source, _membership, filtered, delta, _include):
                filtered.write_text("<osmChange/>", encoding="utf-8")
                delta.write_text(
                    "\n".join(
                        json.dumps({"op": "add", "id": key, "root": True})
                        for key in ("node:10", "node:11")
                    )
                    + "\n"
                    + json.dumps(
                        {
                            "op": "state",
                            "state": {"roots": ["node:10", "node:11"], "dependencies": []},
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return 0.1

            def fake_query(_config, root_key, known_keys, output_path):
                del known_keys
                nonlocal active, maximum_active
                with active_lock:
                    active += 1
                    maximum_active = max(maximum_active, active)
                time.sleep(0.05)
                with active_lock:
                    active -= 1
                return RemoteRefresh(
                    output_path,
                    {root_key: f'<node id="{root_key.split(":")[1]}" lat="40" lon="-3"/>'.encode()},
                    {root_key: set()},
                    {},
                    {},
                    0.05,
                )

            def fake_update(_config, osc_path, _timestamp, description):
                write_order.append(description)
                self.assertTrue(osc_path.exists())

            with (
                patch("radio_overpass.replicator.quick_check", return_value="tag prefix"),
                patch("radio_overpass.replicator.gzip_contains", return_value=False),
                patch("radio_overpass.replicator.filter_file", side_effect=fake_filter),
                patch("radio_overpass.replicator.query_overpass", side_effect=fake_query),
                patch("radio_overpass.replicator.update_database", side_effect=fake_update),
            ):
                process_one(config, "day", downloaded)

            self.assertEqual(maximum_active, 2)
            self.assertEqual(len(write_order), 2)

    def test_catalog_roots_are_submitted_to_background_query_lane(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            catalog = root / "catalog.json"
            catalog.write_text(
                json.dumps({"roots": ["node:10", "node:11"], "dependencies": []}),
                encoding="utf-8",
            )
            config = {
                "membership_file": str(root / "membership.json"),
                "catalog_file": str(catalog),
                "catalog_query_workers": 2,
                "catalog_query_interval_seconds": 1,
                "tag_key_prefix": "communication:amateur_radio",
                "retry_initial_seconds": 1,
                "retry_max_seconds": 1,
            }
            queried: list[str] = []

            def fake_query(_config, root_key, _known_keys, output_path):
                queried.append(root_key)
                return RemoteRefresh(
                    output_path,
                    {root_key: f'<node id="{root_key.split(":")[1]}" lat="40" lon="-3"/>'.encode()},
                    {root_key: set()},
                    {},
                    {},
                    0.01,
                )

            with (
                patch("radio_overpass.replicator.query_overpass", side_effect=fake_query),
                patch("radio_overpass.replicator.update_database"),
            ):
                pool = CatalogQueryPool(config, root / "query-work")
                pool.start()
                self.assertEqual(set(pool.futures), {"node:10", "node:11"})
                pool.drain_ready(wait=True)
                time.sleep(1.05)
                pool.drain_ready()
                pool.close()

            self.assertEqual(set(queried), {"node:10", "node:11"})
            self.assertEqual(len(queried), 4)

    def test_public_overpass_query_returns_root_dependencies_and_dependents(self):
        class Response(BytesIO):
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

        response = Response(
            b'''<osm version="0.6">
              <node id="1" lat="40" lon="-3"/>
              <node id="2" lat="40.1" lon="-3.1"/>
              <way id="9"><nd ref="1"/><nd ref="2"/></way>
            </osm>'''
        )
        config = {
            "overpass_query_url": "https://overpass.example/api/interpreter",
            "overpass_query_timeout": 30,
            "overpass_query_retries": 0,
            "retry_initial_seconds": 1,
            "retry_max_seconds": 1,
            "tag_key_prefix": "communication:amateur_radio",
        }
        with patch("radio_overpass.replicator.urllib.request.urlopen", return_value=response) as urlopen:
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "remote.osc"
                refresh = query_overpass(config, "node:1", set(), output)
                remote_xml = output.read_bytes()

        self.assertEqual(set(refresh.objects), {"node:1", "node:2", "way:9"})
        query = urlopen.call_args.args[0].data.decode("utf-8")
        self.assertIn("node(id:1);", query)
        self.assertIn("(._;>>;);", query)
        self.assertIn("(._;<<;);", query)
        self.assertIn(b"<way id=\"9\"", remote_xml)

    def test_missing_geometry_diagnostics_are_summarized(self):
        diagnostics = """compute_geometry: Node 1756187290 used in way 405816383 not found.
compute_geometry: Way 1206978494 used in relation 6791194 not found.
"""
        summary = overpass_update_diagnostics(diagnostics)
        self.assertIn("2 missing geometry reference(s) across 2 object(s)", summary)
        self.assertIn("node 1756187290 -> way 405816383", summary)
        self.assertIn("way 1206978494 -> relation 6791194", summary)

    def test_public_overpass_missing_root_is_terminal_without_retry(self):
        class Response(BytesIO):
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

        response = Response(b'<osm version="0.6"></osm>')
        config = {
            "overpass_query_url": "https://overpass.example/api/interpreter",
            "overpass_query_timeout": 30,
            "overpass_query_retries": 3,
            "retry_initial_seconds": 1,
            "retry_max_seconds": 1,
            "tag_key_prefix": "communication:amateur_radio",
        }
        with patch(
            "radio_overpass.replicator.urllib.request.urlopen",
            return_value=response,
        ) as urlopen:
            with tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(OverpassRootNotFoundError):
                    query_overpass(config, "node:404", set(), Path(directory) / "remote.osc")

        self.assertEqual(urlopen.call_count, 1)

    def test_startup_membership_is_removed_after_successful_processing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pending = root / "membership.json"
            catalog = root / "catalog.json"
            pending.write_text(json.dumps(["node:1"]), encoding="utf-8")
            config = {
                "membership_file": str(pending),
                "catalog_file": str(catalog),
                "work_dir": str(root / "work"),
            }
            refresh = RemoteRefresh(
                root / "remote.osc",
                {"node:1": b'<node id="1" lat="40" lon="-3"/>'},
                {"node:1": set()},
                {},
                {},
                0.1,
            )
            with (
                patch("radio_overpass.replicator.query_overpass", return_value=refresh),
                patch("radio_overpass.replicator.update_database"),
            ):
                process_pending_membership(config)

            self.assertFalse(pending.exists())
            self.assertIn("node:1", json.loads(catalog.read_text())["roots"])

    def test_startup_membership_remains_after_database_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pending = root / "membership.json"
            catalog = root / "catalog.json"
            pending.write_text(json.dumps(["node:1"]), encoding="utf-8")
            config = {
                "membership_file": str(pending),
                "catalog_file": str(catalog),
                "work_dir": str(root / "work"),
            }
            with (
                patch(
                    "radio_overpass.replicator.query_overpass",
                    side_effect=RuntimeError("database unavailable"),
                ),
                self.assertRaises(RuntimeError),
            ):
                process_pending_membership(config)

            self.assertEqual(json.loads(pending.read_text()), ["node:1"])

    def test_missing_startup_membership_root_is_removed_and_next_is_processed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pending = root / "membership.json"
            catalog = root / "catalog.json"
            pending.write_text(json.dumps(["node:404", "node:1"]), encoding="utf-8")
            config = {
                "membership_file": str(pending),
                "catalog_file": str(catalog),
                "work_dir": str(root / "work"),
            }
            refresh = RemoteRefresh(
                root / "remote.osc",
                {"node:1": b'<node id="1" lat="40" lon="-3"/>'},
                {"node:1": set()},
                {},
                {},
                0.1,
            )
            with (
                patch(
                    "radio_overpass.replicator.refresh_root",
                    side_effect=[OverpassRootNotFoundError("node:404"), (refresh, {})],
                ) as refresh_root_mock,
                patch("radio_overpass.replicator.update_database"),
            ):
                process_pending_membership(config)

            self.assertFalse(pending.exists())
            self.assertEqual(
                [call.args[1] for call in refresh_root_mock.call_args_list],
                ["node:404", "node:1"],
            )

    def test_initial_database_requires_a_clean_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db_dir = root / "db"
            db_dir.mkdir()
            (db_dir / "dispatcher.lock").write_text("partial", encoding="utf-8")
            source = root / "remote.osm"
            source.write_text('<osm version="0.6"></osm>\n', encoding="utf-8")
            config = {
                "db_dir": str(db_dir),
                "overpass_update_from_dir": "/opt/overpass/bin/update_from_dir",
            }

            with self.assertRaisesRegex(RuntimeError, "partially initialized"):
                update_database(config, source, "2026-09-15T12:00:00Z")

            self.assertTrue((db_dir / "dispatcher.lock").exists())


if __name__ == "__main__":
    unittest.main()
