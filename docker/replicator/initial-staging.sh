#!/usr/bin/env bash
set -euo pipefail

planet_pbf="${PLANET_PBF:-/srv/planet/planet-latest.osm.pbf}"
filtered_dir="${FILTERED_DIR:-/srv/filtered}"
staging_db="${STAGING_DB_DIR:-/srv/db/staging}"
state_dir="${STATE_DIR:-/srv/state}"
prefix="${TAG_KEY_PREFIX:-communication:amateur_radio}"

test -s "$planet_pbf"
mkdir -p "$filtered_dir" "$state_dir"
rm -rf -- "$staging_db"
mkdir -p "$staging_db"
rm -f "$state_dir/staging-ready" "$state_dir/cutover-requested" \
    "$state_dir/cutover-complete" "$state_dir/replication-state.json" \
    "$state_dir/full-pbf-state.json"

filter_started=$(date +%s)
echo "Generating the initial filtered PBF from the fresh Planet file"
osmium tags-filter "$planet_pbf" "${prefix}*" \
    -o "$filtered_dir/initial.osm.pbf" \
    --progress --overwrite --verbose
echo "Initial radio tags-filter completed in $(( $(date +%s) - filter_started ))s"

metadata=$(osmium fileinfo --json --no-crc "$planet_pbf")
python -c 'import json, os, sys; x=json.loads(sys.argv[1]); o=x.get("header",{}).get("option",{}); ts=o.get("osmosis_replication_timestamp"); seq=o.get("osmosis_replication_sequence_number");
assert ts, "Planet PBF has no replication timestamp";
target=open(sys.argv[3], "w", encoding="utf-8"); json.dump({"source_file":os.path.basename(sys.argv[2]), "timestamp":ts, "replication_sequence":int(seq) if seq is not None else None}, target, indent=2); target.write("\n"); target.close()' \
    "$metadata" "$planet_pbf" "$state_dir/snapshot-metadata.json"

temporary_dir=$(mktemp -d "$filtered_dir/initial-xml.XXXXXX")
trap 'rm -rf -- "$temporary_dir"' EXIT INT TERM
extract_started=$(date +%s)
osmium cat "$filtered_dir/initial.osm.pbf" -o "$temporary_dir/filtered.osm" \
    --overwrite --progress
echo "Initial filtered PBF extraction to XML completed in $(( $(date +%s) - extract_started ))s"
import_started=$(date +%s)
python -m radio_overpass.import_xml \
    --db-dir "$staging_db" \
    --xml "$temporary_dir/filtered.osm" \
    --every 5000
echo "Initial staging Overpass import completed in $(( $(date +%s) - import_started ))s"
test -f "$staging_db/nodes.map"
printf 'initial filtered staging database ready\n' > "$state_dir/staging-ready"
echo "Initial filtered staging database is ready; no external closure query was performed"
