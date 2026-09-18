#!/usr/bin/env bash
set -euo pipefail

planet_dir="${PLANET_DIR:-/srv/planet}"
planet_file="${PLANET_PBF:-$planet_dir/planet.osm.pbf}"
torrent_url="${PLANET_TORRENT_URL:-https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf.torrent}"
torrent_file="$planet_dir/planet-latest.osm.pbf.torrent"
metadata_file="${PLANET_METADATA_FILE:-$planet_dir/planet-download.json}"
initialized_file="${PLANET_INITIALIZED_FILE:-$planet_dir/planet-initialized}"
incoming_dir="$planet_dir/.incoming"

mkdir -p "$planet_dir" "$incoming_dir"

write_metadata() {
    local payload_name="$1"
    local torrent_sha="$2"
    local payload_path="$3"
    local preserve_download_size="${4:-false}"
    local fileinfo_tmp="$metadata_file.osmium.part"
    local metadata_tmp="$metadata_file.part"
    osmium fileinfo --json --no-crc "$payload_path" > "$fileinfo_tmp"
    python3 - "$fileinfo_tmp" "$metadata_tmp" "$payload_name" "$torrent_sha" "$payload_path" "$metadata_file" "$preserve_download_size" <<'PY'
import json
import os
import sys

info = json.loads(open(sys.argv[1], encoding="utf-8").read())
options = info.get("header", {}).get("option", {})
timestamp = options.get("osmosis_replication_timestamp")
if not timestamp:
    raise SystemExit("Downloaded Planet PBF has no replication timestamp")
sequence = options.get("osmosis_replication_sequence_number")
payload = sys.argv[5]
size = os.path.getsize(payload)
download_size = size
if sys.argv[7] == "true":
    try:
        previous = json.load(open(sys.argv[6], encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        previous = {}
    if isinstance(previous, dict) and isinstance(previous.get("download_size_bytes"), int):
        download_size = previous["download_size_bytes"]
record = {
    "source_file": sys.argv[3],
    "local_file": "planet.osm.pbf",
    "download_size_bytes": download_size,
    "current_size_bytes": size,
    "size_bytes": size,
    "torrent_sha256": sys.argv[4],
    "timestamp": timestamp,
    "replication_sequence": int(sequence) if sequence is not None else None,
    "initial_download_complete": True,
}
with open(sys.argv[2], "w", encoding="utf-8") as target:
    json.dump(record, target, indent=2)
    target.write("\n")
PY
    rm -f -- "$fileinfo_tmp"
    mv -f -- "$metadata_tmp" "$metadata_file"
}

refresh_metadata_after_local_update() {
    local payload_name torrent_sha
    payload_name=$(python3 - "$metadata_file" <<'PY'
import json
import sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8"))
except (OSError, ValueError, TypeError):
    value = {}
print(value.get("source_file", "") if isinstance(value, dict) else "")
PY
)
    torrent_sha=$(sha256sum "$torrent_file" | awk '{print $1}')
    write_metadata "$payload_name" "$torrent_sha" "$planet_file" true
}

echo "Checking latest Planet torrent: $torrent_url"
new_torrent="$torrent_file.part"
curl --fail --location --retry 5 --retry-delay 10 --output "$new_torrent" "$torrent_url"
new_torrent_sha=$(sha256sum "$new_torrent" | awk '{print $1}')

payload_name=$(aria2c --show-files "$new_torrent" 2>/dev/null |
    sed -n -E 's/^[[:space:]]*[0-9]+\|([^|]+)$/\1/p' | head -n 1)
if [[ -z "$payload_name" ]]; then
    echo "Could not determine the PBF payload name from $new_torrent" >&2
    exit 2
fi
payload_name=$(basename "$payload_name")

initialized=false
if [[ -f "$initialized_file" && -s "$planet_file" && -s "$metadata_file" ]]; then
    initialized=true
fi

if [[ "$initialized" != true ]]; then
    echo "No completed Planet initialization marker; removing existing .osm.pbf files"
    for existing in "$planet_dir"/*.osm.pbf; do
        [[ -e "$existing" || -L "$existing" ]] || continue
        rm -f -- "$existing"
    done
    rm -f -- "$metadata_file" "$torrent_file"
    mv -f -- "$new_torrent" "$torrent_file"
else
    old_torrent_sha=$(sha256sum "$torrent_file" 2>/dev/null | awk '{print $1}' || true)
    recorded_source=$(python3 - "$metadata_file" <<'PY'
import json
import sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8"))
except (OSError, ValueError, TypeError):
    value = {}
print(value.get("source_file", "") if isinstance(value, dict) else "")
PY
)
    current_size=$(stat -c '%s' "$planet_file")
    recorded_size=$(python3 - "$metadata_file" <<'PY'
import json
import sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8"))
except (OSError, ValueError, TypeError):
    value = {}
size = value.get("current_size_bytes", 0) if isinstance(value, dict) else 0
print(size if isinstance(size, int) else 0)
PY
)
    if [[ "$new_torrent_sha" == "$old_torrent_sha" &&
        "$recorded_source" == "$payload_name" ]]; then
        rm -f -- "$new_torrent"
        if [[ "$current_size" != "$recorded_size" ]]; then
            echo "Planet PBF was updated locally; refreshing planet-download.json metadata"
            refresh_metadata_after_local_update
        else
            echo "Planet torrent and controlled PBF are unchanged; keeping $planet_file"
        fi
        exit 0
    fi
    mv -f -- "$new_torrent" "$torrent_file"
fi

echo "Downloading/resuming Planet PBF with aria2c into the incoming area"
download_started=$(date +%s)
aria2c \
    --dir="$incoming_dir" \
    --continue=true \
    --allow-overwrite=true \
    --auto-file-renaming=false \
    --file-allocation=none \
    --check-integrity=true \
    --seed-time=0 \
    --bt-stop-timeout=60 \
    --max-connection-per-server=8 \
    --split=8 \
    "$torrent_file"

incoming_payload="$incoming_dir/$payload_name"
test -s "$incoming_payload"
mv -f -- "$incoming_payload" "$planet_file"
for existing in "$planet_dir"/*.osm.pbf; do
    [[ -e "$existing" || -L "$existing" ]] || continue
    [[ "$existing" == "$planet_file" ]] || rm -f -- "$existing"
done
write_metadata "$payload_name" "$new_torrent_sha" "$planet_file"
printf 'source=%s\ntimestamp=%s\n' "$payload_name" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "$initialized_file"
echo "Controlled Planet PBF is ready: $planet_file ($(stat -c '%s' "$planet_file") bytes; download completed in $(( $(date +%s) - download_started ))s)"
