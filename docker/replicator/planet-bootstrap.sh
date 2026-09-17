#!/usr/bin/env bash
set -euo pipefail

planet_dir="${PLANET_DIR:-/srv/planet}"
torrent_url="${PLANET_TORRENT_URL:-https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf.torrent}"
torrent_file="$planet_dir/planet-latest.osm.pbf.torrent"
metadata_file="${PLANET_METADATA_FILE:-$planet_dir/planet-download.json}"
stable_file="$planet_dir/planet-latest.osm.pbf"

mkdir -p "$planet_dir"
echo "Checking latest Planet torrent: $torrent_url"
new_torrent="$torrent_file.part"
curl --fail --location --retry 5 --retry-delay 10 --output "$new_torrent" "$torrent_url"
new_torrent_sha=$(sha256sum "$new_torrent" | awk '{print $1}')
old_torrent_sha=''
if [[ -f "$torrent_file" ]]; then
    old_torrent_sha=$(sha256sum "$torrent_file" | awk '{print $1}')
fi

recorded_size=''
if [[ -s "$metadata_file" ]]; then
    recorded_size=$(python3 - "$metadata_file" <<'PY'
import json
import sys

try:
    value = json.loads(open(sys.argv[1], encoding="utf-8").read())
except (OSError, ValueError, TypeError):
    value = {}
size = (
    value.get("current_size_bytes", value.get("size_bytes", ""))
    if isinstance(value, dict) else ""
)
print(size if isinstance(size, int) and size > 0 else "")
PY
)
fi
current_size=''
if [[ -L "$stable_file" && -s "$stable_file" ]]; then
    current_size=$(stat -c '%s' "$stable_file")
fi

if [[ "$new_torrent_sha" == "$old_torrent_sha" && -n "$recorded_size" && "$current_size" == "$recorded_size" ]]; then
    echo "Planet torrent is unchanged; retaining the controlled local PBF"
    rm -f "$new_torrent"
    exit 0
fi

mv "$new_torrent" "$torrent_file"

payload_name=$(aria2c --show-files "$torrent_file" 2>/dev/null |
    sed -n -E 's/^[[:space:]]*[0-9]+\|([^|]+)$/\1/p' | head -n 1)
if [[ -z "$payload_name" ]]; then
    echo "Could not determine the PBF payload name from $torrent_file" >&2
    exit 2
fi
payload_name=$(basename "$payload_name")
pbf_file="$planet_dir/$payload_name"

previous_name=''
if [[ -s "$metadata_file" ]]; then
    previous_name=$(python3 - "$metadata_file" <<'PY'
import json
import sys

try:
    value = json.loads(open(sys.argv[1], encoding="utf-8").read())
except (OSError, ValueError, TypeError):
    value = {}
name = value.get("source_file", "") if isinstance(value, dict) else ""
print(name if isinstance(name, str) else "")
PY
)
fi
if [[ -z "$previous_name" && -L "$stable_file" ]]; then
    previous_name=$(basename "$(readlink "$stable_file")")
fi

if [[ -n "$previous_name" && "$previous_name" != "$payload_name" ]]; then
    previous_path="$planet_dir/$(basename "$previous_name")"
    if [[ -f "$previous_path" ]]; then
        echo "Removing previous Planet PBF before downloading $payload_name: $previous_path"
        rm -f -- "$previous_path"
    fi
fi

echo "Downloading/resuming the fresh Planet PBF with aria2c: $pbf_file"
download_started=$(date +%s)
aria2c \
    --dir="$planet_dir" \
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

test -s "$pbf_file"
ln -sfn "$payload_name" "$stable_file"
size_bytes=$(stat -c '%s' "$pbf_file")
metadata_tmp="$metadata_file.part"
printf '{\n  "source_file": "%s",\n  "download_size_bytes": %s,\n  "current_size_bytes": %s,\n  "size_bytes": %s,\n  "torrent_sha256": "%s"\n}\n' \
    "$payload_name" "$size_bytes" "$size_bytes" "$size_bytes" "$new_torrent_sha" > "$metadata_tmp"
mv "$metadata_tmp" "$metadata_file"
echo "Fresh Planet PBF is ready: $stable_file -> $payload_name (${size_bytes} bytes; download completed in $(( $(date +%s) - download_started ))s)"
