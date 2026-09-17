#!/usr/bin/env bash
set -euo pipefail

planet_dir="${PLANET_DIR:-/srv/planet}"
torrent_url="${PLANET_TORRENT_URL:-https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf.torrent}"
torrent_file="$planet_dir/planet-latest.osm.pbf.torrent"

mkdir -p "$planet_dir"
echo "Checking latest Planet torrent: $torrent_url"
curl --fail --location --retry 5 --retry-delay 10 --output "$torrent_file.part" "$torrent_url"
mv "$torrent_file.part" "$torrent_file"

payload_name=$(aria2c --show-files "$torrent_file" 2>/dev/null |
    sed -n -E 's/^[[:space:]]*[0-9]+\|([^|]+)$/\1/p' | head -n 1)
if [[ -z "$payload_name" ]]; then
    echo "Could not determine the PBF payload name from $torrent_file" >&2
    exit 2
fi
payload_name=$(basename "$payload_name")
pbf_file="$planet_dir/$payload_name"
stable_file="$planet_dir/planet-latest.osm.pbf"

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
echo "Fresh Planet PBF is ready: $stable_file -> $payload_name (download completed in $(( $(date +%s) - download_started ))s)"
