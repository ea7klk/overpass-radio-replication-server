#!/usr/bin/env bash
set -euo pipefail

planet_dir="${PLANET_DIR:-/srv/planet}"
torrent_url="${PLANET_TORRENT_URL:-https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf.torrent}"
torrent_file="$planet_dir/planet-latest.osm.pbf.torrent"
pbf_file="$planet_dir/planet-latest.osm.pbf"

mkdir -p "$planet_dir"
echo "Checking latest Planet torrent: $torrent_url"
curl --fail --location --retry 5 --retry-delay 10 --output "$torrent_file.part" "$torrent_url"
mv "$torrent_file.part" "$torrent_file"

echo "Downloading/resuming the fresh Planet PBF with aria2c: $pbf_file"
download_started=$(date +%s)
aria2c \
    --dir="$planet_dir" \
    --out="$(basename "$pbf_file")" \
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
echo "Fresh Planet PBF is ready: $pbf_file (download completed in $(( $(date +%s) - download_started ))s)"
