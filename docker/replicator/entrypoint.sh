#!/bin/sh
set -eu

config_path="${CONFIG_PATH:-/etc/overpass-radio.json}"
if [ ! -f "$config_path" ]; then
    mkdir -p "$(dirname "$config_path")"
    cp /app/config.container.json "$config_path"
fi

exec python -m radio_overpass.replicator --config "$config_path"
