#!/bin/sh
set -eu

config_path="${CONFIG_PATH:-/etc/overpass-radio.json}"
if [ ! -f "$config_path" ]; then
    mkdir -p "$(dirname "$config_path")"
    cp /app/config.container.json "$config_path"
fi

case "${WORKER_MODE:-}" in
    minute-filter)
        exec python -m radio_overpass.minute_worker --config "$config_path"
        ;;
    dependencies)
        exec python -m radio_overpass.dependency_worker --config "$config_path"
        ;;
    minute-applier)
        exec /usr/local/bin/radio-overpass-apply-minute
        ;;
    initial-import)
        exec /usr/local/bin/radio-overpass-initial-import
        ;;
    *)
        echo "Set WORKER_MODE to minute-filter, dependencies, minute-applier, or initial-import" >&2
        exit 2
        ;;
esac
