#!/usr/bin/env bash
set -euo pipefail

exec python -m radio_overpass.scheduled_rebuild \
    --config "${CONFIG_PATH:-/etc/overpass-radio.json}"
