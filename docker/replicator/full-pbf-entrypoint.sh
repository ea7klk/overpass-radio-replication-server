#!/usr/bin/env bash
set -euo pipefail
exec python -m radio_overpass.full_pbf_worker --config "${CONFIG_PATH:-/etc/overpass-radio.json}"
