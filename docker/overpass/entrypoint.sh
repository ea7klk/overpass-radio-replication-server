#!/usr/bin/env bash
set -euo pipefail

db_dir="${OVERPASS_DB_DIR:-/srv/overpass-radio/db}"
mkdir -p "$db_dir" /srv/overpass-radio/work

cors_config=/etc/apache2/conf-enabled/overpass-radio-cors.conf
{
    printf '%s\n' '# Generated at container start; do not edit inside the container.'
    printf '%s\n' 'SetEnvIf Referer "^$" radio_referrer_allowed=1'

    IFS=',' read -r -a origins <<< "${ALLOWED_ORIGINS:-}"
    for origin in "${origins[@]}"; do
        origin="${origin//[[:space:]]/}"
        [[ -z "$origin" ]] && continue
        if [[ ! "$origin" =~ ^https?://[A-Za-z0-9._:-]+$ ]]; then
            printf 'Invalid ALLOWED_ORIGINS entry: %s\n' "$origin" >&2
            exit 2
        fi
        escaped_origin=$(printf '%s' "$origin" | sed 's/[.[\*^$()+?{|\\]/\\&/g')
        printf 'SetEnvIf Origin "^%s$" radio_cors_allowed=1\n' "$escaped_origin"
        printf 'SetEnvIf Referer "^%s(/|$)" radio_referrer_allowed=1\n' "$escaped_origin"
    done
} > "$cors_config"

dispatcher_pid=''
apache_pid=''
cleanup() {
    trap - TERM INT EXIT
    [[ -n "$apache_pid" ]] && kill "$apache_pid" 2>/dev/null || true
    [[ -n "$dispatcher_pid" ]] && kill "$dispatcher_pid" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup TERM INT EXIT

/opt/overpass/bin/dispatcher \
    --osm-base \
    --db-dir="$db_dir" \
    --allow-duplicate-queries=yes &
dispatcher_pid=$!

apache2ctl -D FOREGROUND &
apache_pid=$!

while kill -0 "$dispatcher_pid" 2>/dev/null && kill -0 "$apache_pid" 2>/dev/null; do
    sleep 2
done

exit 1
