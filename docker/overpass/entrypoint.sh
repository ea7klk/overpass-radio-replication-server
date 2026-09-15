#!/usr/bin/env bash
set -euo pipefail

db_dir="${OVERPASS_DB_DIR:-/srv/overpass-radio/db}"
mkdir -p "$db_dir" /srv/overpass-radio/work

# The dispatcher cannot create a usable database.  In particular, starting it
# first on a fresh shared volume leaves update_database looking for nodes.map
# while another process is using the same incomplete directory.  A valid
# database is always reused unchanged; only a genuinely empty directory may
# be initialized here.  Never replace or delete an existing database.
if [[ -f "$db_dir/nodes.map" ]]; then
    printf 'Using existing Overpass database in %s; no startup initialization performed\n' "$db_dir"
else
    first_entry=$(find "$db_dir" -mindepth 1 -maxdepth 1 -print -quit)
    if [[ -n "$first_entry" ]]; then
        printf 'Overpass DB is partially initialized: %s is missing nodes.map; leaving it untouched and refusing to start\n' "$db_dir" >&2
        printf 'A partial database must be repaired or restored explicitly while the dispatcher is stopped.\n' >&2
        exit 2
    fi
    printf 'Initializing empty Overpass database in %s before starting dispatcher\n' "$db_dir"
    printf '%s\n' '<osm version="0.6" generator="radio-overpass"></osm>' |
        /opt/overpass/bin/update_database --db-dir="$db_dir" --meta=no
fi

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
        # Treat an approved Origin as an approved request as well.  Browsers
        # are allowed to omit Referer, so access must not depend on both
        # headers being present.
        printf 'SetEnvIf Origin "^%s$" radio_cors_allowed=1 radio_referrer_allowed=1\n' "$escaped_origin"
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
