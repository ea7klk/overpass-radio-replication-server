#!/usr/bin/env bash
set -euo pipefail

db_root="${OVERPASS_DB_ROOT:-/srv/overpass-radio/db}"
live_db_dir="${OVERPASS_LIVE_DB_DIR:-$db_root/live}"
staging_db_dir="${OVERPASS_STAGING_DB_DIR:-$db_root/staging}"
state_dir="${OVERPASS_STATE_DIR:-/srv/overpass-radio/state}"
staging_ready_file="${OVERPASS_STAGING_READY_FILE:-$state_dir/staging-ready}"
cutover_requested_file="${OVERPASS_CUTOVER_REQUESTED_FILE:-$state_dir/cutover-requested}"
cutover_complete_file="${OVERPASS_CUTOVER_COMPLETE_FILE:-$state_dir/cutover-complete}"

mkdir -p "$db_root" "$state_dir" /srv/overpass-radio/work

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
        printf 'SetEnvIf Origin "^%s$" radio_cors_allowed=1 radio_referrer_allowed=1\n' "$escaped_origin"
        printf 'SetEnvIf Referer "^%s(/|$)" radio_referrer_allowed=1\n' "$escaped_origin"
    done
} > "$cors_config"

dispatcher_pid=''
apache_pid=''

start_dispatcher() {
    [[ -f "$live_db_dir/nodes.map" ]] || return 0
    /opt/overpass/bin/dispatcher --osm-base --db-dir="$live_db_dir" --allow-duplicate-queries=yes &
    dispatcher_pid=$!
    printf 'Started Overpass dispatcher for live database (pid %s)\n' "$dispatcher_pid"
}

stop_dispatcher() {
    [[ -z "$dispatcher_pid" ]] && return 0
    if kill -0 "$dispatcher_pid" 2>/dev/null; then
        printf 'Stopping Overpass dispatcher for database cutover\n'
        /opt/overpass/bin/dispatcher --terminate --db-dir="$live_db_dir" >/dev/null 2>&1 || true
        for _ in {1..120}; do
            kill -0 "$dispatcher_pid" 2>/dev/null || break
            sleep 1
        done
    fi
    wait "$dispatcher_pid" 2>/dev/null || true
    dispatcher_pid=''
}

start_apache() {
    [[ -f "$live_db_dir/nodes.map" ]] || return 0
    apache2ctl -D FOREGROUND &
    apache_pid=$!
    printf 'Started Overpass API against %s (pid %s)\n' "$live_db_dir" "$apache_pid"
}

stop_apache() {
    [[ -z "$apache_pid" ]] && return 0
    kill "$apache_pid" 2>/dev/null || true
    wait "$apache_pid" 2>/dev/null || true
    apache_pid=''
}

api_ready() {
    [[ -n "$apache_pid" ]] || return 1
    curl --noproxy '*' --fail --silent --get \
        http://127.0.0.1/api/interpreter \
        --data-urlencode 'data=[out:json];node(1);out;' |
        grep -q '"version"'
}

perform_cutover() {
    [[ -f "$staging_ready_file" ]] || return 0
    [[ -f "$cutover_requested_file" || ! -f "$live_db_dir/nodes.map" ]] || return 0
    [[ -f "$staging_db_dir/nodes.map" ]] || {
        printf 'staging database is incomplete: %s\n' "$staging_db_dir" >&2
        return 1
    }

    printf 'Starting Overpass database cutover from staging; public API will be briefly unavailable\n'
    stop_apache
    stop_dispatcher

    old_db_dir="$db_root/retired-$(date +%s)"
    if [[ -d "$live_db_dir" ]]; then
        mv "$live_db_dir" "$old_db_dir"
    fi
    mv "$staging_db_dir" "$live_db_dir"
    mkdir -p "$staging_db_dir"

    start_dispatcher
    start_apache
    for _ in {1..120}; do
        if api_ready; then
            rm -f "$staging_ready_file" "$cutover_requested_file"
            touch "$cutover_complete_file"
            rm -rf -- "$old_db_dir"
            printf 'Overpass database cutover completed and retired database removed: %s\n' "$old_db_dir"
            return 0
        fi
        sleep 1
    done

    printf 'Replacement database did not become healthy; rolling back\n' >&2
    stop_apache
    stop_dispatcher
    failed_db_dir="$db_root/failed-$(date +%s)"
    mv "$live_db_dir" "$failed_db_dir"
    mv "$old_db_dir" "$live_db_dir"
    start_dispatcher
    start_apache
    printf 'Rollback complete; failed replacement retained at %s\n' "$failed_db_dir" >&2
    return 1
}

cleanup() {
    trap - TERM INT EXIT
    stop_apache
    stop_dispatcher
    wait 2>/dev/null || true
}
trap cleanup TERM INT EXIT

if [[ -f "$staging_ready_file" ]]; then
    perform_cutover
fi
if [[ -f "$live_db_dir/nodes.map" ]]; then
    start_dispatcher
    start_apache
else
    printf 'No live Overpass database yet; waiting for the staging import to complete\n'
fi

while true; do
    if [[ -f "$staging_ready_file" && ( -f "$cutover_requested_file" || -z "$apache_pid" ) ]]; then
        perform_cutover || exit 1
        continue
    fi
    if [[ -n "$apache_pid" ]] && ! kill -0 "$apache_pid" 2>/dev/null; then
        wait "$apache_pid" 2>/dev/null || true
        exit 1
    fi
    if [[ -n "$dispatcher_pid" ]] && ! kill -0 "$dispatcher_pid" 2>/dev/null; then
        wait "$dispatcher_pid" 2>/dev/null || true
        exit 1
    fi
    sleep 2
done
