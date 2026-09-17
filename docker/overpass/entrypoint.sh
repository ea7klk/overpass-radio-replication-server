#!/usr/bin/env bash
set -euo pipefail

db_root="${OVERPASS_DB_ROOT:-/srv/overpass-radio/db}"
state_dir="${OVERPASS_STATE_DIR:-/srv/overpass-radio/state}"
ready_slot_file="${OVERPASS_READY_SLOT_FILE:-$state_dir/ready-slot}"
active_slot_file="${OVERPASS_ACTIVE_SLOT_FILE:-$state_dir/active-slot}"

slot="${OVERPASS_SLOT:-}"
if [[ -z "$slot" ]]; then
    case "${HOSTNAME:-}" in
        *-0) slot=blue ;;
        *-1) slot=green ;;
        *) slot=blue ;;
    esac
fi
case "$slot" in
    blue|green) ;;
    *) printf 'Invalid Overpass slot: %s\n' "$slot" >&2; exit 2 ;;
esac
db_dir="$db_root/$slot"

mkdir -p "$db_root" "$state_dir" /srv/overpass-radio/work
sed -i "s#__OVERPASS_DB_DIR__#${db_dir}#g" /etc/apache2/conf-enabled/overpass-radio.conf

dispatcher_pid=''
apache_pid=''

start_dispatcher() {
    [[ -f "$db_dir/nodes.map" ]] || return 0
    /opt/overpass/bin/dispatcher --osm-base --db-dir="$db_dir" --allow-duplicate-queries=yes &
    dispatcher_pid=$!
    printf 'Started Overpass dispatcher for %s slot (pid %s)\n' "$slot" "$dispatcher_pid"
}

stop_dispatcher() {
    [[ -z "$dispatcher_pid" ]] && return 0
    if kill -0 "$dispatcher_pid" 2>/dev/null; then
        /opt/overpass/bin/dispatcher --terminate --db-dir="$db_dir" >/dev/null 2>&1 || true
        for _ in {1..120}; do
            kill -0 "$dispatcher_pid" 2>/dev/null || break
            sleep 1
        done
    fi
    wait "$dispatcher_pid" 2>/dev/null || true
    dispatcher_pid=''
}

start_apache() {
    [[ -f "$db_dir/nodes.map" ]] || return 0
    apache2ctl -D FOREGROUND &
    apache_pid=$!
    printf 'Started Overpass API against %s (pid %s)\n' "$db_dir" "$apache_pid"
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

active_slot() {
    [[ -f "$active_slot_file" ]] && tr -d '[:space:]' < "$active_slot_file" || true
}

activate_if_ready() {
    [[ -f "$ready_slot_file" ]] || return 0
    [[ "$(tr -d '[:space:]' < "$ready_slot_file")" == "$slot" ]] || return 0
    api_ready || return 0
    temporary="$active_slot_file.tmp.$$"
    printf '%s\n' "$slot" > "$temporary"
    mv -f "$temporary" "$active_slot_file"
    rm -f "$ready_slot_file"
    printf 'Activated healthy Overpass %s slot; Service endpoint switches without stopping this API\n' "$slot"
}

retire_if_inactive() {
    active="$(active_slot)"
    [[ -n "$active" && "$active" != "$slot" ]] || return 0
    [[ "$(tr -d '[:space:]' < "$ready_slot_file" 2>/dev/null)" == "$slot" ]] && return 0
    [[ -f "$db_dir/nodes.map" || -n "$apache_pid" || -n "$dispatcher_pid" ]] || return 0
    stop_apache
    stop_dispatcher
    if [[ -d "$db_dir" ]]; then
        rm -rf -- "$db_dir"
        printf 'Retired inactive Overpass %s slot database\n' "$slot"
    fi
    exit 0
}

cleanup() {
    trap - TERM INT EXIT
    stop_apache
    stop_dispatcher
    wait 2>/dev/null || true
}
trap cleanup TERM INT EXIT

if [[ -f "$db_dir/nodes.map" ]]; then
    start_dispatcher
    start_apache
else
    printf 'No Overpass database in %s slot yet; waiting for the replacement import\n' "$slot"
fi

while true; do
    retire_if_inactive
    if [[ -z "$apache_pid" && -f "$db_dir/nodes.map" ]]; then
        start_dispatcher
        start_apache
    fi
    activate_if_ready
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
