#!/usr/bin/env bash
set -euo pipefail

config_path="${CONFIG_PATH:-/etc/overpass-radio.json}"
rebuild_db_dir="${REBUILD_DB_DIR:-/srv/overpass-radio/db-rebuild}"
replica_dir="${OFFICIAL_REPLICA_DIR:-/srv/overpass-radio/rebuild-replica}"
requested_file="${CUTOVER_REQUESTED_FILE:-/srv/overpass-radio/cutover-requested}"
ready_file="${CUTOVER_READY_FILE:-/srv/overpass-radio/cutover-ready}"
complete_file="${CUTOVER_COMPLETE_FILE:-/srv/overpass-radio/cutover-complete}"
phase_reset_file="${PHASE_RESET_REQUESTED_FILE:-/srv/overpass-radio/phase-reset-requested}"
phase_reset_complete="${PHASE_RESET_COMPLETE_FILE:-/srv/overpass-radio/phase-reset-complete}"
helper=/opt/overpass/bin/apply_osc_to_db.sh
dispatcher_pid=''
helper_pid=''
worker_pid=''

start_dispatcher() {
    /opt/overpass/bin/dispatcher --osm-base --db-dir="$rebuild_db_dir" --allow-duplicate-queries=yes &
    dispatcher_pid=$!
    for _ in {1..120}; do
        [ -e "$rebuild_db_dir/osm3s_osm_base" ] && return 0
        sleep 1
    done
    echo 'private rebuild dispatcher did not start' >&2
    return 1
}

start_helper() {
    mkdir -p "$replica_dir"
    "$helper" "$replica_dir" auto --meta=no &
    helper_pid=$!
    echo "Started official apply_osc_to_db.sh for $replica_dir (pid $helper_pid)"
}

stop_pid() {
    local pid="$1"
    [ -z "$pid" ] && return 0
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}

stop_private_services() {
    if [ -n "$helper_pid" ]; then
        stop_pid "$helper_pid"
        helper_pid=''
    fi
    if [ -n "$dispatcher_pid" ]; then
        /opt/overpass/bin/dispatcher --terminate --db-dir="$rebuild_db_dir" >/dev/null 2>&1 || true
        stop_pid "$dispatcher_pid"
        dispatcher_pid=''
    fi
}

watch_cutover() {
    while kill -0 "$worker_pid" 2>/dev/null; do
        if [ -f "$phase_reset_file" ] && [ ! -f "$complete_file" ]; then
            reset_cursor=$(tr -d '[:space:]' < "$phase_reset_file")
            case "$reset_cursor" in
                ''|*[!0-9]*)
                    echo "invalid phase reset cursor: $reset_cursor" >&2
                    return 1
                    ;;
            esac
            echo "resetting private database/applier cursor to $reset_cursor at cadence boundary"
            stop_private_services
            rm -rf -- "$replica_dir"
            mkdir -p "$replica_dir"
            printf '%s\n' "$reset_cursor" > "$rebuild_db_dir/replicate_id"
            start_dispatcher
            start_helper
            rm -f "$phase_reset_file"
            touch "$phase_reset_complete"
        fi
        if [ -f "$requested_file" ] && [ ! -f "$complete_file" ]; then
            echo 'cutover requested; stopping private dispatcher and official applier before handoff'
            stop_private_services
            rm -f "$requested_file"
            printf 'private rebuild services stopped; public server may switch databases\n' > "$ready_file"
            while [ ! -f "$complete_file" ] && kill -0 "$worker_pid" 2>/dev/null; do
                sleep 2
            done
            if [ -f "$complete_file" ]; then
                echo 'public database cutover completed; restarting official applier on live database'
                start_helper
            fi
            return 0
        fi
        sleep 2
    done
}

cleanup() {
    trap - TERM INT EXIT
    stop_pid "$worker_pid"
    stop_private_services
}
trap cleanup TERM INT EXIT

mkdir -p "$rebuild_db_dir" "$replica_dir"
if [ ! -f "$complete_file" ]; then
    start_dispatcher
fi
start_helper

python -m radio_overpass.rebuild_worker --config "$config_path" &
worker_pid=$!
watch_cutover &
watcher_pid=$!

set +e
wait "$worker_pid"
status=$?
set -e
kill "$watcher_pid" 2>/dev/null || true
wait "$watcher_pid" 2>/dev/null || true
exit "$status"
