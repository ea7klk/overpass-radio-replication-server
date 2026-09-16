#!/bin/sh
set -eu

replica_dir="${FILTERED_REPLICA_DIR:-/srv/overpass-radio/work-from-pbf/filtered-replica}"
db_dir="${OVERPASS_DB_DIR:-/srv/overpass-radio/db}"
helper=/opt/overpass/bin/apply_osc_to_db.sh
log_file="$db_dir/apply_osc_to_db.log"

if [ ! -x "$helper" ]; then
    echo "Official Overpass updater is missing or not executable: $helper" >&2
    exit 2
fi
if [ ! -f "$db_dir/replicate_id" ]; then
    echo "Database replication cursor is missing: $db_dir/replicate_id" >&2
    exit 2
fi

echo "Starting official apply_osc_to_db.sh for $replica_dir from DB cursor $(cat "$db_dir/replicate_id")"
"$helper" "$replica_dir" auto --meta=no &
helper_pid=$!
tail -n 0 -F "$log_file" &
tail_pid=$!

cleanup() {
    kill "$helper_pid" "$tail_pid" 2>/dev/null || true
    wait "$helper_pid" 2>/dev/null || true
    wait "$tail_pid" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

set +e
wait "$helper_pid"
status=$?
set -e
kill "$tail_pid" 2>/dev/null || true
wait "$tail_pid" 2>/dev/null || true
exit "$status"
