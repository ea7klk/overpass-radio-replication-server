#!/bin/sh
set -eu

pbf_path="${INITIAL_PBF_PATH:-/initial/planet-260907.osm.pbf}"
db_dir="${OVERPASS_DB_DIR:-/srv/overpass-radio/db}"
work_dir="${OVERPASS_WORK_DIR:-/srv/overpass-radio/work}"
catalog_file="${CATALOG_FILE:-/srv/overpass-radio/catalog.json}"
metadata_file="${SNAPSHOT_METADATA_FILE:-/srv/overpass-radio/initial-snapshot.json}"
tag_prefix="${TAG_KEY_PREFIX:-communication:amateur_radio}"
initial_replicate_id="${INITIAL_REPLICATE_ID:-}"

seed_replicate_id() {
    [ -z "$initial_replicate_id" ] && return 0
    case "$initial_replicate_id" in
        *[!0-9]*)
            echo "INITIAL_REPLICATE_ID must be a non-negative integer" >&2
            exit 2
            ;;
    esac
    temporary_cursor="$db_dir/.replicate_id.tmp"
    printf '%s\n' "$initial_replicate_id" > "$temporary_cursor"
    mv "$temporary_cursor" "$db_dir/replicate_id"
    echo "Seeded Overpass replicate_id=$initial_replicate_id from the initial PBF boundary"
}

mkdir -p "$db_dir" "$work_dir"

if [ -f "$metadata_file" ]; then
    if [ -f "$db_dir/nodes.map" ]; then
        if [ ! -f "$db_dir/replicate_id" ]; then
            seed_replicate_id
        fi
        echo "Initial PBF import already completed; reusing the existing database"
        exit 0
    fi
    echo "Initial snapshot metadata exists but the Overpass database is missing" >&2
    exit 2
fi

if [ ! -r "$pbf_path" ]; then
    echo "Initial PBF file is not readable: $pbf_path" >&2
    exit 2
fi

first_entry=$(find "$db_dir" -mindepth 1 -maxdepth 1 -print -quit)
if [ -n "$first_entry" ]; then
    echo "Refusing initial PBF import into non-empty database directory: $db_dir" >&2
    exit 2
fi

temporary_dir=$(mktemp -d "$work_dir/initial-pbf.XXXXXX")
trap 'rm -rf "$temporary_dir"' EXIT INT TERM
filtered_xml="$temporary_dir/radio.osm"
temporary_catalog="$temporary_dir/catalog.json"
temporary_metadata="$temporary_dir/initial-snapshot.json"

echo "Filtering initial PBF snapshot $pbf_path"
osmium tags-filter \
    --progress \
    --remove-tags \
    --output="$filtered_xml" \
    "$pbf_path" \
    "nwr/${tag_prefix}*"
test -s "$filtered_xml"

python -m radio_overpass.initial_import \
    --pbf "$pbf_path" \
    --xml "$filtered_xml" \
    --catalog "$temporary_catalog" \
    --metadata "$temporary_metadata" \
    --prefix "$tag_prefix"

echo "Importing filtered initial snapshot into Overpass"
python -m radio_overpass.import_progress "$filtered_xml" \
    | /opt/overpass/bin/update_database \
    --db-dir="$db_dir" \
    --meta=no

test -f "$db_dir/nodes.map"
seed_replicate_id

mv "$temporary_catalog" "$catalog_file"
mv "$temporary_metadata" "$metadata_file"
echo "Initial PBF import completed; the source PBF was not copied to the PVC"
