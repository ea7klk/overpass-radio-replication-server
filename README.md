# Filtered Overpass replication for `communication:amateur_radio*`

This project maintains an Overpass database containing objects tagged with a
key beginning `communication:amateur_radio` plus their referenced
dependencies. The database can be initialized from an OSM PBF snapshot, then
kept current from Planet's minutely replication stream.

## Runtime design

The replacement pipeline runs in one Deployment:

The init container imports the filtered PBF into `db-rebuild` while the current
public database remains online. `radio_overpass.rebuild_worker` then catches
up in order: daily, hourly, and finally minute replication. It processes one
official file at a time, while the permanent Overpass
`/opt/overpass/bin/apply_osc_to_db.sh` consumes each staged OSC immediately.

Every downloaded change first passes a native gzip/grep scan for a tag key
beginning with `communication:amateur_radio`. Rejected files are logged and
deleted. Accepted files are parsed for matching roots and retained
dependencies. Newly discovered roots are queried against public Overpass;
successful dependency/dependent results are applied in that same official
sequence. Query failures leave the phase checkpoint unchanged and retry
indefinitely.

The filter preserves dependencies already present in the snapshot and also
replays a source diff when a new root references another object from earlier
in that same diff. Empty OSC files are deliberately published for discarded
official files so `apply_osc_to_db.sh` advances its cursor instead of waiting
for a filtered file that will never be published. This advances only the
replication cursor; unrelated OSM objects never enter the database.

Generated filtered and dependency-query OSC artifacts from accepted files are
retained under `osc_inspection_dir` for 24 hours. Raw downloaded Planet files,
including files rejected by the pre-filter, are temporary.
The initial import logs progress every 5,000 OSM objects.

## Snapshot boundary and restart

For `planet-260907.osm.pbf`, the header timestamp is `2026-09-07T00:00:04Z`.
The rebuild seeds the official cursor at daily sequence **`5108`**, then
starts daily `5109`, hourly `122586`, and minute `7275790` at the matching
cadence boundaries. The minute state immediately before the snapshot is
**`7275789`**.

Do not rewind `replicate_id` on a database that has already received later
changes. Rebuild in a separate directory, verify it, then switch the served
path. The cutover keeps the previous database temporarily for rollback and
deletes it only after the replacement API passes its readiness check.

The PBF source is mounted read-only and is never copied to the PVC, modified,
or deleted by the importer. It is filtered with native `osmium tags-filter`,
which retains referenced nodes, ways, and relations by default.

## Fleet deployment

Fleet deploys the Overpass API and one replacement replication Deployment in
`overpass-radio`. The config is in
`fleet/overpass-radio/configmap.yaml`; the persistent claim is
`fleet/overpass-radio/storage.yaml`.

The replication init container expects the read-only host file
`/root/planet-260907.osm.pbf`, imports it into an isolated local-path database,
writes the snapshot catalog and metadata, and seeds daily sequence `5108`.
The current Overpass Deployment remains on `db` until the replacement has
caught up through minute replication. A handshake then stops its dispatcher,
swaps in `db-rebuild`, verifies the API, deletes the old database, and restarts
the official applier against the live database. The API remains exposed by
Traefik at `https://overpass.ea7klk.es`.

After deployment, check:

```bash
kubectl -n overpass-radio get pods
kubectl -n overpass-radio logs deployment/overpass-radio-replication -c rebuild-pipeline -f
kubectl -n overpass-radio exec deployment/overpass-radio-overpass -c overpass -- \
  cat /srv/overpass-radio/db/replicate_id
```

The official applier log is also stored at
`/srv/overpass-radio/db/apply_osc_to_db.log`. The database cursor should move
forward once each source file (including an intentionally empty filtered file)
is committed.

## Compose

Set `INITIAL_PBF_PATH` to a local PBF file, then start the services:

```bash
INITIAL_PBF_PATH=/path/to/planet.osm.pbf docker compose up --build -d
docker compose logs -f minute-filter minute-applier dependencies
```

Compose runs the one-shot initial importer, the Overpass API, and the same
single rebuild worker. The default API port is `8080`; set
`OVERPASS_PORT` to change it. Adjust `INITIAL_REPLICATE_ID` if the PBF's
corresponding Planet sequence is different.

## Configuration and validation

`config.example.json` documents the runtime paths and settings. The main
options are `tag_key_prefix`, `daily_base_url`, `hour_base_url`,
`minute_base_url`, `overpass_query_url`, `official_replica_dir`, and
`osc_inspection_dir`. Files are staged and applied one at a time. Newly
discovered roots are queried with up to four concurrent public queries, then
successful results from that one source file are applied as that same official
sequence; there is no cross-file dependency batching.

Run the local checks with:

```bash
python -m unittest discover -s tests -v
python -m compileall -q radio_overpass
bash -n docker/overpass/entrypoint.sh docker/replicator/entrypoint.sh \
  docker/replicator/initial-import.sh docker/replicator/apply-minute.sh \
  docker/replicator/rebuild-entrypoint.sh
docker compose config --quiet
```

Pushing a semantic-version tag runs tests and publishes the replicator image to
GHCR. The Overpass API image is pinned separately in Fleet and does not need to
be rebuilt for replicator releases. To build an Overpass image, run the
`Tests and container images` workflow manually and select the `overpass` target
(or `both` to build both images). Enable `Push manually built images to GHCR`
when the result should be published. Fleet image tags must refer to published
versions before deployment.
