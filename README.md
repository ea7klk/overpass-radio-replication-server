# Filtered Overpass replication for `communication:amateur_radio*`

This project maintains an Overpass database containing objects tagged with a
key beginning `communication:amateur_radio` plus their referenced
dependencies. The database can be initialized from an OSM PBF snapshot, then
kept current from Planet's minutely replication stream.

## Runtime design

The minute pipeline has two cooperating containers:

1. `radio_overpass.minute_worker` downloads minutely `.osc.gz` changes and
   first performs a grep-like scan for a tag key beginning with
   `communication:amateur_radio`. Files failing that pre-filter are discarded
   immediately and are not retained for inspection. Accepted files are then
   filtered to matching roots and retained dependencies, and standard
   `sequence/sequence/sequence.osc.gz` plus `.state.txt` files are staged in a
   local replica directory. It publishes each batch only after all its files
   are complete.
2. The official Overpass `/opt/overpass/bin/apply_osc_to_db.sh` runs
   continuously against that directory. It calls `update_from_dir`, preserves
   the dispatcher's normal query service, and advances the database's
   `replicate_id` only after a successful batch. The helper's log is streamed
   to the container log.

A separate `radio_overpass.dependency_worker` watches for newly tagged roots.
It waits until minutely replication is caught up, then queries public Overpass
for the root, references, and dependents. It writes a generated `.osc.gz` for
inspection and applies a clean one-file OSC directory with `update_from_dir`.
Both writers use the same filesystem lock; the dependency worker also waits
until the official applier has no pending minute batch. Neither path stops the
dispatcher for routine updates.

The filter preserves dependencies already present in the snapshot and also
replays a source diff when a new root references another object from earlier
in that same diff. Root/dependency membership is committed only after the
database cursor confirms the batch was applied. The database's `replicate_id`
is the authoritative applied cursor; pending membership deltas permit recovery
if a worker restarts during a batch.

Generated filtered and dependency-query OSC artifacts from accepted files are
retained under `osc_inspection_dir` for 24 hours. Raw downloaded Planet files,
including files rejected by the pre-filter, are temporary.
The initial import logs progress every 5,000 OSM objects.

## Snapshot boundary and restart

For the preserved `planet-260907.osm.pbf` snapshot, the header timestamp is
`2026-09-07T00:00:04Z`. The last Planet minute state at or before that instant
is sequence **`7275789`** (`2026-09-06T23:59:21Z`); the first update to apply is
**`7275790`** (`2026-09-07T00:00:21Z`). Seed a newly imported database with
`replicate_id=7275789`.

Do not rewind `replicate_id` on a database that has already received later
changes. Rebuild in a separate directory, verify it, then switch the served
path while preserving the previous database as a rollback copy. The Fleet
deployment in this repository follows that pattern; its previous DB is kept at
`/srv/overpass-radio/db-before-pbf`.

The PBF source is mounted read-only and is never copied to the PVC, modified,
or deleted by the importer. `INITIAL_REPLICATE_ID` seeds the cursor only when
the database is first imported or its marker is missing alongside matching
snapshot metadata.

## Fleet deployment

Fleet deploys the Overpass API, a minute-filter/applier Deployment, and a
dependency-worker Deployment in `overpass-radio`. The config is in
`fleet/overpass-radio/configmap.yaml`; the persistent claim is
`fleet/overpass-radio/storage.yaml`.

The cluster-specific init container expects the read-only host file
`/root/planet-260907.osm.pbf`, imports it into the local-path PVC, writes the
snapshot catalog and metadata, and seeds sequence `7275789`. The Overpass
Deployment starts only after the init container succeeds. The application is
exposed by Traefik at `https://overpass.ea7klk.es`.

After deployment, check:

```bash
kubectl -n overpass-radio get pods
kubectl -n overpass-radio logs deployment/overpass-radio-minute -c filterer -f
kubectl -n overpass-radio logs deployment/overpass-radio-minute -c applier -f
kubectl -n overpass-radio logs deployment/overpass-radio-dependencies -f
kubectl -n overpass-radio exec deployment/overpass-radio-overpass -c overpass -- \
  cat /srv/overpass-radio/db/replicate_id
```

The applier logs are also stored at
`/srv/overpass-radio/db/apply_osc_to_db.log`. The database cursor should move
forward as batches complete.

## Compose

Set `INITIAL_PBF_PATH` to a local PBF file, then start the services:

```bash
INITIAL_PBF_PATH=/path/to/planet.osm.pbf docker compose up --build -d
docker compose logs -f minute-filter minute-applier dependencies
```

Compose runs the one-shot initial importer, the Overpass API, and the same
filter/applier/dependency workers. The default API port is `8080`; set
`OVERPASS_PORT` to change it. Adjust `INITIAL_REPLICATE_ID` if the PBF's
corresponding Planet sequence is different.

## Configuration and validation

`config.example.json` documents the runtime paths and settings. The main
options are `minute_prefetch_window`, `minute_max_staged_ahead`,
`tag_key_prefix`, `minute_base_url`, `overpass_query_url`, and
`osc_inspection_dir`. Minute updates are staged in preload windows (20 by
default) while the database is behind. The filterer continues producing later
batches while the official applier consumes earlier ones, up to the bounded
200-file staged lead. Once the worker is within the preload window of the
upstream tip, it stages one file at a time. The official helper applies the
staged standard replication files continuously.
Newly discovered roots are queried in batches of 20
with up to four concurrent public queries; the resulting OSC objects are
combined into one serialized database update per batch.

Run the local checks with:

```bash
python -m unittest discover -s tests -v
python -m compileall -q radio_overpass
bash -n docker/overpass/entrypoint.sh docker/replicator/entrypoint.sh \
  docker/replicator/initial-import.sh docker/replicator/apply-minute.sh
docker compose config --quiet
```

Pushing a semantic-version tag runs tests and publishes the replicator image to
GHCR. The Overpass API image is pinned separately in Fleet and does not need to
be rebuilt for replicator releases. To build an Overpass image, run the
`Tests and container images` workflow manually and select the `overpass` target
(or `both` to build both images). Enable `Push manually built images to GHCR`
when the result should be published. Fleet image tags must refer to published
versions before deployment.
