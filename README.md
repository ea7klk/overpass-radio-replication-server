# Filtered Overpass rebuild for `communication:amateur_radio*`

This repository maintains an Overpass database containing only current OSM
objects with a `communication:amateur_radio*` tag and the referenced nodes,
ways, and relation members needed to represent them. Imports use
`--meta=no`; attic and historical data are not retained.

## Scheduled pipeline

The pipeline runs as the `overpass-radio-replication` Kubernetes CronJob at
`0 */4 * * *`. `concurrencyPolicy: Forbid` prevents overlapping full rebuilds.
The active Overpass API remains available while the next database is built.

The first CronJob execution performs the one-time initialization:

1. If `planet-initialized` is absent, existing `.osm.pbf` files on the Planet
   PVC are removed.
2. The latest Planet torrent is downloaded with `aria2c` and the completed
   payload is stored as `/srv/overpass-radio/planet/planet.osm.pbf`.
3. `planet-download.json` records the original payload filename, torrent hash,
   file size, replication timestamp, and replication sequence. The
   `planet-initialized` marker prevents the destructive initialization from
   happening again.

Every later run checks the latest torrent. An unchanged torrent and payload
are reused. A newer torrent is downloaded into an incoming directory and
atomically moved into place only after completion. The old PBF is removed
after the replacement is ready. Metadata is refreshed after every local
Pyosmium update so a changed file is not downloaded again unnecessarily.

Each run then catches the full PBF up to the current state using only the
minutely replication service and Pyosmium 4.3.1:

```bash
pyosmium-up-to-date -vvv \
  --size=10000 \
  --server https://planet.osm.org/replication/minute \
  --ignore-osmosis-headers \
  /srv/overpass-radio/planet/planet.osm.pbf
```

If Pyosmium returns `1`, that is treated as a normal “more update files are
available” indication, not as a failed job. The CronJob repeats the command
until the server is caught up. Pyosmium stores
replication metadata in the PBF, allowing the next run to resume from the
correct position. The explicit header override is used only for the initial
handoff when the snapshot header does not yet identify the minute service;
after that, the minute replication metadata written by Pyosmium is reused
directly without rescanning the full Planet file.

After minutely replication is current, the job creates a fresh extract:

```bash
osmium tags-filter planet.osm.pbf \
  'communication:amateur_radio*' \
  -o pota_filtered.osm.pbf \
  --progress --overwrite --verbose
```

The command intentionally does not use `--omit-referenced`/`-R`, so referenced
nodes and relation members are retained. The extract is imported into the
inactive blue/green Overpass database slot. The active slot keeps serving until
the replacement is ready and passes the existing readiness-gated cutover.

## Blue/green cutover

The Overpass StatefulSet has two fixed slots: pod `-0` is blue and pod `-1` is
green. Only the slot named by `active-slot` is ready in the Service. The
inactive pod remains alive but unready while its database is absent or being
rebuilt; it must not enter `CrashLoopBackOff` merely because it is a standby.

The scheduled job writes `building-slot` before deleting and recreating the
inactive database, then publishes `ready-slot`. The standby API starts its
dispatcher and Apache, verifies a local query, and atomically changes
`active-slot`. The previous active pod then retires its old database. Apache
and dispatcher stale-socket cleanup remains enabled.

## Progress logging

The CronJob logs elapsed time and progress for:

- torrent/PBF download and replacement;
- minutely Pyosmium catch-up;
- full-planet tag filtering;
- PBF-to-XML extraction; and
- staging Overpass database import, including progress every 5,000 OSM
  objects.

Useful commands:

```bash
kubectl -n overpass-radio get cronjob,jobs,pods,pvc
kubectl -n overpass-radio logs job/<job-name> -f -c scheduled-rebuild
kubectl -n overpass-radio exec overpass-radio-overpass-0 -- \
  cat /srv/overpass-radio/state/active-slot
kubectl -n overpass-radio exec overpass-radio-overpass-0 -- \
  cat /srv/overpass-radio/planet/planet-download.json
```

The API is exposed through Traefik at
`https://overpass.ea7klk.es`. Apache access and error logs are emitted to
container stdout/stderr.

## Persistent volumes

| PVC | Purpose |
| --- | --- |
| `overpass-radio-planet` | Controlled full Planet PBF, torrent, metadata, and initialization marker |
| `overpass-radio-raw-changes` | Legacy raw-change storage retained for compatibility; unused by the scheduled pipeline |
| `overpass-radio-filtered` | Current filtered PBF extract and temporary XML |
| `overpass-radio-databases` | Blue/green filtered Overpass database slots |
| `overpass-radio-state` | Cutover markers, metadata, and import coordination |

Architecture and storage diagrams are in
[`docs/architecture.puml`](docs/architecture.puml) and
[`docs/storage.puml`](docs/storage.puml). Both use standard PlantUML syntax and
are renderable by the public PlantUML server; see
[`docs/plantuml.md`](docs/plantuml.md).

## Validation

```bash
python -m unittest discover -s tests -v
python -m compileall -q radio_overpass
bash -n docker/overpass/entrypoint.sh docker/replicator/*.sh
docker compose config --quiet
```

Pushing a semantic-version tag runs tests and publishes the replicator and
Overpass images to GHCR. The image uses the prebuilt
`wiktorn/overpass-api:v0.7.62.11` binary distribution and installs
`osmium-tool`, `aria2`, and Pyosmium `4.3.1`.
