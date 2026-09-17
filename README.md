# Filtered Overpass replication for `communication:amateur_radio*`

This repository maintains an Overpass database containing only objects with a
`communication:amateur_radio*` tag and the referenced OSM dependencies needed
to represent those objects. The initial Planet file is downloaded and kept
under the pipeline's control; no host `hostPath` PBF is used.
Only current OSM objects are imported: all database imports use `--meta=no`
and no attic or historical data is requested.

## Runtime design

The Fleet deployment uses one blue/green Overpass StatefulSet (two API/
dispatcher pods) and one `full-pbf-replication` worker Deployment. The worker uses a fresh
`planet-latest.osm.pbf.torrent`, resumes the PBF download with `aria2c`, and
keeps that full PBF as its source of truth.
The replicator image installs the Debian `aria2` package and verifies that
`aria2c` is available during the image build.

The Planet bootstrap records the torrent SHA-256, the original payload
filename and download size, and the current payload size in
`planet-download.json`. If the torrent and current recorded file size are
unchanged, the existing PBF is reused. If a newer torrent points to another
payload, the previous payload is removed before the replacement download
starts; the new file is downloaded into the same controlled PVC and then
exposed through the stable symlink. After each `osmium apply-changes`, the
updated payload replaces the previous payload in place and the current size
is recorded. This keeps one full Planet payload on disk rather than
accumulating old versions.

The worker processes replication in order: daily, hourly, then minutely. Each
raw `.osc.gz` file is first scanned for a
`communication:amateur_radio*` tag. Rejected files are logged and deleted.
Up to ten contiguous files are prefetched concurrently. The worker waits for
the one-hour batch window to be due, then merges and applies the batch in one
full-PBF rewrite, even if the backlog is still growing. Every file is still
applied to the full Planet PBF so the source of truth stays complete. For
accepted files, `osmium tags-filter` creates a new filtered PBF;
that PBF is extracted to XML and imported into an isolated staging Overpass
database. No external Overpass query is needed: the full Planet PBF is the
source of truth, and `osmium tags-filter` retains the referenced nodes, ways,
and relation members needed by each matching object.

When a replacement slot is ready, a second Overpass dispatcher/API instance
serves it and passes a local health check before the active slot marker is
switched. Traefik's Service then routes only to the healthy active slot, so the
old API remains available during the handoff. The retired slot is removed only
after its dispatcher has stopped. Files without matching tags do not trigger a
filtered rebuild or slot switch.

`osmium tags-filter` retains referenced nodes and relation members by default;
do not pass `--omit-referenced`/`-R`. The explicit CLI used for the initial
extract is:

```bash
osmium tags-filter planet-latest.osm.pbf \
  'communication:amateur_radio*' \
  -o initial.osm.pbf --progress --overwrite --verbose
```

## Timing and progress logs

The init containers and worker log elapsed seconds for:

- Planet torrent/PBF download;
- `osmium tags-filter` filtering;
- filtered PBF to XML extraction;
- staging Overpass database import; and
- replication downloads and full-PBF `osmium apply-changes`.

Initial and staging imports additionally log every 5,000 OSM objects received,
followed by a final object count. Typical messages include
`radio tags-filter ... completed in`, `extracted filtered PBF ... in`,
`staging Overpass import ... completed in`, and
`Overpass DB import processed at least 5000 OSM objects`.

## Persistent volumes

The five claims in `fleet/overpass-radio/storage.yaml` have separate purposes:

| PVC | Purpose |
| --- | --- |
| `overpass-radio-planet` | Full Planet PBF and torrent, the controlled source of truth |
| `overpass-radio-raw-changes` | Temporary raw daily/hourly/minute downloads |
| `overpass-radio-filtered` | Filtered PBF extracts and temporary XML |
| `overpass-radio-databases` | Blue/green filtered Overpass database slots |
| `overpass-radio-state` | Replication checkpoints, metadata, work, and cutover markers |

The architecture and storage diagrams are in
[`docs/architecture.puml`](docs/architecture.puml) and
[`docs/storage.puml`](docs/storage.puml). Both contain only standard syntax and
are renderable by the public PlantUML server; see
[`docs/plantuml.md`](docs/plantuml.md).

## Fleet deployment

The API is exposed through Traefik at
`https://overpass.ea7klk.es`. The service remains unready while the fresh
Planet file is downloading or the initial filtered staging database is being
imported, then becomes ready after the first cutover.

Useful checks:

```bash
kubectl -n overpass-radio get pods,pvc
kubectl -n overpass-radio logs deployment/overpass-radio-replication \
  -c full-pbf-replication -f
kubectl -n overpass-radio logs deployment/overpass-radio-overpass -f
kubectl -n overpass-radio exec deployment/overpass-radio-replication \
  -c full-pbf-replication -- cat /srv/overpass-radio/state/replication-state.json
```

## Validation

```bash
python -m unittest discover -s tests -v
python -m compileall -q radio_overpass
bash -n docker/overpass/entrypoint.sh docker/replicator/*.sh
docker compose config --quiet
```

Pushing a semantic-version tag runs tests and publishes the replicator image
to GHCR. The Overpass and replicator images are released together because the
database cutover protocol is shared between them. The image uses the recent
prebuilt `wiktorn/overpass-api:v0.7.62.11` binary distribution as a build
stage, so the osm-3s compiler stage is no longer run for every release. The
image remains wrapped in this repository's Debian/Apache image so the custom
cutover entrypoint and Traefik-facing API layout are preserved.
The redesigned pipeline begins at release `v0.1.0`; subsequent redesigned
releases should increment from that version.
The torrent payload name is discovered from the torrent metadata (for example,
`planet-260907.osm.pbf`) and is resumed in place. A stable
`planet-latest.osm.pbf` symlink points to that controlled payload for the
filtering and replication workers.

## Safe replication-worker restarts

Do not roll or recreate the `full-pbf-replication` pod while it has pending
replication work. Kubernetes reruns the init containers whenever the pod is
recreated, so an unnecessary rollout can repeat the expensive initial filter
and staging import. Before a planned restart, wait until the daily/hourly/
minute stream has caught up and confirm both conditions below:

1. The state PVC contains `accepted-prefilter`, written only after a file logs
   `accepted tag prefilter: matching communication:amateur_radio*`.
2. The raw-change PVC contains no pending `.osc.gz` or `.part` files under
   `/srv/overpass-radio/raw/day`, `/srv/overpass-radio/raw/hour`, or
   `/srv/overpass-radio/raw/minute`.

Example checks:

```bash
kubectl -n overpass-radio exec deployment/overpass-radio-replication \
  -c full-pbf-replication -- test -s /srv/overpass-radio/state/accepted-prefilter
kubectl -n overpass-radio exec deployment/overpass-radio-replication \
  -c full-pbf-replication -- sh -c \
  'find /srv/overpass-radio/raw -type f \( -name "*.osc.gz" -o -name "*.part" \) -print'
```

The second command must produce no output before the restart is authorized.
