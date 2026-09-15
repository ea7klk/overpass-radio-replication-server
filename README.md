# Filtered Overpass replication for `communication:amateur_radio*`

This project builds a small Overpass API database by replaying OSM replication
changes. Matching nodes, ways, and relations with a tag key beginning with
`communication:amateur_radio` are retained as roots, together with the
reference closure needed to resolve their geometry and members.

It does not retain downloaded upstream `.osc.gz` files. A change is fetched,
filtered into a short-lived local batch, applied to Overpass, and then deleted.
The raw upstream change is never written to disk.

## Reference-complete scope

The database contains matching roots plus untagged dependencies. A way causes
its node references to be retained; a relation causes its member objects to be
retained, and known dependency references are followed recursively. Updates to
dependency nodes, ways, and relations are applied as well. Clients should
select roots explicitly, for example:

```overpass
[out:json];
nwr["communication:amateur_radio"];
out geom;
```

The dependency set is implementation data. If the endpoint must prevent
clients from querying dependencies directly, put a query-restricting proxy in
front of Overpass.

For each successful sequence, the updater logs every applied root and
dependency object, plus removals, with the cadence and sequence number.

The standard OSM replication feed is OsmChange XML (`.osc.gz`), not PBF. The
Overpass updater consumes the filtered OSC XML.

## Setup

Install:

```text
Python 3.10+
Overpass osm-3s binaries, including update_from_dir and dispatcher
curl or wget
```

Copy `config.example.json` to a private configuration file and adjust paths.
The updater expects an empty Overpass database directory for the initial
replay. Start the Overpass dispatcher separately, for example:

```bash
/opt/overpass/bin/dispatcher \
  --osm-base \
  --db-dir=/srv/overpass-radio/db \
  --allow-duplicate-queries=yes
```

Then run:

```bash
python3 -m radio_overpass.replicator --config /etc/overpass-radio.json
```

The first run discovers the oldest daily sequence by traversing the public
3/3/3 directory index when `daily_start_sequence` is `null`, then replays the
daily stream through the latest published daily sequence. It resolves the
corresponding minutely sequence by binary-searching per-sequence state files
and continues polling the minute stream. The minute index is not enumerated.

Starting from the oldest daily sequence allows dependencies to be promoted as
their referencing roots appear. The implementation persists the root set,
dependency set, and known reference graph in `membership_file`.

When a change file introduces a new dependency, the source `.osc.gz` is
re-streamed with the newly discovered IDs seeded into the filter. This handles
references that occur earlier in the same change file without retaining the
upstream file on disk.

## Checkpointing and retries

The checkpoint is written atomically after a successful Overpass update. A
failed download or failed database update leaves the checkpoint unchanged, so
the same sequence is retried. Temporary filtered batches are removed after a
successful application and are safe to remove after an interrupted run.

The updater verifies that each next sequence is exactly the previous sequence
plus one. It never skips a missing or temporarily unavailable file.

## Sources

- Daily replication: https://planet.openstreetmap.org/replication/day/
- Minute replication: https://planet.openstreetmap.org/replication/minute/
- Osmium tag-filter syntax: https://docs.osmcode.org/osmium/latest/osmium-tags-filter.html
- Overpass installation and update model: https://dev.overpass-api.de/overpass-doc/en/more_info/setup.html

## Security

Do not put GitHub tokens in this repository, configuration, shell history, or
logs. The PAT supplied during setup must be revoked and replaced.
