# Filtered Overpass replication for `communication:amateur_radio*`

This project builds a small Overpass API database by replaying OSM replication
changes, retaining only nodes, ways, and relations with a tag key beginning
with `communication:amateur_radio`.

It does not retain downloaded upstream `.osc.gz` files. A change is fetched,
filtered into a short-lived local batch, applied to Overpass, and then deleted.
The raw upstream change is never written to disk.

## Important scope

This is a **tagged-object-only** database. Referenced nodes and members are
not retained unless they themselves match the prefix. Consequently, queries
for matching objects work, but `out geom` for a matching way/relation may not
have complete coordinates. Add a dependency-aware extraction layer before
using this in a geometry-dependent service.

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
