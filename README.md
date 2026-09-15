# Filtered Overpass replication for `communication:amateur_radio*`

This project builds a small Overpass API database by replaying OSM replication
changes. Matching nodes, ways, and relations with a tag key beginning with
`communication:amateur_radio` are retained as roots. For each root, the
replicator queries the configured public Overpass API for the root, its
downward dependencies, and upward dependents, then imports that complete
result into the local database.

It does not retain downloaded upstream `.osc.gz` files. A change is fetched,
filtered into a short-lived local batch, applied to Overpass, and then deleted.
The raw upstream change is never written to disk.

## Reference-complete scope

The database contains matching roots plus the untagged dependencies and
dependents returned by the public Overpass query. A way causes its node
references to be retrieved; a relation causes its member objects to be
retrieved, and upward recursion retrieves ways and relations that contain the
root. Clients should select roots explicitly, for example:

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
During the discovery phase, matching root-node messages are red in interactive
terminals. Set `FORCE_COLOR=1` when the output is being viewed through a log
wrapper that does not expose a TTY; set `NO_COLOR=1` to disable ANSI colors.
Named objects include their `name` tag, and matching objects also include each
matching tag label and value, for example
`name='Local Repeater' tags=communication:amateur_radio='repeater'`.
Successful public Overpass queries and retrieved objects are light green;
query failures are red, and writes to the local Overpass database are light
blue.
It also logs a completion line for every source file, for example:

```text
completed day replication file ...: passes=1 download=2.4s quick-check=0.1s filter=2.3s apply=0.8s process=3.2s
```

Download time overlaps with processing of the preceding file, but filtering
and applying a particular file happen after its download completes. Multiple
passes mean the local file was re-read to resolve dependencies introduced
earlier in that same file; they do not cause additional downloads.

The standard OSM replication feed is OsmChange XML (`.osc.gz`), not PBF. The
Overpass updater consumes the filtered OSC XML.

The filter uses `lxml.etree.iterparse` with C-accelerated parsing. It asks the
parser to report only `osmChange`, action-group, and `node`/`way`/`relation`
events, avoiding Python callbacks for every `tag`, `nd`, and `member` child.
Processed siblings are removed from the parse tree immediately, keeping memory
bounded even for large replication files.

Before invoking the XML filter, the replicator performs a native `gzip`/`grep`
quick-check. Files with neither the configured tag prefix nor an already
retained object ID are logged as discarded and never enter XML parsing. Replay
passes are similarly skipped when none of their newly discovered dependency
IDs occurs in the file.

## Setup

Install:

```text
Python 3.10+
Overpass osm-3s binaries, including update_database, update_from_dir, and dispatcher
curl or wget
```

Copy `config.example.json` to a private configuration file and adjust paths.
The updater expects an initialized Overpass database directory for the initial
replay. The dispatcher must not be started against a fresh or partially
initialized directory. Initialize it once before starting the dispatcher:

```bash
sudo install -d -o overpass-radio -g overpass-radio /srv/overpass-radio/db
printf '%s\n' '<osm version="0.6" generator="radio-overpass"></osm>' |
  sudo -u overpass-radio /opt/overpass/bin/update_database \
    --db-dir=/srv/overpass-radio/db --meta=no
```

Then start the Overpass dispatcher separately, for example:

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

The example starts a two-phase bootstrap at daily sequence `1572`, corresponding
to January 2017. The discovery phase scans from `daily_start_sequence` through
the latest daily file and records matching roots, references, and dependencies
without updating Overpass. It then starts the apply phase at
`daily_dependency_start_sequence` (`1` in the example) and replays the daily
stream from the beginning, importing only the retained objects. This safely
backfills older dependency versions before the database is built.

Set `daily_dependency_start_sequence` to `null` to disable the discovery phase.
When `daily_start_sequence` is `null`, the oldest sequence is discovered by
traversing the public 3/3/3 directory index. After the daily replay, the
corresponding minutely sequence is found by binary-searching per-sequence state
files and the minute stream is polled continuously. The minute index is not
enumerated.

Starting from the oldest daily sequence allows roots to be found in historical
changes. The implementation persists the root set, dependency set, and known
reference graph in `catalog_file`. `membership_file` is a resumable queue of
roots to fetch from the public Overpass API. If it exists at startup, each
queued root is processed successfully before it is removed; a failed query or
local update leaves the entry queued for the next restart.

The public query endpoint defaults to
`https://overpass.private.coffee/api/interpreter` and can be changed with
`overpass_query_url`. Each root is queried individually; queries are POST
requests containing both recursive directions, `>;` and `<<;`. The response
must have a successful HTTP status, valid OSM XML, no Overpass
`<remark>`/`<error>`, and must contain the requested root. The replicator
retries unsuccessful queries and does not advance the replication checkpoint
until the local update succeeds.

When a change file introduces a new dependency, the source `.osc.gz` is
re-read locally with the newly discovered IDs seeded into the filter. This
handles references that occur earlier in the same change file without
downloading the source again.

## Container deployment

The repository includes a multi-stage `Dockerfile` and a Compose definition
with two services:

- `overpass` runs the dispatcher and Apache CGI endpoint.
- `replicator` runs the resumable updater and includes `osmium-tool` plus the
  Overpass update scripts it may need.

Both services mount the named `radio-data` volume at
`/srv/overpass-radio`. The database, checkpoint, persistent catalog, membership
queue, and work files therefore survive container replacement and are available
to the replicator and Overpass process. Source replication files remain
temporary and are deleted after processing.

Build and start locally:

```bash
docker compose up --build -d
docker compose logs -f replicator
```

The API is published on port `8080` by default. Set `OVERPASS_PORT` in a `.env`
file or in the environment to change it. To use a private configuration,
create a Compose override that adds this read-only mount to `replicator`:

```yaml
services:
  replicator:
    volumes:
      - radio-data:/srv/overpass-radio
      - ./overpass-radio.json:/etc/overpass-radio.json:ro
```

The default container configuration starts the January 2017 two-phase
bootstrap described above. It uses the container paths `/srv/overpass-radio`
and `/opt/overpass/bin/update_from_dir`. On startup, any roots queued in
`/srv/overpass-radio/membership.json` are queried and imported first; the queue
file is removed only after successful processing.

Set `ALLOWED_ORIGINS` to a comma-separated list of browser origins, including
the scheme and optional port, for example:

```bash
ALLOWED_ORIGINS=https://radio.example.org,https://map.example.org:8443 \
  docker compose up -d
```

Apache returns CORS headers only for those exact `Origin` values. Requests with
no `Referer` remain usable for command-line clients; requests with a non-empty
`Referer` must begin with one of the approved origins. This is an allowlist,
not authentication, so put TLS, authentication, and rate limiting in front of
an Internet-facing deployment as appropriate.

The release workflow runs the tests for pull requests and pushes to `main`.
It builds both images only for a published GitHub release or an explicit
workflow dispatch. A published semantic-version release such as `v1.2.3`
publishes:

```text
ghcr.io/ea7klk/overpass-radio-replicator:1.2.3
ghcr.io/ea7klk/overpass-radio-replicator:latest
ghcr.io/ea7klk/overpass-radio-overpass:1.2.3
ghcr.io/ea7klk/overpass-radio-overpass:latest
```

Manual dispatch builds the images without pushing by default; the
`push_images` input enables an intentional manual push.

## Debian / Ubuntu server installation

The following is a source installation of Overpass osm-3s and a package
installation of Osmium. It assumes a dedicated data volume mounted at
`/srv`, an administrator account with `sudo`, and a public DNS name if the API
will be exposed to the Internet. The filter uses `lxml` for C-accelerated,
incremental XML parsing. `osmium-tool` is useful for diagnostics and optional
offline OSM processing but is not required by the Python replicator itself.

The systemd instructions below create a virtual environment and install the
single Python dependency from `requirements.txt`.

### Install prerequisites

```bash
sudo apt-get update
sudo apt-get install -y \
  ca-certificates curl wget git \
  build-essential autoconf automake libtool \
  expat libexpat1-dev zlib1g-dev liblz4-dev \
  python3 python3-venv python3-pip osmium-tool apache2

python3 --version
osmium --version
```

`osmium-tool` is available in current Debian releases and many Ubuntu
releases. If APT cannot find it, install it from the distribution repository
appropriate for the server rather than mixing packages from another release.

### Build and install Overpass

Overpass is kept under `/opt/overpass` so its `bin` and `cgi-bin` directories
remain together, as expected by the upstream scripts:

```bash
cd /tmp
wget -O osm-3s_latest.tar.gz \
  https://dev.overpass-api.de/releases/osm-3s_latest.tar.gz
tar -xzf osm-3s_latest.tar.gz
cd osm-3s_*

./configure --enable-lz4
make -j"$(nproc)"
chmod 755 bin/*.sh cgi-bin/*

sudo install -d /opt/overpass
sudo cp -a bin cgi-bin /opt/overpass/
/opt/overpass/bin/osm3s_query --help >/dev/null
```

Do not copy only individual Overpass binaries: keep `bin` and `cgi-bin`
together. The upstream installation guide recommends this layout and build
procedure.

### Create the service account and directories

```bash
sudo adduser --system --group --home /srv/overpass-radio \
  --no-create-home overpass-radio
sudo install -d -o overpass-radio -g overpass-radio \
  /srv/overpass-radio/db \
  /srv/overpass-radio/work
sudo chmod 755 /srv /srv/overpass-radio

sudo cp config.example.json /etc/overpass-radio.json
sudo editor /etc/overpass-radio.json
python3 -m json.tool /etc/overpass-radio.json >/dev/null
```

The validation command must complete without output. JSON requires commas
between fields, double quotes around keys and strings, and no trailing comma
after the final field.

Set these values in `/etc/overpass-radio.json`:

```json
{
  "db_dir": "/srv/overpass-radio/db",
  "overpass_update_database": "/opt/overpass/bin/update_database",
  "overpass_update_from_dir": "/opt/overpass/bin/update_from_dir",
  "work_dir": "/srv/overpass-radio/work",
  "state_file": "/srv/overpass-radio/state.json",
  "membership_file": "/srv/overpass-radio/membership.json",
  "catalog_file": "/srv/overpass-radio/catalog.json",
  "overpass_query_url": "https://overpass.private.coffee/api/interpreter",
  "overpass_query_timeout": 180,
  "overpass_query_retries": 3
}
```

For the included example, leave `daily_start_sequence` at `1572`,
`daily_dependency_start_sequence` at `1`, and `minute_start_sequence` at
`null`. The two-phase first run can take a long time. Do not use Overpass's `download_clone.sh`
for this project: it would create a complete worldwide database instead of
the filtered database built by this repository.

If `/srv/overpass-radio/membership.json` contains a JSON list of root keys such
as `node:123`, those roots are queried from the public Overpass API at startup.
The file is deleted only after all its entries have been successfully imported;
failed entries remain available for retry. The persistent root/dependency
catalog is stored separately in `catalog_file`.

The container performs this empty-database initialization automatically before
starting its dispatcher. For a systemd installation, run the command above
once before enabling the dispatcher. If a previous attempt left files in
`db_dir` but no `nodes.map`, stop the dispatcher and move that incomplete
directory aside before retrying; the replicator refuses to write into a
partially initialized database. The first successful public Overpass result
then uses `update_database`; later results use `update_from_dir`. This is why
both Overpass binaries are required. A startup membership entry is
acknowledged only after the corresponding database write has completed
successfully.

The two-phase bootstrap settings are intended for a new empty database. If a
checkpoint already exists, the persisted `phase` controls resumption and
changing these settings does not restart the bootstrap automatically.

### Start Overpass and the replicator with systemd

Clone this repository to a stable location:

```bash
sudo git clone https://github.com/ea7klk/overpass-radio-replication-server \
  /opt/overpass-radio-replication-server
sudo chown -R overpass-radio:overpass-radio \
  /opt/overpass-radio-replication-server

sudo -u overpass-radio /usr/bin/python3 -m venv /srv/overpass-radio/venv
sudo -u overpass-radio /srv/overpass-radio/venv/bin/python -m pip install \
  --requirement /opt/overpass-radio-replication-server/requirements.txt
```

Create the dispatcher unit:

```bash
sudo editor /etc/systemd/system/overpass-radio-dispatcher.service
```

```ini
[Unit]
Description=Overpass radio database dispatcher
After=local-fs.target

[Service]
User=overpass-radio
Group=overpass-radio
UMask=0007
ExecStart=/opt/overpass/bin/dispatcher --osm-base --db-dir=/srv/overpass-radio/db --allow-duplicate-queries=yes
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Create the replication unit:

```bash
sudo editor /etc/systemd/system/overpass-radio-replicator.service
```

```ini
[Unit]
Description=Filtered OSM replication for Overpass radio data
After=network-online.target overpass-radio-dispatcher.service
Wants=network-online.target
Requires=overpass-radio-dispatcher.service

[Service]
User=overpass-radio
Group=overpass-radio
WorkingDirectory=/opt/overpass-radio-replication-server
ExecStart=/srv/overpass-radio/venv/bin/python -m radio_overpass.replicator --config=/etc/overpass-radio.json
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Enable the dispatcher first, then the historical replay:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now overpass-radio-dispatcher.service
sudo systemctl enable --now overpass-radio-replicator.service

systemctl status overpass-radio-dispatcher.service
systemctl status overpass-radio-replicator.service
journalctl -u overpass-radio-replicator.service -f
```

The replicator only advances its checkpoint after a successful database
update. If it is stopped or a download fails, restart the same unit; it will
retry the current sequence.

After updating an existing checkout, install or refresh the parser dependency
and restart the service:

```bash
cd /opt/overpass-radio-replication-server
sudo git pull --ff-only
sudo -u overpass-radio /srv/overpass-radio/venv/bin/python -m pip install \
  --requirement requirements.txt
sudo systemctl restart overpass-radio-replicator.service
```

### Optional Apache CGI endpoint

Apache is not needed for command-line queries, but it is the simplest way to
serve the Overpass API over HTTP. Grant Apache read/traverse access to the
database socket and files without granting it write access:

```bash
sudo usermod -aG overpass-radio www-data
sudo chmod 2750 /srv/overpass-radio/db
sudo a2enmod cgi env
sudo editor /etc/apache2/conf-available/overpass-radio.conf
```

Use this configuration, replacing `api.example.org` with the server name:

```apache
ServerName api.example.org

ScriptAlias /api/ "/opt/overpass/cgi-bin/"
<Directory "/opt/overpass/cgi-bin/">
    AllowOverride None
    Options +ExecCGI -MultiViews +SymLinksIfOwnerMatch
    Require all granted
    SetEnv OVERPASS_DB_DIR /srv/overpass-radio/db
</Directory>
```

Enable the configuration and restart Apache so the new group membership is
loaded:

```bash
sudo a2enconf overpass-radio
sudo systemctl reload apache2
```

Test locally:

```bash
curl --fail --get http://127.0.0.1/api/interpreter \
  --data-urlencode 'data=[out:json];nwr["communication:amateur_radio"];out geom;'
```

Put TLS and authentication/rate limiting in front of a publicly reachable
endpoint. Do not expose the unrestricted Overpass interpreter to the public
Internet unless that is intentional; the database includes dependency
objects needed for geometry, even though normal client queries select only
the radio-tagged roots.

## Checkpointing and retries

The checkpoint is written atomically after a successful Overpass update. A
failed download or failed database update leaves the checkpoint unchanged, so
the same sequence is retried. Temporary filtered batches are removed after a
successful application and are safe to remove after an interrupted run.

OSC files are fetched with the system `curl` command, with redirects and
transient retries enabled. While one file is being filtered and applied, up to
the next ten files are queued, with two downloads running concurrently. A
source file is removed after processing, and dependency re-reads use that same
local temporary file.

The updater verifies that each next sequence is exactly the previous sequence
plus one. It never skips a missing or temporarily unavailable file.

For throughput, keep `work_dir` and the Overpass database on SSD storage,
give the server as much RAM as practical for filesystem caching, and build
Overpass with `make -j"$(nproc)"`. The replicator already avoids retaining
source files and avoids extra passes unless new dependency IDs were found.

## Sources

- Daily replication: https://planet.openstreetmap.org/replication/day/
- Minute replication: https://planet.openstreetmap.org/replication/minute/
- Osmium tag-filter syntax: https://docs.osmcode.org/osmium/latest/osmium-tags-filter.html
- Overpass installation and update model: https://dev.overpass-api.de/overpass-doc/en/more_info/setup.html
