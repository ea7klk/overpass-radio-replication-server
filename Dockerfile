# syntax=docker/dockerfile:1.7

FROM wiktorn/overpass-api:v0.7.62.11 AS overpass-prebuilt


FROM debian:bookworm-slim AS apache-brotli-module

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       apache2-dev ca-certificates curl libbrotli-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/mod-brotli
RUN curl --fail --location --silent --show-error \
       https://raw.githubusercontent.com/apache/httpd/2.4.x/modules/filters/mod_brotli.c \
       --output mod_brotli.c \
    && apxs -c \
       -I/usr/include/brotli \
       -l brotlienc \
       -l brotlidec \
       -l brotlicommon \
       mod_brotli.c \
    && mkdir -p /out \
    && cp .libs/mod_brotli.so /out/mod_brotli.so


FROM debian:bookworm-slim AS overpass-runtime

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       bash ca-certificates curl gzip grep \
       libbz2-1.0 libexpat1 libgcc-s1 liblz4-1 liblzma5 \
       libstdc++6 libbrotli1 zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=overpass-prebuilt /app /opt/overpass


FROM overpass-runtime AS overpass

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends apache2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=apache-brotli-module /out/mod_brotli.so /usr/lib/apache2/modules/mod_brotli.so
RUN printf '%s\n' 'LoadModule brotli_module /usr/lib/apache2/modules/mod_brotli.so' \
       > /etc/apache2/mods-available/brotli.load \
    && a2enmod brotli cgi deflate env filter headers
COPY docker/overpass/apache.conf /etc/apache2/conf-available/overpass-radio.conf
COPY docker/overpass/000-default.conf /etc/apache2/sites-available/000-default.conf
COPY docker/overpass/entrypoint.sh /usr/local/bin/overpass-radio-entrypoint
RUN chmod 755 /usr/local/bin/overpass-radio-entrypoint \
    && a2enconf overpass-radio

ENV OVERPASS_DB_DIR=/srv/overpass-radio/db/live \
    ALLOWED_ORIGINS=
EXPOSE 80
VOLUME ["/srv/overpass-radio"]
ENTRYPOINT ["/usr/local/bin/overpass-radio-entrypoint"]


FROM python:3.12-slim-bookworm AS replicator

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl gzip grep \
       aria2 libbz2-1.0 libexpat1 liblz4-1 liblzma5 zlib1g osmium-tool \
    && aria2c --version \
    && rm -rf /var/lib/apt/lists/*

COPY --from=overpass-prebuilt /app /opt/overpass

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --requirement /app/requirements.txt

COPY radio_overpass /app/radio_overpass
COPY docker/replicator/config.json /app/config.container.json
COPY docker/replicator/entrypoint.sh /usr/local/bin/radio-overpass-entrypoint
COPY docker/replicator/initial-import.sh /usr/local/bin/radio-overpass-initial-import
COPY docker/replicator/apply-minute.sh /usr/local/bin/radio-overpass-apply-minute
COPY docker/replicator/rebuild-entrypoint.sh /usr/local/bin/radio-overpass-rebuild-entrypoint
COPY docker/replicator/planet-bootstrap.sh /usr/local/bin/radio-overpass-planet-bootstrap
COPY docker/replicator/initial-staging.sh /usr/local/bin/radio-overpass-initial-staging
COPY docker/replicator/full-pbf-entrypoint.sh /usr/local/bin/radio-overpass-full-pbf-entrypoint
COPY docker/replicator/scheduled-rebuild-entrypoint.sh /usr/local/bin/radio-overpass-scheduled-rebuild
RUN chmod 755 /usr/local/bin/radio-overpass-entrypoint \
    && chmod 755 /usr/local/bin/radio-overpass-initial-import \
    && chmod 755 /usr/local/bin/radio-overpass-apply-minute \
    && chmod 755 /usr/local/bin/radio-overpass-rebuild-entrypoint \
    && chmod 755 /usr/local/bin/radio-overpass-planet-bootstrap \
    && chmod 755 /usr/local/bin/radio-overpass-initial-staging \
    && chmod 755 /usr/local/bin/radio-overpass-full-pbf-entrypoint \
    && chmod 755 /usr/local/bin/radio-overpass-scheduled-rebuild

ENV CONFIG_PATH=/etc/overpass-radio.json
VOLUME ["/srv/overpass-radio"]
ENTRYPOINT ["/usr/local/bin/radio-overpass-entrypoint"]
