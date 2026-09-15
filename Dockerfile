# syntax=docker/dockerfile:1.7

FROM debian:bookworm-slim AS overpass-builder

ARG DEBIAN_FRONTEND=noninteractive
ARG OVERPASS_URL=https://dev.overpass-api.de/releases/osm-3s_latest.tar.gz

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl make g++ autoconf automake libtool \
       expat libexpat1-dev zlib1g-dev liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /tmp/overpass \
    && curl --fail --location --retry 3 --retry-delay 5 --output /tmp/overpass.tar.gz "$OVERPASS_URL" \
    && tar -xzf /tmp/overpass.tar.gz --strip-components=1 -C /tmp/overpass \
    && cd /tmp/overpass \
    && ./configure --enable-lz4 \
    && make -j"$(nproc)" \
    && chmod 755 bin/*.sh cgi-bin/* \
    && mkdir -p /opt/overpass \
    && cp -a bin cgi-bin /opt/overpass/


FROM debian:bookworm-slim AS overpass-runtime

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       bash ca-certificates curl gzip grep \
       libbz2-1.0 libexpat1 liblz4-1 liblzma5 zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=overpass-builder /opt/overpass /opt/overpass


FROM overpass-runtime AS overpass

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends apache2 \
    && a2enmod cgi env headers \
    && rm -rf /var/lib/apt/lists/*

COPY docker/overpass/apache.conf /etc/apache2/conf-available/overpass-radio.conf
COPY docker/overpass/entrypoint.sh /usr/local/bin/overpass-radio-entrypoint
RUN chmod 755 /usr/local/bin/overpass-radio-entrypoint \
    && a2enconf overpass-radio

ENV OVERPASS_DB_DIR=/srv/overpass-radio/db \
    ALLOWED_ORIGINS=
EXPOSE 80
VOLUME ["/srv/overpass-radio"]
ENTRYPOINT ["/usr/local/bin/overpass-radio-entrypoint"]


FROM python:3.12-slim-bookworm AS replicator

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl gzip grep \
       libbz2-1.0 libexpat1 liblz4-1 liblzma5 zlib1g osmium-tool \
    && rm -rf /var/lib/apt/lists/*

COPY --from=overpass-builder /opt/overpass /opt/overpass

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --requirement /app/requirements.txt

COPY radio_overpass /app/radio_overpass
COPY docker/replicator/config.json /app/config.container.json
COPY docker/replicator/entrypoint.sh /usr/local/bin/radio-overpass-entrypoint
RUN chmod 755 /usr/local/bin/radio-overpass-entrypoint

ENV CONFIG_PATH=/etc/overpass-radio.json
VOLUME ["/srv/overpass-radio"]
ENTRYPOINT ["/usr/local/bin/radio-overpass-entrypoint"]
