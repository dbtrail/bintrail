# bintrail appliance — single-container evaluation demo (#350).
#
# Bundles: bintrail (built from this repo) + MySQL 8.0 (source AND index
# in one instance) + ProxySQL preloaded for Time Travel SQL + a traffic
# generator. EVALUATION ONLY — stateless, restart = fresh demo.
#
#   docker build -f appliance/Dockerfile -t bintrail-appliance .
#   docker run --rm -p 6033:6033 bintrail-appliance
#
# Base-image note: the runtime MUST be glibc-compatible with the builder.
# mysql:8.0 (Oracle Linux 9, glibc 2.34) cannot run a bookworm-built
# (glibc 2.36) binary — so the runtime is debian:bookworm-slim with
# MySQL 8.0 from MySQL's apt repo and ProxySQL from repo.proxysql.com
# (same install path docs/time-travel-sql.md documents). Alpine is out:
# DuckDB's static libs need glibc (see docs/docker.md "Why not Alpine?").
#
# amd64-only: MySQL's apt repo ships no arm64 packages for Debian
# (Architectures: i386 amd64). Apple Silicon runs the image via
# Rosetta/QEMU emulation — fine for an evaluation. On an ARM host build
# with: docker build --platform linux/amd64 -f appliance/Dockerfile .

FROM golang:1.25-bookworm AS build

WORKDIR /src

# go.mod / go.sum first to keep the layer cached across source edits.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=1
RUN go build -trimpath -ldflags "-s -w" -o /out/bintrail ./cmd/bintrail

FROM debian:bookworm-slim

# MySQL 8.0 (community, NOT default-mysql-server which is MariaDB and
# has a different binlog format) + ProxySQL 2.6 + bintrail runtime deps.
# policy-rc.d blocks the packages' postinst from trying to start daemons
# during the image build.
RUN set -eux; \
    printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d && chmod +x /usr/sbin/policy-rc.d; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates gnupg wget; \
    wget -qO /tmp/mysql.key 'https://repo.mysql.com/RPM-GPG-KEY-mysql-2025'; \
    gpg --dearmor -o /usr/share/keyrings/mysql.gpg /tmp/mysql.key; \
    echo 'deb [signed-by=/usr/share/keyrings/mysql.gpg] http://repo.mysql.com/apt/debian bookworm mysql-8.0' \
        > /etc/apt/sources.list.d/mysql.list; \
    wget -qO /tmp/proxysql.key 'https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/repo_pub_key'; \
    gpg --dearmor -o /usr/share/keyrings/proxysql.gpg /tmp/proxysql.key; \
    echo 'deb [signed-by=/usr/share/keyrings/proxysql.gpg] https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/bookworm/ ./' \
        > /etc/apt/sources.list.d/proxysql.list; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        mysql-server mysql-client proxysql libstdc++6 procps; \
    apt-get purge -y gnupg wget; \
    apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/* /tmp/*.key; \
    # The appliance is stateless: drop whatever datadir the package
    # postinst initialized (wrong config) — entrypoint.sh re-initializes
    # with appliance/my.cnf at first boot.
    rm -rf /var/lib/mysql && mkdir -p /var/lib/mysql && chown mysql:mysql /var/lib/mysql; \
    mkdir -p /var/lib/proxysql

COPY --from=build /out/bintrail /usr/local/bin/bintrail
# Gate the glibc/base coupling at build time: a symbol mismatch fails
# here, not in the user's first `docker run`.
RUN bintrail --version

COPY appliance/my.cnf /etc/mysql/conf.d/appliance.cnf
COPY appliance/proxysql.cnf /etc/proxysql.cnf
COPY appliance/shim.yaml appliance/seed.sql /opt/bintrail/
COPY appliance/entrypoint.sh appliance/traffic.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/traffic.sh

# 6033 = ProxySQL (Time Travel SQL entry point), 3306 = raw MySQL.
EXPOSE 6033 3306

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
