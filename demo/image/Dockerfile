# bintrail demo image — single-container evaluation demo (#350).
#
# Bundles: bintrail (built from this repo) + Percona Server 8.0 (source
# AND index in one instance) + ProxySQL preloaded for Time Travel SQL + a
# traffic generator. EVALUATION ONLY — stateless, restart = fresh demo.
#
#   docker build -f demo/image/Dockerfile -t bintrail-demo .
#   docker run --rm -p 6033:6033 bintrail-demo
#
# Base-image note: the runtime MUST be glibc-compatible with the builder.
# The bintrail binary is built on golang:1.25-bookworm (glibc 2.36), so the
# runtime is debian:bookworm-slim with Percona Server 8.0 and ProxySQL 2.6
# from their bookworm apt repos (both built for bookworm, so glibc stays in
# lockstep). Alpine is out: DuckDB's static libs need glibc (see
# docs/docker.md "Why not Alpine?").
#
# Multi-arch (amd64 + arm64): MySQL's own Debian apt repo ships no arm64
# packages (Architectures: i386 amd64), which is why this image used to be
# amd64-only. Percona Server 8.0 is a drop-in MySQL with arm64 packages
# (identical mysqld/mysql binaries and ROW binlog format), so the image now
# builds and runs natively on ARM64 hosts (AWS Graviton, etc.).

FROM golang:1.25-bookworm AS build

WORKDIR /src

# go.mod / go.sum first to keep the layer cached across source edits.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=1
RUN go build -trimpath -ldflags "-s -w" -o /out/bintrail ./cmd/bintrail

FROM debian:bookworm-slim

# Percona Server 8.0 (a drop-in MySQL — NOT default-mysql-server, which is
# MariaDB with a different binlog format) + ProxySQL 2.6 + bintrail runtime
# deps. Percona is used over MySQL community because its apt repo ships
# arm64 packages (MySQL's Debian repo is amd64/i386-only); the mysqld/mysql
# binaries and ROW binlog format are identical to upstream MySQL 8.0.
# policy-rc.d blocks the packages' postinst from trying to start daemons
# during the image build.
RUN set -eux; \
    printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d && chmod +x /usr/sbin/policy-rc.d; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates gnupg wget; \
    # Percona Server 8.0 via percona-release: the official installer wires
    # the repo + signing key for the detected architecture (amd64 or arm64).
    # `enable ps-80 release` is the documented non-interactive form and adds
    # only this repo (vs `setup`, which can disable/reset other Percona repos).
    # percona-release writes its own signed-by keyring entry
    # (/usr/share/keyrings/percona-keyring.gpg), so the http apt repo is
    # key-verified — same posture as the MySQL community repo this replaced.
    wget -qO /tmp/percona-release.deb 'https://repo.percona.com/apt/percona-release_latest.generic_all.deb'; \
    apt-get install -y --no-install-recommends /tmp/percona-release.deb; \
    percona-release enable ps-80 release; \
    # ProxySQL 2.6 — already multi-arch (amd64 + arm64).
    wget -qO /tmp/proxysql.key 'https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/repo_pub_key'; \
    gpg --dearmor -o /usr/share/keyrings/proxysql.gpg /tmp/proxysql.key; \
    echo 'deb [signed-by=/usr/share/keyrings/proxysql.gpg] https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/bookworm/ ./' \
        > /etc/apt/sources.list.d/proxysql.list; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        percona-server-server percona-server-client proxysql libstdc++6 procps; \
    # Gate the swap: confirm the repo actually yielded Percona Server 8.0 (the
    # ROW-binlog flavor the demo depends on). A wrong repo/component would
    # otherwise install a different flavor or major and fail only at runtime.
    v="$(mysqld --version)"; echo "$v"; \
    case "$v" in *"Percona Server"*) ;; *) echo "FATAL: not Percona Server: $v"; exit 1;; esac; \
    case "$v" in *"Ver 8.0."*) ;; *) echo "FATAL: not MySQL 8.0: $v"; exit 1;; esac; \
    apt-get purge -y gnupg wget; \
    apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/* /tmp/*.key /tmp/percona-release.deb; \
    # The demo is stateless: drop whatever datadir the package
    # postinst initialized (wrong config) — entrypoint.sh re-initializes
    # with demo/image/my.cnf at first boot.
    rm -rf /var/lib/mysql && mkdir -p /var/lib/mysql && chown mysql:mysql /var/lib/mysql; \
    mkdir -p /var/lib/proxysql

COPY --from=build /out/bintrail /usr/local/bin/bintrail
# Gate the glibc/base coupling at build time: a symbol mismatch fails
# here, not in the user's first `docker run`.
RUN bintrail --version

COPY demo/image/my.cnf /etc/mysql/conf.d/demo.cnf
COPY demo/image/proxysql.cnf /etc/proxysql.cnf
COPY demo/image/shim.yaml demo/image/seed.sql /opt/bintrail/
COPY demo/image/entrypoint.sh demo/image/traffic.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/traffic.sh

# 6033 = ProxySQL (Time Travel SQL entry point), 3306 = raw MySQL.
EXPOSE 6033 3306

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
