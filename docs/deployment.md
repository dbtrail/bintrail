# Bintrail Production Deployment Guide

This guide covers everything needed to run bintrail in production: infrastructure sizing, network topology, deployment options, observability, and operational procedures.

## 1. Architecture Overview

```
┌──────────────┐    replication     ┌─────────────────┐
│ Source MySQL  │◄──────────────────│ bintrail stream  │
│ (binlogs)    │    protocol        │ (long-running)   │──── :9090 /metrics
└──────────────┘                    └────────┬─────────┘
                                             │ SQL writes
                                     ┌───────▼────────┐
                                     │  Index MySQL   │
                                     │ (binlog_events │
                                     │  partitioned)  │
                                     └────────────────┘
                                             ▲
┌──────────────┐   scrape :9090     ┌────────┴─────────┐
│  Prometheus  │◄───────────────────│ bintrail stream  │
└──────┬───────┘                    └─────────────────-┘
       │ query
┌──────▼───────┐
│   Grafana    │  ← dashboard provisioned from JSON
└──────────────┘
       │ alerts
┌──────▼───────┐
│ Alertmanager │  (optional)
└──────────────┘

On-demand (DBA workstation):
  bintrail query   ──► Index MySQL ──► stdout
  bintrail recover ──► Index MySQL ──► .sql file
  bintrail rotate  ──► Index MySQL ──► partition maintenance
```

### Component summary

| Component | Role | Always-on? |
|-----------|------|-----------|
| Source MySQL | Origin database being tracked | Yes (pre-existing) |
| Index MySQL | Stores `binlog_events`, `stream_state`, `schema_snapshots` | Yes |
| `bintrail stream` | Replication client; writes events to index | Yes |
| Prometheus | Scrapes `/metrics` from stream process | Yes |
| Grafana | Dashboards and alerting | Yes |
| Alertmanager | Routes alert notifications | Optional |
| `bintrail query` / `recover` | DBA tools; read from index | On-demand |
| `bintrail rotate` | Drops old partitions; adds future ones | Scheduled (daily cron) |

## 2. Source MySQL Requirements

### Version and configuration

- MySQL 8.0 or later
- `binlog_format = ROW` (required — bintrail refuses to index non-ROW binlogs)
- `binlog_row_image = FULL` (required — bintrail validates this on startup)
- GTID mode strongly recommended for reliable resume after restarts:
  ```
  gtid_mode = ON
  enforce_gtid_consistency = ON
  ```

### Replication user

```sql
CREATE USER 'bintrail'@'%' IDENTIFIED BY 'strong-password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'bintrail'@'%';
GRANT SELECT ON *.* TO 'bintrail'@'%';  -- needed for bintrail snapshot
FLUSH PRIVILEGES;
```

The `SELECT` grant is only needed during `bintrail snapshot`. If you prefer minimal ongoing permissions, grant `SELECT` temporarily for the snapshot, then revoke it.

### Managed MySQL (RDS / Aurora / Cloud SQL)

| Platform | Notes |
|----------|-------|
| AWS RDS MySQL 8.0 | Enable automated backups (required for binlog). Set `binlog_format=ROW`, `binlog_row_image=FULL` in parameter group. Use `rds_replication` role instead of `REPLICATION SLAVE`. |
| AWS Aurora MySQL | Same parameter group requirements. Binlog retention: `CALL mysql.rds_set_configuration('binlog retention hours', 168)`. |
| Google Cloud SQL | Enable binary logging in instance settings. Create user with `REPLICATION SLAVE` via IAM or native auth. |
| Azure Database for MySQL | Enable `binlog_row_image=FULL` in server parameters. Flexible Server supports replication. |

## 3. Index MySQL Requirements

### Version

MySQL 8.0 or later. The `binlog_events` table uses `RANGE (TO_DAYS(...))` partitioning and generated stored columns — both require MySQL 8.0+.

### Separate server recommended

Run the index database on a separate MySQL instance from the source. This provides:
- **Failure isolation**: index failures don't affect the source
- **Write amplification**: bintrail generates significant write traffic — separate I/O from application queries
- **Security**: index credentials don't grant access to application data

### Sizing

Event storage baseline: ~500 bytes/event (varies with row width and JSON column content).

```
required_storage_GB = daily_events × retention_days × avg_event_bytes / 1e9
```

Example: 1 million events/day × 30 days × 800 bytes = ~24 GB

Add 30% overhead for InnoDB page structure, indexes, and partition metadata.

Monitor `information_schema.PARTITIONS` (the `TABLE_ROWS` estimate) and disk usage with:

```bash
bintrail status --index-dsn "$INDEX_DSN"
```

### InnoDB tuning

```ini
[mysqld]
innodb_buffer_pool_size = 70%           # 50-70% of available RAM
innodb_flush_log_at_trx_commit = 2      # acceptable for index (not source)
innodb_log_file_size = 1G
innodb_flush_method = O_DIRECT
```

`innodb_flush_log_at_trx_commit = 2` trades a tiny recovery window (up to 1s of events) for significantly better write throughput. Since bintrail can replay from the binlog position in `stream_state`, this tradeoff is acceptable.

## 4. Network Topology

### Required connections

| From | To | Port | Protocol |
|------|----|------|----------|
| `bintrail stream` | Source MySQL | 3306 | MySQL replication (COM_BINLOG_DUMP_GTID) |
| `bintrail stream` | Index MySQL | 3306 | Standard MySQL |
| Prometheus | `bintrail stream` | 9090 | HTTP GET /metrics |
| Grafana | Prometheus | 9090 | HTTP PromQL API |
| DBA workstation | Index MySQL | 3306 | Standard MySQL (for query/recover/rotate) |

### Firewall rules

```
# bintrail host outbound
TCP 3306 → source MySQL host
TCP 3306 → index MySQL host

# bintrail host inbound
TCP 9090 ← Prometheus host (metrics scrape)

# Prometheus host outbound
TCP 9090 → bintrail host

# Grafana host outbound
TCP 9090 → Prometheus host

# DBA workstation outbound
TCP 3306 → index MySQL host
```

The bintrail metrics port (9090) does not need to be exposed to the public internet.

## 5. Deployment Options

### systemd (recommended)

Create `/etc/systemd/system/bintrail-stream.service`:

```ini
[Unit]
Description=Bintrail stream replication indexer
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=bintrail
Group=bintrail
EnvironmentFile=/etc/bintrail/stream.env
ExecStart=/usr/local/bin/bintrail stream \
    --index-dsn  "${INDEX_DSN}" \
    --source-dsn "${SOURCE_DSN}" \
    --server-id  "${SERVER_ID}" \
    --batch-size "${BATCH_SIZE:-500}" \
    --checkpoint "${CHECKPOINT:-10}" \
    --schemas    "${SCHEMAS}" \
    --metrics-addr "${METRICS_ADDR:-:9090}" \
    --log-format json
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=bintrail-stream

[Install]
WantedBy=multi-user.target
```

`/etc/bintrail/stream.env` (mode 0600, owned by root):

```bash
INDEX_DSN=bintrail:password@tcp(index-mysql:3306)/bintrail_index
SOURCE_DSN=bintrail:password@tcp(source-mysql:3306)/
SERVER_ID=1234
SCHEMAS=myapp,myapp_archive
METRICS_ADDR=:9090
```

```bash
systemctl daemon-reload
systemctl enable --now bintrail-stream
journalctl -u bintrail-stream -f
```

### Docker

The demo `compose.yml` is a working example. For production, adjust:
- Use Docker secrets or environment files instead of inline credentials
- Pin image versions (`FROM golang:1.25.7-alpine` in your Dockerfile)
- Mount a named volume for any persistent state (the index MySQL is the real persistent state — bintrail itself is stateless)
- Set resource limits (`mem_limit`, `cpus`) on the bintrail container

```yaml
services:
  bintrail:
    image: your-registry/bintrail:v1.2.3
    env_file: ./stream.env
    restart: always
    deploy:
      resources:
        limits:
          memory: 512M
```

### Kubernetes

Minimal Deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bintrail-stream
spec:
  replicas: 1   # must be 1 — multiple replicas would create duplicate events
  selector:
    matchLabels:
      app: bintrail-stream
  template:
    metadata:
      labels:
        app: bintrail-stream
    spec:
      containers:
        - name: bintrail
          image: your-registry/bintrail:v1.2.3
          args:
            - stream
            - --index-dsn=$(INDEX_DSN)
            - --source-dsn=$(SOURCE_DSN)
            - --server-id=$(SERVER_ID)
            - --metrics-addr=:9090
            - --log-format=json
          envFrom:
            - secretRef:
                name: bintrail-stream
          ports:
            - containerPort: 9090
              name: metrics
          resources:
            requests:
              memory: "128Mi"
              cpu: "100m"
            limits:
              memory: "512Mi"
```

For Prometheus Operator, add a `ServiceMonitor`:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: bintrail-stream
spec:
  selector:
    matchLabels:
      app: bintrail-stream
  endpoints:
    - port: metrics
      interval: 15s
```

> **Important:** Always run exactly one replica of `bintrail stream` per source MySQL. Multiple replicas would index duplicate events with different `server_id` values. If you need HA, use a leader-election sidecar or rely on the systemd/Kubernetes restart mechanism.

## 6. Initial Setup Procedure

```bash
# 1. Provision the index database (run once)
bintrail init \
    --index-dsn "$INDEX_DSN" \
    --partitions 30          # 30 daily partitions + p_future

# 2. Snapshot current schema (run once per schema change after this)
bintrail snapshot \
    --source-dsn "$SOURCE_DSN" \
    --index-dsn  "$INDEX_DSN" \
    --schemas    "myapp"

# 3. Capture current GTID position (start streaming from here)
mysql -h source-host -u bintrail -p -e "SELECT @@global.gtid_executed\G"
# → e.g. "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-12345"

# 4. Start streaming
bintrail stream \
    --index-dsn  "$INDEX_DSN" \
    --source-dsn "$SOURCE_DSN" \
    --server-id  "1234" \
    --start-gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-12345" \
    --schemas    "myapp" \
    --metrics-addr ":9090" \
    --log-format json

# 5. Verify with status
bintrail status --index-dsn "$INDEX_DSN"
```

After the first successful checkpoint, restart without `--start-gtid` — the position is persisted in `stream_state` and will be used automatically.

## 7. Observability

### Prometheus scrape config

```yaml
# /etc/prometheus/prometheus.yml
global:
  scrape_interval: 15s     # 15s for production (5s is demo only)
  evaluation_interval: 15s

scrape_configs:
  - job_name: bintrail-stream
    static_configs:
      - targets: ['bintrail-host:9090']
    # If on Kubernetes, use kubernetes_sd_configs instead
```

### Alerting rules

```yaml
# /etc/prometheus/rules/bintrail.yml
groups:
  - name: bintrail
    rules:
      - alert: BintrailReplicationLagHigh
        expr: bintrail_stream_replication_lag_seconds > 60
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Bintrail replication lag is high"
          description: "Lag is {{ $value | humanizeDuration }} — stream may be falling behind."

      - alert: BintrailReplicationLagCritical
        expr: bintrail_stream_replication_lag_seconds > 300
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Bintrail replication lag is critical"
          description: "Lag is {{ $value | humanizeDuration }} — stream is severely behind or stalled."

      - alert: BintrailStreamErrors
        expr: rate(bintrail_stream_errors_total[1m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Bintrail stream is experiencing errors"
          description: "Error type {{ $labels.type }} — check logs for details."

      - alert: BintrailStreamDown
        expr: up{job="bintrail-stream"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Bintrail stream process is unreachable"
          description: "Prometheus cannot scrape bintrail metrics endpoint."
```

### Grafana dashboard

The demo ships a pre-built dashboard at `demo/grafana/dashboards/bintrail-stream.json`. To use it in your production Grafana:

1. Go to **Dashboards → Import**
2. Upload `bintrail-stream.json` or paste its contents
3. Select your Prometheus datasource
4. Adjust the replication lag thresholds (currently 5s/30s) to match your SLO

### Log aggregation

Run with `--log-format json` for structured output compatible with log aggregators:

```bash
# Loki / Promtail: configure to scrape from journald or container logs
journalctl -u bintrail-stream -o json | promtail --stdin ...

# CloudWatch Logs (ECS/EC2)
# Set awslogs log driver — structured JSON maps to CloudWatch Log Insights

# ELK / OpenSearch
# Filebeat with JSON input reads from the log file or journald
```

Key log fields emitted by bintrail commands:

| Field | Type | Description |
|-------|------|-------------|
| `level` | string | `info`, `warn`, `error` |
| `msg` | string | Log message |
| `files_processed` | int | (`index` complete) |
| `events_indexed` | int | (`index`/`stream` complete) |
| `results` | int | (`query` complete) |
| `statements` | int | (`recover` complete) |
| `duration_ms` | int | Operation duration |

## 8. Partition Rotation

Add a daily cron job or systemd timer:

```bash
# /etc/cron.d/bintrail-rotate
0 2 * * * bintrail /usr/local/bin/bintrail rotate \
    --index-dsn "$INDEX_DSN" \
    --retain 30d \
    --add-future 3 \
    --log-format json >> /var/log/bintrail-rotate.log 2>&1
```

`--retain 30d` drops partitions older than 30 days. `--add-future 3` ensures at least 3 future daily partitions exist beyond today (prevents the catch-all `p_future` from accumulating all new events).

Monitor the `p_future` row count via `bintrail status` — a growing `p_future` means rotation isn't running.

## 9. Security

### Credential management

Never pass DSN credentials as CLI arguments — they appear in `ps aux` and process tables. Use:
- **systemd**: `EnvironmentFile=/etc/bintrail/stream.env` (mode 0600, owned root:root)
- **Docker**: `env_file:` with a secrets-managed file, or Docker Swarm/BuildKit secrets
- **Kubernetes**: `envFrom.secretRef` pointing to a `Secret` object

### TLS for database connections

Append `?tls=true` (or `?tls=skip-verify` for self-signed certs in dev) to both DSNs:

```bash
INDEX_DSN="bintrail:password@tcp(index-mysql:3306)/bintrail_index?tls=true"
SOURCE_DSN="bintrail:password@tcp(source-mysql:3306)/?tls=true"
```

### Metrics endpoint security

The `/metrics` endpoint has no built-in authentication. Bind it to an internal interface:

```bash
--metrics-addr "10.0.0.5:9090"   # internal IP only
```

Or use a reverse proxy (nginx, Caddy) with basic auth in front if the endpoint must be accessible across network boundaries.

## 10. Schema Change Workflow

When you run `ALTER TABLE` on the source:

1. Run `ALTER TABLE` on the source as normal — the stream will continue.
2. Re-run `bintrail snapshot` to update the schema snapshot in the index:
   ```bash
   bintrail snapshot \
       --source-dsn "$SOURCE_DSN" \
       --index-dsn  "$INDEX_DSN" \
       --schemas    "myapp"
   ```
3. No stream restart is needed. The new snapshot is used for all subsequent queries and recovery.

> **Note:** Bintrail logs a `column count mismatch` warning for events on a changed table until the snapshot is updated. Those events are skipped — they are not indexed. Take the snapshot promptly after DDL changes.

## 11. Backup and Recovery

### Index database backups

The index database is reconstructable by re-indexing from binlogs, but this is slow (hours for large histories). Schedule regular backups:

```bash
# Logical backup (small-to-medium indexes)
mysqldump --single-transaction --databases bintrail_index \
  | gzip > /backups/bintrail-index-$(date +%Y%m%d).sql.gz

# Physical backup (large indexes, minimal downtime)
xtrabackup --backup --target-dir=/backups/bintrail-$(date +%Y%m%d)/
```

### stream_state is critical

The `stream_state` table contains the resume position (GTID set or file+position). Without it, you must specify `--start-gtid` manually on the next start, or stream will start from the beginning of available binlogs.

Back up `stream_state` before any index database maintenance:

```bash
mysqldump --single-transaction bintrail_index stream_state \
  > /backups/stream_state-$(date +%Y%m%d-%H%M%S).sql
```

### Reconstructing the index

If the index is lost and binlogs are still available on the source:

```bash
# 1. Re-init
bintrail init --index-dsn "$INDEX_DSN" --partitions 90

# 2. Re-snapshot
bintrail snapshot --source-dsn "$SOURCE_DSN" --index-dsn "$INDEX_DSN" --schemas "myapp"

# 3. Re-index from all available binlog files
bintrail index \
    --index-dsn   "$INDEX_DSN" \
    --source-dsn  "$SOURCE_DSN" \
    --binlog-dir  /var/lib/mysql \
    --all \
    --schemas     "myapp"
```

`bintrail index --all` processes every binlog file in `--binlog-dir` in order. This is I/O intensive — run during off-peak hours.

## 12. Troubleshooting

See also: `docs/guide.md` for scenario-based walkthroughs and a detailed FAQ.

### High replication lag

```bash
# Check current lag
curl -s 'localhost:9090/api/v1/query?query=bintrail_stream_replication_lag_seconds' | jq '.data.result[0].value[1]'

# Check event throughput
bintrail status --index-dsn "$INDEX_DSN"

# Check index MySQL write latency
mysql -h index-mysql -e "SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits'"
```

Causes: index MySQL under heavy load, batch size too small (increase `--batch-size`), network latency to source.

### Disk full on index MySQL

```bash
# Check partition sizes
bintrail status --index-dsn "$INDEX_DSN"

# Emergency: rotate with shorter retention
bintrail rotate --index-dsn "$INDEX_DSN" --retain 7d

# Reclaim space immediately (InnoDB does not auto-shrink)
# For each dropped partition, space is reclaimed automatically (DROP PARTITION is O(1) for InnoDB)
```

### Stream process crash recovery

With systemd `Restart=always`, the process restarts automatically. Bintrail resumes from the GTID in `stream_state`. Check:

```bash
journalctl -u bintrail-stream --since "5 minutes ago"
```

If `stream_state` is empty (fresh install or lost), provide `--start-gtid` explicitly. The safest value is the `gtid_executed` from the source at the time of the last `bintrail snapshot`.

### "column count mismatch" warnings

Expected after `ALTER TABLE`. Run `bintrail snapshot` to update the schema. Events for that table are skipped until the snapshot is updated.

### GTID gaps / duplicate server-id

If you see GTID-related errors:
- Ensure `--server-id` is unique across all MySQL replication clients connected to the source
- Do not use server IDs in the range the source MySQL uses (check `SHOW VARIABLES LIKE 'server_id'` on source)
- For RDS/Aurora: use server IDs > 1000000 to avoid conflicts with AWS-managed replicas
