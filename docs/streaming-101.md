# Bintrail Streaming 101

A step-by-step guide to set up bintrail with replication streaming, S3 archiving, Parquet querying, and the MCP client. This is the happy path — every command is shown in order with expected output.

**What you'll end up with:**

```
Source MySQL ──replication──► bintrail stream ──► Index MySQL (binlog_events)
                                                        │
                                              rotate ──►│──► S3 (Parquet archives)
                                                        │
                                query ◄─────────────────┘──► DuckDB (local/S3 Parquet)
                                                        │
                                MCP server ◄────────────┘
                                  │
                        Claude Code / Claude Desktop
```

---

## Prerequisites

- **Source MySQL 8.0+** with `binlog_format = ROW`, `binlog_row_image = FULL`, and GTIDs enabled (`gtid_mode = ON`, `enforce_gtid_consistency = ON`)
- **Index MySQL 8.0+** — a separate database (can be the same server or a different one)
- **Go 1.24+** installed (to build bintrail from source)
- **AWS account** (for S3 archiving)
- **Docker** (for mydumper)

### Install bintrail

```bash
git clone https://github.com/bintrail/bintrail
cd bintrail
make build
sudo cp bintrail /usr/local/bin/
sudo cp bintrail-mcp /usr/local/bin/
```

### Create the replication user on the source

```sql
CREATE USER 'bintrail_repl'@'%' IDENTIFIED BY 'a-strong-password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'bintrail_repl'@'%';
FLUSH PRIVILEGES;
```

The `SELECT` grant is needed for `bintrail snapshot` to read `information_schema`.

### Verify source settings

```sql
SHOW VARIABLES LIKE 'binlog_format';        -- must be ROW
SHOW VARIABLES LIKE 'binlog_row_image';     -- must be FULL
SHOW VARIABLES LIKE 'gtid_mode';            -- must be ON
```

---

## Step 1: Initialize the index database

```bash
bintrail init \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --partitions 48
```

Expected output:

```
Database "binlog_index" created (or already exists).
Tables created: binlog_events, schema_snapshots, index_state, stream_state, ...
Partitions created: 48 hourly partitions + p_future catch-all
```

This creates the index database and all required tables. The `--partitions 48` flag pre-allocates 48 hourly partitions (~2 days). Partitions are managed automatically later by `rotate`.

---

## Step 2: Set up AWS access for S3

Bintrail uses the standard AWS SDK credential chain. You need an IAM user (or role) with permissions to create and write to an S3 bucket.

### 2a. Create an IAM policy

In the AWS Console (IAM > Policies > Create policy), create a policy with this JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BintrailS3Setup",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:PutBucketPublicAccessBlock",
        "s3:PutLifecycleConfiguration",
        "s3:PutObject",
        "s3:GetObject",
        "s3:HeadObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::my-bintrail-archives",
        "arn:aws:s3:::my-bintrail-archives/*"
      ]
    }
  ]
}
```

Name it `BintrailS3Access`. The `CreateBucket`, `PutBucketPublicAccessBlock`, and `PutLifecycleConfiguration` actions are only needed once during `bintrail init --s3-bucket` — you can remove them from the policy after the bucket is created.

### 2b. Create an IAM user and attach the policy

```
IAM > Users > Create user > "bintrail" > Attach policy "BintrailS3Access"
```

Create an access key (IAM > Users > bintrail > Security credentials > Create access key) and note the Access Key ID and Secret Access Key.

### 2c. Configure credentials locally

Option A — environment variables (recommended for servers):

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_REGION=us-east-1
```

Option B — credentials file (recommended for development):

```bash
mkdir -p ~/.aws

cat > ~/.aws/credentials << 'EOF'
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
EOF

cat > ~/.aws/config << 'EOF'
[default]
region = us-east-1
EOF

chmod 600 ~/.aws/credentials
```

Option C — EC2/ECS instance role (recommended for production on AWS):

Attach the IAM policy to the instance profile or task role. No credential files needed — the SDK discovers credentials from the instance metadata service automatically.

---

## Step 3: Create the S3 bucket

```bash
bintrail init \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --s3-bucket  my-bintrail-archives \
  --s3-region  us-east-1
```

This creates the S3 bucket, blocks all public access, and sets a 1-year lifecycle expiry rule. If the bucket already exists, it verifies connectivity.

**Using an existing bucket instead?** Pass `--s3-arn` instead of `--s3-bucket`:

```bash
bintrail init \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --s3-arn     arn:aws:s3:::my-existing-bucket
```

---

## Step 4: Install mydumper via Docker

Mydumper creates logical dumps of MySQL databases. Bintrail uses its output to generate Parquet baseline snapshots.

Pull the mydumper Docker image:

```bash
docker pull mydumper/mydumper:latest
```

Verify it works:

```bash
docker run --rm mydumper/mydumper:latest mydumper --version
```

To use mydumper through bintrail's `dump` command, create a wrapper script that Docker runs transparently:

```bash
cat > /usr/local/bin/mydumper << 'SCRIPT'
#!/bin/bash
exec docker run --rm \
  --network host \
  -v "${@: -1}:${@: -1}" \
  mydumper/mydumper:latest \
  mydumper "$@"
SCRIPT
chmod +x /usr/local/bin/mydumper
```

> **Note:** If the wrapper above doesn't fit your networking setup, you can run mydumper directly inside Docker (see the dump step below for the full `docker run` command).

---

## Step 5: Take a schema snapshot

The snapshot captures column names, types, and primary key metadata from the source. The indexer uses this to resolve binlog column ordinals to names.

```bash
bintrail snapshot \
  --source-dsn "bintrail_repl:a-strong-password@tcp(source-db:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

Expected output:

```
Snapshotting schemas: mydb, appdb, ...
Snapshot complete: 42 tables captured (snapshot_id=1)
```

> Re-run `snapshot` after any schema changes (`ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`).

---

## Step 6: Take a full backup and move it to S3

This creates a Parquet baseline of all your table data — a point-in-time reference of every row.

### 6a. Dump the source database with mydumper

Using bintrail's `dump` command (requires the mydumper wrapper from step 4):

```bash
bintrail dump \
  --source-dsn "bintrail_repl:a-strong-password@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-output \
  --schemas mydb
```

Or directly with Docker (if you skipped the wrapper script):

```bash
docker run --rm \
  --network host \
  -v /tmp/mydumper-output:/tmp/mydumper-output \
  mydumper/mydumper:latest \
  mydumper \
    --host source-db --port 3306 \
    --user bintrail_repl --password a-strong-password \
    --database mydb \
    --outputdir /tmp/mydumper-output \
    --compress-protocol \
    --complete-insert \
    --threads 4
```

### 6b. Convert to Parquet and upload to S3

```bash
bintrail baseline \
  --input         /tmp/mydumper-output \
  --output        /tmp/baselines \
  --upload        s3://my-bintrail-archives/baselines/ \
  --upload-region us-east-1
```

Expected output:

```
Processing mydb.orders ... 125000 rows → /tmp/baselines/2026-03-03T09-00-00Z/mydb/orders.parquet
Processing mydb.users  ... 50000 rows  → /tmp/baselines/2026-03-03T09-00-00Z/mydb/users.parquet
Uploading to s3://my-bintrail-archives/baselines/ ... 2 files uploaded
```

No database connection required for the conversion — it operates purely on the mydumper output files.

If the upload is interrupted, re-run with `--retry` to skip already-uploaded files:

```bash
bintrail baseline \
  --input         /tmp/mydumper-output \
  --output        /tmp/baselines \
  --upload        s3://my-bintrail-archives/baselines/ \
  --upload-region us-east-1 \
  --retry
```

---

## Step 7: Start the replication stream

Get the current GTID position from the source:

```sql
SHOW MASTER STATUS;
-- Note the Executed_Gtid_Set value, e.g.:
-- 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50000
```

Start streaming (first run — provide the GTID):

```bash
bintrail stream \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --source-dsn "bintrail_repl:a-strong-password@tcp(source-db:3306)/" \
  --server-id  99999 \
  --start-gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50000" \
  --metrics-addr :9090
```

The stream runs indefinitely, indexing every row event as it happens. It saves a checkpoint every 10 seconds to `stream_state`, so it can resume from where it left off if stopped.

**Subsequent runs** resume automatically from the checkpoint — no `--start-gtid` needed:

```bash
bintrail stream \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --source-dsn "bintrail_repl:a-strong-password@tcp(source-db:3306)/" \
  --server-id  99999 \
  --metrics-addr :9090
```

**Graceful shutdown:** Send `SIGTERM` or press Ctrl-C. The current batch is flushed and the checkpoint is written before exit — no events are lost.

### Run as a systemd service (recommended for production)

Create `/etc/bintrail/env` (mode `0600`):

```ini
INDEX_DSN=user:pass@tcp(127.0.0.1:3306)/binlog_index
SOURCE_DSN=bintrail_repl:a-strong-password@tcp(source-db:3306)/
```

Create `/etc/systemd/system/bintrail-stream.service`:

```ini
[Unit]
Description=Bintrail replication stream
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=5
User=bintrail
EnvironmentFile=/etc/bintrail/env
ExecStart=/usr/local/bin/bintrail stream \
  --index-dsn  "${INDEX_DSN}" \
  --source-dsn "${SOURCE_DSN}" \
  --server-id  99999 \
  --metrics-addr :9090
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now bintrail-stream
sudo journalctl -u bintrail-stream -f
```

### Verify it's working

Check the Prometheus metrics:

```bash
curl -s localhost:9090/metrics | grep bintrail_stream_events_indexed_total
# bintrail_stream_events_indexed_total 12345

curl -s localhost:9090/metrics | grep bintrail_stream_replication_lag_seconds
# bintrail_stream_replication_lag_seconds 0.5
```

Or check the index status:

```bash
bintrail status --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

---

## Step 8: Set up partition rotation as a daemon

Rotation drops old partitions (reclaiming disk space), archives them to Parquet, uploads to S3, and adds new partitions to keep the rolling window.

First, find your server's bintrail ID (shown in `bintrail status` output or in the `bintrail_servers` table):

```bash
bintrail status --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
# Look for the bintrail_id in the output
```

Or query it directly:

```sql
SELECT bintrail_id FROM bintrail_servers LIMIT 1;
```

### Run rotation as a daemon

```bash
bintrail rotate \
  --index-dsn           "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain              7d \
  --archive-dir         /var/lib/bintrail/archives \
  --archive-compression zstd \
  --bintrail-id         YOUR-BINTRAIL-ID \
  --archive-s3          s3://my-bintrail-archives/events/ \
  --archive-s3-region   us-east-1 \
  --daemon \
  --interval            1h
```

This runs every hour and:
1. Archives partitions older than 7 days to local Parquet files
2. Uploads the Parquet files to S3
3. Drops the archived partitions from MySQL
4. Adds new future partitions to replace the dropped ones

### Run as a systemd service

Create `/etc/systemd/system/bintrail-rotate.service`:

```ini
[Unit]
Description=Bintrail partition rotation daemon
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=5
User=bintrail
EnvironmentFile=/etc/bintrail/env
ExecStart=/usr/local/bin/bintrail rotate \
  --index-dsn           "${INDEX_DSN}" \
  --retain              7d \
  --archive-dir         /var/lib/bintrail/archives \
  --archive-compression zstd \
  --bintrail-id         YOUR-BINTRAIL-ID \
  --archive-s3          s3://my-bintrail-archives/events/ \
  --archive-s3-region   us-east-1 \
  --daemon \
  --interval            1h
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now bintrail-rotate
```

---

## Step 9: Query local Parquet archives

Once rotation has archived partitions to local Parquet files, you can query them alongside the live index:

```bash
bintrail query \
  --index-dsn   "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema      mydb \
  --table       orders \
  --since       "2026-02-01 00:00:00" \
  --archive-dir /var/lib/bintrail/archives \
  --bintrail-id YOUR-BINTRAIL-ID
```

This fetches results from both the live MySQL index and the local Parquet archives, merges them (deduplicating by event ID), sorts by timestamp, and applies the limit.

The `--bintrail-id` flag scopes the archive path to your server's UUID — it builds the glob pattern `bintrail_id=<uuid>/**/*.parquet`.

**Query only archived data** (no live index):

```bash
bintrail query \
  --index-dsn   "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema      mydb \
  --table       orders \
  --event-type  DELETE \
  --since       "2026-01-15 00:00:00" \
  --until       "2026-01-16 00:00:00" \
  --archive-dir /var/lib/bintrail/archives \
  --bintrail-id YOUR-BINTRAIL-ID \
  --format      json
```

---

## Step 10: Query S3 Parquet archives

Same syntax, but use `--archive-s3` instead of `--archive-dir`:

```bash
bintrail query \
  --index-dsn   "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema      mydb \
  --table       orders \
  --since       "2026-01-01 00:00:00" \
  --archive-s3  s3://my-bintrail-archives/events/ \
  --bintrail-id YOUR-BINTRAIL-ID
```

This uses DuckDB's `httpfs` extension under the hood to read Parquet files directly from S3. AWS credentials are resolved from the standard chain (environment variables, `~/.aws/credentials`, or instance metadata).

**Combine local and S3 archives** by using both flags:

```bash
bintrail query \
  --index-dsn   "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema      mydb \
  --table       orders \
  --archive-dir /var/lib/bintrail/archives \
  --archive-s3  s3://my-bintrail-archives/events/ \
  --bintrail-id YOUR-BINTRAIL-ID
```

Results from all three sources (live index + local Parquet + S3 Parquet) are merged, deduplicated, and sorted.

> **Note:** Archive query failures (e.g., S3 connection issues) are logged as warnings and skipped — they don't block results from the live index or local archives.

---

## Step 11: Set up the MCP client

The MCP server lets Claude Code or Claude Desktop query your binlog index conversationally — ask questions in natural language instead of typing CLI commands.

### Option A: Claude Code (same machine — automatic)

If you're using Claude Code in the bintrail project directory, the `.mcp.json` file registers the MCP server automatically.

1. Edit `.mcp.json` in the project root to set your DSN:

   ```json
   {
     "mcpServers": {
       "bintrail": {
         "command": "go",
         "args": ["run", "./cmd/bintrail-mcp"],
         "env": {
           "BINTRAIL_INDEX_DSN": "user:pass@tcp(127.0.0.1:3306)/binlog_index"
         }
       }
     }
   }
   ```

2. Open a Claude Code session in the project directory. The bintrail tools (`query`, `recover`, `status`) are available immediately.

3. Try asking:

   ```
   "What tables had deletions in the last hour?"
   "Show me all changes to mydb.orders since 2pm today"
   "Generate SQL to restore customer 42"
   ```

### Option B: Claude Code (any directory — pre-built binary)

If you want to use the MCP tools from any directory (not just the bintrail project), register the server globally:

```bash
# Build the binary
cd /path/to/bintrail
go build -o /usr/local/bin/bintrail-mcp ./cmd/bintrail-mcp

# Register with Claude Code
claude mcp add bintrail -- env BINTRAIL_INDEX_DSN='user:pass@tcp(127.0.0.1:3306)/binlog_index' bintrail-mcp
```

### Option C: Claude Desktop (same or different machine — via HTTP + proxy)

This setup lets any machine on the network use the bintrail tools in Claude Desktop, without installing Go.

**On the machine with bintrail installed:**

```bash
# Start the HTTP MCP server
BINTRAIL_INDEX_DSN='user:pass@tcp(127.0.0.1:3306)/binlog_index' \
  bintrail-mcp --http :8080
```

Keep it running with systemd, launchd, or tmux.

**On the machine where Claude Desktop runs:**

1. Copy the proxy script from the bintrail repo (no dependencies — pure Python stdlib):

   ```bash
   scp user@bintrail-host:~/bintrail/cmd/bintrail-mcp/proxy.py ~/proxy.py
   ```

2. Test the connection:

   ```bash
   BINTRAIL_SERVER=http://bintrail-host:8080/mcp python3 ~/proxy.py <<'EOF'
   {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
   {"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
   EOF
   ```

   You should see two JSON responses. If you do, the server is reachable.

3. Edit the Claude Desktop config:

   **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
   **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

   ```json
   {
     "mcpServers": {
       "bintrail": {
         "command": "python3",
         "args": ["/Users/you/proxy.py"],
         "env": {
           "BINTRAIL_SERVER": "http://bintrail-host:8080/mcp"
         }
       }
     }
   }
   ```

4. Restart Claude Desktop. The `query`, `recover`, and `status` tools appear automatically.

### Troubleshooting MCP

| Symptom | Cause | Fix |
|---------|-------|-----|
| Tools fail after server restart | Proxy has stale session ID | Restart Claude Desktop |
| "Both calls failed" in test | HTTP server not running or wrong IP | Check the server is running and the IP/port are correct |
| No bintrail tools appear | Config JSON is malformed | Validate JSON syntax; check for missing commas or braces |

---

## What to do next

- **After schema changes** (`ALTER TABLE`, etc.): re-run `bintrail snapshot` to keep the column metadata current
- **Monitor replication lag**: `curl localhost:9090/metrics | grep replication_lag`
- **Check index health**: `bintrail status --index-dsn "..."`
- **Recover deleted rows**: see [Practical Guide — Scenario A](guide.md#scenario-a-someone-accidentally-deleted-rows)
- **Roll back a bad UPDATE**: see [Practical Guide — Scenario B](guide.md#scenario-b-a-bad-update-went-out--need-to-roll-back-column-values)
- **Debug logging**: `bintrail --log-level debug --log-format json stream ...`
