# Bintrail — EC2 + RDS Deployment

One `docker compose up` on a Graviton EC2 instance that streams from RDS, indexes events, and shows the Grafana dashboard.

## Architecture

```
                      your IP only
                          │
                SSH (22)  │  Grafana (3000)
                          ▼
┌─────────────────────────────────────────────────────┐
│  EC2 (t4g.small, Graviton ARM64, Ubuntu 24.04)      │
│                                                      │
│  docker compose up:                                  │
│    ├── bintrail stream ──► RDS (index + source)     │
│    │       └── :9090 /metrics (internal only)        │
│    ├── traffic generator ──► RDS (demo writes)      │
│    ├── prometheus (scrapes :9090, internal only)     │
│    └── grafana (:3000, exposed to your IP)           │
└──────────────────────────────────────────────────────┘
         │                          │
         │  TCP 3306 (SG-to-SG)    │  TCP 3306 (SG-to-SG)
         ▼                          ▼
┌─────────────────────────────────────────────────────┐
│  RDS MySQL 8.0 (db.t4g.micro, Graviton ARM64)       │
│    ├── demo           (source — traffic writes)      │
│    ├── sbtest         (sysbench workload)            │
│    └── bintrail_index (index — bintrail writes)      │
│    Public access: NO                                 │
└─────────────────────────────────────────────────────┘
```

RDS has public access OFF — only reachable from EC2 via security group.
Prometheus binds to `127.0.0.1:9090` only (not internet-exposed).

---

## Part A — Create RDS Parameter Group

Do this **before** creating the RDS instance.

1. RDS → **Parameter groups** → **Create parameter group**
2. Family: `mysql8.0`, Name: `bintrail-binlog`, Description: "Binlog ROW+FULL for bintrail"
3. **Edit parameters** — set these values:

   | Parameter | Value |
   |---|---|
   | `binlog_format` | `ROW` |
   | `binlog_row_image` | `FULL` |
   | `gtid-mode` | `ON` |
   | `enforce_gtid_consistency` | `ON` |
   | `log_bin_trust_function_creators` | `1` |
   | `binlog_expire_logs_seconds` | `604800` |

4. **Save changes**

---

## Part B — Create RDS MySQL Instance

1. RDS → **Create database**
2. Engine: **MySQL 8.0**
3. Template: **Free tier**
4. DB instance identifier: `bintrail-demo`
5. Instance class: **db.t4g.micro** (Graviton, free tier eligible)
6. Master username: `admin` — set a strong password
7. Connectivity:
   - VPC: default
   - **Public access: No**
   - Create new security group: `bintrail-rds-sg`
8. Additional configuration:
   - DB parameter group: `bintrail-binlog` (from Part A)
   - Initial database name: `bintrail_index`
   - Enable automated backups: **yes** (required for binlogs on RDS)
   - Backup retention: 7 days
9. **Create database** — wait ~5 min for status: Available
10. Note the **Endpoint** (e.g. `bintrail-demo.abc123.us-east-1.rds.amazonaws.com`)

---

## Part C — Create EC2 Instance

1. EC2 → **Launch instance**
2. Name: `bintrail-demo`
3. AMI: **Ubuntu Server 24.04 LTS** — select the **arm64** version
4. Instance type: **t4g.small** (2 vCPU, 2 GB RAM, Graviton2)
5. Key pair: create or select existing
6. Network settings → **Create security group**: `bintrail-ec2-sg`
   - SSH (22): Source = **My IP**
   - Custom TCP 3000: Source = **My IP** (Grafana)
   - ⚠️ Do NOT expose 9090 — Prometheus stays internal
7. Storage: 20 GB gp3
8. **Launch instance** — note the public IP

---

## Part D — Connect Security Groups

Allow EC2 to reach RDS on port 3306:

1. RDS → **bintrail-demo** → **Security** tab → click `bintrail-rds-sg`
2. **Edit inbound rules** → **Add rule**:
   - Type: MySQL/Aurora (3306)
   - Source: `bintrail-ec2-sg` (select by name)
3. **Save rules**

The RDS security group should now have **exactly one inbound rule**: 3306 from `bintrail-ec2-sg`. No `0.0.0.0/0` rules.

---

## Part E — Initialize Demo Schema on RDS

Since RDS has public access OFF, connect through EC2:

```bash
ssh -i key.pem ubuntu@<EC2_IP>

# Install MySQL client
sudo apt-get update && sudo apt-get install -y mysql-client

# Test connectivity
mysql -h <RDS_ENDPOINT> -u admin -p -e "SELECT 1"

# Load demo schema (creates demo + sbtest databases)
mysql -h <RDS_ENDPOINT> -u admin -p < ~/bintrail/demo/sql/00-schema.sql

# Verify
mysql -h <RDS_ENDPOINT> -u admin -p -e "SHOW DATABASES"
# Expected: bintrail_index, demo, sbtest, ...
```

> `bintrail_index` was created by RDS via the "Initial database name" setting in Part B.

---

## Part F — Deploy on EC2

```bash
# SSH into EC2
ssh -i key.pem ubuntu@<EC2_IP>

# Run bootstrap script (installs Docker, clones repo, creates .env)
curl -fsSL https://raw.githubusercontent.com/bintrail/bintrail/main/deploy/setup.sh | bash
# OR after cloning manually:
bash ~/bintrail/deploy/setup.sh

# Re-login for Docker group (or run: newgrp docker)
exit
ssh -i key.pem ubuntu@<EC2_IP>

# Edit .env
nano ~/bintrail/deploy/.env
```

Fill in `.env`:
```bash
RDS_ENDPOINT=bintrail-demo.abc123.us-east-1.rds.amazonaws.com
RDS_USER=admin
RDS_PASSWORD=your-strong-password
SCHEMAS=demo,sbtest
SERVER_ID=99999
BATCH_SIZE=500
CHECKPOINT=5
```

```bash
# Lock down credentials
chmod 600 ~/bintrail/deploy/.env

# Build and launch
cd ~/bintrail/deploy
docker compose up --build -d

# Verify
docker compose logs -f bintrail   # look for "Checkpoint saved"
```

---

## Part G — Access & Verify

**From your laptop:**

- **Grafana**: `http://<EC2_PUBLIC_IP>:3000` — opens directly to the dashboard (no login)

**From EC2 (via SSH):**

```bash
# Prometheus targets (bintrail-stream should be UP)
curl -s localhost:9090/targets | grep -A3 bintrail

# Logs
cd ~/bintrail/deploy && docker compose logs -f

# Test checkpoint recovery
docker compose down && docker compose up -d
docker compose logs -f bintrail   # should resume from saved GTID position
```

---

## Security checklist

- [ ] RDS **public access: No**
- [ ] RDS security group has **only** one inbound rule: 3306 from `bintrail-ec2-sg`
- [ ] EC2 security group has **only**: SSH (22) from your IP, TCP 3000 from your IP
- [ ] No `0.0.0.0/0` inbound rules anywhere
- [ ] Prometheus (9090) bound to **127.0.0.1 only** in `compose.yml`
- [ ] `.env` is `chmod 600`
- [ ] RDS master password is not in any committed file (`.env` is gitignored)

---

## Teardown

```bash
# Stop services
cd ~/bintrail/deploy && docker compose down

# AWS Console: terminate EC2 instance + delete RDS instance
# (delete automated backups when prompted if you don't need them)
```
