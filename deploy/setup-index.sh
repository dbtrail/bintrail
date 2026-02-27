#!/usr/bin/env bash
# Bootstrap a Percona Server 8.0 index node on Ubuntu 24.04 ARM64.
# Run as: bash setup-index.sh
set -euo pipefail

read -rsp "Enter password for the 'bintrail' MySQL user: " BINTRAIL_PASSWORD
echo

echo "==> Installing Percona release package..."
TMP_DEB="$(mktemp /tmp/percona-release-XXXXXX.deb)"
curl -fsSL \
    "https://repo.percona.com/apt/percona-release_latest.generic_all.deb" \
    -o "$TMP_DEB"
sudo dpkg -i "$TMP_DEB"
rm -f "$TMP_DEB"

echo "==> Setting up Percona Server 8.0 repository..."
sudo percona-release setup ps80

echo "==> Upgrading OS packages..."
sudo apt-get upgrade -y -qq

echo "==> Installing Percona Server 8.0 (this may take a few minutes)..."
sudo apt-get install -y percona-server-server

echo "==> Configuring MySQL to bind on all interfaces..."
OVERRIDE_DIR="/etc/mysql/mysql.conf.d"
sudo tee "$OVERRIDE_DIR/bintrail-bind.cnf" > /dev/null <<'EOF'
[mysqld]
bind-address = 0.0.0.0
EOF

echo "==> Restarting MySQL..."
sudo systemctl restart mysql

echo "==> Creating bintrail_index database and user..."
# Run as root (Percona 8.0 uses unix_socket auth for root by default)
sudo mysql <<SQL
CREATE DATABASE IF NOT EXISTS bintrail_index
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'bintrail'@'%'
    IDENTIFIED BY '${BINTRAIL_PASSWORD}';

GRANT CREATE, ALTER, DROP, SELECT, INSERT, UPDATE, DELETE
    ON bintrail_index.* TO 'bintrail'@'%';

FLUSH PRIVILEGES;
SQL

echo ""
echo "==> Percona Server 8.0 is ready."
echo ""
echo "Next steps:"
echo "  1. Note this instance's private IP (from AWS console or: hostname -I)"
echo "  2. On the app EC2, set in deploy/.env:"
echo "       INDEX_HOST=<this-private-ip>"
echo "       INDEX_USER=bintrail"
echo "       INDEX_PASSWORD=<the password you just set>"
echo "  3. Verify connectivity from the app EC2:"
echo "       mysql -h <INDEX_PRIVATE_IP> -u bintrail -p -e 'SHOW DATABASES'"
echo "  4. bintrail will create tables automatically on first 'docker compose up'."
