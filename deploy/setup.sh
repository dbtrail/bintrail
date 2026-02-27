#!/usr/bin/env bash
# Bootstrap an Ubuntu 24.04 ARM64 (Graviton) EC2 instance for bintrail.
# Run as: bash setup.sh
set -euo pipefail

echo "==> Installing packages..."
sudo apt-get update -qq
sudo apt-get install -y -qq docker.io docker-compose-v2 mysql-client git

echo "==> Enabling Docker..."
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"

echo "==> Cloning bintrail..."
if [ ! -d "$HOME/bintrail" ]; then
    git clone https://github.com/bintrail/bintrail.git "$HOME/bintrail"
fi

echo "==> Creating .env from template..."
cd "$HOME/bintrail/deploy"
if [ ! -f .env ]; then
    cp .env.example .env
    chmod 600 .env
    echo "    Edit .env with your RDS endpoint and password:"
    echo "    nano $HOME/bintrail/deploy/.env"
else
    echo "    .env already exists — skipping."
fi

echo ""
echo "==> Next steps:"
echo "  1. Log out and back in (for docker group), or run: newgrp docker"
echo "  2. Edit .env:  nano ~/bintrail/deploy/.env"
echo "     Set RDS_ENDPOINT/RDS_USER/RDS_PASSWORD (source database)"
echo "     Set INDEX_HOST/INDEX_USER/INDEX_PASSWORD (index EC2 Percona)"
echo "  3. Load demo schema (from this host, since RDS is private):"
echo "     mysql -h <RDS_ENDPOINT> -u admin -p < ~/bintrail/demo/sql/00-schema.sql"
echo "  4. Initialize bintrail index on the index EC2 — see deploy/README.md Part F"
echo "  5. Launch:  cd ~/bintrail/deploy && docker compose up --build -d"
