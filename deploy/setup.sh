#!/usr/bin/env bash
# Bootstrap an Ubuntu 24.04 ARM64 (Graviton) EC2 instance for bintrail.
# Run as: bash setup.sh
set -euo pipefail

echo "==> Installing packages..."
sudo apt-get update -qq
sudo apt-get upgrade -y -qq
sudo apt-get install -y -qq docker.io docker-compose-v2 mysql-client git

echo "==> Enabling Docker..."
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"

echo ""
echo "==> Done. Next steps:"
echo "  1. Log out and back in (for docker group), or run: newgrp docker"
echo "  2. Set up a deploy key — see deploy/README.md Part H"
echo "  3. Clone the repo:  git clone git@github.com:nethalo/bintrail.git ~/bintrail"
echo "  4. Create .env:  cd ~/bintrail/deploy && cp .env.example .env && chmod 600 .env"
echo "  5. Edit .env:  nano ~/bintrail/deploy/.env"
echo "     Set RDS_ENDPOINT, BINTRAIL_RDS_USER/PASSWORD, TRAFFIC_RDS_USER/PASSWORD"
echo "     Set INDEX_HOST/INDEX_USER/INDEX_PASSWORD, GRAFANA_PASSWORD"
echo "  6. Launch:  cd ~/bintrail/deploy && docker compose up --build -d"
