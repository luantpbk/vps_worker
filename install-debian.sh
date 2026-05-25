#!/bin/bash

set -e

echo "=============================="
echo " TikTok Debian Worker Install "
echo "=============================="

# ==========================================
# PARAMS
# ==========================================
# Usage:
# bash install.sh <worker_name> <env_base64>
#
# Example:
# bash install.sh worker01 QVBJX0tFWT1hYmMKVE9LRU49eHl6
# ==========================================

WORKER_NAME="$1"
ENV_CONTENT="$2"

if [ -z "$WORKER_NAME" ] || [ -z "$ENV_CONTENT" ]; then
    echo ""
    echo "Usage:"
    echo "bash install.sh <worker_name> <env_content>"
    echo ""
    exit 1
fi

# ==========================================
# CHECK ROOT
# ==========================================

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi


ENV_CONTENT="$2"

# ==========================================
# UPDATE SYSTEM
# ==========================================

echo ""
echo "=== Update system ==="

apt update -y
apt upgrade -y

# ==========================================
# INSTALL PACKAGES
# ==========================================

echo ""
echo "=== Install packages ==="

apt install -y \
    curl \
    wget \
    git \
    nano \
    ca-certificates \
    gnupg

# ==========================================
# INSTALL NODEJS 20
# ==========================================

echo ""
echo "=== Install NodeJS 20 ==="

mkdir -p /etc/apt/keyrings

curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
| gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg

echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" \
> /etc/apt/sources.list.d/nodesource.list

apt update -y

apt install -y nodejs

echo ""
echo "=== Node version ==="

node -v
npm -v

# ==========================================
# CLONE SOURCE
# ==========================================

echo ""
echo "=== Clone source ==="

rm -rf /root/worker

git clone https://github.com/luantpbk/vps_worker.git /root/worker

cd /root/worker

# ==========================================
# INSTALL NPM
# ==========================================

echo ""
echo "=== Install npm packages ==="

npm install

# ==========================================
# UPDATE workerName
# ==========================================

echo ""
echo "=== Update workerName ==="

sed -i "s/\"workerName\": *\"VPS_Worker_08\"/\"workerName\": \"$WORKER_NAME\"/g" vps_config.json

# ==========================================
# CREATE .ENV
# ==========================================

echo ""
echo "=== Create .env ==="

echo -e "SOCKET_SECRET=\"$ENV_CONTENT\"" > .env

# ==========================================
# INSTALL PM2
# ==========================================

echo ""
echo "=== Install PM2 ==="

npm install -g pm2

# ==========================================
# START WORKER
# ==========================================

echo ""
echo "=== Start worker ==="

pm2 delete worker || true

pm2 start vps_worker.js --name worker

pm2 save

pm2 startup systemd -u root --hp /root

# ==========================================
# DONE
# ==========================================

echo ""
echo "===================="
echo " INSTALL COMPLETED "
echo "===================="

pm2 list