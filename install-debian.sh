#!/bin/bash

set -e

echo "=============================="
echo " TikTok Debian Worker Install "
echo "=============================="

WORKER_NAME="$1"
ENV_CONTENT="$2"

if [ -z "$WORKER_NAME" ] || [ -z "$ENV_CONTENT" ]; then
    echo "Usage: bash install.sh <worker_name> <env_content>"
    exit 1
fi

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# ==========================================
# UPDATE SYSTEM (skip safe)
# ==========================================

echo "=== System update ==="

apt update -y

if ! dpkg -s curl >/dev/null 2>&1; then
    apt install -y curl
else
    echo "[OK] curl already installed"
fi

# ==========================================
# BASIC PACKAGES
# ==========================================

PACKAGES=(wget git nano ca-certificates gnupg)

for pkg in "${PACKAGES[@]}"; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
        echo "[INSTALL] $pkg"
        apt install -y "$pkg"
    else
        echo "[OK] $pkg already installed"
    fi
done

# ==========================================
# NODEJS CHECK
# ==========================================

if command -v node >/dev/null 2>&1; then
    echo "[OK] Node already installed: $(node -v)"
else
    echo "=== Install NodeJS 20 ==="

    mkdir -p /etc/apt/keyrings

    if [ ! -f /etc/apt/keyrings/nodesource.gpg ]; then
        curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
        | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
    else
        echo "[OK] NodeSource key exists"
    fi

    if [ ! -f /etc/apt/sources.list.d/nodesource.list ]; then
        echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" \
        > /etc/apt/sources.list.d/nodesource.list
    else
        echo "[OK] NodeSource repo exists"
    fi

    apt update -y
    apt install -y nodejs
fi

echo "Node: $(node -v)"
echo "NPM: $(npm -v)"

# ==========================================
# CLONE SOURCE
# ==========================================

echo "=== Clone source ==="

if [ -d "/root/worker/.git" ]; then
    echo "[OK] Repo exists -> pulling updates"
    cd /root/worker
    git pull
else
    rm -rf /root/worker
    git clone https://github.com/luantpbk/vps_worker.git /root/worker
    cd /root/worker
fi

# ==========================================
# NPM INSTALL (skip if node_modules exists)
# ==========================================

if [ -d "node_modules" ]; then
    echo "[OK] node_modules exists -> skip npm install"
else
    echo "=== Install npm packages ==="
    npm install
fi

# ==========================================
# WORKER NAME UPDATE
# ==========================================

echo "=== Update workerName ==="

sed -i "s/\"workerName\": *\"VPS_Worker_08\"/\"workerName\": \"$WORKER_NAME\"/g" vps_config.json

# ==========================================
# ENV FILE
# ==========================================

echo "=== Create .env ==="
echo "SOCKET_SECRET=\"$ENV_CONTENT\"" > .env

# ==========================================
# PM2 CHECK
# ==========================================

if command -v pm2 >/dev/null 2>&1; then
    echo "[OK] PM2 already installed"
else
    echo "=== Install PM2 ==="
    npm install -g pm2
fi

# ==========================================
# START WORKER
# ==========================================

echo "=== Start worker ==="

pm2 describe worker >/dev/null 2>&1 && pm2 delete worker || true

pm2 start vps_worker.js --name worker
pm2 save
pm2 startup systemd -u root --hp /root

# ==========================================
# DONE
# ==========================================

echo "===================="
echo " INSTALL COMPLETED "
echo "===================="

pm2 list