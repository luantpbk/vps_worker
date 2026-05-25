#!/bin/bash

set -e

echo "=============================="
echo " TikTok VPS Worker Installer "
echo "=============================="

WORKER_NAME="$1"
ENV_CONTENT="$2"

if [ -z "$WORKER_NAME" ] || [ -z "$ENV_CONTENT" ]; then
    echo "Usage:"
    echo "bash install-ubuntu.sh <worker_name> <socket_secret>"
    exit 1
fi

# ==========================================
# ROOT CHECK
# ==========================================

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# ==========================================
# SAFE APT FIX
# ==========================================

echo "=== Fix APT / NodeSource conflicts ==="

rm -f /etc/apt/sources.list.d/nodesource.list
rm -f /etc/apt/sources.list.d/node*.list
rm -f /etc/apt/keyrings/nodesource.gpg
rm -f /usr/share/keyrings/nodesource.gpg

apt clean || true

# ==========================================
# SYSTEM UPDATE
# ==========================================

echo "=== System update ==="

apt update -y || {
    echo "[WARN] apt update failed, retrying..."
    apt --fix-missing update -y || true
}

# ==========================================
# INSTALL BASE PACKAGES
# ==========================================

echo "=== Install base packages ==="

PACKAGES=(curl wget git nano ca-certificates gnupg)

for pkg in "${PACKAGES[@]}"; do
    if ! dpkg -s "$pkg" >/dev/null 2>&1; then
        echo "[INSTALL] $pkg"
        apt install -y "$pkg"
    else
        echo "[OK] $pkg exists"
    fi
done

# ==========================================
# INSTALL NODEJS 20
# ==========================================

if command -v node >/dev/null 2>&1; then
    echo "[OK] Node exists: $(node -v)"
else
    echo "=== Install NodeJS 20 ==="

    mkdir -p /etc/apt/keyrings

    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
    | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg

    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" \
    > /etc/apt/sources.list.d/nodesource.list

    apt update -y
    apt install -y nodejs
fi

echo "Node: $(node -v)"
echo "NPM: $(npm -v)"

# ==========================================
# WORKER SYNC
# ==========================================

echo "=== Worker sync ==="

if [ -d "~/worker" ]; then
    echo "[OK] Worker exists -> updating"

    cd ~/worker

    if [ -d ".git" ]; then
        git fetch origin || true
        git reset --hard origin/main 2>/dev/null || git reset --hard origin/master
        git clean -fd
    else
        cd ~
        rm -rf worker
        git clone https://github.com/luantpbk/vps_worker.git ~/worker
        cd ~/worker
    fi
else
    git clone https://github.com/luantpbk/vps_worker.git ~/worker
    cd ~/worker
fi

# ==========================================
# NPM INSTALL
# ==========================================

echo "=== Install npm packages ==="

if [ -d "node_modules" ]; then
    echo "[OK] node_modules exists -> skip npm install"
else
    npm install
fi

# ==========================================
# UPDATE CONFIG
# ==========================================

echo "=== Config worker ==="

sed -i "s/\"workerName\": *\".*\"/\"workerName\": \"$WORKER_NAME\"/g" vps_config.json || true

echo "SOCKET_SECRET=\"$ENV_CONTENT\"" > .env

# ==========================================
# INSTALL PM2
# ==========================================

echo "=== Check PM2 ==="

if ! command -v pm2 >/dev/null 2>&1; then
    npm install -g pm2
else
    echo "[OK] PM2 exists"
fi

# ==========================================
# START WORKER
# ==========================================

echo "=== Restart worker ==="

pm2 delete worker || true
pm2 start vps_worker.js --name worker

pm2 save
pm2 startup systemd -u root --hp ~ || true

# ==========================================
# DONE
# ==========================================

echo ""
echo "===================="
echo " INSTALL COMPLETED "
echo "===================="

pm2 list