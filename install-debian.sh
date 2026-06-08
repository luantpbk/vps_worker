#!/bin/bash

set -e

echo "=============================="
echo " TikTok Worker Installer PRO "
echo "=============================="

WORKER_NAME="$1"
ENV_CONTENT="$2"
PROXY_COUNT="$3"
LOCAL_LOAD="$4"

# Kiểm tra đảm bảo truyền đủ 4 tham số
if [ -z "$WORKER_NAME" ] || [ -z "$ENV_CONTENT" ] || [ -z "$PROXY_COUNT" ] || [ -z "$LOCAL_LOAD" ]; then
    echo "Usage: bash install.sh <worker_name> <env_content> <proxy_count> <local_load>"
    exit 1
fi

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit 1
fi

# ==========================================
# OS LIMITS TUNING (ULIMIT 65535)
# ==========================================

echo "=== Opening VPS Limits (65535) ==="

# 1. Tăng file descriptors cho phiên bản người dùng (User Session)
cat <<EOF > /etc/security/limits.d/99-worker-limits.conf
* soft nofile 65535
* hard nofile 65535
root soft nofile 65535
root hard nofile 65535
EOF

# 2. Tăng file descriptors cho cấu hình Systemd (Quan trọng cho PM2 startup)
sed -i 's/.*DefaultLimitNOFILE.*/DefaultLimitNOFILE=65535/' /etc/systemd/system.conf || true
sed -i 's/.*DefaultLimitNOFILE.*/DefaultLimitNOFILE=65535/' /etc/systemd/user.conf || true

# 3. Mở rộng TCP Backlog & Sysctl để chịu tải hàng ngàn request Socket
cat <<EOF > /etc/sysctl.d/99-worker-net.conf
fs.file-max = 100000
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
EOF
sysctl -p /etc/sysctl.d/99-worker-net.conf >/dev/null 2>&1

# Áp dụng thay đổi cho systemd
systemctl daemon-reload

echo "[OK] VPS limits opened to 65535"

# ==========================================
# SAFE APT FIX (CRITICAL)
# ==========================================

echo "=== Fix APT / NodeSource conflicts ==="

rm -f /etc/apt/sources.list.d/nodesource.list
rm -f /etc/apt/sources.list.d/node*.list
rm -f /etc/apt/keyrings/nodesource.gpg
rm -f /usr/share/keyrings/nodesource.gpg

apt clean || true

# ==========================================
# SYSTEM UPDATE (SAFE)
# ==========================================

echo "=== System update ==="

apt update -y || {
    echo "[WARN] apt update failed, retrying..."
    apt --fix-missing update -y || true
}

# ==========================================
# BASE PACKAGES (SKIP IF EXISTS)
# ==========================================

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
# NODEJS INSTALL (SAFE + NO DUPLICATE)
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
# WORKER SYNC (PRO GIT MODE)
# ==========================================

echo "=== Worker sync ==="

if [ -d "/root/worker" ]; then
    echo "[OK] Worker exists -> updating"

    cd /root/worker

    if [ -d ".git" ]; then
        git fetch origin || true
        git reset --hard origin/main 2>/dev/null || git reset --hard origin/master
        git clean -fd
    else
        cd /root
        rm -rf worker
        git clone https://github.com/luantpbk/vps_worker.git /root/worker
        cd /root/worker
    fi
else
    git clone https://github.com/luantpbk/vps_worker.git /root/worker
    cd /root/worker
fi

# ==========================================
# NPM INSTALL (SMART)
# ==========================================

if [ -d "node_modules" ]; then
    echo "[OK] node_modules exists -> skip npm install"
else
    npm install
fi


# ==========================================
# CONFIG UPDATE
# ==========================================

echo "=== Config worker ==="

# Cập nhật Worker Name
sed -i "s/\"workerName\": *\".*\"/\"workerName\": \"$WORKER_NAME\"/g" vps_config.json || true

# Cập nhật số lượng Proxy
sed -i "s/\"proxyCount\": *[0-9]*/\"proxyCount\": $PROXY_COUNT/g" vps_config.json || true

# Cập nhật số lượng Local Load
sed -i "s/\"localLoad\": *[0-9]*/\"localLoad\": $LOCAL_LOAD/g" vps_config.json || true

echo "SOCKET_SECRET=\"$ENV_CONTENT\"" > .env

# ==========================================
# PM2 INSTALL CHECK
# ==========================================

if ! command -v pm2 >/dev/null 2>&1; then
    npm install -g pm2
else
    echo "[OK] PM2 exists"
fi

# ==========================================
# START WORKER (CLEAN RESTART)
# ==========================================

echo "=== Restart worker ==="

pm2 delete worker || true
pm2 start vps_worker.js --name worker --node-args="--max-old-space-size=4096"
pm2 save
pm2 startup systemd -u root --hp /root || true

# ==========================================
# DONE
# ==========================================

echo "===================="
echo " INSTALL COMPLETED "
echo "===================="

pm2 list