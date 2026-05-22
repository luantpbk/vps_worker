#!/bin/bash

set -e

echo "=============================="
echo " TikTok VPS Worker Installer "
echo "=============================="

read -p "Nhap workerName: " WORKER_NAME

echo ""
echo "Nhap noi dung file .env"
echo "Nhan ENTER 2 lan de ket thuc:"
echo ""

ENV_CONTENT=""
while IFS= read -r line; do
    [ -z "$line" ] && break
    ENV_CONTENT="${ENV_CONTENT}${line}\n"
done

echo "=== Update system ==="

wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -

apt update && apt upgrade -y

echo "=== Install packages ==="

apt install curl wget nano git -y

echo "=== Install NodeJS 20 ==="

curl -fsSL https://deb.nodesource.com/setup_20.x | bash -

apt install -y nodejs

echo "=== Clone source ==="

rm -rf /root/tiktok_worker

git clone https://github.com/luantpbk/vps_worker.git /root/tiktok_worker

cd /root/tiktok_worker

echo "=== Install npm packages ==="

npm install

echo "=== Update workerName ==="

sed -i "s/\"workerName\": *\"VPS_Worker_08\"/\"workerName\": \"$WORKER_NAME\"/g" vps_config.json

echo "=== Create .env ==="

echo -e "$ENV_CONTENT" > .env

echo "=== Install PM2 ==="

npm install -g pm2

echo "=== Start worker ==="

pm2 delete tiktok_worker || true

pm2 start vps_worker.js --name "tiktok_worker"

pm2 save

echo ""
echo "=== DONE ==="

pm2 list