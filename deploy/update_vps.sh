#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/stock-trading-desk"
BRANCH="${DESK_BRANCH:-main}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "[ERROR] 请使用 root 或 sudo 运行此脚本。" >&2
  exit 1
fi

echo "==> 拉取最新代码 (${BRANCH})..."
git -C "${APP_DIR}" fetch origin "${BRANCH}"
git -C "${APP_DIR}" checkout "${BRANCH}"
git -C "${APP_DIR}" reset --hard "origin/${BRANCH}"

echo "==> 同步 Python 依赖..."
"${APP_DIR}/.venv/bin/pip" install --quiet -r "${APP_DIR}/requirements.txt"

echo "==> 更新 systemd 服务并重启..."
cp "${APP_DIR}/deploy/stock-trading-desk.service" /etc/systemd/system/stock-trading-desk.service
systemctl daemon-reload
systemctl restart stock-trading-desk.service

sleep 2
echo "==> 健康检查:"
curl -fsS http://127.0.0.1:8787/api/health | "${APP_DIR}/.venv/bin/python" -m json.tool
