#!/usr/bin/env bash
set -euo pipefail

# Stock Trading Desk v2.0 — Ubuntu 24.04/22.04 一键安装与无缝迁移脚本
# 用法示例：
#   1) 零域名即刻上线（自动用 <IP>.sslip.io 申请正式 HTTPS 证书）：
#      curl -fsSL https://raw.githubusercontent.com/pirateradio9017/stock-trading-desk/main/deploy/install_vps.sh | sudo bash
#   2) 绑定自有域名（先将域名 A 记录指向本机公网 IP）：
#      sudo DESK_DOMAIN=desk.example.com bash install_vps.sh
#   3) 使用 Cloudflare Tunnel（最稳，不开放 80/443）：
#      sudo CF_TUNNEL_TOKEN=eyJ... bash install_vps.sh

APP_DIR="/opt/stock-trading-desk"
REPO_URL="${DESK_REPO_URL:-https://github.com/pirateradio9017/stock-trading-desk.git}"
BRANCH="${DESK_BRANCH:-main}"
LEGACY_ORIGIN="${DESK_LEGACY_ORIGIN:-https://stock-trading-desk.pirateradio9017.workers.dev}"
DESK_DOMAIN="${DESK_DOMAIN:-}"
CF_TUNNEL_TOKEN="${CF_TUNNEL_TOKEN:-}"
SKIP_PROXY="${DESK_SKIP_PROXY:-0}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "[ERROR] 请使用 root 或 sudo 运行此脚本。" >&2
  exit 1
fi

echo "==> [1/7] 安装系统基础依赖 (Python 3 / venv / git / curl)..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip git curl ca-certificates gnupg debian-keyring debian-archive-keyring apt-transport-https

echo "==> [2/7] 同步交易台仓库到 ${APP_DIR}..."
if [[ -d "${APP_DIR}/.git" ]]; then
  git -C "${APP_DIR}" fetch origin "${BRANCH}" --quiet
  git -C "${APP_DIR}" checkout "${BRANCH}" --quiet
  git -C "${APP_DIR}" reset --hard "origin/${BRANCH}" --quiet
else
  rm -rf "${APP_DIR}"
  git clone --depth 1 --branch "${BRANCH}" "${REPO_URL}" "${APP_DIR}"
fi
mkdir -p "${APP_DIR}/data/snapshots"

echo "==> [3/7] 创建 Python 虚拟环境并安装依赖..."
if [[ ! -x "${APP_DIR}/.venv/bin/python" ]]; then
  python3 -m venv "${APP_DIR}/.venv"
fi
"${APP_DIR}/.venv/bin/pip" install --quiet --upgrade pip
"${APP_DIR}/.venv/bin/pip" install --quiet -r "${APP_DIR}/requirements.txt"

echo "==> [4/7] 从现网平滑迁移历史状态 (state/live/news)..."
for key in state live news; do
  target="${APP_DIR}/data/${key}.json"
  if [[ ! -s "${target}" ]]; then
    if curl -fsSL --max-time 15 "${LEGACY_ORIGIN}/api/${key}" -o "${target}.tmp"; then
      if "${APP_DIR}/.venv/bin/python" -c "import json,sys; json.load(open(sys.argv[1], encoding='utf-8'))" "${target}.tmp" 2>/dev/null; then
        mv "${target}.tmp" "${target}"
        echo "    已迁移现网 ${key}.json"
      else
        rm -f "${target}.tmp"
        echo "    [WARN] 现网 ${key} 返回非 JSON，跳过"
      fi
    else
      rm -f "${target}.tmp"
      echo "    [WARN] 未能拉取现网 ${key}，将在服务启动后自动生成"
    fi
  else
    echo "    保留本地已有 ${key}.json"
  fi
done

echo "==> [5/7] 配置生产环境变量 (${APP_DIR}/.env)..."
ENV_FILE="${APP_DIR}/.env"
if [[ ! -f "${ENV_FILE}" ]]; then
  DS_KEY="${DEEPSEEK_API_KEY:-}"
  GM_KEY="${GEMINI_API_KEY:-}"
  if [[ -z "${DS_KEY}" && -t 0 ]]; then
    read -r -s -p "请输入 DEEPSEEK_API_KEY（回车可稍后填入 .env）: " DS_KEY || true
    echo ""
  fi
  if [[ -z "${GM_KEY}" && -t 0 ]]; then
    read -r -s -p "请输入 GEMINI_API_KEY（可选备用，回车跳过）: " GM_KEY || true
    echo ""
  fi
  ADMIN_TOK="${DESK_ADMIN_TOKEN:-$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')}"
  ORIGIN_SEC="${DESK_ORIGIN_SECRET:-}"
  cat > "${ENV_FILE}" <<EOF
DEEPSEEK_API_KEY=${DS_KEY}
GEMINI_API_KEY=${GM_KEY}
DESK_LLM_PROVIDER=auto
DEEPSEEK_MODEL=deepseek-flash

DESK_HOST=127.0.0.1
DESK_PORT=8787
DESK_DATA_DIR=${APP_DIR}/data
DESK_LOG_LEVEL=INFO
DESK_ENABLE_SCHEDULER=1
DESK_SCHEDULER_USE_LLM=1
DESK_SCHEDULER_PUBLISH_KV=0

DESK_LIVE_ACTIVE_SEC=20
DESK_LIVE_IDLE_SEC=90
DESK_LIVE_PERSIST_SEC=60
DESK_NEWS_ACTIVE_SEC=60
DESK_NEWS_IDLE_SEC=180
DESK_RESEARCH_ACTIVE_MIN=20
DESK_RESEARCH_WEEKDAY_IDLE_MIN=60
DESK_RESEARCH_IDLE_MIN=120

DESK_ADMIN_TOKEN=${ADMIN_TOK}
DESK_ORIGIN_SECRET=${ORIGIN_SEC}
EOF
  chmod 600 "${ENV_FILE}"
  echo "    已生成 ${ENV_FILE}（权限 600）"
else
  chmod 600 "${ENV_FILE}"
  echo "    保留已有 ${ENV_FILE}"
fi

echo "==> [6/7] 注册并启动 systemd 常驻服务..."
cp "${APP_DIR}/deploy/stock-trading-desk.service" /etc/systemd/system/stock-trading-desk.service
systemctl daemon-reload
systemctl enable stock-trading-desk.service >/dev/null
systemctl restart stock-trading-desk.service

for _ in $(seq 1 20); do
  if curl -fsS http://127.0.0.1:8787/api/health >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

PUBLIC_IP="$(curl -fsS --max-time 6 https://api.ipify.org || curl -fsS --max-time 6 https://ifconfig.me || echo '')"
ACCESS_URL="http://127.0.0.1:8787"

echo "==> [7/7] 配置公网 HTTPS 入口..."
if [[ "${SKIP_PROXY}" == "1" ]]; then
  echo "    已跳过反向代理配置 (DESK_SKIP_PROXY=1)"
elif [[ -n "${CF_TUNNEL_TOKEN}" ]]; then
  echo "    检测到 CF_TUNNEL_TOKEN，安装并启动 Cloudflare Tunnel (cloudflared)..."
  if ! command -v cloudflared >/dev/null 2>&1; then
    curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg | tee /usr/share/keyrings/cloudflare-main.gpg >/dev/null
    echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main" > /etc/apt/sources.list.d/cloudflared.list
    apt-get update -qq
    apt-get install -y -qq cloudflared
  fi
  cloudflared service uninstall >/dev/null 2>&1 || true
  cloudflared service install "${CF_TUNNEL_TOKEN}"
  systemctl enable cloudflared >/dev/null
  systemctl restart cloudflared
  ACCESS_URL="已通过 Cloudflare Tunnel 接入（在 Zero Trust 控制台绑定您的域名 -> http://127.0.0.1:8787）"
else
  if ! command -v caddy >/dev/null 2>&1; then
    echo "    安装 Caddy 自动 HTTPS 反向代理..."
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor --yes -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' > /etc/apt/sources.list.d/caddy-stable.list
    apt-get update -qq
    apt-get install -y -qq caddy
  fi
  if [[ -z "${DESK_DOMAIN}" && -n "${PUBLIC_IP}" ]]; then
    DESK_DOMAIN="${PUBLIC_IP//./-}.sslip.io"
  fi
  if [[ -n "${DESK_DOMAIN}" ]]; then
    cat > /etc/caddy/Caddyfile <<EOF
${DESK_DOMAIN} {
    encode zstd gzip
    reverse_proxy 127.0.0.1:8787
}
EOF
    systemctl enable caddy >/dev/null
    systemctl restart caddy
    ACCESS_URL="https://${DESK_DOMAIN}"
  else
    echo "    [WARN] 未检测到公网 IP 且未传 DESK_DOMAIN，暂未配置 Caddy。"
  fi
fi

echo ""
echo "==> 运行机房数据源与服务自检..."
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a
"${APP_DIR}/.venv/bin/python" "${APP_DIR}/deploy/check_connectivity.py" || true

echo "============================================================"
echo " 部署完成！"
echo " 访问入口: ${ACCESS_URL}"
echo " 服务状态: systemctl status stock-trading-desk"
echo " 实时日志: journalctl -u stock-trading-desk -f"
echo " 密钥配置: nano ${ENV_FILE}  (修改后执行 systemctl restart stock-trading-desk)"
echo " 热更新  : sudo bash ${APP_DIR}/deploy/update_vps.sh"
echo "============================================================"
