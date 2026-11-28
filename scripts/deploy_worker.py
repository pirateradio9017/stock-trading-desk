"""Deploy frontend SPA, Cloudflare Worker modules, and optional KV data via Cloudflare REST API."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FRONTEND_HTML = PROJECT_ROOT / "frontend" / "index.html"
WORKER_DIR = PROJECT_ROOT / "worker"
WORKER_INDEX_JS = WORKER_DIR / "src" / "index.js"
WORKER_SPA_JS = WORKER_DIR / "src" / "spa_html.js"
DATA_DIR = PROJECT_ROOT / "data"

DEFAULT_WORKER_NAME = "stock-trading-desk"
DEFAULT_KV_NAMESPACE_ID = "abc99f1cc4ae4314a085daa5d0006076"
DEFAULT_CRON_SCHEDULES: tuple[str, ...] = ("*/5 * * * *",)


def sync_spa_module() -> Path:
    """Wrap frontend/index.html into worker/src/spa_html.js as a single-source ES module."""
    if not FRONTEND_HTML.is_file():
        raise FileNotFoundError(f"Missing frontend SPA: {FRONTEND_HTML}")
    html = FRONTEND_HTML.read_text(encoding="utf-8")
    WORKER_SPA_JS.parent.mkdir(parents=True, exist_ok=True)
    module_src = f"const INDEX_HTML = {json.dumps(html, ensure_ascii=False)};\nexport default INDEX_HTML;\n"
    WORKER_SPA_JS.write_text(module_src, encoding="utf-8")
    return WORKER_SPA_JS


def deploy_worker_rest(
    *,
    account_id: str,
    api_token: str,
    worker_name: str = DEFAULT_WORKER_NAME,
    kv_namespace_id: str = DEFAULT_KV_NAMESPACE_ID,
    publish_data: bool = True,
) -> dict[str, object]:
    """Upload ES module Worker + KV bindings and optionally push local data/*.json to KV."""
    if not account_id or not api_token:
        raise ValueError("CF_ACCOUNT_ID and CF_API_TOKEN are required")

    sync_spa_module()
    index_js = WORKER_INDEX_JS.read_text(encoding="utf-8")
    spa_js = WORKER_SPA_JS.read_text(encoding="utf-8")

    headers = {"Authorization": f"Bearer {api_token}"}
    base = f"https://api.cloudflare.com/client/v4/accounts/{account_id}"

    vps_origin_url = os.getenv("CF_VPS_ORIGIN_URL", "").strip()
    bindings: list[dict[str, str]] = [
        {
            "type": "kv_namespace",
            "name": "DESK_STATE",
            "namespace_id": kv_namespace_id,
        }
    ]
    keep_bindings = ["secret_text", "plain_text"]
    if vps_origin_url:
        bindings.append(
            {
                "type": "plain_text",
                "name": "VPS_ORIGIN_URL",
                "text": vps_origin_url,
            }
        )
        keep_bindings = ["secret_text"]

    metadata = {
        "main_module": "index.js",
        "compatibility_date": "2024-11-01",
        "keep_bindings": keep_bindings,
        "bindings": bindings,
    }

    with httpx.Client(timeout=30.0) as client:
        files = {
            "metadata": ("metadata.json", json.dumps(metadata), "application/json"),
            "index.js": ("index.js", index_js.encode("utf-8"), "application/javascript+module"),
            "spa_html.js": ("spa_html.js", spa_js.encode("utf-8"), "application/javascript+module"),
        }
        resp = client.put(
            f"{base}/workers/scripts/{worker_name}",
            headers=headers,
            files=files,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"Worker upload failed ({resp.status_code}): {resp.text[:400]}")
        upload_json = resp.json()
        if not upload_json.get("success", False):
            raise RuntimeError(f"Worker upload rejected: {upload_json}")

        sub_resp = client.post(
            f"{base}/workers/scripts/{worker_name}/subdomain",
            headers={**headers, "Content-Type": "application/json"},
            json={"enabled": True},
        )
        if sub_resp.status_code >= 400:
            raise RuntimeError(f"Subdomain enable failed ({sub_resp.status_code}): {sub_resp.text[:300]}")

        disable_cron = os.getenv("CF_DISABLE_CRON", "").strip().lower() in {"1", "true", "yes"}
        active_crons: tuple[str, ...] = () if disable_cron else DEFAULT_CRON_SCHEDULES
        sched_payload = [{"cron": expr} for expr in active_crons]
        sched_resp = client.put(
            f"{base}/workers/scripts/{worker_name}/schedules",
            headers={**headers, "Content-Type": "application/json"},
            json=sched_payload,
        )
        if sched_resp.status_code >= 400 or not sched_resp.json().get("success", False):
            raise RuntimeError(f"Worker schedules update failed ({sched_resp.status_code}): {sched_resp.text[:300]}")

        kv_pushed: list[str] = []
        if publish_data:
            for key, fname in (("state", "state.json"), ("live", "live.json"), ("news", "news.json")):
                fp = DATA_DIR / fname
                if fp.is_file():
                    raw_bytes = fp.read_bytes()
                    kv_resp = client.put(
                        f"{base}/storage/kv/namespaces/{kv_namespace_id}/values/{key}",
                        headers={**headers, "Content-Type": "application/json; charset=utf-8"},
                        content=raw_bytes,
                    )
                    if kv_resp.status_code >= 400 or not kv_resp.json().get("success", False):
                        raise RuntimeError(f"KV push '{key}' failed: {kv_resp.text[:300]}")
                    kv_pushed.append(key)

    return {
        "worker": worker_name,
        "kv_namespace_id": kv_namespace_id,
        "kv_pushed": kv_pushed,
        "schedules": list(active_crons),
        "vps_origin_url": vps_origin_url or None,
        "worker_etag": upload_json.get("result", {}).get("etag", ""),
        "url": f"https://{worker_name}.pirateradio9017.workers.dev",
    }


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="Deploy Stock Trading Desk Worker & KV")
    parser.add_argument("--sync-only", action="store_true", help="Only regenerate worker/src/spa_html.js")
    parser.add_argument("--no-data", action="store_true", help="Skip pushing local data/*.json to KV")
    args = parser.parse_args(argv)

    spa_path = sync_spa_module()
    if args.sync_only:
        print(f"Synced SPA module: {spa_path}")
        return 0

    account_id = os.getenv("CF_ACCOUNT_ID", "").strip()
    api_token = os.getenv("CF_API_TOKEN", "").strip()
    kv_id = os.getenv("CF_KV_NAMESPACE_ID", "").strip() or DEFAULT_KV_NAMESPACE_ID
    result = deploy_worker_rest(
        account_id=account_id,
        api_token=api_token,
        kv_namespace_id=kv_id,
        publish_data=not args.no_data,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
