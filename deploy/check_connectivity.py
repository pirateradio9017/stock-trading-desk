#!/usr/bin/env python3
"""VPS network & upstream market/AI connectivity self-check."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import Any

import httpx

CHECKS: list[tuple[str, str, dict[str, str]]] = [
    (
        "CNBC Batch 行情",
        "https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol?symbols=SPY|NVDA|.SPX&requestMethod=itv&noform=1&partnerId=2&fund=1&exthrs=1&output=json&events=1",
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json"},
    ),
    (
        "Yahoo Finance v8",
        "https://query1.finance.yahoo.com/v8/finance/chart/000660.KS?range=5d&interval=1d",
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) StockTradingDesk/2.0"},
    ),
    (
        "Nasdaq 盘前逐笔",
        "https://api.nasdaq.com/api/quote/SPY/info?assetclass=etf",
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json", "Referer": "https://www.nasdaq.com/"},
    ),
    (
        "SEC EDGAR 一手公告",
        "https://data.sec.gov/submissions/CIK0001045810.json",
        {"User-Agent": "StockTradingDesk/2.0 (research@stock-trading-desk.local)"},
    ),
    (
        "华尔街见闻快讯",
        "https://api-one-wscn.awtmt.com/apiv1/content/lives?channel=us-stock-channel&limit=3",
        {"User-Agent": "Mozilla/5.0 StockTradingDesk/2.0"},
    ),
    (
        "DeepSeek API 连通性",
        "https://api.deepseek.com/models",
        {"User-Agent": "StockTradingDesk/2.0"},
    ),
    (
        "Gemini API 连通性",
        "https://generativelanguage.googleapis.com/v1beta/models",
        {"User-Agent": "StockTradingDesk/2.0"},
    ),
]


async def _probe(
    client: httpx.AsyncClient,
    name: str,
    url: str,
    headers: dict[str, str],
) -> tuple[str, bool, int, str]:
    t0 = time.perf_counter()
    req_headers = dict(headers)
    if "deepseek.com" in url:
        key = os.getenv("DEEPSEEK_API_KEY", "").strip()
        if key:
            req_headers["Authorization"] = f"Bearer {key}"
    if "generativelanguage.googleapis.com" in url:
        key = os.getenv("GEMINI_API_KEY", "").strip()
        if key:
            url = f"{url}?key={key}"

    try:
        resp = await client.get(url, headers=req_headers, timeout=8.0)
        ms = int((time.perf_counter() - t0) * 1000)
        # 401/403 on AI endpoints when key not passed still proves regional reachability (non-geo-blocked).
        if resp.status_code == 200:
            return name, True, ms, f"HTTP 200 ({len(resp.content)}B)"
        if ("deepseek.com" in url or "generativelanguage.googleapis.com" in url) and resp.status_code in (401, 403):
            body_preview = resp.text[:120].replace("\n", " ")
            if "location is not supported" in body_preview.lower():
                return name, False, ms, f"GEO_BLOCKED ({resp.status_code}): {body_preview}"
            return name, True, ms, f"HTTP {resp.status_code} (区域可达，待校验 Key)"
        return name, False, ms, f"HTTP {resp.status_code}: {resp.text[:100].replace(chr(10), ' ')}"
    except Exception as exc:
        ms = int((time.perf_counter() - t0) * 1000)
        return name, False, ms, f"{type(exc).__name__}: {exc}"


async def main() -> int:
    port = os.getenv("DESK_PORT", "8787").strip() or "8787"
    local_url = f"http://127.0.0.1:{port}/api/health"

    async with httpx.AsyncClient(follow_redirects=True) as client:
        results = await asyncio.gather(*(_probe(client, n, u, h) for n, u, h in CHECKS))
        local_ok = False
        local_detail = ""
        try:
            r = await client.get(local_url, timeout=5.0)
            local_ok = r.status_code == 200
            if local_ok:
                data: dict[str, Any] = r.json()
                local_detail = (
                    f"HTTP 200 | mode={data.get('runtimeMode')} | quotes={data.get('quotesCount')} "
                    f"| news={data.get('newsCount')} | stateAge={data.get('stateAgeMinutes')}m"
                )
            else:
                local_detail = f"HTTP {r.status_code}"
        except Exception as exc:
            local_detail = f"{type(exc).__name__}: {exc}"

    print("\n================ VPS 数据源与服务自检 ================")
    failed = 0
    for name, ok, ms, detail in results:
        icon = "PASS" if ok else "FAIL"
        if not ok:
            failed += 1
        print(f"  [{icon}] {name:<18} | {ms:>4}ms | {detail}")
    print(f"  [{'PASS' if local_ok else 'FAIL'}] {'本地交易台 /api/health':<18} |        | {local_detail}")
    print("======================================================\n")
    return 0 if (failed == 0 and local_ok) else 1


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    raise SystemExit(asyncio.run(main()))
