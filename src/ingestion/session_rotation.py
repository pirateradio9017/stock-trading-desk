"""Compare timestamped Nasdaq trades with SPY within one US session."""
import asyncio
import math
from datetime import datetime, timedelta
from statistics import mean

import requests

from src.config import ROTATION_THEMES, SYMBOL_SPEC_MAP, TZ_ET, format_utc_iso, get_market_session
from src.models.schemas import SessionPrint, SessionRotation, SessionTheme


def parse_print(sym, data, now, session):
    if not isinstance(data, dict):
        return None
    allowed = {"pre-market"} if session == "PRE_MARKET" else {"open", "market open"}
    if str(data.get("marketStatus")).lower() not in allowed:
        return None
    trade = data.get("primaryData") or {}
    try:
        stamp = datetime.strptime(trade["lastTradeTimestamp"], "%b %d, %Y %I:%M %p ET").replace(tzinfo=TZ_ET)
        change = float(trade["percentageChange"].removesuffix("%").replace(",", ""))
        price = float(trade["lastSalePrice"].strip("$").replace(",", ""))
    except (TypeError, KeyError, ValueError, AttributeError):
        return None
    if not (math.isfinite(change) and math.isfinite(price) and price > 0):
        return None
    if (stamp.date() != now.astimezone(TZ_ET).date() or get_market_session(stamp) != session
            or not timedelta(0) <= now - stamp <= timedelta(minutes=20)):
        return None
    px_digits = 4 if price < 10 else 2
    return SessionPrint(sym=sym, change=round(change, 2), trade_utc=format_utc_iso(stamp), price=round(price, px_digits))


def compare_prints(prints, now, session):
    base = prints.get("SPY")
    rows = []
    for name, symbols in ROTATION_THEMES.items():
        members = [prints.get(s) for s in symbols]
        if not base or not all(members):
            continue
        times = [datetime.fromisoformat(p.trade_utc) for p in [base, *members]]
        if max(times) - min(times) > timedelta(minutes=10):
            continue
        change = mean(p.change for p in members)
        rows.append(SessionTheme(name=name, members=members, change=round(change, 2),
                                 excess=round(change-base.change, 2),
                                 outperformers=sum(p.change > base.change for p in members)))
    rows.sort(key=lambda r: (-r.excess, r.name))
    return SessionRotation(session=session, sampled_utc=format_utc_iso(now),
                           benchmark=base, themes=rows, universe=len(ROTATION_THEMES))


async def fetch_session_snapshot(now, session):
    """Fetch timestamped same-session Nasdaq prints and aggregate theme rotation."""
    if session not in {"PRE_MARKET", "INTRADAY"}:
        return None, {}
    loop = asyncio.get_running_loop()
    start = loop.time()
    sem = asyncio.Semaphore(6)
    symbols = sorted({"SPY"}.union(*ROTATION_THEMES.values()))

    async def fetch(sym):
        asset = "etf" if "etf" in SYMBOL_SPEC_MAP[sym].category else "stocks"
        for attempt in range(2):
            try:
                async with sem:
                    r = await asyncio.to_thread(
                        requests.get, f"https://api.nasdaq.com/api/quote/{sym}/info",
                        params={"assetclass": asset}, headers={"User-Agent": "Mozilla/5.0",
                        "Accept": "application/json", "Referer": "https://www.nasdaq.com/"}, timeout=7)
                if r.status_code in (502, 503, 504) and not attempt:
                    continue
                r.raise_for_status()
                return parse_print(sym, r.json().get("data"), now + timedelta(seconds=loop.time()-start), session)
            except (requests.Timeout, requests.ConnectionError):
                continue
            except (requests.RequestException, ValueError, AttributeError):
                break
        return None

    results = await asyncio.gather(*(fetch(s) for s in symbols))
    prints = {p.sym: p for p in results if p}
    return compare_prints(prints, now + timedelta(seconds=loop.time()-start), session), prints


async def fetch_session_rotation(now, session):
    rotation, _ = await fetch_session_snapshot(now, session)
    return rotation

