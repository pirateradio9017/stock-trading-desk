"""Compare timestamped real-time US session trades (CNBC NLS/CTA primary, Nasdaq fallback) with SPY."""
import asyncio
import math
from datetime import datetime, timedelta, timezone
from statistics import mean

import requests

from src.config import ROTATION_THEMES, SYMBOL_SPEC_MAP, TZ_ET, format_utc_iso, get_market_session
from src.models.schemas import SessionPrint, SessionRotation, SessionTheme

CNBC_BATCH_URL = "https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol"
EXTRA_SESSION_SYMBOLS = ("QQQ", "TLT", "XLP", "SOXX", "SMH", "AAPL", "LMT", "HYG", "LQD", "GLD")


def _parse_iso_ts(raw):
    if not isinstance(raw, str) or "T" not in raw:
        return None
    text = raw.strip()
    if len(text) >= 5 and text[-5] in "+-" and text[-4:].isdigit():
        text = text[:-2] + ":" + text[-2:]
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else None


def parse_cnbc_print(item, now, session):
    if not isinstance(item, dict):
        return None
    sym = str(item.get("symbol") or "").strip().upper()
    if not sym:
        return None
    if session == "PRE_MARKET":
        ext = item.get("ExtendedMktQuote")
        if not isinstance(ext, dict):
            return None
        ext_type = str(ext.get("type") or item.get("curmktstatus") or "").strip().upper()
        if ext_type != "PRE_MKT":
            return None
        px_raw = str(ext.get("last") or "").replace("$", "").replace(",", "").strip()
        chg_raw = str(ext.get("change_pct") or "").replace("%", "").replace(",", "").strip()
        stamp = _parse_iso_ts(ext.get("last_time"))
    elif session == "INTRADAY":
        if str(item.get("curmktstatus") or "").strip().upper() != "REG_MKT":
            return None
        px_raw = str(item.get("last") or "").replace("$", "").replace(",", "").strip()
        chg_raw = str(item.get("change_pct") or "").replace("%", "").replace(",", "").strip()
        stamp = _parse_iso_ts(item.get("last_time"))
        if stamp is None and item.get("last_time_msec"):
            try:
                ms = float(item["last_time_msec"])
                if math.isfinite(ms) and ms > 1_000_000_000_000:
                    stamp = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            except (TypeError, ValueError):
                stamp = None
    else:
        return None
    if not px_raw or not chg_raw or stamp is None:
        return None
    try:
        price = float(px_raw)
        change = float(chg_raw)
    except ValueError:
        return None
    if not (math.isfinite(change) and math.isfinite(price) and price > 0):
        return None
    stamp_et = stamp.astimezone(TZ_ET)
    if (stamp_et.date() != now.astimezone(TZ_ET).date() or get_market_session(stamp) != session
            or not -timedelta(seconds=30) <= now - stamp <= timedelta(minutes=20)):
        return None
    px_digits = 4 if price < 10 else 2
    return SessionPrint(sym=sym, change=round(change, 2), trade_utc=format_utc_iso(stamp), price=round(price, px_digits))


def parse_print(sym, data, now, session):
    if not isinstance(data, dict):
        return None
    allowed = {"pre-market"} if session == "PRE_MARKET" else {"open", "market open"}
    if str(data.get("marketStatus")).lower() not in allowed:
        return None
    trade = data.get("primaryData") or {}
    try:
        stamp = datetime.strptime(trade["lastTradeTimestamp"], "%b %d, %Y %I:%M %p ET").replace(tzinfo=TZ_ET)
        chg_str = str(trade["percentageChange"]).removesuffix("%").replace(",", "").strip()
        px_str = str(trade["lastSalePrice"]).strip("$").replace(",", "").strip()
        if not chg_str or not px_str:
            return None
        change = float(chg_str)
        price = float(px_str)
    except (TypeError, KeyError, ValueError, AttributeError):
        return None
    if not (math.isfinite(change) and math.isfinite(price) and price > 0):
        return None
    if (stamp.date() != now.astimezone(TZ_ET).date() or get_market_session(stamp) != session
            or not -timedelta(seconds=30) <= now - stamp <= timedelta(minutes=20)):
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


def _fetch_cnbc_batch_sync(symbols, now, session):
    params = {
        "symbols": "|".join(symbols),
        "requestMethod": "itv",
        "noform": "1",
        "partnerId": "2",
        "fund": "1",
        "exthrs": "1",
        "output": "json",
        "events": "1",
    }
    try:
        r = requests.get(
            CNBC_BATCH_URL,
            params=params,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=8,
        )
        r.raise_for_status()
        raw = (r.json().get("FormattedQuoteResult") or {}).get("FormattedQuote")
    except (requests.RequestException, ValueError, AttributeError):
        return {}
    items = raw if isinstance(raw, list) else ([raw] if isinstance(raw, dict) else [])
    out = {}
    for item in items:
        p = parse_cnbc_print(item, now, session)
        if p is not None:
            out[p.sym] = p
    return out


async def fetch_session_snapshot(now, session):
    """Fetch timestamped same-session prints (CNBC batch primary, Nasdaq fallback) and aggregate rotation."""
    if session not in {"PRE_MARKET", "INTRADAY"}:
        return None, {}
    loop = asyncio.get_running_loop()
    start = loop.time()
    rotation_symbols = sorted({"SPY"}.union(*ROTATION_THEMES.values()))
    batch_symbols = sorted(set(rotation_symbols).union(EXTRA_SESSION_SYMBOLS))

    prints = await asyncio.to_thread(_fetch_cnbc_batch_sync, batch_symbols, now, session)
    rotation = compare_prints(prints, now + timedelta(seconds=loop.time() - start), session)
    if len(rotation.themes) >= 2:
        return rotation, prints

    # Fallback to Nasdaq per-symbol endpoint for any missing rotation symbols.
    missing = [s for s in rotation_symbols if s not in prints] or rotation_symbols
    sem = asyncio.Semaphore(6)

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

    results = await asyncio.gather(*(fetch(s) for s in missing))
    for p in results:
        if p is not None:
            prints[p.sym] = p
    return compare_prints(prints, now + timedelta(seconds=loop.time()-start), session), prints


async def fetch_session_rotation(now, session):
    rotation, _ = await fetch_session_snapshot(now, session)
    return rotation

