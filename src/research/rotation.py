"""Rank comparable US sector baskets from aligned daily observations."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from math import isfinite
from statistics import mean

from src.config import SYMBOL_NAME_MAP, TZ_ET, is_us_trading_day
from src.models.schemas import CandidateItem

THEMES = {
    "晶圆制造与半导体设备": ("TSM", "ASML"),
    "光模块与高速互联": ("COHR", "LITE"),
    "芯片设计与计算": ("AMD", "NVDA", "AVGO"),
    "住宅建筑与开发": ("ITB", "XHB"),
    "银行与金融": ("XLF", "KBE"),
    "能源与油气生产": ("XLE", "XOP"),
    "云算力基础设施": ("NBIS", "CRWV", "IREN"),
    "数字资产相关股票": ("COIN", "MSTR"),
    "公用事业与发电": ("XLU", "VST"),
}
RULE = "以标普500 ETF 为基准。1日、5日超额均为正，5日涨幅为正且过半成员5日跑赢，列入晋升观察；1日、5日超额均为负且5日下跌，列入转弱。"
LABELS = {"upgrade": ("晋升观察", "sage"), "watch": ("继续观察", "ochre"), "demote": ("转弱观察", "slate")}


def ranked_baskets(market):
    et = market.fetched_at_utc.astimezone(TZ_ET)
    today = et.date()
    last = today - timedelta(days=1)
    while not is_us_trading_day(last):
        last -= timedelta(days=1)
    benchmark = market.series_map.get("SPY")
    if not benchmark:
        return []
    closes = dict(zip(benchmark.dates, benchmark.closes))
    end = today if is_us_trading_day(today) and et.hour * 60 + et.minute >= 570 and today.isoformat() in closes else last
    dates = []
    day = end
    while len(dates) < 21:
        if is_us_trading_day(day):
            dates.append(day.isoformat())
        day -= timedelta(days=1)

    def returns(symbol):
        series = market.series_map.get(symbol)
        if not series:
            return None
        prices = dict(zip(series.dates, series.closes))
        if not all(d in prices and isfinite(prices[d]) and prices[d] > 0 for d in dates):
            return None
        return [(prices[dates[0]] / prices[dates[i]] - 1) * 100 for i in (1, 5, 20)]

    base = returns("SPY")
    if base is None:
        return []
    rows = []
    for name, members in THEMES.items():
        observations = [returns(sym) for sym in members]
        if any(row is None for row in observations):
            continue
        r1, r5, r20 = map(mean, zip(*observations))
        rs1, rs5, rs20 = r1-base[0], r5-base[1], r20-base[2]
        breadth = sum(row[1] > base[1] for row in observations)
        tier = "watch"
        if min(rs1, rs5, r5) > 0 and breadth * 2 > len(members):
            tier = "upgrade"
        elif max(rs1, rs5, r5) < 0:
            tier = "demote"
        score = .4 * rs1 + .45 * rs5 + .15 * rs20
        rows.append(dict(name=name, members=members, r5=round(r5, 2), r20=round(r20, 2),
                         tier=tier, score=score, as_of=dates[0], rs5=round(rs5, 2),
                         plain=f"1日相对标普 {rs1:+.2f} 个百分点，5日 {rs5:+.2f} 个百分点。{len(members)} 个成员中有 {breadth} 个近5日跑赢。"))
    return sorted(rows, key=lambda row: (-row["score"], row["name"]))


def rotation_candidates(rows, mainlines, previous, today):
    old = {c.name: c for c in previous}
    result = []
    for i, row in enumerate(rows[:6], 1):
        members = row["members"]
        matches = [(len(set(members) & {p.split(":")[0].strip() for p in m.stocks.split(",")}), m.id) for m in mainlines]
        overlap, linked = max(matches, default=(0, ""))
        label, tone = LABELS[row["tier"]]
        prior = old.get(row["name"])
        transition = f"{prior.tier_label} → {label}" if prior and prior.tier != row["tier"] else label
        result.append(CandidateItem(id=f"C{i}", tier=row["tier"], tier_label=label, tone=tone,
            name=row["name"], stocks=", ".join(f"{s}:{SYMBOL_NAME_MAP[s]}" for s in members),
            linked_to=linked if overlap else "独立观察", first_seen=prior.first_seen if prior else today,
            r5=row["r5"], r20=row["r20"], plain=row["plain"], rule=RULE,
            as_of=row["as_of"], excess5=row["rs5"], history_log=f"{row['as_of']} · {transition}"))
    return result

def overseas_watch(market):
    """Show same-local-day semiconductor quotes during US premarket only."""
    if market.live.marketSession != "PRE_MARKET":
        return ""
    points = []
    for sym, label, zone in (("2330.TW", "台积电台股", "Asia/Taipei"),
                             ("000660.KS", "SK海力士", "Asia/Seoul"),
                             ("ASML.AS", "阿斯麦欧股", "Europe/Amsterdam")):
        quote = market.get_quote(sym)
        if quote is None or quote.chg is None:
            continue
        try:
            stamp = datetime.fromisoformat(quote.tsUtc.replace("Z", "+00:00"))
        except ValueError:
            continue
        if stamp.tzinfo is None:
            continue
        local = ZoneInfo(zone)
        age = market.fetched_at_utc - stamp
        if (timedelta(0) <= age <= timedelta(hours=15)
                and stamp.astimezone(local).date() == market.fetched_at_utc.astimezone(local).date()):
            points.append(f"{label} {quote.chg:+.2f}%")
    return "、".join(points) + "。开盘后核对美国晶圆与设备股是否承接，光模块须另看自身报价。" if points else ""
