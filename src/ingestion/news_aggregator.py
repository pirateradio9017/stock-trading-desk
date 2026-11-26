"""Multi-wire financial news aggregation, event clustering deduplication, and Chinese breaking feed."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse, urlunparse

import feedparser
import httpx
from bs4 import BeautifulSoup

from src.config import FEED_SOURCES, FeedSpec, HTTP_USER_AGENT, TZ_CN, format_cn_timestamp

from src.models.schemas import NewsItem, NewsResponse

logger = logging.getLogger(__name__)

_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_ASCII_ALPHA_RE = re.compile(r"[A-Za-z]")
_STRIP_SUFFIX_RE = re.compile(
    r"\s*[-–—|]\s*(华尔街见闻|Google\s*新闻|CNBC|MarketWatch|Investing\.com|Seeking Alpha|Nasdaq|Reuters|Bloomberg).*$",
    re.IGNORECASE,
)
_PUNCT_RE = re.compile(r"[^\w\u4e00-\u9fff]+")

_TAG_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("M1 算力", ("英伟达", "nvidia", "nvda", "博通", "avgo", "amd", "芯片", "半导体", "算力", "gpu", "ai", "台积电", "asml", "光模块", "lumentum")),
    ("电力基建", ("电力", "核电", "电网", "能源", "原油", "布伦特", "wti", "opec", "数据中心", "iren", "nebius", "coreweave", "公用事业")),
    ("宏观利率", ("美联储", "fed", "fomc", "鲍威尔", "降息", "加息", "通胀", "cpi", "pce", "非农", "美债", "收益率", "黄金", "白银", "日元", "美元")),
    ("加密联动", ("比特币", "bitcoin", "btc", "以太坊", "加密", "crypto", "coinbase", "strategy", "mstr")),
    ("并购监管", ("sec", "8-k", "425", "反垄断", "关税", "制裁", "出口管制", "并购", "裁决")),
    ("全球市场", ("日经", "日本", "韩国", "欧洲", "英国", "中国", "港股", "恒生", "a股", "亚太")),
)


@dataclass
class RawWireItem:
    """Normalized headline & context record prior to Chinese-feed filtering."""

    event_id: str
    title: str
    summary: str
    tag: str
    source: str
    tier: str
    lang: str
    pub_dt_utc: datetime
    pub_date_cn: str
    link: str
    score: int = 0


@dataclass
class AggregatedNews:
    """Aggregated news output: /api/news Chinese response + full clustered wires for M3."""

    fetched_at_utc: datetime
    response: NewsResponse
    raw_items: list[RawWireItem] = field(default_factory=list)
    feed_status: dict[str, int] = field(default_factory=dict)


def _strip_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    text = BeautifulSoup(raw_html, "lxml").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def _clean_url(url: str) -> str:
    s = (url or "").strip()
    if not s:
        return ""
    parsed = urlparse(s)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return ""
    return urlunparse(parsed._replace(fragment=""))


def is_predominantly_chinese(text: str) -> bool:
    """Verify title meets >= 4 CJK chars and >= 30% CJK ratio."""
    cjk_count = len(_CJK_RE.findall(text))
    ascii_count = len(_ASCII_ALPHA_RE.findall(text))
    total = cjk_count + ascii_count
    return cjk_count >= 4 and total > 0 and (cjk_count / total) >= 0.30


def _classify_tag(text: str) -> str:
    low = text.lower()
    for tag, keywords in _TAG_RULES:
        if any(kw in low for kw in keywords):
            return tag
    return "美股快讯"


def _parse_dt_utc(raw_dt: Any, fallback: datetime) -> datetime:
    if isinstance(raw_dt, (int, float)) and raw_dt > 1_000_000_000:
        return datetime.fromtimestamp(int(raw_dt), tz=timezone.utc)
    if isinstance(raw_dt, str) and raw_dt.strip():
        s = raw_dt.strip()
        try:
            dt = parsedate_to_datetime(s)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return fallback


def _format_pubdate_cn(dt_utc: datetime) -> str:
    return dt_utc.astimezone(TZ_CN).strftime("%Y-%m-%d %H:%M UTC+8")


def _normalize_title_for_cluster(title: str) -> str:
    cleaned = _STRIP_SUFFIX_RE.sub("", title or "").strip().lower()
    return _PUNCT_RE.sub("", cleaned)


def _shingles(norm_text: str) -> set[str]:
    if len(norm_text) <= 2:
        return {norm_text} if norm_text else set()
    return {norm_text[i : i + 2] for i in range(len(norm_text) - 1)}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _split_wscn_title_and_summary(raw_title: str, raw_content: str) -> tuple[str, str]:
    """Handle WSCN live items where `title` is often empty and text sits in `content_text`."""
    t = _strip_html(raw_title)
    c = _strip_html(raw_content)

    # Strip leading 【...】 prefix duplication cleanly while preserving headline readability.
    if t and len(t) >= 6:
        summary = c if (c and len(c) >= 4) else f"{t}（华尔街见闻实时快讯）"
        return t[:110], summary[:260]

    if not c:
        return "", ""

    # Derive crisp title from first sentence or clause, keeping >= 4 chars.
    parts = re.split(r"[。！？\n]", c, maxsplit=1)
    first_clause = parts[0].strip()
    if len(first_clause) < 8:
        first_clause = c[:64].strip()
    title = first_clause[:80]
    summary = c[:260] if len(c) >= 6 else f"{title}（市场实时快讯）"
    return title, summary


def _parse_wscn_payload(spec: FeedSpec, payload: dict[str, Any], now_utc: datetime) -> list[RawWireItem]:
    items = ((payload.get("data") or {}).get("items")) or []
    out: list[RawWireItem] = []
    for entry in items:
        title, summary = _split_wscn_title_and_summary(
            str(entry.get("title") or ""),
            str(entry.get("content_text") or entry.get("content") or ""),
        )
        link = _clean_url(str(entry.get("uri") or entry.get("global_more_uri") or ""))
        if not title or not link:
            continue
        dt_utc = _parse_dt_utc(entry.get("display_time"), now_utc)
        tag = _classify_tag(f"{title} {summary}")
        out.append(
            RawWireItem(
                event_id="",
                title=title,
                summary=summary,
                tag=tag,
                source="华尔街见闻",
                tier=spec.tier,
                lang="zh",
                pub_dt_utc=dt_utc,
                pub_date_cn=_format_pubdate_cn(dt_utc),
                link=link,
                score=100,
            )
        )
    return out


def _parse_rss_payload(spec: FeedSpec, xml_text: str, now_utc: datetime) -> list[RawWireItem]:
    parsed = feedparser.parse(xml_text)
    out: list[RawWireItem] = []
    is_zh = spec.tier.endswith("_ZH")
    for entry in parsed.entries[:35]:
        raw_title = _strip_html(str(getattr(entry, "title", "") or ""))
        title = _STRIP_SUFFIX_RE.sub("", raw_title).strip()
        raw_summary = _strip_html(
            str(getattr(entry, "summary", "") or getattr(entry, "description", "") or "")
        )
        link = _clean_url(str(getattr(entry, "link", "") or ""))
        if not title or not link:
            continue

        dt_utc = _parse_dt_utc(
            getattr(entry, "published", None) or getattr(entry, "updated", None),
            now_utc,
        )
        if is_zh:
            summary = raw_summary[:240] if len(raw_summary) >= 6 else f"{title}（来源：{spec.name}）"
            score = 80
        else:
            summary = raw_summary[:240] if raw_summary else title
            score = 60

        out.append(
            RawWireItem(
                event_id="",
                title=title[:130],
                summary=summary,
                tag=_classify_tag(f"{title} {summary}"),
                source=spec.name,
                tier=spec.tier,
                lang="zh" if is_zh else "en",
                pub_dt_utc=dt_utc,
                pub_date_cn=_format_pubdate_cn(dt_utc),
                link=link,
                score=score,
            )
        )
    return out


def _parse_finviz_html(spec: FeedSpec, html_text: str, now_utc: datetime) -> list[RawWireItem]:
    soup = BeautifulSoup(html_text, "lxml")
    out: list[RawWireItem] = []
    seen_links: set[str] = set()
    for a in soup.select("a.nn-tab-link, table.styled-table-new a"):
        href = str(a.get("href") or "").strip()
        if href.startswith("/"):
            href = f"https://finviz.com{href}"
        link = _clean_url(href)
        title = _strip_html(a.get_text(" ", strip=True))
        if not link or not title or len(title) < 12 or link in seen_links:
            continue
        seen_links.add(link)
        out.append(
            RawWireItem(
                event_id="",
                title=title[:130],
                summary=title[:220],
                tag=_classify_tag(title),
                source=spec.name,
                tier=spec.tier,
                lang="en",
                pub_dt_utc=now_utc,
                pub_date_cn=_format_pubdate_cn(now_utc),
                link=link,
                score=45,
            )
        )
        if len(out) >= 25:
            break
    return out


def deduplicate_wire_items(items: list[RawWireItem]) -> list[RawWireItem]:
    """Cluster near-duplicate stories across wires and assign deterministic `event_id`."""
    # Sort best tier & newest timestamp first so the strongest Chinese wire wins each cluster.
    ranked = sorted(
        items,
        key=lambda x: (x.lang == "zh", x.score, x.pub_dt_utc.timestamp()),
        reverse=True,
    )

    clusters: list[tuple[str, set[str], RawWireItem]] = []
    seen_ids: set[str] = set()

    for item in ranked:
        norm = _normalize_title_for_cluster(item.title)
        if not norm:
            continue
        sh = _shingles(norm)

        matched = False
        for _, rep_sh, _ in clusters:
            if _jaccard(sh, rep_sh) >= 0.55:
                matched = True
                break
        if matched:
            continue

        base_hash = hashlib.sha1(norm.encode("utf-8")).hexdigest()[:12]
        event_id = base_hash
        suffix = 1
        while event_id in seen_ids:
            event_id = f"{base_hash[:10]}{suffix:02x}"
            suffix += 1
        seen_ids.add(event_id)

        item.event_id = event_id
        clusters.append((norm, sh, item))

    deduped = [entry[2] for entry in clusters]
    deduped.sort(key=lambda x: x.pub_dt_utc, reverse=True)
    return deduped


async def _fetch_feed(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    spec: FeedSpec,
    now_utc: datetime,
) -> tuple[str, list[RawWireItem]]:
    async with sem:
        try:
            resp = await client.get(
                spec.url,
                headers={"User-Agent": HTTP_USER_AGENT},
            )
            if resp.status_code != 200:
                logger.debug("Feed %s returned HTTP %d", spec.key, resp.status_code)
                return spec.key, []
            if spec.kind == "json":
                return spec.key, _parse_wscn_payload(spec, resp.json(), now_utc)
            if spec.kind in ("rss", "atom"):
                return spec.key, _parse_rss_payload(spec, resp.text, now_utc)
            if spec.kind == "html":
                return spec.key, _parse_finviz_html(spec, resp.text, now_utc)
            return spec.key, []
        except Exception as exc:
            logger.debug("Feed %s failed: %s", spec.key, exc)
            return spec.key, []


_US_RELEVANCE_RE = re.compile(
    r"(美股|纳指|标普|道指|华尔街|美联储|联储|鲍威尔|美债|美元|日元|黄金|白银|原油|布伦特|wti|"
    r"英伟达|nvidia|nvda|苹果|微软|谷歌|亚马逊|meta|特斯拉|博通|amd|台积电|asml|光模块|半导体|芯片|算力|ai|"
    r"比特币|btc|加密|coinbase|mstr|关税|通胀|cpi|pce|非农|pmi|gdp|降息|加息|高盛|摩根|sec|财报|并购|科技股)",
    re.IGNORECASE,
)


def _is_relevant_to_us_desk(raw: RawWireItem) -> bool:
    """Keep US-channel / Google-News-ZH items, and filter global-channel noise to market-relevant news."""
    if raw.tag != "美股快讯":
        return True
    combined = f"{raw.title} {raw.summary}"
    if _US_RELEVANCE_RE.search(combined):
        return True
    return False


def build_chinese_news_response(
    deduped_items: list[RawWireItem],
    now_utc: datetime,
    *,
    limit: int = 25,
) -> NewsResponse:
    """Select verified pure-Chinese deduped items relevant to the US trading desk for `/api/news`."""
    zh_items: list[NewsItem] = []
    seen_event_ids: set[str] = set()

    # Pass 1: US-desk relevant Chinese stories; Pass 2: backfill only if < 10 relevant items.
    ordered_candidates = [
        item for item in deduped_items if _is_relevant_to_us_desk(item)
    ] + [
        item for item in deduped_items if not _is_relevant_to_us_desk(item)
    ]

    for raw in ordered_candidates:
        if raw.event_id in seen_event_ids:
            continue
        if not is_predominantly_chinese(raw.title):
            continue
        summary = raw.summary.strip()
        if len(summary) < 4:
            summary = f"{raw.title}（{raw.source}实时快讯）"
        try:
            news_item = NewsItem(
                event_id=raw.event_id,
                title=raw.title,
                summary=summary,
                tag=raw.tag,
                source=raw.source,
                pubDate=raw.pub_date_cn,
                link=raw.link,
            )
        except Exception:
            continue
        seen_event_ids.add(raw.event_id)
        zh_items.append(news_item)
        if len(zh_items) >= limit:
            break

    return NewsResponse(
        updatedAt=format_cn_timestamp(now_utc),
        count=len(zh_items),
        items=zh_items,
    )


async def fetch_aggregated_news(
    *,
    now_utc: datetime | None = None,
    timeout_sec: float = 10.0,
) -> AggregatedNews:
    """Fetch all financial news wires, deduplicate events, and build `/api/news`."""
    ts_now = now_utc or datetime.now(timezone.utc)
    if ts_now.tzinfo is None:
        ts_now = ts_now.replace(tzinfo=timezone.utc)

    news_specs = [
        spec
        for spec in FEED_SOURCES.values()
        if spec.tier in ("L1_WIRE_ZH", "L2_WIRE_ZH", "L2_WIRE_EN", "L2_AGGREGATOR")
    ]
    sem = asyncio.Semaphore(6)
    async with httpx.AsyncClient(timeout=timeout_sec, follow_redirects=True) as client:
        results = await asyncio.gather(
            *(_fetch_feed(client, sem, spec, ts_now) for spec in news_specs)
        )

    feed_status: dict[str, int] = {}
    all_raw: list[RawWireItem] = []
    for key, batch in results:
        feed_status[key] = len(batch)
        all_raw.extend(batch)

    deduped = deduplicate_wire_items(all_raw)
    response = build_chinese_news_response(deduped, ts_now, limit=25)

    return AggregatedNews(
        fetched_at_utc=ts_now,
        response=response,
        raw_items=deduped,
        feed_status=feed_status,
    )
