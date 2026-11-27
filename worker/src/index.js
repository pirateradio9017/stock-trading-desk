import INDEX_HTML from "./spa_html.js";

const APP_VERSION = "2.0.0";
const CACHE_TTL_MS = 180_000;

const WORKER_CONFIG = {
  YAHOO_TIMEOUT_MS: 3500,
  WSCN_TIMEOUT_MS: 4000,
  YAHOO_CONCURRENCY: 8,
  LIVE_REFRESH_BUDGET_MS: 8500,
  LIVE_CRON_BUDGET_MS: 20_000,
  LIVE_FRESH_ACTIVE_MS: 60_000,
  LIVE_FRESH_IDLE_MS: CACHE_TTL_MS,
  LIVE_SWR_MAX_AGE_MS: 15 * 60_000,
  LIVE_STALE_WAIT_MS: 2500,
  LIVE_FORCE_WAIT_MS: 4500,
  LIVE_EDGE_MAX_SEC: 180,
  NEWS_FRESH_MS: 60_000,
  NEWS_SWR_MAX_AGE_MS: 15 * 60_000,
  NEWS_STALE_WAIT_MS: 2500,
  NEWS_FORCE_WAIT_MS: 4500,
  NEWS_EDGE_MAX_SEC: 180,
  STATE_FRESH_MS: 30_000,
  STATE_EDGE_MAX_SEC: 60,
  FALLBACK_COOLDOWN_SEC: 15,
  CRON_LIVE_ACTIVE_MS: 10 * 60_000,
  CRON_LIVE_IDLE_MS: 15 * 60_000,
  CRON_NEWS_ACTIVE_MS: 15 * 60_000,
  CRON_NEWS_IDLE_MS: 20 * 60_000,
  RESEARCH_ACTIVE_MAX_AGE_MS: 55 * 60_000,
  RESEARCH_IDLE_MAX_AGE_MS: 115 * 60_000,
  DISPATCH_COOLDOWN_MS: 25 * 60_000,
  DISPATCH_TIMEOUT_MS: 8000,
  GITHUB_REPO: "pirateradio9017/stock-trading-desk",
  GITHUB_WORKFLOW: "pipeline.yml",
  GITHUB_REF: "main",
};

const LIVE_SYMBOLS = [
  ["^GSPC", "标普500"],
  ["^IXIC", "纳斯达克"],
  ["^VIX", "恐慌指数 VIX"],
  ["^MOVE", "美债波动率 MOVE"],
  ["^TNX", "10Y 美债收益率"],
  ["BTC-USD", "比特币"],
  ["GC=F", "黄金"],
  ["SI=F", "白银"],
  ["HG=F", "COMEX 铜"],
  ["BZ=F", "布伦特原油"],
  ["JPY=X", "美元/日元"],
  ["QQQ", "纳指100 ETF"],
  ["TLT", "20Y+美债 ETF"],
  ["XLP", "必需消费 ETF"],
  ["XLE", "能源 ETF"],
  ["XLU", "公用事业 ETF"],
  ["NVDA", "英伟达"],
  ["AVGO", "博通"],
  ["AMD", "超威半导体"],
  ["LITE", "Lumentum"],
  ["SOXX", "半导体 ETF"],
  ["SMH", "半导体龙头 ETF"],
  ["ASML", "阿斯麦"],
  ["ITB", "房屋建筑 ETF"],
  ["AAPL", "苹果"],
  ["IREN", "IREN"],
  ["NBIS", "Nebius"],
  ["CRWV", "CoreWeave"],
  ["COIN", "Coinbase"],
  ["MSTR", "Strategy"],
  ["HYG", "高收益债 ETF"],
  ["LQD", "投资级债 ETF"],
  ["DX-Y.NYB", "美元指数 DXY"],
  ["CL=F", "WTI 原油"],
  ["XLF", "金融 ETF"],
  ["SPY", "标普500 ETF"],
  ["GLD", "黄金 ETF"],
  ["TSM", "台积电"],
  ["LMT", "洛克希德·马丁"],
  ["ES=F", "标普500期货"],
  ["NQ=F", "纳指100期货"],
  ["^N225", "日经225"],
  ["^KS11", "韩国 KOSPI"],
  ["^TWII", "台湾加权"],
  ["^HSI", "恒生指数"],
  ["000001.SS", "上证指数"],
  ["2330.TW", "台积电台股(TWD)"],
  ["000660.KS", "SK海力士(KRW)"],
  ["005930.KS", "三星电子(KRW)"],
  ["^GDAXI", "德国 DAX"],
  ["^STOXX50E", "欧洲斯托克50"],
  ["ASML.AS", "阿斯麦欧股(EUR)"],
];

// Ribbon, hero KPI, and primary anchor tickers run in the first fetch waves.
const PRIORITY_LIVE_SYMBOLS = new Set([
  "ES=F",
  "NQ=F",
  "SPY",
  "QQQ",
  "^VIX",
  "^TNX",
  "BTC-USD",
  "^GSPC",
  "^IXIC",
  "NVDA",
  "AVGO",
  "TSM",
  "2330.TW",
  "ASML.AS",
  "^KS11",
  "^N225",
  "LMT",
  "XLE",
  "ITB",
  "ASML",
  "TLT",
  "GC=F",
  "CL=F",
  "XLU",
  "HYG",
  "LQD",
]);

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, If-None-Match",
  "Access-Control-Expose-Headers": "ETag, X-Desk-Cache",
};

const SECURITY_HEADERS = {
  "X-Content-Type-Options": "nosniff",
  "Referrer-Policy": "strict-origin-when-cross-origin",
};

const CACHE_CONTROL_HTML = "public, max-age=0, must-revalidate";
const CACHE_CONTROL_STATE = "public, max-age=30, s-maxage=60, stale-while-revalidate=300";
const CACHE_CONTROL_LIVE = "public, max-age=15, s-maxage=30, stale-while-revalidate=180";
const CACHE_CONTROL_NEWS = "public, max-age=30, s-maxage=60, stale-while-revalidate=180";

const ISOLATE_CACHE = new Map();
const IN_FLIGHT = new Map();
let cachedHtmlEtag = null;

const ET_PARTS_FMT = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  weekday: "short",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

const ET_DATE_FMT = new Intl.DateTimeFormat("en-CA", {
  timeZone: "America/New_York",
});

function resetWorkerCaches() {
  ISOLATE_CACHE.clear();
  IN_FLIGHT.clear();
  cachedHtmlEtag = null;
}

function fnv1aHex(str) {
  let h1 = 0x811c9dc5;
  let h2 = 0x9e3779b9;
  for (let i = 0; i < str.length; i++) {
    const c = str.charCodeAt(i);
    h1 ^= c;
    h1 = Math.imul(h1, 0x01000193);
    h2 ^= (c + i) & 0xffff;
    h2 = Math.imul(h2, 0x85ebca6b);
  }
  return `${str.length.toString(16)}-${(h1 >>> 0).toString(16).padStart(8, "0")}${(h2 >>> 0).toString(16).padStart(8, "0")}`;
}

function makeWeakEtag(prefix, body) {
  return `W/"${prefix}-${fnv1aHex(body)}"`;
}

function getHtmlEtag() {
  if (!cachedHtmlEtag) {
    cachedHtmlEtag = makeWeakEtag("html", String(INDEX_HTML || ""));
  }
  return cachedHtmlEtag;
}

function normalizeEtagValue(tag) {
  const trimmed = String(tag || "").trim();
  return trimmed.startsWith("W/") || trimmed.startsWith("w/")
    ? trimmed.slice(2).trim()
    : trimmed;
}

function etagMatches(ifNoneMatchHeader, etag) {
  if (!ifNoneMatchHeader || !etag) return false;
  const raw = String(ifNoneMatchHeader).trim();
  if (raw === "*") return true;
  const target = normalizeEtagValue(etag);
  return raw.split(",").some((part) => normalizeEtagValue(part) === target);
}

function getDefaultCache() {
  try {
    return typeof caches !== "undefined" && caches && caches.default ? caches.default : null;
  } catch (_) {
    return null;
  }
}

function buildCacheKey(requestUrl, canonicalPath) {
  const base = requestUrl ? new URL(requestUrl) : new URL("https://desk.local");
  base.pathname = canonicalPath;
  base.search = "";
  base.hash = "";
  return new Request(base.toString(), { method: "GET" });
}

function scheduleBackground(ctx, promise) {
  const safe = Promise.resolve(promise).catch(() => {});
  if (ctx && typeof ctx.waitUntil === "function") {
    ctx.waitUntil(safe);
  }
  return safe;
}

function singleFlight(key, taskFn) {
  const existing = IN_FLIGHT.get(key);
  if (existing) return existing;
  const promise = Promise.resolve()
    .then(taskFn)
    .finally(() => {
      if (IN_FLIGHT.get(key) === promise) {
        IN_FLIGHT.delete(key);
      }
    });
  IN_FLIGHT.set(key, promise);
  return promise;
}

async function raceWithDeadline(promise, timeoutMs, fallbackValue) {
  let timer = null;
  const timeoutPromise = new Promise((resolve) => {
    timer = setTimeout(() => resolve({ timedOut: true, value: fallbackValue }), timeoutMs);
  });
  try {
    return await Promise.race([
      promise.then(
        (value) => ({ timedOut: false, value }),
        () => ({ timedOut: true, value: fallbackValue })
      ),
      timeoutPromise,
    ]);
  } finally {
    if (timer !== null) clearTimeout(timer);
  }
}

async function getCachedPayload(cacheKey, keyName) {
  const nowMs = Date.now();
  const mem = ISOLATE_CACHE.get(keyName);
  if (mem) {
    if (nowMs <= mem.expiresAt) {
      return { ...mem, layer: "MEM" };
    }
    ISOLATE_CACHE.delete(keyName);
  }

  const cache = getDefaultCache();
  if (!cache || !cacheKey) return null;
  try {
    const hit = await cache.match(cacheKey);
    if (!hit) return null;
    const body = await hit.text();
    const data = JSON.parse(body);
    const cachedAt = Number(hit.headers.get("X-Desk-Cached-At")) || nowMs;
    const ttlSec = Number(hit.headers.get("X-Desk-Edge-Ttl")) || 60;
    const isFallback = hit.headers.get("X-Desk-Fallback") === "1";
    const etag = hit.headers.get("ETag") || makeWeakEtag(keyName, body);
    const expiresAt = cachedAt + ttlSec * 1000;
    if (nowMs > expiresAt) return null;
    const entry = { data, body, etag, cachedAt, expiresAt, isFallback };
    ISOLATE_CACHE.set(keyName, entry);
    return { ...entry, layer: "EDGE" };
  } catch (_) {
    return null;
  }
}

async function putCachedPayload(cacheKey, keyName, data, maxEdgeSec, ctx, isFallback = false) {
  const cachedAt = Date.now();
  const body = JSON.stringify(data);
  const etag = makeWeakEtag(keyName, body);
  const ttlSec = Math.max(1, Number(maxEdgeSec) || 60);
  const entry = {
    data,
    body,
    etag,
    cachedAt,
    expiresAt: cachedAt + ttlSec * 1000,
    isFallback: Boolean(isFallback),
  };
  ISOLATE_CACHE.set(keyName, entry);

  const cache = getDefaultCache();
  if (cache && cacheKey) {
    try {
      const headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": `public, max-age=${ttlSec}`,
        "ETag": etag,
        "X-Desk-Cached-At": String(cachedAt),
        "X-Desk-Edge-Ttl": String(ttlSec),
      };
      if (isFallback) headers["X-Desk-Fallback"] = "1";
      const resp = new Response(body, { status: 200, headers });
      const putPromise = cache.put(cacheKey, resp);
      if (ctx && typeof ctx.waitUntil === "function") {
        ctx.waitUntil(Promise.resolve(putPromise).catch(() => {}));
      } else {
        await Promise.resolve(putPromise).catch(() => {});
      }
    } catch (_) {
      // Cache API errors must never fail responses.
    }
  }
  return entry;
}

function jsonResponse(data, statusOrOpts = 200) {
  const opts = typeof statusOrOpts === "number" ? { status: statusOrOpts } : (statusOrOpts || {});
  const status = opts.status ?? 200;
  const cacheControl = opts.cacheControl || "no-store";
  const request = opts.request || null;
  const method = opts.method || (request ? request.method : "GET");
  const body = opts.preSerializedBody !== undefined ? opts.preSerializedBody : JSON.stringify(data);
  const etag = opts.etag || (status === 200 && cacheControl !== "no-store" ? makeWeakEtag(opts.etagPrefix || "json", body) : null);

  const headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": cacheControl,
    ...SECURITY_HEADERS,
    ...CORS_HEADERS,
  };
  if (etag) headers["ETag"] = etag;
  if (opts.cacheStatus) headers["X-Desk-Cache"] = opts.cacheStatus;

  if (
    status === 200 &&
    etag &&
    cacheControl !== "no-store" &&
    request &&
    etagMatches(request.headers.get("If-None-Match"), etag)
  ) {
    headers["X-Desk-Cache"] = opts.cacheStatus ? `${opts.cacheStatus}-304` : "304";
    return new Response(null, { status: 304, headers });
  }

  return new Response(method === "HEAD" ? null : body, { status, headers });
}

function isForce(url) {
  const f = (url.searchParams.get("force") || "").trim().toLowerCase();
  return f === "1" || f === "true" || f === "yes" || f === "on";
}

function formatUtcIso(date = new Date()) {
  return date.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function formatCnTimestamp(date = new Date()) {
  const cn = new Date(date.getTime() + 8 * 3600_000);
  const y = cn.getUTCFullYear();
  const m = String(cn.getUTCMonth() + 1).padStart(2, "0");
  const d = String(cn.getUTCDate()).padStart(2, "0");
  const hh = String(cn.getUTCHours()).padStart(2, "0");
  const mm = String(cn.getUTCMinutes()).padStart(2, "0");
  const ss = String(cn.getUTCSeconds()).padStart(2, "0");
  return `${y}/${m}/${d} ${hh}:${mm}:${ss} UTC+8`;
}

function formatCnNewsDate(date = new Date()) {
  const cn = new Date(date.getTime() + 8 * 3600_000);
  const y = cn.getUTCFullYear();
  const m = String(cn.getUTCMonth() + 1).padStart(2, "0");
  const d = String(cn.getUTCDate()).padStart(2, "0");
  const hh = String(cn.getUTCHours()).padStart(2, "0");
  const mm = String(cn.getUTCMinutes()).padStart(2, "0");
  return `${y}-${m}-${d} ${hh}:${mm} UTC+8`;
}

function getEtParts(date = new Date()) {
  const map = {};
  for (const part of ET_PARTS_FMT.formatToParts(date)) {
    map[part.type] = part.value;
  }
  return {
    year: Number(map.year),
    month: Number(map.month),
    day: Number(map.day),
    weekday: map.weekday,
    hour: Number(map.hour === "24" ? "0" : map.hour),
    minute: Number(map.minute),
    mm: map.month,
    dd: map.day,
    hh: map.hour === "24" ? "00" : map.hour,
    min: map.minute,
  };
}

function easterSundayUtc(year) {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const ell = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * ell) / 451);
  const month = Math.floor((h + ell - 7 * m + 114) / 31);
  const day = ((h + ell - 7 * m + 114) % 31) + 1;
  return new Date(Date.UTC(year, month - 1, day));
}

function isNyseHolidayEt(year, month, day) {
  const d = new Date(Date.UTC(year, month - 1, day));
  const wd = d.getUTCDay();
  const observed = (m, dy) => {
    const dt = new Date(Date.UTC(year, m - 1, dy));
    if (dt.getUTCDay() === 6) dt.setUTCDate(dt.getUTCDate() - 1);
    else if (dt.getUTCDay() === 0) dt.setUTCDate(dt.getUTCDate() + 1);
    return `${dt.getUTCFullYear()}-${dt.getUTCMonth() + 1}-${dt.getUTCDate()}`;
  };
  const nthWeekday = (m, targetWd, n) => {
    const first = new Date(Date.UTC(year, m - 1, 1));
    const off = (targetWd - first.getUTCDay() + 7) % 7;
    const dy = 1 + off + (n - 1) * 7;
    return `${year}-${m}-${dy}`;
  };
  const lastWeekday = (m, targetWd) => {
    const last = new Date(Date.UTC(year, m, 0));
    const off = (last.getUTCDay() - targetWd + 7) % 7;
    return `${year}-${m}-${last.getUTCDate() - off}`;
  };
  const goodFriday = easterSundayUtc(year);
  goodFriday.setUTCDate(goodFriday.getUTCDate() - 2);
  const gfKey = `${goodFriday.getUTCFullYear()}-${goodFriday.getUTCMonth() + 1}-${goodFriday.getUTCDate()}`;

  const key = `${year}-${month}-${day}`;
  if (wd === 0 || wd === 6) return false;
  const fixed = new Set([
    observed(1, 1),
    nthWeekday(1, 1, 3),
    nthWeekday(2, 1, 3),
    gfKey,
    lastWeekday(5, 1),
    observed(6, 19),
    observed(7, 4),
    nthWeekday(9, 1, 1),
    nthWeekday(11, 4, 4),
    observed(12, 25),
  ]);
  return fixed.has(key);
}

function detectMarketSession(date = new Date()) {
  const et = getEtParts(date);
  const etShort = `${et.mm}-${et.dd} ${et.hh}:${et.min} ET`;
  if (isNyseHolidayEt(et.year, et.month, et.day)) {
    return { marketSession: "HOLIDAY", updatedAtEt: etShort };
  }
  if (et.weekday === "Sat" || et.weekday === "Sun") {
    return { marketSession: "OFF_HOURS", updatedAtEt: etShort };
  }
  const mins = et.hour * 60 + et.minute;
  if (mins >= 240 && mins < 570) return { marketSession: "PRE_MARKET", updatedAtEt: etShort };
  const early = new Set(["2026-11-27", "2026-12-24", "2027-11-26", "2028-7-3", "2028-11-24"]);
  const close = early.has(`${et.year}-${et.month}-${et.day}`) ? 780 : 960;
  if (mins >= 570 && mins < close) return { marketSession: "INTRADAY", updatedAtEt: etShort };
  if (mins >= close && mins < (close === 780 ? 1020 : 1200)) return { marketSession: "AFTER_HOURS", updatedAtEt: etShort };
  return { marketSession: "OFF_HOURS", updatedAtEt: etShort };
}

function isActiveMarketSession(date = new Date()) {
  const s = detectMarketSession(date).marketSession;
  return s === "PRE_MARKET" || s === "INTRADAY" || s === "AFTER_HOURS";
}

function finiteOrNull(v) {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function roundOrNull(v, digits = 2) {
  const n = finiteOrNull(v);
  if (n === null) return null;
  const f = 10 ** digits;
  return Math.round(n * f) / f;
}

async function fetchJsonWithTimeout(url, options, timeoutMs) {
  let controller = null;
  let timer = null;
  let signal = null;
  if (typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function") {
    signal = AbortSignal.timeout(timeoutMs);
  } else if (typeof AbortController !== "undefined") {
    controller = new AbortController();
    signal = controller.signal;
    timer = setTimeout(() => controller.abort(), timeoutMs);
  }
  try {
    const fetchOpts = signal ? { ...options, signal } : { ...options };
    const resp = await fetch(url, fetchOpts);
    if (!resp || !resp.ok) return null;
    return await resp.json();
  } catch (_) {
    return null;
  } finally {
    if (timer !== null) clearTimeout(timer);
  }
}

async function mapWithConcurrency(items, concurrency, deadlineMs, workerFn) {
  const results = new Array(items.length).fill(null);
  let cursor = 0;
  const workerCount = Math.max(1, Math.min(concurrency, items.length));
  async function runWorker() {
    while (cursor < items.length) {
      if (deadlineMs && Date.now() >= deadlineMs) break;
      const idx = cursor++;
      try {
        results[idx] = await workerFn(items[idx], idx);
      } catch (_) {
        results[idx] = null;
      }
    }
  }
  await Promise.all(Array.from({ length: workerCount }, runWorker));
  return results;
}

async function fetchYahooSymbol(sym, name, nowIso, isPreMarketOverride) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?range=3mo&interval=1d&includePrePost=true`;
  try {
    const payload = await fetchJsonWithTimeout(
      url,
      { headers: { "User-Agent": "Mozilla/5.0 StockTradingDesk/2.0" } },
      WORKER_CONFIG.YAHOO_TIMEOUT_MS
    );
    const res = payload?.chart?.result?.[0];
    if (!res) return null;
    const meta = res.meta || {};
    const timestamps = res.timestamp || [];
    const rawCloses = res.indicators?.quote?.[0]?.close || [];
    const seenDates = new Map();
    for (let i = 0; i < Math.min(timestamps.length, rawCloses.length); i++) {
      const c = finiteOrNull(rawCloses[i]);
      const ts = Number(timestamps[i]);
      if (c === null || c <= 0 || !Number.isFinite(ts)) continue;
      seenDates.set(ET_DATE_FMT.format(new Date(ts * 1000)), c);
    }
    const closes = Array.from(seenDates.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map((entry) => entry[1]);

    const price = finiteOrNull(meta.regularMarketPrice) ?? (closes.length ? closes[closes.length - 1] : null);
    // Prefer regularMarketChangePercent or closes[-2]; never rely on range-wide previous close.
    let chg = finiteOrNull(meta.regularMarketChangePercent);
    if (chg === null && price !== null) {
      const prevClose = closes.length >= 2
        ? closes[closes.length - 2]
        : (finiteOrNull(meta.regularMarketPreviousClose) ?? finiteOrNull(meta.previousClose));
      if (prevClose !== null && prevClose > 0) {
        chg = ((price / prevClose) - 1) * 100;
      }
    }
    const pre = typeof isPreMarketOverride === "boolean"
      ? isPreMarketOverride
      : detectMarketSession(new Date(nowIso)).marketSession === "PRE_MARKET";
    const usePre = (pre && finiteOrNull(meta.preMarketPrice) !== null) || finiteOrNull(meta.postMarketPrice) === null;
    let ahPrice = finiteOrNull(usePre ? meta.preMarketPrice : meta.postMarketPrice);
    let ahChg = finiteOrNull(usePre ? meta.preMarketChangePercent : meta.postMarketChangePercent);
    if (ahChg === null && ahPrice !== null && price !== null && price > 0) {
      ahChg = ((ahPrice / price) - 1) * 100;
    }
    let r5 = null;
    if (closes.length >= 6 && closes[closes.length - 6] > 0) {
      const base = price !== null ? price : closes[closes.length - 1];
      r5 = ((base - closes[closes.length - 6]) / closes[closes.length - 6]) * 100;
    }
    let r20 = null;
    if (closes.length >= 21) {
      const idx20 = closes.length - 21;
      if (closes[idx20] > 0) {
        const base = price !== null ? price : closes[closes.length - 1];
        r20 = ((base - closes[idx20]) / closes[idx20]) * 100;
      }
    }
    const regTs = Number(meta.regularMarketTime);
    const quoteTs = Number.isFinite(regTs) && regTs > 0 ? formatUtcIso(new Date(regTs * 1000)) : nowIso;
    const priceDigits = price !== null && price < 10 ? 4 : 2;
    const ahDigits = ahPrice !== null && ahPrice < 10 ? 4 : 2;
    return {
      sym,
      name,
      price: roundOrNull(price, priceDigits),
      chg: roundOrNull(chg, 2),
      ahPrice: roundOrNull(ahPrice, ahDigits),
      ahChg: roundOrNull(ahChg, 2),
      r5: roundOrNull(r5, 2),
      r20: roundOrNull(r20, 2),
      source: "yahoo_v8",
      tsUtc: quoteTs,
    };
  } catch (_) {
    return null;
  }
}

async function safeKvJson(env, key) {
  try {
    if (!env || !env.DESK_STATE || typeof env.DESK_STATE.get !== "function") return null;
    return await env.DESK_STATE.get(key, "json");
  } catch (_) {
    return null;
  }
}

function safeKvPut(env, key, value, ctx) {
  if (!env || !env.DESK_STATE || typeof env.DESK_STATE.put !== "function") return;
  try {
    const putPromise = env.DESK_STATE.put(key, typeof value === "string" ? value : JSON.stringify(value));
    scheduleBackground(ctx, putPromise);
  } catch (_) {
    // Ignore KV write scheduling errors.
  }
}

function parseCnTimestampAgeMs(tsStr, nowMs = Date.now()) {
  if (!tsStr || typeof tsStr !== "string") return null;
  const m = tsStr.match(/^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2})/);
  if (!m) return null;
  const utcMs = Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]) - 8, Number(m[5]), Number(m[6]));
  const age = nowMs - utcMs;
  return Number.isFinite(age) ? age : null;
}

function isFreshTimestamp(tsStr, maxAgeMs) {
  const age = parseCnTimestampAgeMs(tsStr);
  return age !== null && age >= 0 && age <= maxAgeMs;
}

function isValidLivePayload(payload) {
  return Boolean(payload && Array.isArray(payload.quotes) && payload.quotes.length >= 27);
}

function getLiveFreshWindowMs(now = new Date()) {
  return isActiveMarketSession(now)
    ? WORKER_CONFIG.LIVE_FRESH_ACTIVE_MS
    : WORKER_CONFIG.LIVE_FRESH_IDLE_MS;
}

function buildPriorityFetchOrder() {
  const priority = [];
  const rest = [];
  for (let i = 0; i < LIVE_SYMBOLS.length; i++) {
    if (PRIORITY_LIVE_SYMBOLS.has(LIVE_SYMBOLS[i][0])) {
      priority.push(i);
    } else {
      rest.push(i);
    }
  }
  return [...priority, ...rest];
}

const LIVE_FETCH_ORDER = buildPriorityFetchOrder();

async function fetchAndStoreLive(env, ctx, cacheKey, cachedSeed, checkKvFirst = false, budgetMsOverride = null) {
  let cached = cachedSeed || null;
  if (checkKvFirst) {
    const kvLatest = await safeKvJson(env, "live");
    if (isValidLivePayload(kvLatest)) {
      cached = kvLatest;
      const freshMs = getLiveFreshWindowMs();
      if (isFreshTimestamp(kvLatest.updatedAt, freshMs)) {
        await putCachedPayload(cacheKey, "live", kvLatest, WORKER_CONFIG.LIVE_EDGE_MAX_SEC, ctx);
        return kvLatest;
      }
    }
  }

  const now = new Date();
  const nowIso = formatUtcIso(now);
  const { marketSession, updatedAtEt } = detectMarketSession(now);
  const isPreMarket = marketSession === "PRE_MARKET";
  const cachedBySym = Object.create(null);
  if (cached && Array.isArray(cached.quotes)) {
    for (const q of cached.quotes) {
      if (q && q.sym) cachedBySym[q.sym] = q;
    }
  }

  const budgetMs = Number.isFinite(budgetMsOverride) && budgetMsOverride > 0
    ? budgetMsOverride
    : WORKER_CONFIG.LIVE_REFRESH_BUDGET_MS;
  const deadlineMs = Date.now() + budgetMs;
  const fetchedInOrder = await mapWithConcurrency(
    LIVE_FETCH_ORDER,
    WORKER_CONFIG.YAHOO_CONCURRENCY,
    deadlineMs,
    (symIdx) => {
      const [sym, name] = LIVE_SYMBOLS[symIdx];
      return fetchYahooSymbol(sym, name, nowIso, isPreMarket);
    }
  );
  const fetched = new Array(LIVE_SYMBOLS.length).fill(null);
  let freshCount = 0;
  for (let i = 0; i < LIVE_FETCH_ORDER.length; i++) {
    const symIdx = LIVE_FETCH_ORDER[i];
    const item = fetchedInOrder[i];
    fetched[symIdx] = item;
    if (item && item.price !== null) freshCount++;
  }

  // Never stamp a fake fresh updatedAt if upstream returned zero live quotes.
  if (freshCount === 0 && cached && Array.isArray(cached.quotes) && cached.quotes.length > 0) {
    await putCachedPayload(cacheKey, "live", cached, WORKER_CONFIG.FALLBACK_COOLDOWN_SEC, ctx, true);
    return cached;
  }

  const liveSymSet = new Set(LIVE_SYMBOLS.map(([sym]) => sym));
  const quotes = LIVE_SYMBOLS.map(([sym, name], idx) => {
    const fresh = fetched[idx];
    const old = cachedBySym[sym];
    if (fresh && fresh.price !== null) {
      return {
        ...fresh,
        r5: fresh.r5 !== null ? fresh.r5 : (old ? old.r5 : null),
        r20: fresh.r20 !== null ? fresh.r20 : (old ? old.r20 : null),
      };
    }
    if (old) return old;
    return {
      sym,
      name,
      price: null,
      chg: null,
      ahPrice: null,
      ahChg: null,
      r5: null,
      r20: null,
      source: "yahoo_v8",
      tsUtc: nowIso,
    };
  });
  // Preserve extra regional benchmarks cached by the Python pipeline (APAC/EU).
  for (const [sym, old] of Object.entries(cachedBySym)) {
    if (!liveSymSet.has(sym) && old) {
      quotes.push(old);
    }
  }

  const payload = {
    updatedAt: formatCnTimestamp(now),
    updatedAtEt,
    marketSession,
    count: quotes.length,
    quotes,
  };

  if (freshCount > 0) {
    await putCachedPayload(cacheKey, "live", payload, WORKER_CONFIG.LIVE_EDGE_MAX_SEC, ctx);
    safeKvPut(env, "live", payload, ctx);
  }
  return payload;
}

async function resolveLive(env, ctx, force, requestUrl) {
  const cacheKey = buildCacheKey(requestUrl, "/api/live");
  const freshMs = getLiveFreshWindowMs();

  if (!force) {
    const hit = await getCachedPayload(cacheKey, "live");
    if (hit && isValidLivePayload(hit.data)) {
      const age = parseCnTimestampAgeMs(hit.data.updatedAt);
      const withinFresh = age !== null && age >= 0 && age <= freshMs;
      const withinSwr = age !== null && age >= -5000 && age <= WORKER_CONFIG.LIVE_SWR_MAX_AGE_MS;
      const inCooldown = Boolean(hit.isFallback) && (Date.now() - hit.cachedAt < WORKER_CONFIG.FALLBACK_COOLDOWN_SEC * 1000);
      if (withinFresh) {
        return { payload: hit.data, body: hit.body, etag: hit.etag, cacheStatus: `${hit.layer}-HIT` };
      }
      if (withinSwr || inCooldown) {
        if (!inCooldown) {
          scheduleBackground(
            ctx,
            singleFlight("live-refresh", () => fetchAndStoreLive(env, ctx, cacheKey, hit.data, true))
          );
        }
        return {
          payload: hit.data,
          body: hit.body,
          etag: hit.etag,
          cacheStatus: inCooldown ? `${hit.layer}-FALLBACK` : `${hit.layer}-STALE`,
        };
      }
    }
  }

  const cached = await safeKvJson(env, "live");
  const hasValidKv = isValidLivePayload(cached);

  if (!force && hasValidKv) {
    const age = parseCnTimestampAgeMs(cached.updatedAt);
    if (age !== null && age >= 0 && age <= freshMs) {
      const entry = await putCachedPayload(cacheKey, "live", cached, WORKER_CONFIG.LIVE_EDGE_MAX_SEC, ctx);
      return { payload: cached, body: entry.body, etag: entry.etag, cacheStatus: "KV-HIT" };
    }
    if (age !== null && age >= -5000 && age <= WORKER_CONFIG.LIVE_SWR_MAX_AGE_MS) {
      const entry = await putCachedPayload(cacheKey, "live", cached, WORKER_CONFIG.LIVE_EDGE_MAX_SEC, ctx);
      scheduleBackground(
        ctx,
        singleFlight("live-refresh", () => fetchAndStoreLive(env, ctx, cacheKey, cached, false))
      );
      return { payload: cached, body: entry.body, etag: entry.etag, cacheStatus: "KV-STALE" };
    }
  }

  const refreshPromise = singleFlight("live-refresh", () =>
    fetchAndStoreLive(env, ctx, cacheKey, cached, false)
  );

  const reuseMem = (payload) => {
    const mem = ISOLATE_CACHE.get("live");
    return mem && mem.data === payload ? { body: mem.body, etag: mem.etag } : {};
  };

  if (hasValidKv) {
    const waitMs = force ? WORKER_CONFIG.LIVE_FORCE_WAIT_MS : WORKER_CONFIG.LIVE_STALE_WAIT_MS;
    const outcome = await raceWithDeadline(refreshPromise, waitMs, cached);
    if (outcome.timedOut) {
      scheduleBackground(ctx, refreshPromise);
      return { payload: cached, ...reuseMem(cached), cacheStatus: "KV-FALLBACK" };
    }
    return {
      payload: outcome.value,
      ...reuseMem(outcome.value),
      cacheStatus: outcome.value === cached ? "KV-FALLBACK" : (force ? "FORCE-FRESH" : "ORIGIN-FRESH"),
    };
  }

  const payload = await refreshPromise;
  return { payload, ...reuseMem(payload), cacheStatus: force ? "FORCE-COLD" : "ORIGIN-COLD" };
}

async function getLivePayload(env, ctx, force) {
  const res = await resolveLive(env, ctx, force, null);
  return res.payload;
}

function countChineseChars(s) {
  const m = String(s || "").match(/[\u4e00-\u9fff]/g);
  return m ? m.length : 0;
}

const US_RELEVANCE_RE = /(美股|纳指|标普|道指|华尔街|美股期货|纳指期货|标普期货|美联储|联储|鲍威尔|欧央行|日央行|植田|东京|美债|美元|日元|黄金|白银|原油|布伦特|英伟达|苹果|微软|谷歌|亚马逊|特斯拉|博通|台积电|海力士|三星电子|日经|台股|恒生科技|欧股|洛克希德|光模块|半导体|芯片|算力|比特币|加密|关税|出口管制|出口禁令|商务部|镓|锗|锑|301调查|通胀|非农|降息|加息|高盛|摩根|财报|并购|科技股|军工|(?<![a-z0-9])(wti|opec|fed|fomc|ecb|boj|nvidia|nvda|meta|avgo|amd|tsm|tsmc|asml|kospi|hbm|dram|lmt|ai|btc|coinbase|mstr|cpi|pce|pmi|gdp|sec)(?![a-z0-9]))/i;
const BOILERPLATE_TITLE_RE = /(^提醒[：:]\s*日内请重点关注|^华尔街见闻早餐|网络视听.*版权|行业自律公约|沪指|深成指|创业板指|北证50|两市成交额|A股三大股指|板块午后|涨停潮|高考|作弊|相亲|婚恋|景区|赏秋|旅游客流|票房|春晚|明星|综艺|足球|篮球|奥运|世界杯|羽毛球|象鼻|夹爪|宠物|气象台|天气预报|地震台网|台风预警|客服回应)/i;
const GLOBAL_OVERRIDE_RE = /(美股|纳指|标普|美联储|英伟达|台积电|苹果|特斯拉|比特币|原油|黄金|欧央行|日央行|东京|CPI|PCE|FOMC|OPEC)/i;

function classifyNewsTag(text) {
  const t = String(text || "");
  if (/(比特币|以太坊|加密|(?<![a-z0-9])(bitcoin|btc|crypto|coinbase|mstr)(?![a-z0-9]))/i.test(t)) return "加密联动";
  if (/(英伟达|博通|芯片|半导体|算力|台积电|海力士|三星电子|晶圆|光刻机|光模块|存储芯片|恒生科技|(?<![a-z0-9])(nvidia|nvda|avgo|amd|gpu|ai|openai|tsm|tsmc|2330|asml|lumentum|hbm|dram|cowos)(?![a-z0-9]))/i.test(t)) return "M1 算力";
  if (/(原油|布伦特|炼油|裂解价差|霍尔木兹|天然气|黄金|白银|铜矿|铜价|大宗商品|(?<![a-z0-9])(wti|opec)(?![a-z0-9]))/i.test(t)) return "能源大宗";
  if (/(电力|核电|电网|数据中心|公用事业|燃气轮机|变电|(?<![a-z0-9])(iren|nebius|coreweave)(?![a-z0-9]))/i.test(t)) return "电力基建";
  if (/(美联储|鲍威尔|欧央行|日央行|植田|降息|加息|通胀|非农|美债|国债收益率|收益率|日元|美元指数|按揭利率|(?<![a-z0-9])(fed|fomc|ecb|boj|cpi|pce)(?![a-z0-9]))/i.test(t)) return "宏观利率";
  if (/(反垄断|关税|制裁|出口管制|出口禁令|商务部|禁令|镓|锗|锑|稀土|301调查|并购|裁决|听证会|军工|国防|(?<![a-z0-9])(sec|8-k|425)(?![a-z0-9]))/i.test(t)) return "并购监管";
  if (/(日经|东证|韩股|韩国股市|韩国央行|韩元|台股|台湾加权|欧股|欧洲股市|欧元区|德国dax|法国cac|英国富时|英国央行|港股|恒生|恒指|亚太股市|亚太|新兴市场|标普期货|纳指期货|美股期货|(?<![a-z0-9])kospi(?![a-z0-9]))/i.test(t)) return "全市场宏观";
  return "美股快讯";
}

const HARD_NOISE_TITLE_RE = /(个股掘金|概念热股|证券之星|\(NVDA\.US\)|-\s*Moomoo$|-\s*富途牛牛$|涨停[：:].*概念)/i;

function isBoilerplateNoise(title) {
  if (HARD_NOISE_TITLE_RE.test(title)) return true;
  return BOILERPLATE_TITLE_RE.test(title) && !GLOBAL_OVERRIDE_RE.test(title);
}

function isRelevantToUsDesk(tag, title, text) {
  if (isBoilerplateNoise(title)) return false;
  if (tag !== "美股快讯") return true;
  return US_RELEVANCE_RE.test(String(text || ""));
}

async function fetchWscnChannel(channel, limit = 30) {
  const url = `https://api-one-wscn.awtmt.com/apiv1/content/lives?channel=${channel}&limit=${limit}`;
  const data = await fetchJsonWithTimeout(
    url,
    { headers: { "User-Agent": "Mozilla/5.0 StockTradingDesk/2.0" } },
    WORKER_CONFIG.WSCN_TIMEOUT_MS
  );
  return data?.data?.items || [];
}

function isValidNewsPayload(payload) {
  return Boolean(payload && Array.isArray(payload.items) && payload.items.length > 0);
}

async function fetchAndStoreNews(env, ctx, cacheKey, cachedSeed, checkKvFirst = false) {
  let cached = cachedSeed || null;
  if (checkKvFirst) {
    const kvLatest = await safeKvJson(env, "news");
    if (isValidNewsPayload(kvLatest)) {
      cached = kvLatest;
      if (isFreshTimestamp(kvLatest.updatedAt, WORKER_CONFIG.NEWS_FRESH_MS)) {
        await putCachedPayload(cacheKey, "news", kvLatest, WORKER_CONFIG.NEWS_EDGE_MAX_SEC, ctx);
        return kvLatest;
      }
    }
  }

  const [usItems, globalItems] = await Promise.all([
    fetchWscnChannel("us-stock-channel", 60),
    fetchWscnChannel("global-channel", 60),
  ]);

  const seen = new Set();
  const relevant = [];
  const backfill = [];
  for (const raw of [...usItems, ...globalItems]) {
    const titleRaw = (raw.title || "").trim();
    const contentText = String(raw.content_text || "").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    let title = titleRaw;
    let summary = contentText;
    if (!title && contentText) {
      const idx = contentText.search(/[。！？!?\n]/);
      title = idx > 6 && idx < 80 ? contentText.slice(0, idx) : contentText.slice(0, 60);
      summary = contentText;
    }
    if (!title || countChineseChars(title) < 4) continue;
    const link = [raw.uri, raw.global_more_uri, raw.reference?.uri]
      .filter((value) => typeof value === "string")
      .map((value) => value.trim())
      .find((value) => /^https?:\/\//i.test(value));
    if (!link) continue;
    const key = title.slice(0, 28);
    if (seen.has(key)) continue;
    seen.add(key);

    const tsSec = Number(raw.display_time);
    const validTs = Number.isFinite(tsSec) && tsSec > 1_000_000_000 && tsSec * 1000 <= Date.now();
    const publishedAt = validTs
      ? formatCnNewsDate(new Date(tsSec * 1000))
      : "发布时间未提供";
    const tag = classifyNewsTag(`${title} ${summary}`);
    const sumClean = summary && summary !== title
      ? (summary.length > 180 ? `${summary.slice(0, 179).replace(/[，,、；;：:\s]+$/, "")}…` : summary)
      : `对应 ${tag} 市场动态追踪。`;
    const eventId = `wscn-${raw.id || seen.size}`;

    const entry = {
      sortTs: validTs ? tsSec : 0,
      item: {
        event_id: String(eventId),
        title,
        summary: sumClean,
        tag,
        source: "华尔街见闻",
        pubDate: publishedAt,
        link,
      },
    };
    if (isRelevantToUsDesk(tag, title, `${title} ${summary}`)) {
      relevant.push(entry);
    } else if (!isBoilerplateNoise(title)) {
      backfill.push(entry);
    }
  }

  relevant.sort((a, b) => b.sortTs - a.sortTs);
  backfill.sort((a, b) => b.sortTs - a.sortTs);
  const picked = relevant.slice(0, 25);
  if (picked.length < 10) {
    picked.push(...backfill.slice(0, 10 - picked.length));
  }
  picked.sort((a, b) => b.sortTs - a.sortTs);
  const items = picked.map((entry) => entry.item);

  if (items.length === 0 && isValidNewsPayload(cached)) {
    await putCachedPayload(cacheKey, "news", cached, WORKER_CONFIG.FALLBACK_COOLDOWN_SEC, ctx, true);
    return cached;
  }

  const payload = {
    updatedAt: formatCnTimestamp(new Date()),
    count: items.length,
    items,
  };
  if (items.length > 0) {
    await putCachedPayload(cacheKey, "news", payload, WORKER_CONFIG.NEWS_EDGE_MAX_SEC, ctx);
    safeKvPut(env, "news", payload, ctx);
  }
  return payload;
}

async function resolveNews(env, ctx, force, requestUrl) {
  const cacheKey = buildCacheKey(requestUrl, "/api/news");

  if (!force) {
    const hit = await getCachedPayload(cacheKey, "news");
    if (hit && isValidNewsPayload(hit.data)) {
      const age = parseCnTimestampAgeMs(hit.data.updatedAt);
      const withinFresh = age !== null && age >= 0 && age <= WORKER_CONFIG.NEWS_FRESH_MS;
      const withinSwr = age !== null && age >= -5000 && age <= WORKER_CONFIG.NEWS_SWR_MAX_AGE_MS;
      const inCooldown = Boolean(hit.isFallback) && (Date.now() - hit.cachedAt < WORKER_CONFIG.FALLBACK_COOLDOWN_SEC * 1000);
      if (withinFresh) {
        return { payload: hit.data, body: hit.body, etag: hit.etag, cacheStatus: `${hit.layer}-HIT` };
      }
      if (withinSwr || inCooldown) {
        if (!inCooldown) {
          scheduleBackground(
            ctx,
            singleFlight("news-refresh", () => fetchAndStoreNews(env, ctx, cacheKey, hit.data, true))
          );
        }
        return {
          payload: hit.data,
          body: hit.body,
          etag: hit.etag,
          cacheStatus: inCooldown ? `${hit.layer}-FALLBACK` : `${hit.layer}-STALE`,
        };
      }
    }
  }

  const cached = await safeKvJson(env, "news");
  const hasValidKv = isValidNewsPayload(cached);

  if (!force && hasValidKv) {
    const age = parseCnTimestampAgeMs(cached.updatedAt);
    if (age !== null && age >= 0 && age <= WORKER_CONFIG.NEWS_FRESH_MS) {
      const entry = await putCachedPayload(cacheKey, "news", cached, WORKER_CONFIG.NEWS_EDGE_MAX_SEC, ctx);
      return { payload: cached, body: entry.body, etag: entry.etag, cacheStatus: "KV-HIT" };
    }
    if (age !== null && age >= -5000 && age <= WORKER_CONFIG.NEWS_SWR_MAX_AGE_MS) {
      const entry = await putCachedPayload(cacheKey, "news", cached, WORKER_CONFIG.NEWS_EDGE_MAX_SEC, ctx);
      scheduleBackground(
        ctx,
        singleFlight("news-refresh", () => fetchAndStoreNews(env, ctx, cacheKey, cached, false))
      );
      return { payload: cached, body: entry.body, etag: entry.etag, cacheStatus: "KV-STALE" };
    }
  }

  const refreshPromise = singleFlight("news-refresh", () =>
    fetchAndStoreNews(env, ctx, cacheKey, cached, false)
  );

  const reuseMem = (payload) => {
    const mem = ISOLATE_CACHE.get("news");
    return mem && mem.data === payload ? { body: mem.body, etag: mem.etag } : {};
  };

  if (hasValidKv) {
    const waitMs = force ? WORKER_CONFIG.NEWS_FORCE_WAIT_MS : WORKER_CONFIG.NEWS_STALE_WAIT_MS;
    const outcome = await raceWithDeadline(refreshPromise, waitMs, cached);
    if (outcome.timedOut) {
      scheduleBackground(ctx, refreshPromise);
      return { payload: cached, ...reuseMem(cached), cacheStatus: "KV-FALLBACK" };
    }
    return {
      payload: outcome.value,
      ...reuseMem(outcome.value),
      cacheStatus: outcome.value === cached ? "KV-FALLBACK" : (force ? "FORCE-FRESH" : "ORIGIN-FRESH"),
    };
  }

  const payload = await refreshPromise;
  return { payload, ...reuseMem(payload), cacheStatus: force ? "FORCE-COLD" : "ORIGIN-COLD" };
}

async function getNewsPayload(env, ctx, force) {
  const res = await resolveNews(env, ctx, force, null);
  return res.payload;
}

async function fetchAndCacheState(env, ctx, cacheKey) {
  const state = await safeKvJson(env, "state");
  if (!state) return null;
  return putCachedPayload(cacheKey, "state", state, WORKER_CONFIG.STATE_EDGE_MAX_SEC, ctx);
}

async function resolveState(env, ctx, force, requestUrl) {
  const cacheKey = buildCacheKey(requestUrl, "/api/state");

  if (!force) {
    const hit = await getCachedPayload(cacheKey, "state");
    if (hit && hit.data) {
      const cacheAge = Date.now() - hit.cachedAt;
      if (cacheAge <= WORKER_CONFIG.STATE_FRESH_MS) {
        return { payload: hit.data, body: hit.body, etag: hit.etag, cacheStatus: `${hit.layer}-HIT` };
      }
      scheduleBackground(
        ctx,
        singleFlight("state-refresh", () => fetchAndCacheState(env, ctx, cacheKey))
      );
      return { payload: hit.data, body: hit.body, etag: hit.etag, cacheStatus: `${hit.layer}-STALE` };
    }
  }

  const entry = force
    ? await fetchAndCacheState(env, ctx, cacheKey)
    : await singleFlight("state-refresh", () => fetchAndCacheState(env, ctx, cacheKey));
  if (!entry) {
    return { payload: null, cacheStatus: "MISS" };
  }
  return {
    payload: entry.data,
    body: entry.body,
    etag: entry.etag,
    cacheStatus: force ? "FORCE-KV" : "KV-HIT",
  };
}

const EARLY_CLOSE_ET_DATES = new Set([
  "2026-11-27",
  "2026-12-24",
  "2027-11-26",
  "2028-7-3",
  "2028-11-24",
]);

const REBALANCE_HISTORY_TYPES = new Set(["晋升主线", "降级淘汰", "主线复盘"]);

function isPostCloseRebalanceDue(stateObj, now = new Date()) {
  const et = getEtParts(now);
  if (et.weekday === "Sat" || et.weekday === "Sun") return { due: false, forceRebalance: false };
  if (isNyseHolidayEt(et.year, et.month, et.day)) return { due: false, forceRebalance: false };
  const closeMin = EARLY_CLOSE_ET_DATES.has(`${et.year}-${et.month}-${et.day}`) ? 13 * 60 : 16 * 60;
  const nowMin = et.hour * 60 + et.minute;
  if (nowMin < closeMin + 30 || nowMin >= closeMin + 150) {
    return { due: false, forceRebalance: false };
  }
  const history = Array.isArray(stateObj?.history) ? stateObj.history : [];
  const etTodayKey = `${et.year}-${et.month}-${et.day}`;
  for (const entry of history) {
    if (!entry || !REBALANCE_HISTORY_TYPES.has(entry.type)) continue;
    const m = String(entry.time || "").match(/^(\d{4})[-/](\d{2})[-/](\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?/);
    if (!m) continue;
    const utcMs = Date.UTC(
      Number(m[1]),
      Number(m[2]) - 1,
      Number(m[3]),
      Number(m[4]) - 8,
      Number(m[5]),
      Number(m[6] || 0)
    );
    const entryEt = getEtParts(new Date(utcMs));
    const entryKey = `${entryEt.year}-${entryEt.month}-${entryEt.day}`;
    const entryMin = entryEt.hour * 60 + entryEt.minute;
    if (entryKey === etTodayKey && entryMin >= closeMin + 30) {
      return { due: false, forceRebalance: false };
    }
  }
  return { due: true, forceRebalance: nowMin >= closeMin + 60 };
}

function isCrossMarketWindowDue(stateObj, now = new Date()) {
  const nowMs = now.getTime();
  const cn = new Date(nowMs + 8 * 3600_000);
  const wd = cn.getUTCDay();
  if (wd === 0 || wd === 6) return { due: false, reason: "" };
  const cnMin = cn.getUTCHours() * 60 + cn.getUTCMinutes();
  let winStartMin = null;
  let winReason = "";
  if (cnMin >= 14 * 60 + 35 && cnMin < 15 * 60 + 15) {
    winStartMin = 14 * 60 + 35;
    winReason = "cross_market_apac_close";
  } else if (cnMin >= 16 * 60 + 5 && cnMin < 16 * 60 + 40) {
    winStartMin = 16 * 60 + 5;
    winReason = "cross_market_eu_open";
  } else {
    return { due: false, reason: "" };
  }
  const winStartUtcMs = Date.UTC(
    cn.getUTCFullYear(),
    cn.getUTCMonth(),
    cn.getUTCDate(),
    Math.floor(winStartMin / 60) - 8,
    winStartMin % 60,
    0
  );
  const ageMs = parseCnTimestampAgeMs(stateObj?.serverSyncedAt, nowMs);
  const syncedMs = ageMs !== null && ageMs >= 0 ? nowMs - ageMs : null;
  if (syncedMs === null || syncedMs < winStartUtcMs) {
    return { due: true, reason: winReason };
  }
  return { due: false, reason: "" };
}

function evaluateResearchDispatch(stateObj, dispatchMeta, now = new Date()) {
  const nowMs = now.getTime();
  const stateAgeMs = parseCnTimestampAgeMs(stateObj?.serverSyncedAt, nowMs);
  const stateAgeMin = stateAgeMs !== null && stateAgeMs >= 0 ? Math.round(stateAgeMs / 60_000) : null;

  const lastTs = dispatchMeta?.lastDispatchedAt ? Date.parse(dispatchMeta.lastDispatchedAt) : NaN;
  if (Number.isFinite(lastTs) && nowMs - lastTs >= 0 && nowMs - lastTs < WORKER_CONFIG.DISPATCH_COOLDOWN_MS) {
    return { shouldDispatch: false, reason: "cooldown", forceRebalance: false, stateAgeMin };
  }

  const rebal = isPostCloseRebalanceDue(stateObj, now);
  if (rebal.due) {
    return {
      shouldDispatch: true,
      reason: rebal.forceRebalance ? "post_close_rebalance_catchup" : "post_close_rebalance",
      forceRebalance: rebal.forceRebalance,
      stateAgeMin,
    };
  }

  const crossMarket = isCrossMarketWindowDue(stateObj, now);
  if (crossMarket.due) {
    return {
      shouldDispatch: true,
      reason: crossMarket.reason,
      forceRebalance: false,
      stateAgeMin,
    };
  }

  const active = isActiveMarketSession(now);
  const maxAgeMs = active ? WORKER_CONFIG.RESEARCH_ACTIVE_MAX_AGE_MS : WORKER_CONFIG.RESEARCH_IDLE_MAX_AGE_MS;
  if (stateAgeMs === null || stateAgeMs < 0 || stateAgeMs >= maxAgeMs) {
    return {
      shouldDispatch: true,
      reason: active ? "active_hourly_due" : "idle_2h_due",
      forceRebalance: false,
      stateAgeMin,
    };
  }
  return { shouldDispatch: false, reason: "fresh", forceRebalance: false, stateAgeMin };
}

async function maybeDispatchResearchWorkflow(env, ctx, now = new Date()) {
  const [stateObj, dispatchMeta] = await Promise.all([
    safeKvJson(env, "state"),
    safeKvJson(env, "dispatch_meta"),
  ]);
  const decision = evaluateResearchDispatch(stateObj, dispatchMeta, now);
  const token = String(env?.GH_DISPATCH_TOKEN || "").trim();
  if (!decision.shouldDispatch) {
    return { dispatched: false, configured: Boolean(token), ...decision };
  }
  if (!token) {
    return { dispatched: false, configured: false, reason: "missing_token", ...decision, pendingReason: decision.reason };
  }

  const repo = String(env?.GH_DISPATCH_REPO || WORKER_CONFIG.GITHUB_REPO).trim();
  const workflow = String(env?.GH_DISPATCH_WORKFLOW || WORKER_CONFIG.GITHUB_WORKFLOW).trim();
  const ref = String(env?.GH_DISPATCH_REF || WORKER_CONFIG.GITHUB_REF).trim();
  const url = `https://api.github.com/repos/${repo}/actions/workflows/${workflow}/dispatches`;
  const nowIso = formatUtcIso(now);

  let statusCode = 0;
  let ok = false;
  try {
    const signal = typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function"
      ? AbortSignal.timeout(WORKER_CONFIG.DISPATCH_TIMEOUT_MS)
      : undefined;
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "StockTradingDesk-Worker/2.0",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({
        ref,
        inputs: {
          deploy_worker: "false",
          force_rebalance: decision.forceRebalance ? "true" : "false",
        },
      }),
      signal,
    });
    statusCode = resp ? resp.status : 0;
    ok = Boolean(resp && (resp.ok || resp.status === 204));
  } catch (_) {
    statusCode = 0;
    ok = false;
  }

  const nextMeta = {
    lastDispatchedAt: nowIso,
    lastStatus: ok ? "dispatched" : `error_${statusCode || "network"}`,
    lastHttpStatus: statusCode,
    reason: decision.reason,
    stateAgeMin: decision.stateAgeMin,
  };
  safeKvPut(env, "dispatch_meta", nextMeta, ctx);
  return { dispatched: ok, configured: true, statusCode, ...decision };
}

async function runScheduledMaintenance(env, ctx, now = new Date()) {
  const active = isActiveMarketSession(now);
  const liveMaxAgeMs = active ? WORKER_CONFIG.CRON_LIVE_ACTIVE_MS : WORKER_CONFIG.CRON_LIVE_IDLE_MS;
  const newsMaxAgeMs = active ? WORKER_CONFIG.CRON_NEWS_ACTIVE_MS : WORKER_CONFIG.CRON_NEWS_IDLE_MS;
  const [cachedLive, cachedNews] = await Promise.all([
    safeKvJson(env, "live"),
    safeKvJson(env, "news"),
  ]);

  const tasks = [];
  const liveAge = isValidLivePayload(cachedLive) ? parseCnTimestampAgeMs(cachedLive.updatedAt, now.getTime()) : null;
  const newsAge = isValidNewsPayload(cachedNews) ? parseCnTimestampAgeMs(cachedNews.updatedAt, now.getTime()) : null;
  let refreshedLive = false;
  let refreshedNews = false;

  if (liveAge === null || liveAge < 0 || liveAge >= liveMaxAgeMs) {
    refreshedLive = true;
    tasks.push(
      fetchAndStoreLive(env, ctx, null, cachedLive, false, WORKER_CONFIG.LIVE_CRON_BUDGET_MS).catch(() => null)
    );
  }
  if (newsAge === null || newsAge < 0 || newsAge >= newsMaxAgeMs) {
    refreshedNews = true;
    tasks.push(
      fetchAndStoreNews(env, ctx, null, cachedNews, false).catch(() => null)
    );
  }
  const dispatchPromise = maybeDispatchResearchWorkflow(env, ctx, now).catch((err) => ({
    dispatched: false,
    reason: `error:${err?.name || "unknown"}`,
  }));
  tasks.push(dispatchPromise);

  await Promise.allSettled(tasks);
  const dispatch = await dispatchPromise;
  return { refreshedLive, refreshedNews, dispatch };
}

export default {
  async scheduled(event, env, ctx) {
    const now = event && Number.isFinite(event.scheduledTime) ? new Date(event.scheduledTime) : new Date();
    const task = runScheduledMaintenance(env, ctx, now);
    if (ctx && typeof ctx.waitUntil === "function") {
      ctx.waitUntil(task);
    }
    return task;
  },

  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const path = url.pathname;

      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: { ...SECURITY_HEADERS, ...CORS_HEADERS } });
      }

      if (path === "/" || path === "/index.html") {
        if (request.method !== "GET" && request.method !== "HEAD") {
          return jsonResponse({ ok: false, detail: "Method Not Allowed" }, 405);
        }
        const htmlEtag = getHtmlEtag();
        const headers = {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": CACHE_CONTROL_HTML,
          "ETag": htmlEtag,
          "X-Frame-Options": "SAMEORIGIN",
          "X-Desk-Cache": "MEM-HIT",
          ...SECURITY_HEADERS,
        };
        if (etagMatches(request.headers.get("If-None-Match"), htmlEtag)) {
          headers["X-Desk-Cache"] = "MEM-304";
          return new Response(null, { status: 304, headers });
        }
        return new Response(request.method === "HEAD" ? null : INDEX_HTML, {
          status: 200,
          headers,
        });
      }

      if (path === "/favicon.ico") {
        return new Response(null, { status: 204, headers: SECURITY_HEADERS });
      }

      if (path.startsWith("/api/")) {
        if (request.method !== "GET" && request.method !== "HEAD") {
          return jsonResponse({ ok: false, detail: "Method Not Allowed" }, 405);
        }

        const force = isForce(url);

        if (path === "/api/health") {
          const [stateRes, liveHit, newsHit, dispatchMeta] = await Promise.all([
            resolveState(env, ctx, force, request.url),
            force
              ? resolveLive(env, ctx, true, request.url).then((r) => r.payload)
              : getCachedPayload(buildCacheKey(request.url, "/api/live"), "live")
                  .then((h) => (h && h.data ? h.data : safeKvJson(env, "live"))),
            force
              ? resolveNews(env, ctx, true, request.url).then((r) => r.payload)
              : getCachedPayload(buildCacheKey(request.url, "/api/news"), "news")
                  .then((h) => (h && h.data ? h.data : safeKvJson(env, "news"))),
            safeKvJson(env, "dispatch_meta"),
          ]);
          const stateObj = stateRes.payload;
          const live = liveHit || (await getLivePayload(env, ctx, false));
          const news = newsHit || (await getNewsPayload(env, ctx, false));
          let stateUpdatedAt = null;
          let stateAgeMinutes = null;
          if (stateObj && stateObj.serverSyncedAt) {
            const m = String(stateObj.serverSyncedAt).match(/^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2})/);
            if (m) {
              const dt = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]) - 8, Number(m[5]), Number(m[6])));
              stateUpdatedAt = formatUtcIso(dt);
              stateAgeMinutes = Math.max(0, Math.round((Date.now() - dt.getTime()) / 60_000));
            }
          }
          return jsonResponse(
            {
              ok: true,
              version: APP_VERSION,
              serverTimeUtc: formatUtcIso(new Date()),
              stateUpdatedAt,
              stateAgeMinutes,
              liveUpdatedAt: live ? live.updatedAt || null : null,
              newsCount: news ? news.count : 0,
              quotesCount: live ? live.count : 0,
              cronDispatchConfigured: Boolean(String(env?.GH_DISPATCH_TOKEN || "").trim()),
              lastDispatchAt: dispatchMeta?.lastDispatchedAt || null,
              lastDispatchStatus: dispatchMeta?.lastStatus || null,
            },
            { status: 200, cacheControl: "no-store", request }
          );
        }

        if (path === "/api/live") {
          const liveRes = await resolveLive(env, ctx, force, request.url);
          let live = liveRes.payload;
          const sym = (url.searchParams.get("sym") || "").trim().toUpperCase();
          if (sym) {
            const filtered = (live.quotes || []).filter((q) => String(q.sym).toUpperCase() === sym);
            live = { ...live, count: filtered.length, quotes: filtered };
            return jsonResponse(live, {
              status: 200,
              cacheControl: force ? "no-store" : CACHE_CONTROL_LIVE,
              etagPrefix: "live",
              cacheStatus: liveRes.cacheStatus,
              request,
            });
          }
          return jsonResponse(live, {
            status: 200,
            cacheControl: force ? "no-store" : CACHE_CONTROL_LIVE,
            etag: liveRes.etag,
            etagPrefix: "live",
            preSerializedBody: liveRes.body,
            cacheStatus: liveRes.cacheStatus,
            request,
          });
        }

        if (path === "/api/state") {
          const stateRes = await resolveState(env, ctx, force, request.url);
          if (!stateRes.payload) {
            return jsonResponse(
              { ok: false, error: "Research state not initialized in KV" },
              { status: 503, cacheControl: "no-store", request }
            );
          }
          return jsonResponse(stateRes.payload, {
            status: 200,
            cacheControl: force ? "no-store" : CACHE_CONTROL_STATE,
            etag: stateRes.etag,
            preSerializedBody: stateRes.body,
            cacheStatus: stateRes.cacheStatus,
            request,
          });
        }

        if (path === "/api/news") {
          const newsRes = await resolveNews(env, ctx, force, request.url);
          let news = newsRes.payload;
          const limitRaw = url.searchParams.get("limit");
          if (limitRaw !== null) {
            const n = parseInt(limitRaw, 10);
            if (Number.isFinite(n)) {
              const clamped = Math.max(1, Math.min(n, 50));
              const sliced = (news.items || []).slice(0, clamped);
              news = { ...news, count: sliced.length, items: sliced };
              return jsonResponse(news, {
                status: 200,
                cacheControl: force ? "no-store" : CACHE_CONTROL_NEWS,
                etagPrefix: "news",
                cacheStatus: newsRes.cacheStatus,
                request,
              });
            }
          }
          return jsonResponse(news, {
            status: 200,
            cacheControl: force ? "no-store" : CACHE_CONTROL_NEWS,
            etag: newsRes.etag,
            etagPrefix: "news",
            preSerializedBody: newsRes.body,
            cacheStatus: newsRes.cacheStatus,
            request,
          });
        }

        return jsonResponse({ ok: false, detail: "Not Found" }, { status: 404, request });
      }

      return jsonResponse({ ok: false, detail: "Not Found" }, { status: 404, request });
    } catch (err) {
      return jsonResponse({ ok: false, detail: "Internal server error" }, 500);
    }
  },
};
