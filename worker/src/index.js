import INDEX_HTML from "./spa_html.js";

const APP_VERSION = "2.0.0";
const CACHE_TTL_MS = 180_000;

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
];

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const SECURITY_HEADERS = {
  "X-Content-Type-Options": "nosniff",
  "Referrer-Policy": "strict-origin-when-cross-origin",
};

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      ...SECURITY_HEADERS,
      ...CORS_HEADERS,
    },
  });
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
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const map = {};
  for (const part of fmt.formatToParts(date)) {
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

function finiteOrNull(v) {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function roundOrNull(v, digits = 2) {
  const n = finiteOrNull(v);
  if (n === null) return null;
  const f = 10 ** digits;
  return Math.round(n * f) / f;
}

async function fetchYahooSymbol(sym, name, nowIso) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?range=3mo&interval=1d&includePrePost=true`;
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 StockTradingDesk/2.0" },
    });
    if (!resp.ok) return null;
    const payload = await resp.json();
    const res = payload?.chart?.result?.[0];
    if (!res) return null;
    const meta = res.meta || {};
    const timestamps = res.timestamp || [];
    const rawCloses = res.indicators?.quote?.[0]?.close || [];
    const seenDates = new Map();
    const etFmt = new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York" });
    for (let i = 0; i < Math.min(timestamps.length, rawCloses.length); i++) {
      const c = finiteOrNull(rawCloses[i]);
      const ts = Number(timestamps[i]);
      if (c === null || c <= 0 || !Number.isFinite(ts)) continue;
      seenDates.set(etFmt.format(new Date(ts * 1000)), c);
    }
    const closes = Array.from(seenDates.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map((entry) => entry[1]);

    const price = finiteOrNull(meta.regularMarketPrice) ?? (closes.length ? closes[closes.length - 1] : null);
    // Prefer regularMarketChangePercent or closes[-2]; never use range-wide chartPreviousClose.
    let chg = finiteOrNull(meta.regularMarketChangePercent);
    if (chg === null && price !== null) {
      const prevClose = closes.length >= 2
        ? closes[closes.length - 2]
        : (finiteOrNull(meta.regularMarketPreviousClose) ?? finiteOrNull(meta.previousClose));
      if (prevClose !== null && prevClose > 0) {
        chg = ((price / prevClose) - 1) * 100;
      }
    }
    const pre = detectMarketSession(new Date(nowIso)).marketSession === "PRE_MARKET";
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
    return await env.DESK_STATE.get(key, "json");
  } catch (_) {
    return null;
  }
}

function isFreshTimestamp(tsStr, maxAgeMs) {
  if (!tsStr || typeof tsStr !== "string") return false;
  const m = tsStr.match(/^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2})/);
  if (!m) return false;
  const utcMs = Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]) - 8, Number(m[5]), Number(m[6]));
  const age = Date.now() - utcMs;
  return age >= 0 && age <= maxAgeMs;
}

async function getLivePayload(env, ctx, force) {
  const cached = await safeKvJson(env, "live");
  if (!force && cached && Array.isArray(cached.quotes) && cached.quotes.length >= 27 && isFreshTimestamp(cached.updatedAt, CACHE_TTL_MS)) {
    return cached;
  }

  const now = new Date();
  const nowIso = formatUtcIso(now);
  const { marketSession, updatedAtEt } = detectMarketSession(now);
  const cachedBySym = Object.create(null);
  if (cached && Array.isArray(cached.quotes)) {
    for (const q of cached.quotes) {
      if (q && q.sym) cachedBySym[q.sym] = q;
    }
  }

  const fetched = await Promise.all(
    LIVE_SYMBOLS.map(([sym, name]) => fetchYahooSymbol(sym, name, nowIso))
  );
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
  if (ctx && typeof ctx.waitUntil === "function") {
    ctx.waitUntil(env.DESK_STATE.put("live", JSON.stringify(payload)));
  }
  return payload;
}

function countChineseChars(s) {
  const m = String(s || "").match(/[\u4e00-\u9fff]/g);
  return m ? m.length : 0;
}

const US_RELEVANCE_RE = /(美股|纳指|标普|道指|华尔街|美联储|联储|鲍威尔|美债|美元|日元|黄金|白银|原油|布伦特|wti|英伟达|nvidia|nvda|苹果|微软|谷歌|亚马逊|meta|特斯拉|博通|amd|台积电|asml|光模块|半导体|芯片|算力|ai|比特币|btc|加密|coinbase|mstr|关税|通胀|cpi|pce|非农|pmi|gdp|降息|加息|高盛|摩根|sec|财报|并购|科技股)/i;

function classifyNewsTag(text) {
  const t = String(text || "");
  if (/英伟达|nvidia|nvda|博通|avgo|amd|芯片|半导体|算力|gpu|ai|台积电|asml|光模块|lumentum/i.test(t)) return "M1 算力";
  if (/电力|核电|电网|能源|原油|布伦特|wti|opec|数据中心|iren|nebius|coreweave|公用事业/i.test(t)) return "电力基建";
  if (/美联储|fed|fomc|鲍威尔|降息|加息|通胀|cpi|pce|非农|美债|收益率|黄金|白银|日元|美元/i.test(t)) return "宏观利率";
  if (/比特币|bitcoin|btc|以太坊|加密|crypto|coinbase|strategy|mstr/i.test(t)) return "加密联动";
  if (/sec|8-k|425|反垄断|关税|制裁|出口管制|并购|裁决/i.test(t)) return "并购监管";
  if (/日经|日本|韩国|欧洲|英国|中国|港股|恒生|a股|亚太/i.test(t)) return "全市场宏观";
  return "美股快讯";
}

function isRelevantToUsDesk(tag, text) {
  if (tag !== "美股快讯") return true;
  return US_RELEVANCE_RE.test(String(text || ""));
}

async function fetchWscnChannel(channel) {
  const url = `https://api-one-wscn.awtmt.com/apiv1/content/lives?channel=${channel}&limit=25`;
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 StockTradingDesk/2.0" },
    });
    if (!resp.ok) return [];
    const data = await resp.json();
    return data?.data?.items || [];
  } catch (_) {
    return [];
  }
}

async function getNewsPayload(env, ctx, force) {
  const cached = await safeKvJson(env, "news");
  if (!force && cached && Array.isArray(cached.items) && cached.items.length > 0 && isFreshTimestamp(cached.updatedAt, CACHE_TTL_MS)) {
    return cached;
  }

  const [usItems, globalItems] = await Promise.all([
    fetchWscnChannel("us-stock-channel"),
    fetchWscnChannel("global-channel"),
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
    const link = (raw.uri || raw.reference?.uri || "").trim() || `https://wallstreetcn.com/live/global`;
    if (!/^https?:\/\//i.test(link)) continue;
    const key = title.slice(0, 28);
    if (seen.has(key)) continue;
    seen.add(key);

    const tsSec = Number(raw.display_time);
    const dt = Number.isFinite(tsSec) && tsSec > 0 ? new Date(tsSec * 1000) : new Date();
    const tag = classifyNewsTag(`${title} ${summary}`);
    const sumClean = summary && summary !== title ? summary.slice(0, 140) : `对应 ${tag} 市场动态追踪。`;
    const eventId = `wscn-${raw.id || seen.size}`;

    const item = {
      event_id: String(eventId),
      title,
      summary: sumClean,
      tag,
      source: "华尔街见闻",
      pubDate: formatCnNewsDate(dt),
      link,
    };
    if (isRelevantToUsDesk(tag, `${title} ${summary}`)) {
      relevant.push(item);
    } else {
      backfill.push(item);
    }
  }

  const items = [...relevant, ...backfill].slice(0, 25);

  if (items.length === 0 && cached && Array.isArray(cached.items) && cached.items.length > 0) {
    return cached;
  }

  const payload = {
    updatedAt: formatCnTimestamp(new Date()),
    count: items.length,
    items,
  };
  if (items.length > 0 && ctx && typeof ctx.waitUntil === "function") {
    ctx.waitUntil(env.DESK_STATE.put("news", JSON.stringify(payload)));
  }
  return payload;
}

export default {
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
        return new Response(request.method === "HEAD" ? null : INDEX_HTML, {
          status: 200,
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-store",
            "X-Frame-Options": "SAMEORIGIN",
            ...SECURITY_HEADERS,
          },
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
          const [stateObj, live, news] = await Promise.all([
            safeKvJson(env, "state"),
            getLivePayload(env, ctx, force),
            getNewsPayload(env, ctx, force),
          ]);
          let stateUpdatedAt = null;
          if (stateObj && stateObj.serverSyncedAt) {
            const m = String(stateObj.serverSyncedAt).match(/^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2})/);
            if (m) {
              const dt = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]) - 8, Number(m[5]), Number(m[6])));
              stateUpdatedAt = formatUtcIso(dt);
            }
          }
          return jsonResponse({
            ok: true,
            version: APP_VERSION,
            serverTimeUtc: formatUtcIso(new Date()),
            stateUpdatedAt,
            newsCount: news.count,
            quotesCount: live.count,
          });
        }

        if (path === "/api/live") {
          let live = await getLivePayload(env, ctx, force);
          const sym = (url.searchParams.get("sym") || "").trim().toUpperCase();
          if (sym) {
            const filtered = live.quotes.filter((q) => String(q.sym).toUpperCase() === sym);
            live = { ...live, count: filtered.length, quotes: filtered };
          }
          return jsonResponse(live);
        }

        if (path === "/api/state") {
          const state = await safeKvJson(env, "state");
          if (!state) {
            return jsonResponse({ ok: false, error: "Research state not initialized in KV" }, 503);
          }
          return jsonResponse(state);
        }

        if (path === "/api/news") {
          let news = await getNewsPayload(env, ctx, force);
          const limitRaw = url.searchParams.get("limit");
          if (limitRaw !== null) {
            const n = parseInt(limitRaw, 10);
            if (Number.isFinite(n)) {
              const clamped = Math.max(1, Math.min(n, 50));
              const sliced = news.items.slice(0, clamped);
              news = { ...news, count: sliced.length, items: sliced };
            }
          }
          return jsonResponse(news);
        }

        return jsonResponse({ ok: false, detail: "Not Found" }, 404);
      }

      return jsonResponse({ ok: false, detail: "Not Found" }, 404);
    } catch (err) {
      return jsonResponse({ ok: false, detail: "Internal server error" }, 500);
    }
  },
};
