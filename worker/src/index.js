import INDEX_HTML from "./spa_html.js";

const APP_VERSION = "2.0.0";
const CACHE_TTL_MS = 180_000;

const CORE_27 = [
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
  ["NVDA", "英伟达"],
  ["AVGO", "博通"],
  ["AMD", "超威半导体"],
  ["LITE", "Lumentum"],
  ["SOXX", "半导体 ETF"],
  ["ITB", "房屋建筑 ETF"],
  ["AAPL", "苹果"],
  ["IREN", "IREN"],
  ["NBIS", "Nebius"],
  ["CRWV", "CoreWeave"],
  ["COIN", "Coinbase"],
  ["MSTR", "Strategy"],
];

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
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
  const key = `${year}-${month}-${day}`;
  if (wd === 0 || wd === 6) return false;
  const fixed = new Set([
    observed(1, 1),
    nthWeekday(1, 1, 3),
    nthWeekday(2, 1, 3),
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
  if (mins >= 570 && mins < 960) return { marketSession: "INTRADAY", updatedAtEt: etShort };
  if (mins >= 960 && mins < 1200) return { marketSession: "AFTER_HOURS", updatedAtEt: etShort };
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
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(sym)}?range=1mo&interval=1d&includePrePost=true`;
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0 StockTradingDesk/2.0" },
    });
    if (!resp.ok) return null;
    const payload = await resp.json();
    const res = payload?.chart?.result?.[0];
    if (!res) return null;
    const meta = res.meta || {};
    const rawCloses = res.indicators?.quote?.[0]?.close || [];
    const closes = rawCloses.filter((x) => typeof x === "number" && Number.isFinite(x));
    const price = finiteOrNull(meta.regularMarketPrice) ?? (closes.length ? closes[closes.length - 1] : null);
    const prev = finiteOrNull(meta.chartPreviousClose) ?? finiteOrNull(meta.previousClose) ?? (closes.length >= 2 ? closes[closes.length - 2] : null);
    let chg = null;
    if (price !== null && prev !== null && prev !== 0) {
      chg = ((price - prev) / prev) * 100;
    }
    let ahPrice = finiteOrNull(meta.postMarketPrice) ?? finiteOrNull(meta.preMarketPrice);
    let ahChg = finiteOrNull(meta.postMarketChangePercent) ?? finiteOrNull(meta.preMarketChangePercent);
    let r5 = null;
    if (closes.length >= 6 && closes[closes.length - 6] > 0) {
      r5 = ((closes[closes.length - 1] - closes[closes.length - 6]) / closes[closes.length - 6]) * 100;
    }
    let r20 = null;
    if (closes.length >= 2) {
      const idx20 = Math.max(0, closes.length - 21);
      if (closes[idx20] > 0) {
        r20 = ((closes[closes.length - 1] - closes[idx20]) / closes[idx20]) * 100;
      }
    }
    return {
      sym,
      name,
      price: roundOrNull(price, 2),
      chg: roundOrNull(chg, 2),
      ahPrice: roundOrNull(ahPrice, 2),
      ahChg: roundOrNull(ahChg, 2),
      r5: roundOrNull(r5, 2),
      r20: roundOrNull(r20, 2),
      source: "yahoo_v8",
      tsUtc: nowIso,
    };
  } catch (_) {
    return null;
  }
}

function isFreshTimestamp(tsStr, maxAgeMs) {
  if (!tsStr || typeof tsStr !== "string") return false;
  // Convert "YYYY/MM/DD HH:MM:SS UTC+8" to ISO
  const m = tsStr.match(/^(\d{4})\/(\d{2})\/(\d{2}) (\d{2}):(\d{2}):(\d{2})/);
  if (!m) return false;
  const utcMs = Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]) - 8, Number(m[5]), Number(m[6]));
  return Date.now() - utcMs <= maxAgeMs;
}

async function getLivePayload(env, ctx, force) {
  const cached = await env.DESK_STATE.get("live", "json");
  if (!force && cached && Array.isArray(cached.quotes) && cached.quotes.length >= 27 && isFreshTimestamp(cached.updatedAt, CACHE_TTL_MS)) {
    return cached;
  }

  const now = new Date();
  const nowIso = formatUtcIso(now);
  const { marketSession, updatedAtEt } = detectMarketSession(now);
  const cachedBySym = {};
  if (cached && Array.isArray(cached.quotes)) {
    for (const q of cached.quotes) {
      if (q && q.sym) cachedBySym[q.sym] = q;
    }
  }

  const fetched = await Promise.all(
    CORE_27.map(([sym, name]) => fetchYahooSymbol(sym, name, nowIso))
  );
  const quotes = CORE_27.map(([sym, name], idx) => {
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

function classifyNewsTag(text) {
  const t = String(text || "");
  if (/英伟达|NVDA|博通|AVGO|算力|半导体|芯片|光模块|AI/i.test(t)) return "M1 算力";
  if (/电力|核电|电网|能源|原油|OPEC/i.test(t)) return "M2 电力资源";
  if (/美联储|FOMC|CPI|PCE|利率|美债|降息|通胀|非农/i.test(t)) return "宏观利率";
  if (/比特币|BTC|加密|Coinbase|MicroStrategy|MSTR/i.test(t)) return "M5 加密链路";
  return "美股快讯";
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
  const cached = await env.DESK_STATE.get("news", "json");
  if (!force && cached && Array.isArray(cached.items) && cached.items.length > 0 && isFreshTimestamp(cached.updatedAt, CACHE_TTL_MS)) {
    return cached;
  }

  const [usItems, globalItems] = await Promise.all([
    fetchWscnChannel("us-stock-channel"),
    fetchWscnChannel("global-channel"),
  ]);

  const seen = new Set();
  const items = [];
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

    items.push({
      event_id: String(eventId),
      title,
      summary: sumClean,
      tag,
      source: "华尔街见闻",
      pubDate: formatCnNewsDate(dt),
      link,
    });
    if (items.length >= 25) break;
  }

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
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (path === "/" || path === "/index.html") {
      if (request.method !== "GET" && request.method !== "HEAD") {
        return jsonResponse({ ok: false, detail: "Method Not Allowed" }, 405);
      }
      return new Response(INDEX_HTML, {
        status: 200,
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-store",
        },
      });
    }

    if (path === "/favicon.ico") {
      return new Response(null, { status: 204 });
    }

    if (path.startsWith("/api/")) {
      if (request.method !== "GET" && request.method !== "HEAD") {
        return jsonResponse({ ok: false, detail: "Method Not Allowed" }, 405);
      }

      const force = isForce(url);

      if (path === "/api/health") {
        const [stateMeta, live, news] = await Promise.all([
          env.DESK_STATE.getWithMetadata("state", "json"),
          getLivePayload(env, ctx, force),
          getNewsPayload(env, ctx, force),
        ]);
        const stateObj = stateMeta?.value || null;
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
        const state = await env.DESK_STATE.get("state", "json");
        if (!state) {
          return jsonResponse({ ok: false, error: "Research state not initialized in KV" }, 503);
        }
        if (force) {
          state.serverSyncedAt = formatCnTimestamp(new Date());
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
  },
};
