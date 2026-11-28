const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const path = require('node:path');

let source = fs.readFileSync(path.join(__dirname, '../worker/src/index.js'), 'utf8');
source = source
  .replace(/^import .*;\s*$/m, 'const INDEX_HTML = "<!doctype html><html><body>test</body></html>"; const SPA_HTML = INDEX_HTML;')
  .replace('export default {', 'globalThis.worker = {');

function createMockCache() {
  const store = new Map();
  return {
    store,
    puts: 0,
    matches: 0,
    async match(req) {
      this.matches++;
      const key = typeof req === 'string' ? req : req.url;
      const saved = store.get(key);
      return saved ? saved.clone() : undefined;
    },
    async put(req, resp) {
      this.puts++;
      const key = typeof req === 'string' ? req : req.url;
      store.set(key, resp.clone());
    },
  };
}

const mockCache = createMockCache();
const ctx = vm.createContext({
  console,
  URL,
  Response,
  Request,
  Headers,
  Date,
  Intl,
  setTimeout,
  clearTimeout,
  AbortController,
  AbortSignal,
  caches: { default: mockCache },
});
vm.runInContext(source, ctx);

function makeLiveSnapshot(ageMs, priceBase = 100) {
  const tsDate = new Date(Date.now() - ageMs);
  const syms = vm.runInContext('LIVE_SYMBOLS', ctx);
  return {
    updatedAt: ctx.formatCnTimestamp(tsDate),
    updatedAtEt: '11-27 10:00 ET',
    marketSession: 'INTRADAY',
    count: syms.length,
    quotes: syms.map(([sym, name], i) => ({
      sym,
      name,
      price: priceBase + i,
      chg: 0.5,
      ahPrice: null,
      ahChg: null,
      r5: 1.2,
      r20: 3.4,
      source: 'yahoo_v8',
      tsUtc: tsDate.toISOString().replace(/\.\d{3}Z$/, 'Z'),
    })),
  };
}

function makeYahooResponse(price = 250) {
  return {
    ok: true,
    json: async () => ({
      chart: {
        result: [{
          meta: {
            regularMarketPrice: price,
            regularMarketChangePercent: 1.25,
            regularMarketTime: Math.floor(Date.now() / 1000) - 5,
          },
          timestamp: [1795528800, 1795615200],
          indicators: { quote: [{ close: [price - 5, price] }] },
        }],
      },
    }),
  };
}

let testsCompleted = false;
process.on('beforeExit', () => {
  if (!testsCompleted) {
    console.error('FAIL: worker_regressions exited before async assertions completed');
    process.exitCode = 1;
  }
});

(async () => {
  // --- Existing baseline assertions ---
  assert.equal(ctx.detectMarketSession(new Date('2026-11-27T18:00:00Z')).marketSession, 'AFTER_HOURS');
  assert.equal(ctx.detectMarketSession(new Date('2026-11-27T22:00:00Z')).marketSession, 'OFF_HOURS');
  assert.equal(ctx.isFreshTimestamp(new Date(Date.now() + 3600000).toISOString(), 60000), false);
  const meta = {
    regularMarketPrice: 110,
    preMarketPrice: 112,
    preMarketChangePercent: 1.82,
    postMarketPrice: 109,
    postMarketChangePercent: -0.91,
  };
  ctx.fetch = async () => ({
    ok: true,
    json: async () => ({
      chart: { result: [{ meta, timestamp: [1795528800, 1795615200], indicators: { quote: [{ close: [100, 110] }] } }] },
    }),
  });
  const quote = await ctx.fetchYahooSymbol('TEST', 'Test', '2026-11-25T13:00:00Z');
  assert.equal(quote.r20, null);
  assert.equal(quote.ahPrice, 112);
  assert.equal(quote.ahChg, 1.82);

  const state = { serverSyncedAt: '2026/11/25 21:00:00 UTC+8', mainlines: [] };
  const env = { DESK_STATE: { get: async () => state } };
  const response = await ctx.worker.fetch(new Request('https://test/api/state?force=1'), env, {});
  assert.equal((await response.json()).serverSyncedAt, state.serverSyncedAt);
  assert.equal(response.headers.get('cache-control'), 'no-store');

  const edgeSymbols = vm.runInContext('LIVE_SYMBOLS.map(([sym]) => sym)', ctx);
  assert.ok(edgeSymbols.includes('TSM') && edgeSymbols.includes('LMT') && edgeSymbols.includes('SPY') && edgeSymbols.includes('XLU'));

  ctx.fetch = async () => ({
    ok: true,
    json: async () => ({
      data: {
        items: [
          { id: 1, title: '美股将迎来财报季', uri: 'https://example.com/undated' },
          { id: 2, title: '联储公布经济数据', uri: 'https://example.com/dated', display_time: 1700000000 },
          { id: 3, title: '美国标普盘中反弹', uri: 'https://example.com/future', display_time: Math.ceil(Date.now() / 1000) + 60 },
          { id: 4, title: '纳指实时震荡', display_time: 1700000000 },
        ],
      },
    }),
  });
  const news = await ctx.getNewsPayload({ DESK_STATE: { get: async () => null } }, {}, true);
  assert.equal(news.count, 3);
  assert.equal(news.items.find((item) => item.event_id === 'wscn-1').pubDate, '发布时间未提供');
  assert.equal(news.items.find((item) => item.event_id === 'wscn-2').pubDate, '2023-11-15 06:13 UTC+8');
  assert.equal(news.items.find((item) => item.event_id === 'wscn-3').pubDate, '发布时间未提供');
  assert.equal(ctx.classifyNewsTag('智利铜矿工会Copper Workers Federation呼吁恢复铜冶炼'), '能源大宗');
  assert.notEqual(ctx.classifyNewsTag('法国Caisse基础设施部门暂停发债'), 'M1 算力');
  assert.equal(ctx.classifyNewsTag('比特币升破6万美元关口'), '加密联动');

  const nowSec = Math.floor(Date.now() / 1000) - 60;
  ctx.fetch = async () => ({
    ok: true,
    json: async () => ({
      data: {
        items: [
          ...Array.from({ length: 12 }, (_, i) => ({
            id: 100 + i,
            title: `美股英伟达跟踪标题测试${i}`,
            uri: `https://example.com/r${i}`,
            display_time: nowSec - i * 60,
          })),
          { id: 201, title: '本地公园赏秋风景摄影活动', uri: 'https://example.com/noise1', display_time: nowSec + 10 },
          { id: 202, title: '提醒：日内请重点关注（以下均为北京时间）', uri: 'https://example.com/noise2', display_time: nowSec + 20 },
        ],
      },
    }),
  });
  const filteredNews = await ctx.getNewsPayload({ DESK_STATE: { get: async () => null } }, {}, true);
  assert.equal(filteredNews.count, 12);
  assert.ok(filteredNews.items.every((item) => !item.title.includes('公园赏秋') && !item.title.includes('提醒：日内')));

  // --- New performance, caching, ETag/304, SWR, concurrency & timeout fallback assertions ---

  // 1. HTML `/` ETag + 304 + HEAD + Cache-Control
  const rootRes = await ctx.worker.fetch(new Request('https://test/'), env, {});
  assert.equal(rootRes.status, 200);
  assert.equal(rootRes.headers.get('cache-control'), 'public, max-age=0, must-revalidate');
  assert.equal(rootRes.headers.get('x-frame-options'), 'SAMEORIGIN');
  const htmlEtag = rootRes.headers.get('etag');
  assert.ok(htmlEtag && htmlEtag.startsWith('W/"html-'));
  const root304 = await ctx.worker.fetch(new Request('https://test/', { headers: { 'If-None-Match': htmlEtag } }), env, {});
  assert.equal(root304.status, 304);
  assert.equal(await root304.text(), '');
  const rootHead = await ctx.worker.fetch(new Request('https://test/', { method: 'HEAD' }), env, {});
  assert.equal(rootHead.status, 200);
  assert.equal(await rootHead.text(), '');

  // 2. `/api/state` edge cache + normalized cache-buster `?t=` + 304 + force bypass + 503 uncached
  ctx.resetWorkerCaches();
  mockCache.store.clear();
  let stateKvReads = 0;
  let kvStateVal = { serverSyncedAt: '2026/11/27 12:00:00 UTC+8', mainlines: [{ id: 'M1' }] };
  const stateEnv = {
    DESK_STATE: {
      get: async (key) => {
        if (key === 'state') {
          stateKvReads++;
          return kvStateVal;
        }
        return null;
      },
    },
  };
  const st1 = await ctx.worker.fetch(new Request('https://test/api/state?t=11111'), stateEnv, {});
  assert.equal(st1.status, 200);
  assert.equal(st1.headers.get('cache-control'), 'no-cache, must-revalidate');
  const stEtag = st1.headers.get('etag');
  assert.ok(stEtag && stEtag.startsWith('W/"state-'));
  assert.equal(stateKvReads, 1);
  assert.ok(mockCache.store.has('https://test/api/state'), 'canonical state key must strip ?t=');

  // Clear isolate RAM cache to verify caches.default EDGE-HIT works cleanly too
  vm.runInContext('ISOLATE_CACHE.clear()', ctx);
  const st2 = await ctx.worker.fetch(new Request('https://test/api/state?t=22222'), stateEnv, {});
  assert.equal(st2.status, 200);
  assert.equal(st2.headers.get('x-desk-cache'), 'EDGE-HIT');
  assert.equal(stateKvReads, 1, 'EDGE-HIT must not re-read KV');

  const st304 = await ctx.worker.fetch(
    new Request('https://test/api/state', { headers: { 'If-None-Match': stEtag } }),
    stateEnv,
    {}
  );
  assert.equal(st304.status, 304);
  assert.equal(await st304.text(), '');

  kvStateVal = { serverSyncedAt: '2026/11/27 13:00:00 UTC+8', mainlines: [{ id: 'M1' }] };
  const stForce = await ctx.worker.fetch(new Request('https://test/api/state?force=1'), stateEnv, {});
  assert.equal(stForce.status, 200);
  assert.equal(stForce.headers.get('cache-control'), 'no-store');
  assert.equal((await stForce.json()).serverSyncedAt, '2026/11/27 13:00:00 UTC+8');
  assert.equal(stateKvReads, 2);

  // Missing state returns 503 no-store and is never cached
  ctx.resetWorkerCaches();
  mockCache.store.clear();
  const st503 = await ctx.worker.fetch(
    new Request('https://test/api/state'),
    { DESK_STATE: { get: async () => null } },
    {}
  );
  assert.equal(st503.status, 503);
  assert.equal(st503.headers.get('cache-control'), 'no-store');
  assert.equal(mockCache.store.has('https://test/api/state'), false);

  // 3. `/api/live` synchronous fresh refresh via 2-batch CNBC (only 4 total subrequests: 2 CNBC + 2 KR Yahoo) + warm MEM-HIT + distinct ?sym= ETag
  ctx.resetWorkerCaches();
  mockCache.store.clear();
  const staleLive = makeLiveSnapshot(240_000, 100); // 4 minutes old (> fresh window)
  const kvStore = new Map([['live', staleLive]]);
  let kvPutCount = 0;
  let totalFetchCount = 0;
  let cnbcCalls = 0;
  let yahooCalls = 0;
  const liveEnv = {
    DESK_STATE: {
      get: async (k) => kvStore.get(k) || null,
      put: async (k, v) => {
        kvPutCount++;
        kvStore.set(k, JSON.parse(v));
      },
    },
  };
  ctx.fetch = async (url, opts) => {
    assert.ok(opts && opts.signal, 'Upstream fetch must pass AbortSignal');
    totalFetchCount++;
    const u = String(url);
    if (u.includes('quote.cnbc.com')) {
      cnbcCalls++;
      const symsParam = decodeURIComponent(new URL(u).searchParams.get('symbols') || '');
      const syms = symsParam.split('|').filter(Boolean);
      return {
        ok: true,
        json: async () => ({
          FormattedQuoteResult: {
            FormattedQuote: syms.map((s) => ({
              symbol: s,
              code: 0,
              curmktstatus: 'REG_MKT',
              last: '555.00',
              change_pct: '+1.25%',
              last_time: '2026-11-27T08:00:00.000-0500',
            })),
          },
        }),
      };
    }
    yahooCalls++;
    await new Promise((r) => setTimeout(r, 10));
    return makeYahooResponse(555);
  };
  const pendingWaitUntil = [];
  const swrCtx = { waitUntil: (p) => pendingWaitUntil.push(p) };
  const freshResp = await ctx.worker.fetch(new Request('https://test/api/live?t=999'), liveEnv, swrCtx);
  assert.equal(freshResp.status, 200);
  assert.equal(freshResp.headers.get('x-desk-cache'), 'ORIGIN-FRESH');
  assert.equal(freshResp.headers.get('cache-control'), 'no-cache, must-revalidate');
  const freshJson = await freshResp.json();
  assert.notEqual(freshJson.updatedAt, staleLive.updatedAt, 'Fast CNBC batch refresh must return brand-new data on the first request (zero STALE lag)');
  assert.equal(freshJson.quotes[0].price, 555);
  await Promise.all(pendingWaitUntil);
  assert.equal(cnbcCalls, 2, 'Live refresh should issue exactly 2 CNBC batch requests');
  assert.equal(yahooCalls, 2, 'Only the 2 KR stocks not on CNBC should fall back to Yahoo');
  assert.equal(totalFetchCount, 4, 'Full 52-symbol refresh must stay at 4 subrequests (well under CF 50 limit)');
  assert.equal(kvPutCount, 1, 'Refresh should persist fresh live quotes to KV');

  // Next hit serves fresh quotes from warm cache with 0 additional fetches
  const warmResp = await ctx.worker.fetch(new Request('https://test/api/live'), liveEnv, swrCtx);
  assert.equal(warmResp.status, 200);
  assert.equal(warmResp.headers.get('x-desk-cache'), 'MEM-HIT');
  const warmJson = await warmResp.json();
  assert.equal(warmJson.quotes[0].price, 555);
  assert.equal(totalFetchCount, 4, 'Warm hit must not invoke upstream fetch');

  // Filtered ?sym=NVDA gets distinct ETag and shares canonical cache
  const fullEtag = warmResp.headers.get('etag');
  const symResp = await ctx.worker.fetch(new Request('https://test/api/live?sym=NVDA'), liveEnv, swrCtx);
  assert.equal(symResp.status, 200);
  assert.notEqual(symResp.headers.get('etag'), fullEtag, 'Filtered ?sym= must have distinct ETag');
  const symJson = await symResp.json();
  assert.equal(symJson.count, 1);
  assert.equal(symJson.quotes[0].sym, 'NVDA');
  assert.equal(totalFetchCount, 4, 'Filtered ?sym= should reuse canonical cache');

  // 4. Bounded concurrency + single-flight deduplication + hard Yahoo fallback cap (< 50 CF subrequests even during total CNBC outage)
  ctx.resetWorkerCaches();
  mockCache.store.clear();
  let active = 0;
  let maxConcurrent = 0;
  let dedupFetches = 0;
  ctx.fetch = async (url) => {
    if (!String(url).includes('yahoo.com')) return { ok: false, status: 503 };
    dedupFetches++;
    active++;
    if (active > maxConcurrent) maxConcurrent = active;
    await new Promise((r) => setTimeout(r, 8));
    active--;
    return makeYahooResponse(321);
  };
  const concurrentResps = await Promise.all([
    ctx.worker.fetch(new Request('https://test/api/live?force=1'), liveEnv, {}),
    ctx.worker.fetch(new Request('https://test/api/live?force=1'), liveEnv, {}),
    ctx.worker.fetch(new Request('https://test/api/live?force=1'), liveEnv, {}),
  ]);
  assert.ok(concurrentResps.every((r) => r.status === 200));
  const configuredConc = vm.runInContext('WORKER_CONFIG.YAHOO_CONCURRENCY', ctx);
  const maxFallbackCap = vm.runInContext('WORKER_CONFIG.MAX_YAHOO_FALLBACK_CALLS', ctx);
  assert.ok(maxConcurrent <= configuredConc, `maxConcurrent ${maxConcurrent} exceeded cap ${configuredConc}`);
  assert.equal(dedupFetches, maxFallbackCap, '3 concurrent refreshes must deduplicate and cap Yahoo fallback safely below CF 50-subrequest limit');

  // 5. Upstream hang/timeout -> tight deadline KV fallback with honest original timestamp (never hangs, never fabricates)
  ctx.resetWorkerCaches();
  mockCache.store.clear();
  const savedTimings = vm.runInContext('({...WORKER_CONFIG})', ctx);
  vm.runInContext(`
    WORKER_CONFIG.YAHOO_TIMEOUT_MS = 40;
    WORKER_CONFIG.WSCN_TIMEOUT_MS = 40;
    WORKER_CONFIG.LIVE_REFRESH_BUDGET_MS = 100;
    WORKER_CONFIG.LIVE_STALE_WAIT_MS = 60;
    WORKER_CONFIG.LIVE_FORCE_WAIT_MS = 60;
    WORKER_CONFIG.NEWS_STALE_WAIT_MS = 60;
    WORKER_CONFIG.NEWS_FORCE_WAIT_MS = 60;
  `, ctx);
  const oldLive = makeLiveSnapshot(2 * 3600_000, 88); // 2 hours old (beyond SWR window)
  const oldNews = {
    updatedAt: ctx.formatCnTimestamp(new Date(Date.now() - 2 * 3600_000)),
    count: 1,
    items: [{ event_id: 'wscn-old', title: '美股旧缓存测试新闻标题', summary: '旧摘要', tag: '美股快讯', source: '华尔街见闻', pubDate: '2026-11-27 10:00 UTC+8', link: 'https://example.com/old' }],
  };
  let fallbackKvPuts = 0;
  const fallbackEnv = {
    DESK_STATE: {
      get: async (k) => (k === 'live' ? oldLive : k === 'news' ? oldNews : k === 'state' ? kvStateVal : null),
      put: async () => { fallbackKvPuts++; },
    },
  };
  let sawAbort = false;
  ctx.fetch = async (url, opts) => new Promise((_, reject) => {
    // Keep a ref'd timer simulating an open network socket so Node does not exit
    // while waiting for the unref'd AbortSignal.timeout() handle to fire.
    const keepAlive = setTimeout(() => reject(new Error('Safety timeout')), 2000);
    if (opts && opts.signal) {
      opts.signal.addEventListener('abort', () => {
        clearTimeout(keepAlive);
        sawAbort = true;
        reject(new Error('Aborted by timeout'));
      });
    }
  });
  const bgTasks = [];
  const fbCtx = { waitUntil: (p) => bgTasks.push(p) };
  const tStart = Date.now();
  const fbLiveResp = await ctx.worker.fetch(new Request('https://test/api/live'), fallbackEnv, fbCtx);
  const fbLiveMs = Date.now() - tStart;
  assert.equal(fbLiveResp.status, 200);
  assert.equal(fbLiveResp.headers.get('x-desk-cache'), 'KV-FALLBACK');
  const fbLiveJson = await fbLiveResp.json();
  assert.equal(fbLiveJson.updatedAt, oldLive.updatedAt, 'Fallback must preserve honest KV updatedAt');
  assert.ok(fbLiveMs < 350, `Hung upstream must fall back quickly, took ${fbLiveMs}ms`);

  const tNewsStart = Date.now();
  const fbNewsResp = await ctx.worker.fetch(new Request('https://test/api/news?force=1'), fallbackEnv, fbCtx);
  const fbNewsMs = Date.now() - tNewsStart;
  assert.equal(fbNewsResp.status, 200);
  assert.equal(fbNewsResp.headers.get('x-desk-cache'), 'KV-FALLBACK');
  const fbNewsJson = await fbNewsResp.json();
  assert.equal(fbNewsJson.updatedAt, oldNews.updatedAt, 'News fallback must preserve honest KV updatedAt');
  assert.ok(fbNewsMs < 350, `Hung WSCN upstream must fall back quickly, took ${fbNewsMs}ms`);

  await Promise.all(bgTasks);
  assert.ok(sawAbort, 'Hung fetch must receive AbortSignal timeout abort');
  assert.equal(fallbackKvPuts, 0, 'Total upstream failure must NOT overwrite KV with fake fresh timestamp');

  // 6. `/api/health` fast non-blocking path (0 upstream fetches when KV already populated)
  ctx.resetWorkerCaches();
  mockCache.store.clear();
  let healthUpstreamCalls = 0;
  ctx.fetch = async () => {
    healthUpstreamCalls++;
    return makeYahooResponse(999);
  };
  const healthResp = await ctx.worker.fetch(new Request('https://test/api/health'), fallbackEnv, {});
  assert.equal(healthResp.status, 200);
  const healthJson = await healthResp.json();
  assert.equal(healthJson.ok, true);
  assert.equal(healthJson.quotesCount, oldLive.count);
  assert.equal(healthJson.newsCount, oldNews.count);
  assert.equal(healthUpstreamCalls, 0, '/api/health must not trigger synchronous upstream fetches when KV exists');

  // 7. `/api/news` SWR background refresh + limit slicing distinct ETag
  Object.assign(vm.runInContext('WORKER_CONFIG', ctx), savedTimings);
  ctx.resetWorkerCaches();
  mockCache.store.clear();
  const swrNewsSeed = {
    updatedAt: ctx.formatCnTimestamp(new Date(Date.now() - 120_000)), // 2m old (> 60s fresh, < 15m SWR)
    count: 2,
    items: [
      { event_id: 'wscn-a', title: '美股旧快讯一测试', summary: '摘要1', tag: '美股快讯', source: '华尔街见闻', pubDate: '2026-11-27 12:00 UTC+8', link: 'https://example.com/a' },
      { event_id: 'wscn-b', title: '美股旧快讯二测试', summary: '摘要2', tag: '美股快讯', source: '华尔街见闻', pubDate: '2026-11-27 11:55 UTC+8', link: 'https://example.com/b' },
    ],
  };
  const newsKv = new Map([['news', swrNewsSeed]]);
  let newsBgPuts = 0;
  const newsEnv = {
    DESK_STATE: {
      get: async (k) => newsKv.get(k) || null,
      put: async (k, v) => {
        newsBgPuts++;
        newsKv.set(k, JSON.parse(v));
      },
    },
  };
  const freshSec = Math.floor(Date.now() / 1000) - 10;
  ctx.fetch = async () => ({
    ok: true,
    json: async () => ({
      data: {
        items: [
          { id: 901, title: '英伟达新一代算力架构发布更新', uri: 'https://example.com/new1', display_time: freshSec },
          { id: 902, title: '美联储官员释放通胀观察信号', uri: 'https://example.com/new2', display_time: freshSec - 30 },
        ],
      },
    }),
  });
  const newsBg = [];
  const swrNewsResp = await ctx.worker.fetch(
    new Request('https://test/api/news'),
    newsEnv,
    { waitUntil: (p) => newsBg.push(p) }
  );
  assert.equal(swrNewsResp.status, 200);
  assert.equal(swrNewsResp.headers.get('x-desk-cache'), 'KV-STALE');
  assert.equal((await swrNewsResp.json()).items[0].event_id, 'wscn-a');
  await Promise.all(newsBg);
  assert.equal(newsBgPuts, 1);

  const freshNewsResp = await ctx.worker.fetch(new Request('https://test/api/news'), newsEnv, {});
  assert.equal(freshNewsResp.headers.get('x-desk-cache'), 'MEM-HIT');
  const fullNewsEtag = freshNewsResp.headers.get('etag');
  assert.equal((await freshNewsResp.json()).items[0].event_id, 'wscn-901');
  const slicedNewsResp = await ctx.worker.fetch(new Request('https://test/api/news?limit=1'), newsEnv, {});
  assert.notEqual(slicedNewsResp.headers.get('etag'), fullNewsEtag, '?limit=1 must have distinct ETag');
  assert.equal((await slicedNewsResp.json()).count, 1);

  // 8. ORIGIN-FRESH ETag parity (must yield 304 on immediate next revalidation against MEM-HIT)
  ctx.resetWorkerCaches();
  mockCache.store.clear();
  const veryOldLive = makeLiveSnapshot(2 * 3600_000, 77);
  const veryOldNews = {
    updatedAt: ctx.formatCnTimestamp(new Date(Date.now() - 2 * 3600_000)),
    count: 1,
    items: [{ event_id: 'wscn-stale', title: '美股隔夜旧快讯测试', summary: '旧摘要', tag: '美股快讯', source: '华尔街见闻', pubDate: '2026-11-27 08:00 UTC+8', link: 'https://example.com/stale' }],
  };
  const originKv = new Map([['live', veryOldLive], ['news', veryOldNews]]);
  const originEnv = {
    DESK_STATE: {
      get: async (k) => originKv.get(k) || null,
      put: async (k, v) => originKv.set(k, JSON.parse(v)),
    },
  };
  ctx.fetch = async (url) => {
    if (String(url).includes('yahoo.com')) return makeYahooResponse(777);
    return {
      ok: true,
      json: async () => ({
        data: {
          items: [
            { id: 951, title: '标普盘中走强测试新闻一', uri: 'https://example.com/o1', display_time: freshSec },
          ],
        },
      }),
    };
  };
  const originLiveRes = await ctx.worker.fetch(new Request('https://test/api/live'), originEnv, {});
  assert.equal(originLiveRes.status, 200);
  assert.equal(originLiveRes.headers.get('x-desk-cache'), 'ORIGIN-FRESH');
  const originLiveEtag = originLiveRes.headers.get('etag');
  assert.ok(originLiveEtag && originLiveEtag.startsWith('W/"live-'), `Expected W/"live-..." ETag, got ${originLiveEtag}`);
  const nextLive304 = await ctx.worker.fetch(
    new Request('https://test/api/live', { headers: { 'If-None-Match': originLiveEtag } }),
    originEnv,
    {}
  );
  assert.equal(nextLive304.status, 304, 'Immediate revalidation after ORIGIN-FRESH live must return 304');
  assert.equal(await nextLive304.text(), '');

  const originNewsRes = await ctx.worker.fetch(new Request('https://test/api/news'), originEnv, {});
  assert.equal(originNewsRes.status, 200);
  assert.equal(originNewsRes.headers.get('x-desk-cache'), 'ORIGIN-FRESH');
  const originNewsEtag = originNewsRes.headers.get('etag');
  assert.ok(originNewsEtag && originNewsEtag.startsWith('W/"news-'), `Expected W/"news-..." ETag, got ${originNewsEtag}`);
  const nextNews304 = await ctx.worker.fetch(
    new Request('https://test/api/news', { headers: { 'If-None-Match': originNewsEtag } }),
    originEnv,
    {}
  );
  assert.equal(nextNews304.status, 304, 'Immediate revalidation after ORIGIN-FRESH news must return 304');
  assert.equal(await nextNews304.text(), '');

  // 9. raceWithDeadline handles late & immediate promise rejections without unhandledRejection
  let unhandledCount = 0;
  const onUnhandled = () => { unhandledCount++; };
  process.on('unhandledRejection', onUnhandled);
  const immediateRej = await ctx.raceWithDeadline(Promise.reject(new Error('boom')), 50, 'fallback-1');
  assert.equal(immediateRej.timedOut, true);
  assert.equal(immediateRej.value, 'fallback-1');
  const lateRej = await ctx.raceWithDeadline(
    new Promise((_, rej) => setTimeout(() => rej(new Error('late-boom')), 30)),
    10,
    'fallback-2'
  );
  assert.equal(lateRej.timedOut, true);
  assert.equal(lateRej.value, 'fallback-2');
  await new Promise((r) => setTimeout(r, 50));
  process.off('unhandledRejection', onUnhandled);
  assert.equal(unhandledCount, 0, 'raceWithDeadline must never emit unhandledRejection');

  // 10. Scheduled maintenance: APAC/EU live coverage, KV warm throttle, research auto-dispatch cadence & cooldown
  assert.ok(
    edgeSymbols.includes('^N225') &&
    edgeSymbols.includes('^HSI') &&
    edgeSymbols.includes('^GDAXI') &&
    edgeSymbols.includes('ES=F') &&
    edgeSymbols.includes('NQ=F') &&
    edgeSymbols.includes('2330.TW') &&
    edgeSymbols.includes('000660.KS') &&
    edgeSymbols.includes('005930.KS') &&
    edgeSymbols.includes('ASML.AS')
  );
  assert.equal(ctx.isBoilerplateNoise('韩国高考作弊查处量激增 涉智能设备案件增至去年3.6倍'), true);
  assert.equal(ctx.isBoilerplateNoise('传小米AI眼镜2代或将于12月中旬发布，客服回应'), true);
  assert.equal(ctx.isBoilerplateNoise('SK海力士大涨6%带动韩国KOSPI创收盘新高'), false);

  // Cross-market APAC close window (2026-11-27 14:40 UTC+8 = 06:40 UTC) dispatches even if state is only 50m old
  const apacCloseNow = new Date('2026-11-27T06:40:00Z');
  const preApacCloseState = { serverSyncedAt: '2026/11/27 13:50:00 UTC+8', history: [] };
  const apacDec = ctx.evaluateResearchDispatch(preApacCloseState, null, apacCloseNow);
  assert.equal(apacDec.shouldDispatch, true);
  assert.equal(apacDec.reason, 'cross_market_apac_close');
  // Once synced inside the APAC close window (14:38 UTC+8), does not re-dispatch
  const doneApacCloseState = { serverSyncedAt: '2026/11/27 14:38:00 UTC+8', history: [] };
  assert.equal(ctx.evaluateResearchDispatch(doneApacCloseState, null, apacCloseNow).shouldDispatch, false);
  // Cross-market EU open window (2026-11-27 16:10 UTC+8 = 08:10 UTC) dispatches when last sync was 14:40 UTC+8
  const euOpenNow = new Date('2026-11-27T08:10:00Z');
  const euDec = ctx.evaluateResearchDispatch({ serverSyncedAt: '2026/11/27 14:40:00 UTC+8', history: [] }, null, euOpenNow);
  assert.equal(euDec.shouldDispatch, true);
  assert.equal(euDec.reason, 'cross_market_eu_open');

  // Active US intraday (2026-11-25 15:00 UTC = 10:00 ET): 25m old state triggers 20m active dispatch
  const activeNow = new Date('2026-11-25T15:00:00Z');
  const staleActiveState = { serverSyncedAt: ctx.formatCnTimestamp(new Date(activeNow.getTime() - 25 * 60_000)), history: [] };
  const freshActiveState = { serverSyncedAt: ctx.formatCnTimestamp(new Date(activeNow.getTime() - 10 * 60_000)), history: [] };
  assert.equal(ctx.evaluateResearchDispatch(staleActiveState, null, activeNow).shouldDispatch, true);
  assert.equal(ctx.evaluateResearchDispatch(staleActiveState, null, activeNow).reason, 'active_20m_due');
  assert.equal(ctx.evaluateResearchDispatch(freshActiveState, null, activeNow).shouldDispatch, false);
  // Weekday off-hours (2026-11-25 04:00 UTC = 23:00 ET prior day, Wed Asia open): 60m triggers hourly dispatch
  const weekdayIdleNow = new Date('2026-11-25T05:00:00Z');
  const staleWeekdayState = { serverSyncedAt: ctx.formatCnTimestamp(new Date(weekdayIdleNow.getTime() - 60 * 60_000)), history: [] };
  assert.equal(ctx.evaluateResearchDispatch(staleWeekdayState, null, weekdayIdleNow).reason, 'weekday_hourly_due');
  // Weekend off-hours (2026-11-28 15:00 UTC = Sat 10:00 ET): 80m stays fresh, 120m triggers 2h dispatch
  const weekendNow = new Date('2026-11-28T15:00:00Z');
  assert.equal(ctx.evaluateResearchDispatch({ serverSyncedAt: ctx.formatCnTimestamp(new Date(weekendNow.getTime() - 80 * 60_000)), history: [] }, null, weekendNow).shouldDispatch, false);
  assert.equal(ctx.evaluateResearchDispatch({ serverSyncedAt: ctx.formatCnTimestamp(new Date(weekendNow.getTime() - 120 * 60_000)), history: [] }, null, weekendNow).reason, 'idle_2h_due');
  // Cooldown (12m) prevents duplicate dispatch
  assert.equal(
    ctx.evaluateResearchDispatch(staleActiveState, { lastDispatchedAt: new Date(activeNow.getTime() - 8 * 60_000).toISOString() }, activeNow).reason,
    'cooldown'
  );
  // Post-close rebalance on early-close day 2026-11-27 13:35 ET (18:35 UTC) triggers rebalance even if state is 25m old
  const earlyCloseRebalNow = new Date('2026-11-27T18:35:00Z');
  const recentPreCloseState = {
    serverSyncedAt: ctx.formatCnTimestamp(new Date(earlyCloseRebalNow.getTime() - 25 * 60_000)),
    history: [{ time: '2026-11-28 01:55 UTC+8', type: '防抖锁定' }],
  };
  const rebalDecision = ctx.evaluateResearchDispatch(recentPreCloseState, null, earlyCloseRebalNow);
  assert.equal(rebalDecision.shouldDispatch, true);
  assert.equal(rebalDecision.reason, 'post_close_rebalance');
  assert.equal(rebalDecision.forceRebalance, false);
  // Already rebalanced after 13:30 ET skips duplicate rebalance
  const doneRebalState = {
    serverSyncedAt: ctx.formatCnTimestamp(new Date(earlyCloseRebalNow.getTime() - 5 * 60_000)),
    history: [{ time: '2026-11-28 02:32 UTC+8', type: '主线复盘' }],
  };
  assert.equal(ctx.evaluateResearchDispatch(doneRebalState, null, earlyCloseRebalNow).shouldDispatch, false);

  // Full worker.scheduled() execution: refreshes stale live/news and dispatches GitHub workflow when token configured
  ctx.resetWorkerCaches();
  mockCache.store.clear();
  const schedKv = new Map([
    ['state', staleActiveState],
    ['live', makeLiveSnapshot(30 * 60_000, 120)],
    ['news', { updatedAt: ctx.formatCnTimestamp(new Date(activeNow.getTime() - 30 * 60_000)), count: 1, items: [{ event_id: 'old', title: '美股旧新闻测试条目', summary: 'old', tag: '美股快讯', source: '华尔街见闻', pubDate: '2026-11-25 22:00 UTC+8', link: 'https://example.com/old' }] }],
  ]);
  const dispatchCalls = [];
  const callOrder = [];
  ctx.fetch = async (url, opts) => {
    const u = String(url);
    if (u.includes('api.github.com')) {
      callOrder.push('github');
      dispatchCalls.push({ url: u, method: opts.method, body: JSON.parse(opts.body) });
      return { ok: true, status: 204 };
    }
    if (u.includes('quote.cnbc.com')) {
      callOrder.push('cnbc');
      const syms = decodeURIComponent(new URL(u).searchParams.get('symbols') || '').split('|').filter(Boolean);
      return {
        ok: true,
        json: async () => ({
          FormattedQuoteResult: {
            FormattedQuote: syms.map((s) => ({ symbol: s, code: 0, curmktstatus: 'REG_MKT', last: '444.00', change_pct: '+0.80%', last_time: '2026-11-25T10:00:00.000-0500' })),
          },
        }),
      };
    }
    if (u.includes('yahoo.com')) {
      callOrder.push('yahoo');
      return makeYahooResponse(444);
    }
    callOrder.push('wscn');
    return {
      ok: true,
      json: async () => ({
        data: { items: [{ id: 888, title: '标普盘中走强测试快讯', uri: 'https://example.com/s1', display_time: Math.floor(Date.now() / 1000) - 30 }] },
      }),
    };
  };
  const schedBg = [];
  const schedEnv = {
    GH_DISPATCH_TOKEN: 'test-token-only',
    DESK_STATE: {
      get: async (k) => schedKv.get(k) || null,
      put: async (k, v) => schedKv.set(k, JSON.parse(v)),
    },
  };
  const schedResult = await ctx.worker.scheduled(
    { scheduledTime: activeNow.getTime(), cron: '*/5 * * * *' },
    schedEnv,
    { waitUntil: (p) => schedBg.push(p) }
  );
  await Promise.all(schedBg);
  assert.equal(callOrder[0], 'github', 'Scheduled maintenance must dispatch GitHub workflow FIRST before quote/news fetches');
  assert.ok(callOrder.length <= 10, `Scheduled maintenance must stay far under CF 50-subrequest limit (got ${callOrder.length})`);
  assert.equal(schedResult.refreshedLive, true);
  assert.equal(schedResult.refreshedNews, true);
  assert.equal(schedResult.dispatch.dispatched, true);
  assert.equal(dispatchCalls.length, 1);
  assert.equal(dispatchCalls[0].body.inputs.deploy_worker, 'false');
  assert.equal(schedKv.get('dispatch_meta').lastStatus, 'dispatched');

  // Failed GitHub dispatch must NOT stamp lastDispatchedAt cooldown, allowing immediate retry on next tick
  const failKv = new Map([['state', staleActiveState]]);
  const failEnv = {
    GH_DISPATCH_TOKEN: 'test-token-only',
    DESK_STATE: {
      get: async (k) => failKv.get(k) || null,
      put: async (k, v) => failKv.set(k, JSON.parse(v)),
    },
  };
  ctx.fetch = async () => ({ ok: false, status: 502 });
  const failRes = await ctx.maybeDispatchResearchWorkflow(failEnv, {}, activeNow);
  assert.equal(failRes.dispatched, false);
  const failMeta = failKv.get('dispatch_meta');
  assert.equal(failMeta.lastDispatchedAt, null, 'Failed dispatch must never poison cooldown with lastDispatchedAt');
  assert.ok(failMeta.lastErrorAt, 'Failed dispatch should record lastErrorAt for observability');
  const nextTickDecision = ctx.evaluateResearchDispatch(staleActiveState, failMeta, new Date(activeNow.getTime() + 5 * 60_000));
  assert.equal(nextTickDecision.shouldDispatch, true, 'Next 5-min cron tick must retry immediately after failed dispatch');

  // Edge Nasdaq premarket parser (EST & EDT) and session_rotation aggregation
  const estParsed = ctx.parseNasdaqEtTimestamp('Nov 27, 2026 6:31 AM ET');
  assert.equal(estParsed.toISOString(), '2026-11-27T11:31:00.000Z');
  const edtParsed = ctx.parseNasdaqEtTimestamp('Jul 15, 2026 6:30 AM ET');
  assert.equal(edtParsed.toISOString(), '2026-07-15T10:30:00.000Z');
  const preNow = new Date('2026-11-27T11:32:00Z');
  const spyPrint = ctx.parseNasdaqPrint('SPY', {
    marketStatus: 'Pre-Market',
    primaryData: { lastSalePrice: '$729.37', percentageChange: '+1.77%', lastTradeTimestamp: 'Nov 27, 2026 6:31 AM ET' },
  }, preNow, 'PRE_MARKET');
  const litePrint = ctx.parseNasdaqPrint('LITE', {
    marketStatus: 'Pre-Market',
    primaryData: { lastSalePrice: '$517.46', percentageChange: '+8.58%', lastTradeTimestamp: 'Nov 27, 2026 6:31 AM ET' },
  }, preNow, 'PRE_MARKET');
  const cohrPrint = ctx.parseNasdaqPrint('COHR', {
    marketStatus: 'Pre-Market',
    primaryData: { lastSalePrice: '$212.69', percentageChange: '+8.41%', lastTradeTimestamp: 'Nov 27, 2026 6:31 AM ET' },
  }, preNow, 'PRE_MARKET');
  assert.equal(litePrint.price, 517.46);
  const edgeRot = ctx.compareEdgeSessionPrints(new Map([['SPY', spyPrint], ['LITE', litePrint], ['COHR', cohrPrint]]), preNow, 'PRE_MARKET');
  assert.equal(edgeRot.themes.length, 1);
  assert.equal(edgeRot.themes[0].name, '光模块与高速互联');
  assert.equal(edgeRot.themes[0].excess, 6.73);
  // Blank percentageChange in Nasdaq payload must return null, never coerce to 0
  assert.equal(ctx.parseNasdaqPrint('ITB', {
    marketStatus: 'Pre-Market',
    primaryData: { lastSalePrice: '$98.34', percentageChange: '', lastTradeTimestamp: 'Nov 27, 2026 6:31 AM ET' },
  }, preNow, 'PRE_MARKET'), null);

  // CNBC batch real-time parser: PRE_MARKET + INTRADAY + stale/date-only rejection
  const cnbcPreSpy = ctx.parseCnbcPrint({
    symbol: 'SPY',
    curmktstatus: 'PRE_MKT',
    ExtendedMktQuote: { type: 'PRE_MKT', last: '728.46', change_pct: '+1.64%', last_time: '2026-11-27T06:31:36.251-0500' },
  }, preNow, 'PRE_MARKET');
  const cnbcPreAsml = ctx.parseCnbcPrint({
    symbol: 'ASML',
    curmktstatus: 'PRE_MKT',
    ExtendedMktQuote: { type: 'PRE_MKT', last: '1,336.73', change_pct: '+2.58%', last_time: '2026-11-27T06:31:30.000-0500' },
  }, preNow, 'PRE_MARKET');
  assert.equal(cnbcPreSpy.price, 728.46);
  assert.equal(cnbcPreSpy.change, 1.64);
  assert.equal(cnbcPreAsml.price, 1336.73);
  assert.equal(ctx.parseCnbcPrint({
    symbol: 'SPY',
    curmktstatus: 'REG_MKT',
    last: '716.70',
    change_pct: '-0.29%',
    last_time: '2026-11-25',
  }, new Date('2026-11-27T15:00:00Z'), 'INTRADAY'), null);
  const cnbcRegSpy = ctx.parseCnbcPrint({
    symbol: 'SPY',
    curmktstatus: 'REG_MKT',
    last: '731.20',
    change_pct: '+2.02%',
    last_time: '2026-11-27T09:59:10.000-0500',
  }, new Date('2026-11-27T15:00:00Z'), 'INTRADAY');
  assert.equal(cnbcRegSpy.price, 731.2);
  assert.equal(cnbcRegSpy.change, 2.02);

  // 11. KV write throttling protects free 1,000 writes/day cap during frequent live refreshes
  ctx.resetWorkerCaches();
  mockCache.store.clear();
  const recentLive = makeLiveSnapshot(45_000, 150); // 45s old: stale for display (>25s), but < 210s KV write throttle
  let throttledKvPuts = 0;
  const throttleEnv = {
    DESK_STATE: {
      get: async (k) => (k === 'live' ? recentLive : null),
      put: async () => { throttledKvPuts++; },
    },
  };
  ctx.fetch = async (url) => {
    const u = String(url);
    if (u.includes('quote.cnbc.com')) {
      const syms = decodeURIComponent(new URL(u).searchParams.get('symbols') || '').split('|').filter(Boolean);
      return {
        ok: true,
        json: async () => ({
          FormattedQuoteResult: {
            FormattedQuote: syms.map((s) => ({ symbol: s, code: 0, curmktstatus: 'REG_MKT', last: '600.00', change_pct: '+0.50%', last_time: '2026-11-27T10:00:00.000-0500' })),
          },
        }),
      };
    }
    return makeYahooResponse(600);
  };
  const throttleBg = [];
  const throttleRes = await ctx.worker.fetch(
    new Request('https://test/api/live?force=1'),
    throttleEnv,
    { waitUntil: (p) => throttleBg.push(p) }
  );
  await Promise.all(throttleBg);
  assert.equal(throttleRes.status, 200);
  assert.equal((await throttleRes.json()).quotes[0].price, 600);
  assert.equal(throttledKvPuts, 0, 'Recent live KV (<210s) must skip redundant KV write while serving fresh RAM quote');

  // 12. VPS origin proxy mode: zero KV reads/writes, transparent header forwarding, and cron short-circuit
  let proxyKvOps = 0;
  let proxySeenTarget = '';
  let proxySeenSecret = '';
  ctx.fetch = async (url, opts) => {
    proxySeenTarget = String(url);
    proxySeenSecret = opts?.headers?.get ? opts.headers.get('X-Desk-Origin-Secret') : '';
    return new Response(JSON.stringify({ ok: true, runtimeMode: 'vps-daemon' }), {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'ETag': 'W/"vps-1"' },
    });
  };
  const vpsEnv = {
    VPS_ORIGIN_URL: 'https://43-110-59-73.sslip.io/',
    VPS_ORIGIN_SECRET: 'top-secret-origin',
    DESK_STATE: {
      get: async () => { proxyKvOps++; return null; },
      put: async () => { proxyKvOps++; },
    },
  };
  const vpsResp = await ctx.worker.fetch(new Request('https://test/api/health?foo=1'), vpsEnv, {});
  assert.equal(vpsResp.status, 200);
  assert.equal(vpsResp.headers.get('x-desk-proxy'), 'vps-origin');
  assert.equal(proxySeenTarget, 'https://43-110-59-73.sslip.io/api/health?foo=1');
  assert.equal(proxySeenSecret, 'top-secret-origin');
  assert.equal((await vpsResp.json()).runtimeMode, 'vps-daemon');
  const vpsSched = await ctx.worker.scheduled({ scheduledTime: Date.now() }, vpsEnv, {});
  assert.equal(vpsSched.skipped, true);
  assert.equal(proxyKvOps, 0, 'VPS origin mode must perform zero KV operations');

  testsCompleted = true;
  console.log('PASS: Worker assertions (all baseline + KV write throttle + zero-KV VPS origin proxy mode)');
})().catch((e) => {
  testsCompleted = true;
  console.error(e);
  process.exitCode = 1;
});
