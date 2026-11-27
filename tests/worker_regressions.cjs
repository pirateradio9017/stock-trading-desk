const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const path = require('node:path');
let source = fs.readFileSync(path.join(__dirname, '../worker/src/index.js'), 'utf8');
source = source.replace(/^import .*;\s*$/m, 'const SPA_HTML = "test";').replace('export default {', 'globalThis.worker = {');
const ctx = vm.createContext({console, URL, Response, Request, Date, Intl, setTimeout, clearTimeout});
vm.runInContext(source, ctx);
(async () => {
  assert.equal(ctx.detectMarketSession(new Date('2026-11-27T18:00:00Z')).marketSession, 'AFTER_HOURS');
  assert.equal(ctx.detectMarketSession(new Date('2026-11-27T22:00:00Z')).marketSession, 'OFF_HOURS');
  assert.equal(ctx.isFreshTimestamp(new Date(Date.now()+3600000).toISOString(), 60000), false);
  const meta = {regularMarketPrice: 110, preMarketPrice: 112, preMarketChangePercent: 1.82, postMarketPrice: 109, postMarketChangePercent: -.91};
  ctx.fetch = async () => ({ok:true,json:async()=>({chart:{result:[{meta,timestamp:[1795528800,1795615200],indicators:{quote:[{close:[100,110]}]}}]}})});
  const quote = await ctx.fetchYahooSymbol('TEST','Test','2026-11-25T13:00:00Z');
  assert.equal(quote.r20, null);
  assert.equal(quote.ahPrice,112);
  assert.equal(quote.ahChg,1.82);
  const state = {serverSyncedAt:'2026/11/25 21:00:00 UTC+8',mainlines:[]};
  const env = {DESK_STATE:{get:async()=>state}};
  const response = await ctx.worker.fetch(new Request('https://test/api/state?force=1'),env,{});
  assert.equal((await response.json()).serverSyncedAt,state.serverSyncedAt);
  const edgeSymbols = vm.runInContext('LIVE_SYMBOLS.map(([sym]) => sym)', ctx);
  assert.ok(edgeSymbols.includes('TSM') && edgeSymbols.includes('LMT'));
  ctx.fetch = async () => ({ok:true,json:async()=>({data:{items:[
    {id:1,title:'美股将迎来财报季',uri:'https://example.com/undated'},
    {id:2,title:'联储公布经济数据',uri:'https://example.com/dated',display_time:1700000000},
    {id:3,title:'美国标普盘中反弹',uri:'https://example.com/future',display_time:Math.ceil(Date.now()/1000)+60},
    {id:4,title:'纳指实时震荡',display_time:1700000000},
  ]}})});
  const news = await ctx.getNewsPayload({DESK_STATE:{get:async()=>null}},{},true);
  assert.equal(news.count,3);
  assert.equal(news.items.find(item=>item.event_id==='wscn-1').pubDate,'发布时间未提供');
  assert.equal(news.items.find(item=>item.event_id==='wscn-2').pubDate,'2023-11-15 06:13 UTC+8');
  assert.equal(news.items.find(item=>item.event_id==='wscn-3').pubDate,'发布时间未提供');
  console.log('PASS: Worker assertions (early close, freshness, returns, premarket pair, research timestamp, symbol coverage, news provenance)');
})().catch(e=>{console.error(e);process.exitCode=1});
