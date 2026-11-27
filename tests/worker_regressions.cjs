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
  assert.equal(ctx.classifyNewsTag('智利铜矿工会Copper Workers Federation呼吁恢复铜冶炼'), '能源大宗');
  assert.notEqual(ctx.classifyNewsTag('法国Caisse基础设施部门暂停发债'), 'M1 算力');
  assert.equal(ctx.classifyNewsTag('比特币升破6万美元关口'), '加密联动');
  const nowSec = Math.floor(Date.now() / 1000) - 60;
  ctx.fetch = async () => ({ok:true,json:async()=>({data:{items:[
    ...Array.from({length: 12}, (_, i) => ({id: 100+i, title: `美股英伟达跟踪标题测试${i}`, uri: `https://example.com/r${i}`, display_time: nowSec - i*60})),
    {id: 201, title: '本地公园赏秋风景摄影活动', uri: 'https://example.com/noise1', display_time: nowSec + 10},
    {id: 202, title: '提醒：日内请重点关注（以下均为北京时间）', uri: 'https://example.com/noise2', display_time: nowSec + 20},
  ]}})});
  const filteredNews = await ctx.getNewsPayload({DESK_STATE:{get:async()=>null}},{},true);
  assert.equal(filteredNews.count, 12);
  assert.ok(filteredNews.items.every(item => !item.title.includes('公园赏秋') && !item.title.includes('提醒：日内')));
  console.log('PASS: Worker assertions (early close, freshness, returns, premarket pair, research timestamp, symbol coverage, news provenance & word boundaries)');
})().catch(e=>{console.error(e);process.exitCode=1});
