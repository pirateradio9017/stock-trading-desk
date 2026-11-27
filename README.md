# 美股交易链条观察 · 全市场主线与联动研判 (v2.0)

面向个人日常实盘投研的因果链条观察台，围绕 **“美股交易链条观察 · 全市场主线与联动研判”** 构建 5-Tab 编辑流看板（主线与传导 / 替补与轮动 / 跨资产联动 / 宏观与日程 / 中文快讯）。仅做投研观察、监控与风险提示，只研不交易。

## 架构概览

- `src/config.py`: 29 个核心标的 + APAC/EU/信用宏观基准 + 6x6 相关性矩阵与 30 天剪刀差序列 + L1/L2 一手信源定义 + 美东 (`America/New_York`) 与北京时间 (`Asia/Shanghai`) 交易时段及 NYSE 算法休市日历。
- `src/models/schemas.py`: 严格的 Pydantic v2 API 与状态数据契约（`/api/health`、`/api/live`、`/api/state`、`/api/news`）。
- `src/ingestion/` *(M2)*: 多市场实时与历史行情、L1 SEC EDGAR / 宏观代理信号与去重中文财经快讯。
- `src/research/` *(M3)*: `M1–M5` 主线与 `C1–C6` 替补状态机、美东防抖动锁定、DeepSeek 因果链条研判与 6 维自检。
- `frontend/index.html`、`src/server.py`、`worker/` *(M4)*: 暖纸张风 5-Tab 单页应用、本地 FastAPI 服务与 Cloudflare Worker + KV 边缘同步。

## 本地初始化与测试 (Windows PowerShell)

```powershell
py -3.12 -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe -m pytest -v
```

## 环境变量

- `DEEPSEEK_API_KEY`：DeepSeek 研判密钥。
- `GEMINI_API_KEY`：使用 Gemini 时的研判密钥。
- `CF_ACCOUNT_ID`: Cloudflare 账户 ID（Worker 与 KV 部署）
- `CF_API_TOKEN`: Cloudflare API Token（Worker 与 KV 部署）
- `DESK_DATA_DIR`: 本地运行态数据目录（默认 `automation/data`）
- `DESK_PORT`: 本地服务监听端口（默认 `8787`）
- `DESK_LOG_LEVEL`: 日志级别（默认 `INFO`）


## 2026-11-27 独立审查优化版

本包基于用户提供的源码归档修改。保留现有架构和 API；未部署到线上。

- 修复 CI 未恢复历史状态、自审失败仍落盘、损坏状态被当作冷启动的问题。
- 加入已确认的 2026–2028 NYSE 提前收市日期，并同步研究调整窗口。其他年份及临时休市仍需维护。
- 修复 Worker 20D 样本不足、盘前价与涨幅配对、未来时间戳误判新鲜。
- 移除推算的 CPI/PCE/非农公布日期；日历中的研究待办不是经济事件。
- 改进引用约束、相关矩阵修复、缺失历史处理、锁定期身份匹配与候选晋降级。
- 研究输出新增变化、已采集线索／证据缺口、传导假设、反证、下一验证；比例价位明确为观察阈值。
- 修复前端图表释放、请求超时和过期响应处理；刷新不再改变研究生成时间。

验证：`python -m pytest -q`（44 项）；`node tests/worker_regressions.cjs`（7 项断言）。
浏览器以固定测试数据验证五页签、390px 手机宽度与连续五次刷新；不代表当前真实市场或模型回答。
前端修改后执行 `python scripts/deploy_worker.py --sync-only` 同步 Worker 的 HTML 包装。

边界：来源 URL 匹配不是正文事实核验；宏观指标包含行情代理，不能视为真实经济数据。相关矩阵底层对缺失/不可计算配对仍保留零值兜底，样本与缺失元数据尚待扩展；不应将所有零值理解为统计无相关。KV 多键写入不是原子事务，多发布端需要额外协调。首次 KV key 不存在允许建档，正式迁移应确认历史数据已在正确命名空间。真实 LLM、真实调度和 Cloudflare 发布本次未执行。


## 已上线：DeepSeek 主引擎

`DEEPSEEK_API_KEY` 通过环境变量或 GitHub Actions Secret 配置；不得写入代码。
`DESK_LLM_PROVIDER=deepseek` 指定 DeepSeek；`auto` 表示 DeepSeek 优先、Gemini 备用。
`DEEPSEEK_MODEL` 默认 `deepseek-flash`，单次生成输出最多 12000 tokens，超时 120 秒，不做无限重试。
生产研究在美股盘前、盘中和盘后交易时段每两小时调度，闭市期间每四小时调度。正常收市日和提前收市日分别在美东 16:40 和 13:40 增设盘后结构复核，复核须落在交易日的有效窗口。GitHub Actions 可手动运行，前端行情与快讯可独立刷新。

主线页可按主题和观察类别筛选标的，查看估值、52 周价格回撤和情景价位。远期市盈率取同一标的的历史快照做比较，样本不足时留空。每条主线保留证据来源、传导假设、确认条件和失效条件。近期快讯按来源发布时间筛选，未知时间和明确的旧稿回顾仍可在快讯页查看。
