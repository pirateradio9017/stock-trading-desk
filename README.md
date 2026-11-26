# 美股交易链条观察 · 全市场主线与联动研判 (v2.0)

面向个人日常实盘投研的因果链条观察台，围绕 **“美股交易链条观察 · 全市场主线与联动研判”** 构建 5-Tab 编辑流看板（主线与传导 / 替补与轮动 / 跨资产联动 / 宏观与日程 / 中文快讯）。仅做投研观察、监控与风险提示，只研不交易。

## 架构概览

- `src/config.py`: 27 个核心标的 + APAC/EU/信用宏观基准 + 6x6 相关性矩阵与 30 天剪刀差序列 + L1/L2 一手信源定义 + 美东 (`America/New_York`) 与北京时间 (`Asia/Shanghai`) 交易时段及 NYSE 算法休市日历。
- `src/models/schemas.py`: 严格的 Pydantic v2 API 与状态数据契约（`/api/health`、`/api/live`、`/api/state`、`/api/news`）。
- `src/ingestion/` *(M2)*: 多市场实时与历史行情、L1 SEC EDGAR / 宏观代理信号与去重中文财经快讯。
- `src/research/` *(M3)*: `M1–M5` 主线与 `C1–C6` 替补状态机、美东防抖动锁定、Gemini 因果链条研判与 6 维自检。
- `frontend/index.html`、`src/server.py`、`worker/` *(M4)*: 暖纸张风 5-Tab 单页应用、本地 FastAPI 服务与 Cloudflare Worker + KV 边缘同步。

## 本地初始化与测试 (Windows PowerShell)

```powershell
py -3.12 -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe -m pytest -v
```

## 环境变量

- `GEMINI_API_KEY`: Google Gemini 模型研判密钥（主合成模型，无其他第三方大模型密钥依赖）
- `CF_ACCOUNT_ID`: Cloudflare 账户 ID（Worker 与 KV 部署）
- `CF_API_TOKEN`: Cloudflare API Token（Worker 与 KV 部署）
- `DESK_DATA_DIR`: 本地运行态数据目录（默认 `automation/data`）
- `DESK_PORT`: 本地服务监听端口（默认 `8787`）
- `DESK_LOG_LEVEL`: 日志级别（默认 `INFO`）
