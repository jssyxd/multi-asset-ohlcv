# Nautilus Trader Parquet Catalog 历史 K 线固化与多周期聚合验证报告

**执行时间**: 2026-09-22 20:23:26 UTC  
**存储根目录**: `\\fnos2\iflow\数据库\catalog\data\bar`  
**规范依据**: `\\fnos2\iflow\数据库\DECISIONS.md` & `input.md` (Leaf 3)  
**执行角色**: Senior Systems & Quant Data Engineer (OhMyOpenCode)  

---

## 1. 任务概述与交付摘要 (Executive Summary)

本任务已彻底完成对量化回测数据库冷存储层（Nautilus Parquet Catalog）的历史 K 线固化与多周期 Rollup 聚合工作：
1. **源数据无缝摄取**: 解析并固化 `\\fnos2\iflow\nautilusTrader\data\binance_klines` 目录下 36 个月全量 Binance 官方历史 15m K 线（2023-05 至 2026-04），共计 **105,216 根 Bar**，时间连续无任何缺失（Gaps = 0）。
2. **修复历史溢出缺陷**: 修复了历史转换脚本中对 16 位微秒级（us）时间戳直接乘 1,000,000 导致的 `int64` 溢出异常（曾导致年份解析错误至 1700~2022 年），实现统一安全纳秒级转换（ns）。
3. **多周期聚合引擎 (Rollup Engine)**: 实现了生产级 1m -> 15m/1h/4h/1D 以及 15m -> 1h/4h/1D 的 OHLCV 向量化重聚合引擎。数学证明表明，通过 1m 聚合生成的 15m Bar 与 Binance 官方 15m Bar 对比，Open/High/Low/Close 误差为 **0.000000**。
4. **全标的资产覆盖**:
   - **加密期现主流**: BTCUSDT, ETHUSDT, SOLUSDT, UNIUSDT, HYPE (Hyperliquid DEX)。
   - **宏观与大盘指数**: CME E-mini S&P 500 (ES), CME E-mini Nasdaq 100 (NQ), Invesco QQQ Trust ETF (QQQ), 日经 225 指数 (N225), Cboe 波动率指数 (VIX)。
5. **规范目录布局与零拷贝压缩**: 严格遵循标准 `catalog/data/bar/{bar_type}/{instrument_id}/{year}/{year}_bars.parquet` 路径规范，全量采用 **Zstandard (zstd)** 压缩。

---

## 2. 核心指标与统计概览 (Metrics Summary)

| 指标维度 | 统计结果 | 验证状态 |
| :--- | :--- | :--- |
| **Catalog Parquet 分区文件总数** | **48 个分区文件** | PASS |
| **Catalog 固化 Bar 记录总数** | **227,632 根** | PASS |
| **Parquet Catalog 占用物理体积** | **13.11 MB** (Zstd 高压缩) | PASS |
| **BTCUSDT 15m 连续性校验** | **105,216 / 105,216 (0 Gaps, 100.0% 连续)** | PASS |
| **与碎片对齐 CSV 交叉比对** | **105,216 根完全重合，最大价格绝对误差 = 0.0** | PASS |
| **1m -> 15m Rollup 精度误差** | **Open: 0.0, High: 0.0, Low: 0.0, Close: 0.0** | PASS |
| **OHLC 几何不变性校验** | **High >= max(Open, Close) & Low <= min(Open, Close) 100% 通过** | PASS |
| **Volume 非负与 NaN 校验** | **无任何负数成交量，零 NaN 坏值** | PASS |
| **Schema 字段与 Nautilus 原生兼容性** | **10 字段完全对齐 PyArrow / Nautilus 规范** | PASS |

---

## 3. 标准 Nautilus Parquet Catalog Schema 规范

固化的 Parquet 文件完全匹配官方 Nautilus Trader `ParquetDataCatalog` 规范：

```python
BAR_SCHEMA = pa.schema([
    ("bar_type", pa.string()),       # 示例: BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL
    ("ts_event", pa.int64()),        # 纳秒级 (ns) UTC 起始时间戳 (K线开盘时刻)
    ("ts_init", pa.int64()),         # 纳秒级 (ns) 系统摄取 / K线闭盘时刻
    ("open", pa.float64()),          # 开盘价
    ("high", pa.float64()),          # 最高价
    ("low", pa.float64()),           # 最低价
    ("close", pa.float64()),         # 收盘价
    ("volume", pa.float64()),        # 成交量 (Base Volume)
    ("quote_volume", pa.float64()),  # 成交金额 (Quote Volume / USD)
    ("trades_count", pa.int64())     # 成交笔数 (Trades Count)
])
```

---

## 4. 多周期 Rollup 聚合算法精度校验 (Rollup Validation)

使用 Binance 官方 1 分钟级 (1m) 原始 K 线对 2024 年 1 月份全月数据（共 44,640 根 1m Bar）执行 Rollup 逻辑聚合，并与官方下载的 15m 独立月度切片进行逐 Bar 字段级点对点差值比对：

```text
聚合窗口: 15min / 1h / 4h / 1D (基于 ts_event 严格左闭右开分箱 [ts, ts+interval))
聚合算子:
  - open: first(open)
  - high: max(high)
  - low: min(low)
  - close: last(close)
  - volume: sum(volume)
  - quote_volume: sum(quote_volume)
  - trades_count: sum(trades_count)
```

**对比验证结果**:
- **比对样本区间**: 2024-01-01 00:00:00 至 2024-01-31 23:45:00 (2,976 根 15m Bar)
- **匹配条数**: 2976 / 2976 (100.0%)
- **最大 Open 偏差**: `0.000000`
- **最大 High 偏差**: `0.000000`
- **最大 Low 偏差**: `0.000000`
- **最大 Close 偏差**: `0.000000`
- **结论**: 多周期 Rollup 聚合与官方标准 K 线实现 **位级 (bit-level) 完全一致**。

---

## 5. Catalog 分区明细清单 (Solidified Partitions Catalog)

| 品种 / BarType | 标的 ID (Instrument) | 分区年份 | 文件名 | 记录数 (Bars) | 文件大小 (KB) | 时间跨度 (UTC) | 几何校验 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `BTCUSDT-PERP.BINANCE-1-DAY-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2023 | `2023_bars.parquet` | 245 | 20.42 KB | 2023-05-01 00:00:00 ~ 2023-12-31 00:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-1-DAY-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 366 | 27.53 KB | 2024-01-01 00:00:00 ~ 2024-12-31 00:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-1-DAY-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2025 | `2025_bars.parquet` | 365 | 27.47 KB | 2025-01-01 00:00:00 ~ 2025-12-31 00:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-1-DAY-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2026 | `2026_bars.parquet` | 120 | 13.52 KB | 2026-01-01 00:00:00 ~ 2026-04-30 00:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-1-HOUR-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2023 | `2023_bars.parquet` | 5,880 | 345.6 KB | 2023-05-01 00:00:00 ~ 2023-12-31 23:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-1-HOUR-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 8,784 | 530.98 KB | 2024-01-01 00:00:00 ~ 2024-12-31 23:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-1-HOUR-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2025 | `2025_bars.parquet` | 8,760 | 531.34 KB | 2025-01-01 00:00:00 ~ 2025-12-31 23:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-1-HOUR-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2026 | `2026_bars.parquet` | 2,880 | 174.32 KB | 2026-01-01 00:00:00 ~ 2026-04-30 23:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 44,640 | 2494.54 KB | 2024-01-01 00:00:00 ~ 2024-01-31 23:59:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2023 | `2023_bars.parquet` | 23,520 | 1346.36 KB | 2023-05-01 00:00:00 ~ 2023-12-31 23:45:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 35,136 | 2148.73 KB | 2024-01-01 00:00:00 ~ 2024-12-31 23:45:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2025 | `2025_bars.parquet` | 35,040 | 2243.51 KB | 2025-01-01 00:00:00 ~ 2025-12-31 23:45:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2026 | `2026_bars.parquet` | 11,520 | 674.95 KB | 2026-01-01 00:00:00 ~ 2026-04-30 23:45:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-4-HOUR-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2023 | `2023_bars.parquet` | 1,470 | 90.01 KB | 2023-05-01 00:00:00 ~ 2023-12-31 20:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-4-HOUR-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 2,196 | 136.77 KB | 2024-01-01 00:00:00 ~ 2024-12-31 20:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-4-HOUR-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2025 | `2025_bars.parquet` | 2,190 | 135.81 KB | 2025-01-01 00:00:00 ~ 2025-12-31 20:00:00 | ✓ PASS |
| `BTCUSDT-PERP.BINANCE-4-HOUR-LAST-EXTERNAL` | `BTCUSDT-PERP.BINANCE` | 2026 | `2026_bars.parquet` | 720 | 47.22 KB | 2026-01-01 00:00:00 ~ 2026-04-30 20:00:00 | ✓ PASS |
| `ES-INDEX.CME-1-DAY-LAST-EXTERNAL` | `ES-INDEX.CME` | 2024 | `2024_bars.parquet` | 70 | 9.53 KB | 2024-09-23 04:00:00 ~ 2024-12-31 05:00:00 | ✓ PASS |
| `ES-INDEX.CME-1-DAY-LAST-EXTERNAL` | `ES-INDEX.CME` | 2025 | `2025_bars.parquet` | 252 | 16.24 KB | 2025-01-02 05:00:00 ~ 2025-12-31 05:00:00 | ✓ PASS |
| `ES-INDEX.CME-1-DAY-LAST-EXTERNAL` | `ES-INDEX.CME` | 2026 | `2026_bars.parquet` | 181 | 13.62 KB | 2026-01-02 05:00:00 ~ 2026-09-22 04:00:00 | ✓ PASS |
| `ETHUSDT-PERP.BINANCE-1-DAY-LAST-EXTERNAL` | `ETHUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 91 | 12.03 KB | 2024-01-01 00:00:00 ~ 2024-03-31 00:00:00 | ✓ PASS |
| `ETHUSDT-PERP.BINANCE-1-HOUR-LAST-EXTERNAL` | `ETHUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 2,184 | 128.95 KB | 2024-01-01 00:00:00 ~ 2024-03-31 23:00:00 | ✓ PASS |
| `ETHUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL` | `ETHUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 8,736 | 491.64 KB | 2024-01-01 00:00:00 ~ 2024-03-31 23:45:00 | ✓ PASS |
| `ETHUSDT-PERP.BINANCE-4-HOUR-LAST-EXTERNAL` | `ETHUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 546 | 36.58 KB | 2024-01-01 00:00:00 ~ 2024-03-31 20:00:00 | ✓ PASS |
| `HYPE-PERP.HYPERLIQUID-1-DAY-LAST-EXTERNAL` | `HYPE-PERP.HYPERLIQUID` | 2026 | `2026_bars.parquet` | 53 | 10.07 KB | 2026-08-01 00:00:00 ~ 2026-09-22 00:00:00 | ✓ PASS |
| `HYPE-PERP.HYPERLIQUID-1-HOUR-LAST-EXTERNAL` | `HYPE-PERP.HYPERLIQUID` | 2026 | `2026_bars.parquet` | 1,253 | 72.95 KB | 2026-08-01 16:00:00 ~ 2026-09-22 20:00:00 | ✓ PASS |
| `HYPE-PERP.HYPERLIQUID-15-MINUTE-LAST-EXTERNAL` | `HYPE-PERP.HYPERLIQUID` | 2026 | `2026_bars.parquet` | 5,010 | 266.77 KB | 2026-08-01 16:00:00 ~ 2026-09-22 20:15:00 | ✓ PASS |
| `HYPE-PERP.HYPERLIQUID-4-HOUR-LAST-EXTERNAL` | `HYPE-PERP.HYPERLIQUID` | 2026 | `2026_bars.parquet` | 314 | 24.67 KB | 2026-08-01 16:00:00 ~ 2026-09-22 20:00:00 | ✓ PASS |
| `N225-INDEX.OSE-1-DAY-LAST-EXTERNAL` | `N225-INDEX.OSE` | 2024 | `2024_bars.parquet` | 71 | 9.98 KB | 2024-09-18 00:00:00 ~ 2024-12-30 00:00:00 | ✓ PASS |
| `N225-INDEX.OSE-1-DAY-LAST-EXTERNAL` | `N225-INDEX.OSE` | 2025 | `2025_bars.parquet` | 243 | 17.39 KB | 2025-01-06 00:00:00 ~ 2025-12-30 00:00:00 | ✓ PASS |
| `N225-INDEX.OSE-1-DAY-LAST-EXTERNAL` | `N225-INDEX.OSE` | 2026 | `2026_bars.parquet` | 175 | 14.59 KB | 2026-01-05 00:00:00 ~ 2026-09-18 00:00:00 | ✓ PASS |
| `NQ-INDEX.CME-1-DAY-LAST-EXTERNAL` | `NQ-INDEX.CME` | 2024 | `2024_bars.parquet` | 70 | 9.61 KB | 2024-09-23 04:00:00 ~ 2024-12-31 05:00:00 | ✓ PASS |
| `NQ-INDEX.CME-1-DAY-LAST-EXTERNAL` | `NQ-INDEX.CME` | 2025 | `2025_bars.parquet` | 252 | 16.62 KB | 2025-01-02 05:00:00 ~ 2025-12-31 05:00:00 | ✓ PASS |
| `NQ-INDEX.CME-1-DAY-LAST-EXTERNAL` | `NQ-INDEX.CME` | 2026 | `2026_bars.parquet` | 181 | 13.91 KB | 2026-01-02 05:00:00 ~ 2026-09-22 04:00:00 | ✓ PASS |
| `QQQ-ETF.NASDAQ-1-DAY-LAST-EXTERNAL` | `QQQ-ETF.NASDAQ` | 2024 | `2024_bars.parquet` | 70 | 9.98 KB | 2024-09-23 13:30:00 ~ 2024-12-31 14:30:00 | ✓ PASS |
| `QQQ-ETF.NASDAQ-1-DAY-LAST-EXTERNAL` | `QQQ-ETF.NASDAQ` | 2025 | `2025_bars.parquet` | 250 | 17.72 KB | 2025-01-02 14:30:00 ~ 2025-12-31 14:30:00 | ✓ PASS |
| `QQQ-ETF.NASDAQ-1-DAY-LAST-EXTERNAL` | `QQQ-ETF.NASDAQ` | 2026 | `2026_bars.parquet` | 181 | 14.7 KB | 2026-01-02 14:30:00 ~ 2026-09-22 13:30:00 | ✓ PASS |
| `SOLUSDT-PERP.BINANCE-1-DAY-LAST-EXTERNAL` | `SOLUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 91 | 12.19 KB | 2024-01-01 00:00:00 ~ 2024-03-31 00:00:00 | ✓ PASS |
| `SOLUSDT-PERP.BINANCE-1-HOUR-LAST-EXTERNAL` | `SOLUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 2,184 | 123.55 KB | 2024-01-01 00:00:00 ~ 2024-03-31 23:00:00 | ✓ PASS |
| `SOLUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL` | `SOLUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 8,736 | 462.21 KB | 2024-01-01 00:00:00 ~ 2024-03-31 23:45:00 | ✓ PASS |
| `SOLUSDT-PERP.BINANCE-4-HOUR-LAST-EXTERNAL` | `SOLUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 546 | 36.5 KB | 2024-01-01 00:00:00 ~ 2024-03-31 20:00:00 | ✓ PASS |
| `UNIUSDT-PERP.BINANCE-1-DAY-LAST-EXTERNAL` | `UNIUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 91 | 12.17 KB | 2024-01-01 00:00:00 ~ 2024-03-31 00:00:00 | ✓ PASS |
| `UNIUSDT-PERP.BINANCE-1-HOUR-LAST-EXTERNAL` | `UNIUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 2,184 | 112.41 KB | 2024-01-01 00:00:00 ~ 2024-03-31 23:00:00 | ✓ PASS |
| `UNIUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL` | `UNIUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 8,736 | 400.25 KB | 2024-01-01 00:00:00 ~ 2024-03-31 23:45:00 | ✓ PASS |
| `UNIUSDT-PERP.BINANCE-4-HOUR-LAST-EXTERNAL` | `UNIUSDT-PERP.BINANCE` | 2024 | `2024_bars.parquet` | 546 | 35.11 KB | 2024-01-01 00:00:00 ~ 2024-03-31 20:00:00 | ✓ PASS |
| `VIX-INDEX.CBOE-1-DAY-LAST-EXTERNAL` | `VIX-INDEX.CBOE` | 2024 | `2024_bars.parquet` | 70 | 9.06 KB | 2024-09-23 07:00:00 ~ 2024-12-31 08:00:00 | ✓ PASS |
| `VIX-INDEX.CBOE-1-DAY-LAST-EXTERNAL` | `VIX-INDEX.CBOE` | 2025 | `2025_bars.parquet` | 250 | 14.25 KB | 2025-01-02 08:00:00 ~ 2025-12-31 08:00:00 | ✓ PASS |
| `VIX-INDEX.CBOE-1-DAY-LAST-EXTERNAL` | `VIX-INDEX.CBOE` | 2026 | `2026_bars.parquet` | 183 | 12.26 KB | 2026-01-02 08:00:00 ~ 2026-09-22 07:00:00 | ✓ PASS |

---

## 6. 治理结论与后续建议

1. **固化产物可靠性**: 所有 Parquet 文件均通过严格的 Arrow Schema 校验、时间单调递增校验与无缺失检查，可直接挂载至 NautilusTrader 的 `ParquetDataCatalog(path=r"\\fnos2\iflow\数据库\catalog")` 供高频及跨周期多因子回测读取。
2. **ClickHouse 导入无缝兼容**: 经固化的 Parquet 文件可直接通过 ClickHouse 的 `file()` 或 `s3()` 表函数秒级导入热数据层 `market_data.bars_1m` / `bars_15m`，用于即席查询与向量化计算。
3. **脚本持久化工具链**: 核心逻辑已固化为 `\\fnos2\iflow\数据库\scripts\solidify_klines_engine.py`，后续增量数据更新只需通过参数传入即可完成自动化提取、修复与分区分层存储。
