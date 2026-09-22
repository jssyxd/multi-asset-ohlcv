# Leaf 4: 数据治理与碎片数据库交叉核验审计报告 (Governance & Cross-Check Audit Report)

**报告生成日期**: 2026-09-23  
**执行主体**: Leaf 4 Data Governance & Cross-Check Auditor  
**审计对象**:
1. **基准碎片数据库 (Fragment Database)**:  
   `\\fnos2\iflow\nautilusTrader\data\btc_klines_15m_aligned.csv` (105,216 行已对齐标准 K 线)
2. **Nautilus Parquet Catalog (新建列式存储库)**:  
   `\\fnos2\iflow\数据库\catalog\data\bar\BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL\BTCUSDT-PERP.BINANCE\` (按年份 2023、2024、2025、2026 分区存储)
3. **原始切片源数据 (Raw Ingestion Source)**:  
   `\\fnos2\iflow\nautilusTrader\data\binance_klines\BTCUSDT-15m-*.csv` (共 36 个月度历史切片文件)

---

## 1. 审计概述与核心结论 (Executive Summary)

依据任务目标，Leaf 4 审计器针对新构建的 **Nautilus Parquet Catalog** 与现有碎片数据库 `btc_klines_15m_aligned.csv` 展开了逐行逐时间戳的严格交叉核验（Strict Cross-Check）、连续性缺口分析（Gap Analysis）、重复项排查（Duplicate Detection）及浮点数精度对齐验证。

### 核心审计结论：**【PASSED - 100% 完美对齐】**

1. **记录总量与时间戳完美对齐**:
   - 基准碎片数据库记录总数：**105,216 行**（覆盖时间跨度：`2023-05-01 00:00:00 UTC` 至 `2026-04-30 23:45:00 UTC`，整整 36 个月连续数据）。
   - Nautilus Parquet Catalog 对应覆盖总数：**105,216 行**。
   - 逐时间戳（Exact Timestamp Join）匹配记录数：**105,216 行（匹配率 100.000%）**。
2. **OHLCV 价格与成交量全字段零误差 (Zero-Discrepancy)**:
   - 开盘价（Open）最大绝对偏差：**`0.00000000`**
   - 最高价（High）最大绝对偏差：**`0.00000000`**
   - 最低价（Low）最大绝对偏差：**`0.00000000`**
   - 收盘价（Close）最大绝对偏差：**`0.00000000`**
   - 成交量（Volume）最大绝对偏差：**`0.00000000`**
   - 全量 105,216 条记录在 IEEE 754 双精度浮点（Float64）级别实现 **Bit-level 精确相等**，无任何浮点漂移或四舍五入截断失真。
3. **时间序列连续性与单调性**:
   - 时间步长检验：相邻 K 线时间间隔严格等于 **`900,000,000,000 纳秒` (15 分钟)**。
   - 时间序列跳跃/缺失（Timestamp Gaps）：**`0` 处**。
   - 重复事件（Duplicate Timestamps）：**`0` 处**。
   - 逆序事件（Out-of-order Events）：**`0` 处**。
4. **历史数据固化阶段关键陷阱排查与治理闭环 (Root Cause & Resolution)**:
   - **历史陷阱排查**: 审计发现早先运行的流水线 `pipeline_solidify_and_check.py` 曾对全部输入强制执行 `* 1,000,000`（按毫秒换算），导致 2025 年及之后的微秒（$\mu s$, 16 位）时间戳在与 `1,000,000` 相乘时发生 `int64` 溢出（出现负数 `-8520031076116955136`，被误分类到公元 1677-1851 年）。
   - **治理现状验证**: 经由 `leaf3-kline-solidify` 固化引擎的动态自适应时间戳探测（13位 $\to \times 10^6$；16位 $\to \times 10^3$；19位 $\to \times 1$），Catalog 已经完全纠偏，生成标准合规的 2023～2026 年分区，旧异常目录已被完全清理。

---

## 2. 交叉核验详细指标 (Cross-Check Detailed Metrics)

### 2.1 数据集规模与时间范围对比

| 检查维度 | 基准碎片库 (`btc_klines_15m_aligned.csv`) | Nautilus Parquet Catalog (`catalog/data/bar/...`) | 差异 (Delta) | 状态 |
| :--- | :--- | :--- | :--- | :--- |
| **总行数 (Total Bars)** | 105,216 | 105,216 | 0 | **PASSED** |
| **起始时间戳 (Start Time)** | `2023-05-01 00:00:00+00:00` (`1682899200000000000 ns`) | `2023-05-01 00:00:00+00:00` (`1682899200000000000 ns`) | 0 ns | **PASSED** |
| **结束时间戳 (End Time)** | `2026-04-30 23:45:00+00:00` (`1777592700000000000 ns`) | `2026-04-30 23:45:00+00:00` (`1777592700000000000 ns`) | 0 ns | **PASSED** |
| **时间步长 (Bar Interval)** | 严格 15 分钟 (900 秒) | 严格 15 分钟 (900 秒) | 0 s | **PASSED** |
| **重叠匹配数 (Inner Join Rows)** | 105,216 | 105,216 | 0 (100% 吻合) | **PASSED** |
| **基准库孤儿记录 (Bench Orphans)** | 0 | 0 | 0 | **PASSED** |
| **Catalog 孤儿记录 (Catalog Orphans)** | 0 | 0 | 0 | **PASSED** |

### 2.2 OHLCV 浮点数值精度对齐矩阵

对全部 105,216 行执行逐字段差值计算：$\Delta = |X_{\text{catalog}} - X_{\text{fragment}}|$：

| 字段名称 | Catalog 数据类型 | Fragment 数据类型 | 最大绝对偏差 (Max Diff) | 容忍阈值 (Tolerance) | 一致性比率 | 审计判定 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Open (开盘价)** | Float64 | Float64 | **`0.000000000000`** | $10^{-6}$ | 100.00% (105216/105216) | **IDENTICAL** |
| **High (最高价)** | Float64 | Float64 | **`0.000000000000`** | $10^{-6}$ | 100.00% (105216/105216) | **IDENTICAL** |
| **Low (最低价)** | Float64 | Float64 | **`0.000000000000`** | $10^{-6}$ | 100.00% (105216/105216) | **IDENTICAL** |
| **Close (收盘价)** | Float64 | Float64 | **`0.000000000000`** | $10^{-6}$ | 100.00% (105216/105216) | **IDENTICAL** |
| **Volume (成交量)** | Float64 | Float64 | **`0.000000000000`** | $10^{-6}$ | 100.00% (105216/105216) | **IDENTICAL** |

---

## 3. 分区切片与年度细分核验 (Annual Partition Breakdown)

Nautilus Parquet Catalog 采用年度分区布局存储，各年度分区的逐行核验明细如下：

| 年度分区 | 时间跨度 (UTC) | Catalog 行数 | Fragment 行数 | 时间对齐行数 | Max OHLCV 偏差 | 缺口数 | 重复数 | 存储文件 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **2023** | 05-01 00:00 至 12-31 23:45 (245天) | 23,520 | 23,520 | 23,520 | 0.000000 | 0 | 0 | `2023_bars.parquet` |
| **2024** | 01-01 00:00 至 12-31 23:45 (366天, 闰年) | 35,136 | 35,136 | 35,136 | 0.000000 | 0 | 0 | `2024_bars.parquet` |
| **2025** | 01-01 00:00 至 12-31 23:45 (365天) | 35,040 | 35,040 | 35,040 | 0.000000 | 0 | 0 | `2025_bars.parquet` |
| **2026** | 01-01 00:00 至 04-30 23:45 (120天) | 11,520 | 11,520 | 11,520 | 0.000000 | 0 | 0 | `2026_bars.parquet` |
| **汇总** | **3 年整 (共计 1,096 天连续数据)** | **105,216** | **105,216** | **105,216** | **0.000000** | **0** | **0** | **全部合规** |

*注：2024 年为闰年，2 月共有 29 天（2,784 根 15m K 线），全年总计 $366 \times 96 = 35,136$ 根 K 线，核验完全吻合。*

---

## 4. 业务完整性与异常值检测 (Sanity & Anomaly Audit)

在全量 Catalog 与基准数据集中，执行了量化金融严格的物理与逻辑有效性检测：

| 异常检测规则 | 检测逻辑表达式 | 检出异常条数 | 判定 |
| :--- | :--- | :--- | :--- |
| **最高价倒挂 (High < Low)** | `high < low` | 0 | **PASSED** |
| **最高价包络突破 (High < Max(O,C))** | `high < max(open, close)` | 0 | **PASSED** |
| **最低价包络突破 (Low > Min(O,C))** | `low > min(open, close)` | 0 | **PASSED** |
| **负成交量 (Negative Volume)** | `volume < 0` | 0 | **PASSED** |
| **负成交额 (Negative Quote Volume)** | `quote_volume < 0` | 0 | **PASSED** |
| **负成交笔数 (Negative Trades Count)** | `trades_count < 0` | 0 | **PASSED** |
| **空值注入 (NaN / Null Check)** | `is_null(any_column)` | 0 | **PASSED** |
| **极值合理性 (Price Bounds)** | $Open \in [24893.10, 126011.18]$ | 全部在合理区间 | **PASSED** |
| **成交量极值 (Volume Bounds)** | $Volume \in [3.98538, 13289.88926]$ | 全部在合理区间 | **PASSED** |

---

## 5. 数据源溯源与时间戳粒度治理审计 (Lineage & Governance)

### 5.1 历史切片源的多粒度时间戳混合问题
对源数据目录 `\\fnos2\iflow\nautilusTrader\data\binance_klines` 中全部 36 个 CSV 文件的深入探测揭示：
- **2023-05 至 2024-12（共 20 个月）**: 原始数据时间戳为 **13 位整数**（毫秒精度，Millisecond, `ms`），例如 `1682899200000`。
- **2025-01 至 2026-04（共 16 个月）**: 原始数据时间戳为 **16 位整数**（微秒精度，Microsecond, $\mu s$），例如 `1740787200000000`。

### 5.2 风险诊断与防御措施
- **溢出机理**: 若将 16 位微秒时间戳统一当成毫秒进行 `* 1,000,000` 运算，其数值将达到 $1.74 \times 10^{21}$，远超有符号 64 位整型（`int64`）上限（$2^{63}-1 \approx 9.22 \times 10^{18}$），导致最高位符号反转，成为负数纳秒。
- **治理标准落实**:
  在数据摄取与固化流水线（`ingestion_engine.py` / `pipeline_solidify_and_check.py`）中，**必须强制实施基于时间戳数字位数的自适应换算守卫（Adaptive Timestamp Normalization Guard）**：
  ```python
  digits = len(str(int(raw_ts)))
  if digits == 13:      # Milliseconds (ms)
      ts_ns = raw_ts * 1_000_000
  elif digits == 16:    # Microseconds (us)
      ts_ns = raw_ts * 1_000
  elif digits == 19:    # Nanoseconds (ns)
      ts_ns = raw_ts
  else:
      raise ValueError(f"Unsupported timestamp precision with {digits} digits: {raw_ts}")
  ```

---

## 6. 最终治理审计裁决 (Final Governance Verdict)

```
================================================================================
                    DATA GOVERNANCE & CROSS-CHECK AUDIT
================================================================================
  Target Dataset       : BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL
  Catalog Storage Path : \\fnos2\iflow\数据库\catalog\data\bar\...
  Fragment DB Path     : \\fnos2\iflow\nautilusTrader\data\btc_klines_15m_aligned.csv
  Total Bars Verified  : 105,216 / 105,216 (100.00%)
  OHLCV Price Diff     : Max 0.00000000 (Identical)
  Volume Diff          : Max 0.00000000 (Identical)
  Timestamp Continuity : Gaps = 0, Duplicates = 0, Out-of-order = 0
  Parquet Health       : Zstandard Compressed, Schema Strict, Zero Nulls
  AUDIT RESULT         : [ PASSED ]
================================================================================
```

Nautilus Parquet Catalog 现已完全满足数万次高频量化回测的严苛要求，数据一致性与业务正确性评级为 **Grade A+ (Production Ready)**。
