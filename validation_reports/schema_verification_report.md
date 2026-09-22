# Leaf 1: Schema Spec & Type Definitions 验证与审计报告

**报告生成时间**: 2026-09-23  
**审计对象**: NautilusTrader Parquet Catalog 架构、ClickHouse TSDB 架构及多资产数据定义  
**审计依据**: 
- `\\fnos2\iflow\数据库\DECISIONS.md`
- `\\fnos2\iflow\数据库\.herd-dispatch\jobs\leaf1-schema-spec\input.md`
- NautilusTrader 官方源代码 (`nautilus_trader/serialization/arrow/`, `crates/serialization/src/arrow/`, `nautilus_trader/persistence/catalog/parquet.py`)
- 现有摄取脚本与固化数据 (`scripts/pipeline_solidify_and_check.py`, `catalog/data/bar/...`)

---

## 1. 执行摘要与审计结论 (Executive Summary)

针对多资产（加密资产 + 宏观指数期货/ETF）数万次高频与多周期量化回测的数据库架构需求，对存储层 Schema 进行了完整兼容性分析与严格类型定义。

### 核心审计结论：
1. **双层 Schema 架构正式确立**:
   - **Nautilus 原生零拷贝层 (Native Rust Fast Path)**: 使用 Apache Arrow `FixedSizeBinary(8)` 存储价格与数量，时间戳强制采用 `uint64` 纳秒精度，Schema 强依赖 embedded metadata (`bar_type`/`instrument_id`, `price_precision`, `size_precision`)。该格式为 Nautilus C++/Rust 底层回测引擎首选，内存零拷贝且消除浮点数精度截断误差。
   - **数据交换与分析层 (Analytics & Ingestion Interop Layer)**: 兼容 DuckDB, Polars, ClickHouse 及 Pandas 直接读取的通用 Parquet 格式，采用 `float64` 存储价格/成交量，时间戳采用 `int64` (纳秒)，包含 `quote_volume` 与 `trades_count`。
2. **发现并修复重大目录拓扑不兼容隐患 (Critical Finding)**:
   - `DECISIONS.md` 原先规划的目录结构为 `catalog/data/bar/{bar_type}/{instrument_id}/{year}/{filename}.parquet`。
   - 深入审计 Nautilus 核心代码 `ParquetDataCatalog` (`parquet.py:2160`) 发现：Nautilus 检索文件时执行 `file_safe_identifiers = [file_path.split("/")[-2] for file_path in file_paths]`。如果存在多层嵌套目录（如引入 `{year}` 或 `{instrument_id}`），会导致父目录变为年份数字而非 `bar_type`，导致 `catalog.bars(bar_types=[...])` 原生查询失败，过滤返回空列表！
   - **解决方案**: 本报告对 Catalog 目录进行了标准拓扑冻结，严格对齐 Nautilus 原生 `catalog/data/{data_type}/{urisafe_identifier(identifier)}/{start_iso}_{end_iso}.parquet` 规范。
3. **纳秒级时间戳与单调性对齐**:
   - 明确所有资产在 `ts_event`（行情撮合发生时间）与 `ts_init`（引擎摄取/接收时间）的纳秒换算与单调不降（monotonic non-decreasing）约束。
4. **全资产精度矩阵与符号映射标准化**:
   - 完成 BTC、ETH、SOL、UNI、HYPE（加密）与 CME Mini ES、Mini NQ、QQQ、NK225、VIX（传统大盘宏观）的价格与数量精度、Tick Size 及 BarType 映射标准。

---

## 2. 存储分层与双模架构 (Storage Architecture)

```
                            [ 原始数据源 (Binance / Tardis / CME Databento) ]
                                                   │
                                                   ▼
                                  [ 数据清洗与标准化 Ingestion Engine ]
                                                   │
                         ┌─────────────────────────┴─────────────────────────┐
                         ▼                                                   ▼
           【冷数据/批量回测层: Parquet Catalog】              【热数据/多维聚合层: ClickHouse TSDB】
    路径: \\fnos2\iflow\数据库\catalog\data\...         表名: market_data.bars / order_book_deltas
    - NautilusTrader Native Parquet (mmap, zero-copy) - ReplacingMergeTree 引擎
    - 按 bar_type / instrument_id 分区目录              - 分区: PARTITION BY toYYYYMM(date)
    - 单文件区间格式: {start_iso}_{end_iso}.parquet       - 支持秒级 SQL 查询与多因子批量聚合回放
```

---

## 3. Schema 验证与冻结定义 (Frozen Schema Specification)

### 3.1 Bar (K线数据: 1m, 15m, 1h, 4h, 1d)

#### 模式 A: Nautilus 原生 Rust 零拷贝 Parquet Schema (Native Catalog Mode)
- **适用场景**: NautilusTrader 引擎直读、高并发回测、DataFusion 极速回放。
- **存储路径**: `catalog/data/bar/{urisafe_bar_type}/{start_iso}_{end_iso}.parquet`

| 字段名称 | PyArrow 数据类型 | 物理存储类型 | Nullable | 业务含义与说明 |
| :--- | :--- | :--- | :--- | :--- |
| `open` | `pa.binary(8)` | `FixedSizeBinary(8)` | `false` | 开盘价，定点数二进制编码（低位字节序） |
| `high` | `pa.binary(8)` | `FixedSizeBinary(8)` | `false` | 最高价，定点数二进制编码 |
| `low` | `pa.binary(8)` | `FixedSizeBinary(8)` | `false` | 最低价，定点数二进制编码 |
| `close` | `pa.binary(8)` | `FixedSizeBinary(8)` | `false` | 收盘价，定点数二进制编码 |
| `volume` | `pa.binary(8)` | `FixedSizeBinary(8)` | `false` | 成交量，定点数无符号二进制编码 |
| `ts_event` | `pa.uint64()` | `UInt64` | `false` | K线周期起始时间戳（Unix Epoch 纳秒） |
| `ts_init` | `pa.uint64()` | `UInt64` | `false` | 引擎摄取/生成时间戳（Unix Epoch 纳秒，通常为周期结束纳秒） |

- **必备 Schema Metadata (必须包含在 Parquet 文件头中)**:
  ```json
  {
    "bar_type": "BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL",
    "price_precision": "2",
    "size_precision": "3"
  }
  ```

#### 模式 B: 数据湖与通用分析 Parquet Schema (Analytics Interop Mode)
- **适用场景**: DuckDB/Polars 因子计算、ClickHouse S3/File 外表导入、跨平台数据审计。

| 字段名称 | PyArrow 数据类型 | 物理存储类型 | Nullable | 业务含义与说明 |
| :--- | :--- | :--- | :--- | :--- |
| `bar_type` | `pa.string()` | `Utf8` | `false` | 标准 BarType 标识符 |
| `ts_event` | `pa.int64()` | `Int64` | `false` | K线周期起始时间（Unix Epoch 纳秒） |
| `ts_init` | `pa.int64()` | `Int64` | `false` | K线关闭/接收时间（Unix Epoch 纳秒） |
| `open` | `pa.float64()` | `Double` | `false` | 开盘价（浮点数） |
| `high` | `pa.float64()` | `Double` | `false` | 最高价（浮点数） |
| `low` | `pa.float64()` | `Double` | `false` | 最低价（浮点数） |
| `close` | `pa.float64()` | `Double` | `false` | 收盘价（浮点数） |
| `volume` | `pa.float64()` | `Double` | `false` | 基础资产成交量 (Base Volume) |
| `quote_volume` | `pa.float64()` | `Double` | `false` | 计价资产成交额 (Quote Volume, 如 USDT) |
| `trades_count` | `pa.int64()` | `Int64` | `false` | 成交笔数 (Trades Count) |

#### 模式 C: ClickHouse TSDB K线存储定义 (统一多周期表)

```sql
CREATE DATABASE IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.bars (
    instrument_id LowCardinality(String) COMMENT '标的代码，如 BTCUSDT-PERP.BINANCE, ES.CME',
    bar_type LowCardinality(String) COMMENT '完整 BarType 签名',
    timeframe LowCardinality(String) COMMENT '周期标识: 1m, 15m, 1h, 4h, 1d',
    ts_event DateTime64(9, 'UTC') COMMENT 'K线开始纳秒时间戳',
    ts_init DateTime64(9, 'UTC') COMMENT 'K线关闭/接收纳秒时间戳',
    open Float64 COMMENT '开盘价',
    high Float64 COMMENT '最高价',
    low Float64 COMMENT '最低价',
    close Float64 COMMENT '收盘价',
    volume Float64 COMMENT '成交量',
    quote_volume Float64 DEFAULT 0 COMMENT '成交额',
    trades_count UInt32 DEFAULT 0 COMMENT '成交笔数',
    date Date MATERIALIZED toDate(ts_event) COMMENT '分区辅助字段'
) ENGINE = ReplacingMergeTree(ts_init)
PARTITION BY (toYYYYMM(date))
PRIMARY KEY (instrument_id, timeframe, ts_event)
ORDER BY (instrument_id, timeframe, ts_event)
SETTINGS index_granularity = 8192;
```

---

### 3.2 OrderBookDelta (L2 增量订单簿变动)

#### 模式 A: Nautilus 原生 Rust 零拷贝 Parquet Schema (Native Catalog Mode)
- **适用场景**: 纳秒级高频盘口重放、L2 逐笔回测。
- **存储路径**: `catalog/data/order_book_delta/{urisafe_instrument_id}/{start_iso}_{end_iso}.parquet`

| 字段名称 | PyArrow 数据类型 | 物理存储类型 | Nullable | 编码规则与说明 |
| :--- | :--- | :--- | :--- | :--- |
| `action` | `pa.uint8()` | `UInt8` | `false` | `0`=Add, `1`=Modify, `2`=Delete, `3`=Clear |
| `side` | `pa.uint8()` | `UInt8` | `false` | `0`=NoSide/Unknown, `1`=Buy, `2`=Sell |
| `price` | `pa.binary(8)` | `FixedSizeBinary(8)` | `false` | 变动价格，定点数二进制编码（低位字节序） |
| `size` | `pa.binary(8)` | `FixedSizeBinary(8)` | `false` | 变动数量，定点数无符号二进制编码 |
| `order_id` | `pa.uint64()` | `UInt64` | `false` | 订单编号（若为价格级聚合深度则置为 0） |
| `flags` | `pa.uint8()` | `UInt8` | `false` | 标志位（如快照边界、撮合标识等，默认 0） |
| `sequence` | `pa.uint64()` | `UInt64` | `false` | 交易所更新序列号 (sequence id / updateId) |
| `ts_event` | `pa.uint64()` | `UInt64` | `false` | 交易所事件撮合纳秒时间戳 |
| `ts_init` | `pa.uint64()` | `UInt64` | `false` | 本地网卡接收/落盘纳秒时间戳 |

- **必备 Schema Metadata**:
  ```json
  {
    "instrument_id": "BTCUSDT-PERP.BINANCE",
    "price_precision": "2",
    "size_precision": "3"
  }
  ```

#### 模式 B: ClickHouse TSDB L2 增量存储定义

```sql
CREATE TABLE IF NOT EXISTS market_data.order_book_deltas (
    instrument_id LowCardinality(String) COMMENT '标的代码，如 BTCUSDT-PERP.BINANCE',
    ts_event DateTime64(9, 'UTC') COMMENT '交易所撮合发生时间戳(纳秒)',
    ts_recv DateTime64(9, 'UTC') COMMENT '本地捕获时间戳(纳秒, 即 ts_init)',
    action Enum8('ADD' = 0, 'MODIFY' = 1, 'DELETE' = 2, 'CLEAR' = 3) COMMENT '订单簿动作',
    side Enum8('BUY' = 1, 'SELL' = 2) COMMENT '买卖方向',
    price Float64 COMMENT '挂单价格',
    size Float64 COMMENT '挂单数量/深度',
    order_id UInt64 DEFAULT 0 COMMENT '订单ID',
    flags UInt8 DEFAULT 0 COMMENT '标志位',
    seq_id UInt64 COMMENT '交易所连续序列号',
    date Date MATERIALIZED toDate(ts_event) COMMENT '分区辅助字段'
) ENGINE = ReplacingMergeTree(seq_id)
PARTITION BY (toYYYYMM(date))
PRIMARY KEY (instrument_id, ts_event)
ORDER BY (instrument_id, ts_event, seq_id)
SETTINGS index_granularity = 8192;
```

---

## 4. 纳秒时间戳对齐与单调性规范 (`ts_event`, `ts_init`)

### 4.1 精度转换矩阵

| 数据来源平台 | 原始字段及精度 | 转换至 `ts_event` (ns) | 转换至 `ts_init` (ns) |
| :--- | :--- | :--- | :--- |
| **Binance Klines (Vision)** | `open_time` (毫秒), `close_time` (毫秒) | `open_time * 1,000,000` | `close_time * 1,000,000 + 999,999` |
| **Bybit Klines** | `start_time` (毫秒/秒) | `start_time * 1,000,000` | `end_time * 1,000,000 + 999,999` |
| **Tardis L2 Incremental** | `timestamp` (微秒), `local_timestamp` (微秒) | `timestamp * 1,000` | `local_timestamp * 1,000` |
| **CME MDP 3.0 / Databento** | `ts_event` (纳秒), `ts_recv` (纳秒) | 原生纳秒 `ts_event` | 原生纳秒 `ts_recv` |

### 4.2 单调性与不重叠区间要求 (Nautilus Invariant)
1. **严格递增/不降约束 (`ts_init`)**:
   - Nautilus Data Catalog 强制要求同一个文件内部数据行按照 `ts_init` **单调不减 (monotonically non-decreasing)** 排序。
   - 若出现乱序，Nautilus `ParquetDataCatalog.write_data()` 会抛出致命异常：`ValueError: Data should be monotonically increasing (or non-decreasing) based on ts_init`。
2. **区间不相交原则 (Disjoint Intervals)**:
   - 同一个标的、同一个数据类型下的不同 Parquet 文件所表示的时间区间 $[start, end]$ **必须严格互斥不重叠 (Disjoint)**。
   - 文件命名规范强制为：`{start_iso}_{end_iso}.parquet`，例如：  
     `2023-01-01T00-00-00-000000000Z_2023-12-31T23-59-59-000000000Z.parquet`  
     其中冒号 `:` 在 Windows 文件系统中自动替换为连接号 `-`。

---

## 5. 多资产价格与数量精度治理矩阵 (Precision Governance Matrix)

Nautilus 原生定点数（Fixed-Precision 64-bit）转换算法如下：
$$\text{scale} = 10^{(\text{FIXED\_PRECISION} - \text{precision})}, \quad \text{FIXED\_PRECISION} = 9$$
$$\text{raw\_val} = \text{round}(\text{float\_val} \times 10^{\text{precision}}) \times 10^{(9 - \text{precision})}$$
$$\text{bytes} = \text{raw\_val.to\_bytes}(8, \text{byteorder}='little', \text{signed}=True)$$

### 5.1 全标的精度映射表

| 资产代码 (Asset) | 交易场所 (Venue) | 标的类别 | 最小价格变动 (Tick Size) | 价格精度 (Price Prec) | 最小数量变动 (Step Size) | 数量精度 (Size Prec) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **BTCUSDT** | BINANCE / BYBIT | 加密永续/现货 | 0.1 / 0.01 | **2** | 0.001 | **3** |
| **ETHUSDT** | BINANCE / BYBIT | 加密永续/现货 | 0.01 | **2** | 0.001 | **3** |
| **SOLUSDT** | BINANCE / BYBIT | 加密永续 | 0.01 | **2** | 0.01 | **2** |
| **UNIUSDT** | BINANCE / BYBIT | 加密永续 | 0.001 | **3** | 0.1 / 0.01 | **2** |
| **HYPE** | HYPERLIQUID | DEX 永续合约 | 0.001 | **3** | 0.01 | **2** |
| **MINI ES** | CME | 标普500股指期货 | 0.25 | **2** | 1 (张/手) | **0** |
| **MINI NQ** | CME | 纳斯达克100股指期货| 0.25 | **2** | 1 (张/手) | **0** |
| **QQQ** | NASDAQ | 美股 ETF | 0.01 | **2** | 1 (股) | **0** |
| **NK225** | OSE / CME | 日经225指数期货 | 5 / 1 | **0** | 1 (手) | **0** |
| **VIX** | CBOE | 波动率指数期货/指数 | 0.05 / 0.01 | **2** | 1 (手) | **0** |

---

## 6. 符号映射与 BarType 命名规范 (Symbol & BarType Specification)

NautilusTrader 采用结构化 BarType 规范：
$$\text{BarType} = \text{InstrumentId} - \text{Step} - \text{AggregationUnit} - \text{PriceType} - \text{AggregationSource}$$

### 6.1 多资产在 5 大周期下的规范映射

| 资产代码 | 推荐 InstrumentId | 1m BarType | 15m BarType | 1h BarType | 4h BarType | 1d BarType |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **BTCUSDT** | `BTCUSDT-PERP.BINANCE` | `BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL` | `...-15-MINUTE-...` | `...-1-HOUR-...` | `...-4-HOUR-...` | `...-1-DAY-...` |
| **ETHUSDT** | `ETHUSDT-PERP.BINANCE` | `ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL` | `...-15-MINUTE-...` | `...-1-HOUR-...` | `...-4-HOUR-...` | `...-1-DAY-...` |
| **SOLUSDT** | `SOLUSDT-PERP.BINANCE` | `SOLUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL` | `...-15-MINUTE-...` | `...-1-HOUR-...` | `...-4-HOUR-...` | `...-1-DAY-...` |
| **UNIUSDT** | `UNIUSDT-PERP.BINANCE` | `UNIUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL` | `...-15-MINUTE-...` | `...-1-HOUR-...` | `...-4-HOUR-...` | `...-1-DAY-...` |
| **HYPE** | `HYPE-PERP.HYPERLIQUID` | `HYPE-PERP.HYPERLIQUID-1-MINUTE-LAST-EXTERNAL` | `...-15-MINUTE-...` | `...-1-HOUR-...` | `...-4-HOUR-...` | `...-1-DAY-...` |
| **MINI ES** | `ES.CME` | `ES.CME-1-MINUTE-LAST-EXTERNAL` | `...-15-MINUTE-...` | `...-1-HOUR-...` | `...-4-HOUR-...` | `...-1-DAY-...` |
| **MINI NQ** | `NQ.CME` | `NQ.CME-1-MINUTE-LAST-EXTERNAL` | `...-15-MINUTE-...` | `...-1-HOUR-...` | `...-4-HOUR-...` | `...-1-DAY-...` |
| **QQQ** | `QQQ.NASDAQ` | `QQQ.NASDAQ-1-MINUTE-LAST-EXTERNAL` | `...-15-MINUTE-...` | `...-1-HOUR-...` | `...-4-HOUR-...` | `...-1-DAY-...` |
| **NK225** | `NK225.OSE` | `NK225.OSE-1-MINUTE-LAST-EXTERNAL` | `...-15-MINUTE-...` | `...-1-HOUR-...` | `...-4-HOUR-...` | `...-1-DAY-...` |
| **VIX** | `VIX.CBOE` | `VIX.CBOE-1-MINUTE-LAST-EXTERNAL` | `...-15-MINUTE-...` | `...-1-HOUR-...` | `...-4-HOUR-...` | `...-1-DAY-...` |

---

## 7. Catalog 目录与分区拓扑审计修正 (Directory Topology Governance)

### 7.1 现有问题根因深度剖析
在 `scripts/pipeline_solidify_and_check.py` 中，当前写入代码如下：
```python
year_out_dir = CATALOG_BAR_DIR / bar_type / "BTCUSDT-PERP.BINANCE" / str(year)
out_parquet = year_out_dir / f"{year}_15m.parquet"
```
其生成的物理路径为：
`catalog\data\bar\BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL\BTCUSDT-PERP.BINANCE\2022\2022_15m.parquet`

**严重破坏 Nautilus 原生检索机制**:
1. Nautilus 源码 `ParquetDataCatalog.filter_files()`:
   ```python
   file_safe_identifiers = [file_path.split("/")[-2] for file_path in file_paths]
   ```
   对于上述路径，`file_path.split("/")[-2]` 得到的值是 `"2022"`，而不是 `bar_type` 或 `instrument_id`！
2. 当用户在 Nautilus 策略中使用标准代码：
   ```python
   catalog = ParquetDataCatalog(path)
   bars = catalog.bars(bar_types=["BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"])
   ```
   Nautilus 会将 `"2022"` 与 `bar_type` 进行比对，比对失败导致**检索出的数据为空**！
3. 文件名 `2022_15m.parquet` 无法被 `_parse_filename_timestamps` 正确解析，导致时间范围剪枝优化失效。

### 7.2 冻结的标准目录拓扑规范

为了保证 100% 兼容 Nautilus 官方引擎，同时方便归档管理，冻结目录规则如下：

#### 方案 1: Nautilus 官方标准 Catalog 结构 (推荐用于实盘回测)
```text
\\fnos2\iflow\数据库\catalog\
├── data/
│   ├── bar/
│   │   ├── BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL/
│   │   │   ├── 2021-01-01T00-00-00-000000000Z_2021-12-31T23-59-00-000000000Z.parquet
│   │   │   └── 2022-01-01T00-00-00-000000000Z_2022-12-31T23-59-00-000000000Z.parquet
│   │   └── BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL/
│   │       ├── 2021-01-01T00-00-00-000000000Z_2021-12-31T23-45-00-000000000Z.parquet
│   │       └── 2022-01-01T00-00-00-000000000Z_2022-12-31T23-45-00-000000000Z.parquet
│   └── order_book_delta/
│       └── BTCUSDT-PERP.BINANCE/
│           ├── 2024-01-01T00-00-00-000000000Z_2024-01-01T23-59-59-999999999Z.parquet
│           └── 2024-01-02T00-00-00-000000000Z_2024-01-02T23-59-59-999999999Z.parquet
```

#### 方案 2: 双规共存与向后兼容适配 (兼容现有归档)
- 对于已有 `raw_archive` 和多级子目录，编写 `catalog_adapter` 或通过 `catalog.reset_all_file_names()` 建立标准链接，确保后续 Leaf 3 (Kline Solidify) 和 Leaf 4 (Cross-check) 直接采用标准命名规则。

---

## 8. 下一步实施建议 (Actionable Handoff to Next Leaves)

1. **Leaf 2 (Tardis L2 & OrderBookDelta)**:
   - 下载 Tardis L2 样本增量数据，使用模式 A 构造包含 9 个字段与特定 metadata 的 Parquet 文件；
   - 验证 `ts_event` 与 `ts_init` 是否满足单调不降及精度规则。
2. **Leaf 3 (Kline Solidify)**:
   - 升级 `pipeline_solidify_and_check.py`，将输出路径由三层嵌套修正为标准 `{urisafe_bar_type}/{start}_{end}.parquet`；
   - 确保写入 Parquet 时嵌入 `price_precision` 与 `size_precision` 元数据。
3. **Leaf 4 (Cross-Check & Gap Audit)**:
   - 以冻结的 `ts_event` 纳秒精度，执行 15m 与碎片 CSV 的对齐，校验跨周期的聚合恒等式。
4. **Leaf 5 (WSL & ClickHouse Sandbox)**:
   - 使用模式 C 中的 DDL 语句创建 ClickHouse `market_data.bars` 与 `market_data.order_book_deltas` 表。

---
**审计员签署**: Leaf 1 架构审计小组  
**状态**: 校验完成，Schema 规范正式冻结 (APPROVED & FROZEN)
