# 量化高频与多周期回测数据库架构规范 (Decisions & Schema)

## 1. 业务目标与资产清单
- **核心回测框架**: NautilusTrader (Rust Core + Python API)
- **回测规模**: 数万次高频/多因子/统计套利策略高并发回测
- **底层存储**: 
  - 本地 NAS 硬盘 (`\\fnos2\iflow\数据库`，~765GB 空间，WSL2 挂载路径 `/mnt/nas/数据库`)
  - 双层存储架构: 
    1. **冷/批量回测层**: Nautilus 原生 Parquet Catalog (`data/catalog/bar/...`, `data/catalog/order_book_delta/...`)，天然支持零拷贝、多线程 mmap、按品种与日期时间高效分片。
    2. **热/因子探索与多策略并发聚合层**: ClickHouse TSDB（列式压缩、SIMD向量化聚合、时间窗口秒级回放）。
- **标的资产覆盖**:
  - **主流加密**: BTCUSDT, ETHUSDT, SOLUSDT, UNIUSD(T), HYPE (Hyperliquid/DEX)
  - **传统宏观/大盘指数**: MINI ES (CME E-mini S&P 500), MINI NQ (CME E-mini Nasdaq 100), QQQ (Invesco QQQ Trust ETF), 日经225 (NK225/N225), VIX (Cboe 波动率指数)
- **数据粒度**:
  - **OHLCV**: 1m, 15m, 1h, 4h, 1D（时间跨度：2017年至今）
  - **Order Book**: L2 Incremental Book Change / Top-N Snapshots (匹配主流加密期现，重点为 Binance/Bybit/Hyperliquid)

---

## 2. 存储分层与目录拓扑 (NAS 765GB 布局)

```text
\\fnos2\iflow\数据库/
├── catalog/                          # Nautilus Trader ParquetDataCatalog 根目录
│   ├── data/
│   │   ├── bar/                      # OHLCV 柱状数据分区: bar/{bar_type}/{instrument_id}/{year}/
│   │   ├── order_book_delta/         # L2 增量变动分区: order_book_delta/{instrument_id}/{year}/{month}/
│   │   ├── quote_tick/               # BBO / 报价 Tick
│   │   └── trade_tick/               # 逐笔成交
│   └── instruments/                  # Nautilus Instrument 序列化定义 (JSON/MsgPack)
├── clickhouse_data/                  # ClickHouse 数据卷存储路径 (支持 Docker 映射)
├── raw_archive/                      # 原始下载缓存 (CSV.GZ / TAR / ZIP)
├── scripts/                          # 采集、转换、清洗、校验工具链
│   ├── downloaders/                  # 官方历史数据与归一化下载器
│   ├── normalizers/                  # 符号标准化与 Nautilus 适配管道
│   └── validators/                   # 碎片数据库对齐、Gap 检测与校验器
├── validation_reports/               # 治理对齐与审计报告
└── .herd-swarm/                      # Swarm / Loopx 运行状态与决策日志
```

---

## 3. Nautilus 与 ClickHouse Schema 规范

### 3.1 Nautilus Parquet Bar Schema (符合 Nautilus 官方规范)
- `bar_type`: String (如 `BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL`)
- `ts_event`: Int64 (纳秒级时间戳)
- `ts_init`: Int64 (系统摄取时间戳，纳秒)
- `open`: Float64 (或 Int64 Fixed-Precision)
- `high`: Float64
- `low`: Float64
- `close`: Float64
- `volume`: Float64
- 分区格式: 按年/月分块存储为 `.parquet`，使用 Zstandard (zstd) 压缩。

### 3.2 ClickHouse OHLCV 存储定义
```sql
CREATE TABLE IF NOT EXISTS market_data.bars_1m (
    instrument_id LowCardinality(String),
    bar_type LowCardinality(String),
    ts_event DateTime64(9, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    quote_volume Float64 DEFAULT 0,
    trade_count UInt32 DEFAULT 0,
    date Date MATERIALIZED toDate(ts_event)
) ENGINE = ReplacingMergeTree()
PARTITION BY (toYYYYMM(date))
PRIMARY KEY (instrument_id, ts_event)
ORDER BY (instrument_id, ts_event)
SETTINGS index_granularity = 8192;
```

### 3.3 ClickHouse Order Book Delta 存储定义
```sql
CREATE TABLE IF NOT EXISTS market_data.order_book_deltas (
    instrument_id LowCardinality(String),
    ts_event DateTime64(9, 'UTC'),
    ts_recv DateTime64(9, 'UTC'),
    action Enum8('ADD' = 1, 'MODIFY' = 2, 'DELETE' = 3, 'CLEAR' = 4),
    side Enum8('BUY' = 1, 'SELL' = 2),
    price Float64,
    size Float64,
    order_id UInt64 DEFAULT 0,
    seq_id UInt64,
    date Date MATERIALIZED toDate(ts_event)
) ENGINE = ReplacingMergeTree(seq_id)
PARTITION BY (toYYYYMM(date))
PRIMARY KEY (instrument_id, ts_event)
ORDER BY (instrument_id, ts_event, seq_id)
SETTINGS index_granularity = 8192;
```

---

## 4. 碎片数据治理与双向 Cross-Check 机制
1. **参考源**: 现有 `\\fnos2\iflow\nautilusTrader\data\binance_klines` 及 `btc_klines_15m_aligned.csv`。
2. **对齐维度**:
   - 时间连续性：校验每根 K 线是否存在 Timestamp Gap（严格 60s/900s/3600s/14400s/86400s）。
   - 价格合理性：High >= max(Open, Close) 且 Low <= min(Open, Close)，Volume >= 0。
   - 跨周期聚合一致性：15m、1h、4h、1D 必须能与 1m 原始数据完全重聚合对齐（Roll-up validation）。
