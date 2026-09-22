# Nautilus Crypto DataCatalog · 机构级量化数据管线

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)]()
[![NautilusTrader](https://img.shields.io/badge/framework-NautilusTrader-00D26A.svg)]()
[![DuckDB](https://img.shields.io/badge/engine-DuckDB-FFF000.svg?logo=duckdb&logoColor=black)]()
[![Parquet](https://img.shields.io/badge/format-Apache_Parquet_(Zstd)-50C878.svg)]()
[![License](https://img.shields.io/badge/license-MIT-informational.svg)]()

> **English** · Institutional-grade cryptocurrency market data pipeline & `ParquetDataCatalog` engineered for **NautilusTrader** and **DuckDB**. Ingests, normalizes, validates, and queries multi-tier crypto market data (1m Klines with Taker Volume, Tick-level aggTrades, 8-hour Funding Rates, 5-minute Open Interest, and L2 OrderBook Snapshots) with zero-copy analytics and mathematical governance.
>
> **中文** · 专为 **NautilusTrader** 事件驱动回测引擎与 **DuckDB** 向量化分析打造的机构级加密货币高频与微观数据管线。支持将币安（Binance Public Data / Vision S3）及 Bybit 官方全量历史数据（含 Taker Volume 1分钟K线、逐笔成交 aggTrades、8小时资金费率、5分钟全网持仓量 OI 及 L2 订单簿深度）高效标准化并分片入库为 Nautilus 原生 `ParquetDataCatalog`，杜绝整型溢出与前视偏差。

---

## 目录 / Table of Contents

- [1. 为什么比现有开源数据集更优质？(比对矩阵)](#1-为什么比现有开源数据集更优质比对矩阵)
- [2. 解决的关键工程隐患 (2025 时间戳溢出陷阱)](#2-解决的关键工程隐患-2025-时间戳溢出陷阱)
- [3. 架构与目录拓扑 (ParquetCatalog Layout)](#3-架构与目录拓扑-parquetcatalog-layout)
- [4. 五大核心数据治理规则 (Validation Rules)](#4-五大核心数据治理规则-validation-rules)
- [5. 快速上手指南 (Quick Start)](#5-快速上手指南-quick-start)
  - [步骤 1: 零凭证并发下载币安官方数据](#步骤-1-零凭证并发下载币安官方数据)
  - [步骤 2: 归一化为 Nautilus ParquetCatalog](#步骤-2-归一化为-nautilus-parquetcatalog)
  - [步骤 3: 运行 DuckDB 向量化质量审计](#步骤-3-运行-duckdb-向量化质量审计)
  - [步骤 4: DuckDB 极速 ASOF JOIN 与因子聚合](#步骤-4-duckdb-极速-asof-join-与因子聚合)
  - [步骤 5: NautilusTrader 事件驱动回测接入](#步骤-5-nautilustrader-事件驱动回测接入)
- [6. 许可证 (License)](#6-许可证-license)

---

## 1. 为什么比现有开源数据集更优质？(比对矩阵)

在加密货币量化研究中，通用 OHLCV 数据集（如 [`JasonleeQAQ/multi-asset-ohlcv`](https://github.com/JasonleeQAQ/multi-asset-ohlcv)）主要用于低频技术指标测试。但在机构级高频、做市、套利与持仓回测中，裸 OHLCV 会引入严重的**回测欺骗（Backtest Hallucination）**：

| 数据维度 / 属性 | 通用开源基准 (`multi-asset-ohlcv`) | 本数据管线 (`nautilus-crypto-datacatalog`) | 对量化回测的实际决定性影响 |
|---|---|---|---|
| **数据源类型** | 二次转录 Parquet，历史不完整 (MATIC等停更) | **Binance / Bybit 官方 S3 原始归档**，全币种每日/每月持续更新 | 消除存活者偏差，确保数据源权威且永久可追溯 |
| **成交量细分** | 仅单一 `volume`（总成交量） | 标配 `taker_buy_base_volume` 与 `quote_volume` | **支持计算主动买卖差 (CVD)** 与订单流失衡 (OFI)，解锁微观结构 Alpha |
| **微观颗粒度** | 静态 1m~2h 聚合柱状 | 支持 **逐笔聚合交易 (`aggTrades`)** 与 **Tick 级成交** | 支持 NautilusTrader 模拟限价单在盘口队列中的排队位置与真实撮合 |
| **盘口最佳报价** | 无买卖价（No BBO） | 支持 **`bookTicker`（实时买一卖一价量）** | 测算真实买卖点差与微观价格（Micro-price），杜绝无摩擦回测假象 |
| **永续合约资金费率** | **完全缺失 (Fatal)** | 完整收录 **全历史 8h 结算资金费率 (`fundingRate`)** | 牛市多头年化资金成本常达 20%~100%+，漏算费率会导致策略收益严重失真 |
| **未平仓合约量 (OI)** | 无 | 包含 **5 分钟全网持仓合约量与美元市值 (`metrics`)** | 精准捕捉杠杆堆积、逼空行情（Short Squeeze）与暴跌前夕的爆仓预警 |
| **时间戳规范** | Naive `datetime64[ns]`，无显式时区约束 | **严格 UTC 纳秒整型 (`ts_event`, `ts_init`)** | 严格满足 Nautilus Cython/Rust 原生内存零拷贝与因果律 ($ts\_event \le ts\_init$) |
| **分析与查询加速** | 仅按文件读取 | **原生集成 DuckDB 向量化 SQL 引擎** | 支持百亿级数据零拷贝秒级扫描、动态重聚合及原生 `ASOF JOIN` |

---

## 2. 解决的关键工程隐患 (2025 时间戳溢出陷阱)

币安（Binance）官方在 **2025-01-01** 起，将其历史数据的时间戳单位由传统的 **毫秒（13 位数字）** 升级为 **微秒（16 位数字）**。

市面上大量开源代码盲目执行 `ts * 1_000_000` 将时间戳转为纳秒，当 16 位微秒乘以 100 万时，数值达到 $\approx 1.7 \times 10^{22}$，直接突破了 64 位有符号整型上限（$2^{63}-1 \approx 9.22 \times 10^{18}$），发生**整型静默溢出**，时间戳变成负数（如 `-8520031076116955136`），导致生成年份为 1700 年的错误分区！

本仓库内置了工业级动态时间戳精度自适应函数：
```python
def to_nanoseconds(raw_val: int) -> int:
    digits = len(str(abs(raw_val)))
    if digits == 10:   # 秒 (s)
        return raw_val * 1_000_000_000
    elif digits == 13: # 毫秒 (ms) - 2025前币安标准
        return raw_val * 1_000_000
    elif digits == 16: # 微秒 (us) - 2025年后币安新规
        return raw_val * 1_000
    elif digits == 19: # 纳秒 (ns) - Nautilus 原生
        return raw_val
    raise ValueError(f"无法识别的时间戳长度: {digits}")
```

---

## 3. 架构与目录拓扑 (ParquetCatalog Layout)

数据存储完全遵循 NautilusTrader 官方推荐的目录分区规则，采用 **Apache Arrow + Zstandard (Level 7)** 高压缩列式存储：

```text
nautilus-crypto-datacatalog/
├── catalog/                                # NautilusTrader 原生 ParquetDataCatalog 根目录
│   ├── instruments/                        # 标的元数据定义 (Tick size, Lot size, 手续费率等)
│   │   └── BTCUSDT-PERP.BINANCE.json
│   └── data/
│       ├── bar/                            # 柱状数据分区 (按周期 / 标的 / 年份分片)
│       │   └── BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL/
│       │       └── BTCUSDT-PERP.BINANCE/
│       │           ├── 2023/2023_1-minute.parquet
│       │           └── 2024/2024_1-minute.parquet
│       ├── trade_tick/                     # 逐笔成交分区
│       ├── quote_tick/                     # BBO 盘口报价分区
│       └── order_book_delta/               # L2 订单簿深度变动分区
├── scripts/
│   ├── downloaders/                        # 官方源下载器 (S3 / Vision)
│   │   └── binance_public_data.py
│   ├── normalizers/                        # 归一化与 Nautilus Catalog 转换器
│   │   └── binance_to_nautilus.py
│   ├── analytics/                          # DuckDB 向量化分析与 ASOF JOIN
│   │   └── duckdb_analytics.py
│   ├── validators/                         # 5 大数据治理规则验证引擎
│   │   └── nautilus_data_validator.py
│   └── nautilus_examples/                  # NautilusTrader 事件驱动回测示例
│       └── backtest_with_catalog.py
├── validation_reports/                     # 数据质量治理审计报告 (JSON & Markdown)
│   ├── nautilus_data_governance_report.md
│   └── nautilus_data_governance_spec.json
├── pyproject.toml
└── README.md
```

---

## 4. 五大核心数据治理规则 (Validation Rules)

本仓库内置的 `scripts/validators/nautilus_data_validator.py` 实现了 5 维数据治理质量大门：

1. **Rule 1: 时间戳单调性与因果律（Timestamp Integrity）**
   - 检验因果律：$ts\_event \le ts\_init$（禁止未来数据）；
   - 检验严格单调性（分区内无时间倒流）；
   - 纳秒值域范围检查（2010 ~ 2035 年）；零重复时间戳。
2. **Rule 2: OHLCV 物理包络（Envelope Consistency）**
   - $High \ge \max(Open, Close)$ 且 $Low \le \min(Open, Close)$；
   - 价格恒正（Price $> 0$），成交量非负（Volume $\ge 0$）。
3. **Rule 3: 跨周期重聚合不变性（Roll-up Invariance）**
   - 验证 $1\text{m} \to 15\text{m} / 1\text{h}$ 的物理守恒：开盘价守恒、收盘价守恒、最高价上确界、成交量代数和守恒（误差 $\le 10^{-6}$）。
4. **Rule 4: 盘口报价与订单簿倒挂检测（Book Sanity）**
   - 检验买卖价差 $Ask - Bid > 0$（严禁买卖倒挂）；成交量及价格恒正。
5. **Rule 5: 衍生品资金费率与持仓对齐（Derivatives Alignment）**
   - 资金费率时间戳严格对齐 UTC 00:00, 08:00, 16:00；费率绝对值截断在 $[-5\%, +5\%]$ 内。

---

## 5. 快速上手指南 (Quick Start)

### 🚀 极速体验：一键下载预构建的高质量回测数据集 (GitHub Releases)
无需耗时下载数 GB 原始数据，直接从 GitHub Release 获取经过 100% 审计清洗的 Nautilus Parquet Catalog 数据包：

| 数据集压缩包 | 覆盖标的与周期 | 数据行数 / 特性 | 直接下载链接 |
|---|---|---|---|
| **`NautilusTrader_Crypto_Catalog_2024_Q1_Bundle.zip`** | BTC / ETH / SOL (2024 Q1) | **393,120 根 1m K线**，含 Taker 交易量与主动买卖量 | [⬇️ 立即下载 (21.4 MB)](https://github.com/jssyxd/nautilus-crypto-datacatalog/releases/download/v1.0.0/NautilusTrader_Crypto_Catalog_2024_Q1_Bundle.zip) |
| **`BTCUSDT_2024_Q1_1m_nautilus_parquet.zip`** | BTCUSDT 永续 (2024 Q1) | **131,040 根 1m K线**，零缺口，含 CVD 特征 | [⬇️ 下载 BTC 包 (7.4 MB)](https://github.com/jssyxd/nautilus-crypto-datacatalog/releases/download/v1.0.0/BTCUSDT_2024_Q1_1m_nautilus_parquet.zip) |
| **`ETHUSDT_2024_Q1_1m_nautilus_parquet.zip`** | ETHUSDT 永续 (2024 Q1) | **131,040 根 1m K线**，零缺口 | [⬇️ 下载 ETH 包 (7.7 MB)](https://github.com/jssyxd/nautilus-crypto-datacatalog/releases/download/v1.0.0/ETHUSDT_2024_Q1_1m_nautilus_parquet.zip) |
| **`SOLUSDT_2024_Q1_1m_nautilus_parquet.zip`** | SOLUSDT 永续 (2024 Q1) | **131,040 根 1m K线**，零缺口 | [⬇️ 下载 SOL 包 (6.3 MB)](https://github.com/jssyxd/nautilus-crypto-datacatalog/releases/download/v1.0.0/SOLUSDT_2024_Q1_1m_nautilus_parquet.zip) |

你也可以使用内置脚本一键下载并解压到 `./catalog`：
```bash
python scripts/download_release_data.py
```

### 环境安装
```bash
git clone https://github.com/jssyxd/nautilus-crypto-datacatalog.git
cd nautilus-crypto-datacatalog

# 推荐使用 uv 或 pip 安装核心依赖
pip install duckdb pyarrow pandas tqdm
# 可选安装 NautilusTrader
pip install nautilus_trader
```

### 步骤 1: 零凭证并发下载币安官方数据
无需申请 API Key，直接通过公网并发抓取官方历史月度归档：
```bash
# 下载 BTCUSDT 永续合约 2024年 1m K线
python scripts/downloaders/binance_public_data.py \
    --symbol BTCUSDT \
    --market futures/um \
    --type klines \
    --interval 1m \
    --months 2024-01 2024-02 2024-03

# 下载逐笔成交 aggTrades
python scripts/downloaders/binance_public_data.py \
    --symbol BTCUSDT \
    --market futures/um \
    --type aggTrades \
    --months 2024-01

# 下载资金费率历史 fundingRate
python scripts/downloaders/binance_public_data.py \
    --symbol BTCUSDT \
    --market futures/um \
    --type fundingRate \
    --months 2024-01
```

### 步骤 2: 归一化为 Nautilus ParquetCatalog
```bash
python scripts/normalizers/binance_to_nautilus.py \
    --input-dir ./raw_archive/BTCUSDT/klines \
    --catalog-dir ./catalog \
    --symbol BTCUSDT \
    --interval 1-MINUTE
```

### 步骤 3: 运行 DuckDB 向量化质量审计
在数据进入回测前，执行全库自动化审计：
```bash
python scripts/validators/nautilus_data_validator.py \
    --catalog-glob "./catalog/data/bar/*/*/*/*.parquet" \
    --report-dir "./validation_reports"
```
审计结果会自动生成在 `validation_reports/nautilus_data_governance_report.md`。

### 步骤 4: DuckDB 极速 ASOF JOIN 与因子聚合
利用 DuckDB 在内存中执行零拷贝分析，计算主动买卖 CVD 与无前视偏差费率对齐：
```python
from scripts.analytics.duckdb_analytics import NautilusDuckDBAnalytics

analytics = NautilusDuckDBAnalytics()

# 1. 动态生成 15分钟 K线与订单流失衡 (CVD)
df_15m = analytics.dynamic_resample_rollup(
    parquet_glob="./catalog/data/bar/*/*/*/*.parquet",
    timeframe_minutes=15
)
print(df_15m[["bucket_dt", "close", "volume", "cumulative_volume_delta"]].head())

# 2. 原生 ASOF JOIN 完美匹配每根 K 线的最近资金费率与持仓量
df_aligned = analytics.asof_join_funding_and_oi(
    bars_glob="./catalog/data/bar/*/*/*/*.parquet",
    funding_glob="./catalog/data/funding_rate/*/*.parquet",
    oi_glob="./catalog/data/open_interest/*/*.parquet"
)
```

### 步骤 5: NautilusTrader 事件驱动回测接入
在 NautilusTrader 中直接指定 ParquetCatalog 路径进行回放：
```python
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.data import BarType, BarSpecification
from nautilus_trader.model.enums import BarAggregation, PriceType, AggregationSource

# 实例化 Catalog
catalog = ParquetDataCatalog("./catalog")

# 定义合约与 BarType
instrument_id = InstrumentId(Symbol("BTCUSDT-PERP"), Venue("BINANCE"))
bar_type = BarType(
    instrument_id=instrument_id,
    bar_spec=BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
    aggregation_source=AggregationSource.EXTERNAL,
)

# 零拷贝读取全部历史 Bar
bars = catalog.bars([bar_type])
print(f"Loaded {len(bars)} bars into NautilusTrader engine.")
```

---

## 6. 许可证 (License)

本项目采用 [MIT License](LICENSE) 开源许可证。
欢迎提交 Pull Request、扩展更多交易所数据适配器与微观因子特征！
