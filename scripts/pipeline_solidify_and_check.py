"""
Full Ingestion, Pipeline and Validation Harness
1. Parses existing monthly Binance historical klines from \\fnos2\\iflow\\nautilusTrader\\data\\binance_klines
2. Converts and partitions into Nautilus Parquet Catalog:
   \\fnos2\\iflow\\数据库\\catalog\\data\\bar\\BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL\\
3. Cross-checks row-by-row against \\fnos2\\iflow\\nautilusTrader\\data\\btc_klines_15m_aligned.csv
4. Generates data governance audit report in \\fnos2\\iflow\\数据库\\validation_reports\\audit_report.json
"""

import os
import json
import glob
from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

BASE_DIR = Path(r"\\fnos2\iflow\数据库")
SRC_DIR = Path(r"\\fnos2\iflow\nautilusTrader\data\binance_klines")
BENCH_CSV = Path(r"\\fnos2\iflow\nautilusTrader\data\btc_klines_15m_aligned.csv")
CATALOG_BAR_DIR = BASE_DIR / "catalog" / "data" / "bar"
REPORT_DIR = BASE_DIR / "validation_reports"

BAR_SCHEMA = pa.schema([
    ("bar_type", pa.string()),
    ("ts_event", pa.int64()),
    ("ts_init", pa.int64()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
    ("quote_volume", pa.float64()),
    ("trades_count", pa.int64())
])

def normalize_timestamp_to_ns(ts_series: pd.Series) -> pd.Series:
    first_val = int(ts_series.dropna().iloc[0])
    digits = len(str(first_val))
    if digits >= 18:
        mult = 1
    elif digits >= 15:
        mult = 1_000
    elif digits >= 12:
        mult = 1_000_000
    else:
        mult = 1_000_000_000
    return ts_series.astype("int64") * mult

def run_solidification_and_validation():
    print("=== [1/4] Scanning Source Historical Files ===")
    csv_files = sorted(glob.glob(str(SRC_DIR / "BTCUSDT-15m-*.csv")))
    print(f"Found {len(csv_files)} monthly kline files.")
    
    all_rows = []
    bar_type = "BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"
    
    for f in csv_files:
        try:
            df = pd.read_csv(f, header=None)
            if str(df.iloc[0, 0]).startswith("open_time") or not str(df.iloc[0, 0]).isdigit():
                df = df.iloc[1:]
            
            sub = pd.DataFrame()
            sub['ts_event'] = normalize_timestamp_to_ns(df[0])
            sub['ts_init'] = normalize_timestamp_to_ns(df[6])
            sub['bar_type'] = bar_type
            sub['open'] = df[1].astype('float64')
            sub['high'] = df[2].astype('float64')
            sub['low'] = df[3].astype('float64')
            sub['close'] = df[4].astype('float64')
            sub['volume'] = df[5].astype('float64')
            sub['quote_volume'] = df[7].astype('float64')
            sub['trades_count'] = df[8].astype('int64')
            all_rows.append(sub)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    full_df = pd.concat(all_rows, ignore_index=True)
    full_df = full_df.sort_values('ts_event').drop_duplicates(subset=['ts_event'])
    print(f"Total structured bars parsed: {len(full_df)}")
    
    print("=== [2/4] Solidifying into Nautilus Parquet Catalog ===")
    full_df['year'] = pd.to_datetime(full_df['ts_event'], unit='ns', utc=True).dt.year
    for year, group in full_df.groupby('year'):
        year_out_dir = CATALOG_BAR_DIR / bar_type / "BTCUSDT-PERP.BINANCE" / str(year)
        year_out_dir.mkdir(parents=True, exist_ok=True)
        out_parquet = year_out_dir / f"{year}_bars.parquet"
        
        cols = ['bar_type', 'ts_event', 'ts_init', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades_count']
        sub_group = group[cols]
        table = pa.Table.from_pandas(sub_group, schema=BAR_SCHEMA, preserve_index=False)
        pq.write_table(table, out_parquet, compression='zstd')
        print(f"Solidified {year}: {len(sub_group)} bars -> {out_parquet}")

    print("=== [3/4] Cross-Checking with Fragment Database ===")
    bench_df = pd.read_csv(BENCH_CSV)
    bench_df['dt'] = pd.to_datetime(bench_df['timestamp'], utc=True)
    bench_df['ts_event'] = normalize_timestamp_to_ns(pd.Series(bench_df['dt'].astype('int64')))
    
    # Merge and compare
    merged = pd.merge(full_df, bench_df, on='ts_event', suffixes=('_catalog', '_fragment'))
    print(f"Overlapping matched timestamps count: {len(merged)}")
    
    diff_open = np.abs(merged['open_catalog'] - merged['open_fragment']).max()
    diff_close = np.abs(merged['close_catalog'] - merged['close_fragment']).max()
    diff_high = np.abs(merged['high_catalog'] - merged['high_fragment']).max()
    diff_low = np.abs(merged['low_catalog'] - merged['low_fragment']).max()
    diff_volume = np.abs(merged['volume_catalog'] - merged['volume_fragment']).max()
    
    # Gap analysis in full_df
    diff_ts = np.diff(full_df['ts_event'].values)
    expected_step_ns = 15 * 60 * 1_000_000_000
    gaps = np.where(diff_ts != expected_step_ns)[0]
    
    print(f"Validation Results:")
    print(f"- Max Open diff: {diff_open:.8f}")
    print(f"- Max Close diff: {diff_close:.8f}")
    print(f"- Max High diff: {diff_high:.8f}")
    print(f"- Max Low diff: {diff_low:.8f}")
    print(f"- Max Volume diff: {diff_volume:.8f}")
    print(f"- Total Timestamp Gaps found: {len(gaps)}")
    
    print("=== [4/4] Writing Audit Report ===")
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report = {
        "status": "PASSED" if (diff_open < 1e-4 and diff_close < 1e-4) else "WARNING",
        "dataset": "BTCUSDT-PERP.BINANCE",
        "timeframe": "15m",
        "total_bars_catalog": len(full_df),
        "total_bars_fragment": len(bench_df),
        "matched_bars": len(merged),
        "precision_metrics": {
            "max_open_diff": float(diff_open),
            "max_close_diff": float(diff_close),
            "max_high_diff": float(diff_high),
            "max_low_diff": float(diff_low),
            "max_volume_diff": float(diff_volume)
        },
        "governance": {
            "gaps_count": int(len(gaps)),
            "schema_compliant": True,
            "compression": "zstd",
            "format": "Parquet Catalog (Nautilus Native)"
        }
    }
    with open(REPORT_DIR / "audit_report_15m.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"Report written to {REPORT_DIR / 'audit_report_15m.json'}")

if __name__ == "__main__":
    run_solidification_and_validation()
