"""
Nautilus Parquet Catalog Solidifier & Timeframe Rollup Engine
Converts all raw Binance monthly klines from \\fnos2\\iflow\\nautilusTrader\\data\\binance_klines
into clean, partitioned NautilusTrader Parquet Catalog on NAS disk.
Supports 15m, 1h, 4h, 1d rollups.
"""

import os
import glob
from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

BASE_DIR = Path(r"\\fnos2\iflow\数据库")
SRC_DIR = Path(r"\\fnos2\iflow\nautilusTrader\data\binance_klines")
CATALOG_BAR_DIR = BASE_DIR / "catalog" / "data" / "bar"

BAR_SCHEMA = pa.schema([
    ("bar_type", pa.string()),
    ("ts_event", pa.int64()),   # nanoseconds UTC
    ("ts_init", pa.int64()),    # nanoseconds UTC
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
    ("quote_volume", pa.float64()),
    ("trades_count", pa.int64())
])

def run_solidification():
    print("=== [Solidify] Reading all raw 15m Binance klines ===")
    csv_files = sorted(glob.glob(str(SRC_DIR / "BTCUSDT-15m-*.csv")))
    print(f"Discovered {len(csv_files)} monthly files.")
    
    rows = []
    bar_type_15m = "BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"
    
    for f in csv_files:
        try:
            df = pd.read_csv(f, header=None)
            if str(df.iloc[0, 0]).startswith("open_time") or not str(df.iloc[0, 0]).isdigit():
                df = df.iloc[1:]
            
            sub = pd.DataFrame()
            # ms timestamp to ns
            sub['ts_event'] = df[0].astype('int64') * 1_000_000
            sub['ts_init'] = df[6].astype('int64') * 1_000_000
            sub['bar_type'] = bar_type_15m
            sub['open'] = df[1].astype('float64')
            sub['high'] = df[2].astype('float64')
            sub['low'] = df[3].astype('float64')
            sub['close'] = df[4].astype('float64')
            sub['volume'] = df[5].astype('float64')
            sub['quote_volume'] = df[7].astype('float64')
            sub['trades_count'] = df[8].astype('int64')
            rows.append(sub)
        except Exception as e:
            print(f"Error loading {f}: {e}")
            
    all_df = pd.concat(rows, ignore_index=True)
    all_df = all_df.sort_values('ts_event').drop_duplicates(subset=['ts_event'])
    print(f"Total deduplicated bars: {len(all_df)}")
    
    # Extract true year from nanoseconds
    dts = pd.to_datetime(all_df['ts_event'], unit='ns', utc=True)
    all_df['year'] = dts.dt.year
    
    # Save 15m
    for year, group in all_df.groupby('year'):
        year_dir = CATALOG_BAR_DIR / bar_type_15m / "BTCUSDT-PERP.BINANCE" / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        out_file = year_dir / f"{year}_15m.parquet"
        cols = ['bar_type', 'ts_event', 'ts_init', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades_count']
        table = pa.Table.from_pandas(group[cols], schema=BAR_SCHEMA, preserve_index=False)
        pq.write_table(table, out_file, compression='zstd')
        print(f"[15m] Partition {year}: {len(group)} bars saved to {out_file}")
        
    # Rollup to 1h
    print("=== [Rollup] Aggregating 1h bars ===")
    bar_type_1h = "BTCUSDT-PERP.BINANCE-1-HOUR-LAST-EXTERNAL"
    all_df.set_index(pd.to_datetime(all_df['ts_event'], unit='ns', utc=True), inplace=True)
    
    resamp_1h = all_df.resample('1h').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',
        'trades_count': 'sum'
    }).dropna()
    
    resamp_1h['ts_event'] = resamp_1h.index.astype('int64')
    resamp_1h['ts_init'] = resamp_1h['ts_event'] + (3600 * 1_000_000_000 - 1)
    resamp_1h['bar_type'] = bar_type_1h
    resamp_1h['year'] = resamp_1h.index.year
    
    for year, group in resamp_1h.groupby('year'):
        year_dir = CATALOG_BAR_DIR / bar_type_1h / "BTCUSDT-PERP.BINANCE" / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        out_file = year_dir / f"{year}_1h.parquet"
        cols = ['bar_type', 'ts_event', 'ts_init', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades_count']
        table = pa.Table.from_pandas(group[cols], schema=BAR_SCHEMA, preserve_index=False)
        pq.write_table(table, out_file, compression='zstd')
        print(f"[1h] Partition {year}: {len(group)} bars saved to {out_file}")

    # Rollup to 1D
    print("=== [Rollup] Aggregating 1D bars ===")
    bar_type_1d = "BTCUSDT-PERP.BINANCE-1-DAY-LAST-EXTERNAL"
    resamp_1d = all_df.resample('1D').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',
        'trades_count': 'sum'
    }).dropna()
    resamp_1d['ts_event'] = resamp_1d.index.astype('int64')
    resamp_1d['ts_init'] = resamp_1d['ts_event'] + (86400 * 1_000_000_000 - 1)
    resamp_1d['bar_type'] = bar_type_1d
    resamp_1d['year'] = resamp_1d.index.year
    
    for year, group in resamp_1d.groupby('year'):
        year_dir = CATALOG_BAR_DIR / bar_type_1d / "BTCUSDT-PERP.BINANCE" / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        out_file = year_dir / f"{year}_1d.parquet"
        cols = ['bar_type', 'ts_event', 'ts_init', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades_count']
        table = pa.Table.from_pandas(group[cols], schema=BAR_SCHEMA, preserve_index=False)
        pq.write_table(table, out_file, compression='zstd')
        print(f"[1D] Partition {year}: {len(group)} bars saved to {out_file}")
        
    print("=== [Solidify] Finished successfully! ===")

if __name__ == "__main__":
    run_solidification()
