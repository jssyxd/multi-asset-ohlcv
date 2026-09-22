"""
Robust Nautilus Parquet Catalog Solidifier & Rollup Engine
Handles dynamic timestamp units:
- 13-digit milliseconds (<= 2024): multiply by 1,000,000 -> ns
- 16-digit microseconds (>= 2025): multiply by 1,000 -> ns
- 10-digit seconds: multiply by 1,000,000,000 -> ns
- 19-digit nanoseconds: direct
Partitions properly into:
\\fnos2\\iflow\\数据库\\catalog\\data\\bar\\{bar_type}\\{instrument_id}\\{year}\\{year}_{timeframe}.parquet
"""

import os
import glob
import shutil
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

def to_nanoseconds(series: pd.Series) -> pd.Series:
    sample = series.iloc[0]
    num_digits = len(str(int(sample)))
    if num_digits == 10:       # seconds
        return series.astype('int64') * 1_000_000_000
    elif num_digits == 13:     # milliseconds
        return series.astype('int64') * 1_000_000
    elif num_digits == 16:     # microseconds
        return series.astype('int64') * 1_000
    elif num_digits == 19:     # nanoseconds
        return series.astype('int64')
    else:
        raise ValueError(f"Unknown timestamp digit length: {num_digits} for sample {sample}")

def run():
    print("=== [Pruning corrupted partition folders] ===")
    root_p = CATALOG_BAR_DIR / "BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL" / "BTCUSDT-PERP.BINANCE"
    if root_p.exists():
        for d in root_p.iterdir():
            if d.is_dir() and len(d.name) == 4 and int(d.name) < 2000:
                shutil.rmtree(d, ignore_errors=True)
                
    csv_files = sorted(glob.glob(str(SRC_DIR / "BTCUSDT-15m-*.csv")))
    print(f"Reading {len(csv_files)} files...")
    
    bar_type_15m = "BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"
    rows = []
    
    for f in csv_files:
        try:
            df = pd.read_csv(f, header=None)
            if str(df.iloc[0, 0]).startswith("open_time") or not str(df.iloc[0, 0]).isdigit():
                df = df.iloc[1:]
                
            sub = pd.DataFrame()
            sub['ts_event'] = to_nanoseconds(df[0])
            sub['ts_init'] = to_nanoseconds(df[6])
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
            print(f"Failed parsing {f}: {e}")
            
    full_df = pd.concat(rows, ignore_index=True)
    full_df = full_df.sort_values('ts_event').drop_duplicates(subset=['ts_event'])
    print(f"Total bars parsed: {len(full_df)}")
    
    dt = pd.to_datetime(full_df['ts_event'], unit='ns', utc=True)
    full_df['year'] = dt.dt.year
    print(f"Discovered actual years: {sorted(full_df['year'].unique())}")
    
    # Write 15m
    for year, group in full_df.groupby('year'):
        year_dir = CATALOG_BAR_DIR / bar_type_15m / "BTCUSDT-PERP.BINANCE" / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        out_file = year_dir / f"{year}_15m.parquet"
        cols = ['bar_type', 'ts_event', 'ts_init', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades_count']
        t = pa.Table.from_pandas(group[cols], schema=BAR_SCHEMA, preserve_index=False)
        pq.write_table(t, out_file, compression='zstd')
        print(f"[15m] Solidified Year {year}: {len(group)} bars -> {out_file}")
        
    # Write 1h Rollup
    bar_type_1h = "BTCUSDT-PERP.BINANCE-1-HOUR-LAST-EXTERNAL"
    full_df.set_index(pd.to_datetime(full_df['ts_event'], unit='ns', utc=True), inplace=True)
    res_1h = full_df.resample('1h').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'volume': 'sum', 'quote_volume': 'sum', 'trades_count': 'sum'
    }).dropna()
    res_1h['ts_event'] = res_1h.index.astype('int64')
    res_1h['ts_init'] = res_1h['ts_event'] + (3600 * 1_000_000_000 - 1)
    res_1h['bar_type'] = bar_type_1h
    res_1h['year'] = res_1h.index.year
    for year, group in res_1h.groupby('year'):
        year_dir = CATALOG_BAR_DIR / bar_type_1h / "BTCUSDT-PERP.BINANCE" / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        out_file = year_dir / f"{year}_1h.parquet"
        cols = ['bar_type', 'ts_event', 'ts_init', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades_count']
        t = pa.Table.from_pandas(group[cols], schema=BAR_SCHEMA, preserve_index=False)
        pq.write_table(t, out_file, compression='zstd')
        print(f"[1h] Solidified Year {year}: {len(group)} bars -> {out_file}")

    # Write 1D Rollup
    bar_type_1d = "BTCUSDT-PERP.BINANCE-1-DAY-LAST-EXTERNAL"
    res_1d = full_df.resample('1D').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'volume': 'sum', 'quote_volume': 'sum', 'trades_count': 'sum'
    }).dropna()
    res_1d['ts_event'] = res_1d.index.astype('int64')
    res_1d['ts_init'] = res_1d['ts_event'] + (86400 * 1_000_000_000 - 1)
    res_1d['bar_type'] = bar_type_1d
    res_1d['year'] = res_1d.index.year
    for year, group in res_1d.groupby('year'):
        year_dir = CATALOG_BAR_DIR / bar_type_1d / "BTCUSDT-PERP.BINANCE" / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        out_file = year_dir / f"{year}_1d.parquet"
        cols = ['bar_type', 'ts_event', 'ts_init', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades_count']
        t = pa.Table.from_pandas(group[cols], schema=BAR_SCHEMA, preserve_index=False)
        pq.write_table(t, out_file, compression='zstd')
        print(f"[1D] Solidified Year {year}: {len(group)} bars -> {out_file}")
        
    print("=== [Done] Successfully solidified all datasets! ===")

if __name__ == "__main__":
    run()
