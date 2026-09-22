"""
Binance to Nautilus ParquetDataCatalog Normalizer
Converts raw Binance Klines (Spot / Futures) into standard NautilusTrader
Parquet Data Catalog files with strict timestamp scaling, PyArrow schemas,
and Zstandard compression.
"""

import os
import sys
import glob
import argparse
from pathlib import Path
from typing import Optional, List
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Nautilus Standard Bar Schema (Extended with Taker Volumes for Crypto Microstructure)
NAUTILUS_BAR_SCHEMA = pa.schema([
    ("bar_type", pa.string()),
    ("ts_event", pa.int64()),                # nanoseconds UTC
    ("ts_init", pa.int64()),                 # nanoseconds UTC
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
    ("quote_volume", pa.float64()),
    ("trades_count", pa.int64()),
    ("taker_buy_base_volume", pa.float64()),
    ("taker_buy_quote_volume", pa.float64()),
])

def to_nanoseconds(raw_val: int) -> int:
    """
    Robust timestamp converter for cryptocurrency feeds.
    Auto-detects scale:
    - 10 digits: seconds (s)
    - 13 digits: milliseconds (ms) (Pre-2025 standard)
    - 16 digits: microseconds (us) (Binance 2025+ standard)
    - 19 digits: nanoseconds (ns) (Nautilus native)
    Prevents int64 silent overflow into year 1700 or negative values.
    """
    val_abs = abs(raw_val)
    if val_abs == 0:
        return 0
    digits = len(str(val_abs))
    if digits == 10:
        return raw_val * 1_000_000_000
    elif digits == 13:
        return raw_val * 1_000_000
    elif digits == 16:
        return raw_val * 1_000
    elif digits == 19:
        return raw_val
    else:
        # Fallback based on magnitude
        if val_abs < 10**11:
            return raw_val * 1_000_000_000
        elif val_abs < 10**14:
            return raw_val * 1_000_000
        elif val_abs < 10**17:
            return raw_val * 1_000
        return raw_val

def parse_binance_kline_csv(csv_path: Path, bar_type: str) -> pd.DataFrame:
    """Parses a single Binance kline CSV into Nautilus bar records."""
    df = pd.read_csv(csv_path, header=None)
    # Check if header row is present
    if str(df.iloc[0, 0]).startswith("open_time") or not str(df.iloc[0, 0]).isdigit():
        df = df.iloc[1:].reset_index(drop=True)

    # Column mapping:
    # 0: open_time, 1: open, 2: high, 3: low, 4: close, 5: volume, 6: close_time
    # 7: quote_asset_volume, 8: number_of_trades
    # 9: taker_buy_base_asset_volume, 10: taker_buy_quote_asset_volume
    ts_open_raw = df[0].astype("int64")
    ts_close_raw = df[6].astype("int64")

    # Vectorized timestamp scaling
    # Determine unit length from first element
    sample_val = int(ts_open_raw.iloc[0])
    digits = len(str(abs(sample_val)))
    if digits == 13:
        mult = 1_000_000
    elif digits == 16:
        mult = 1_000
    elif digits == 10:
        mult = 1_000_000_000
    elif digits == 19:
        mult = 1
    else:
        mult = 1_000_000

    out = pd.DataFrame()
    out["bar_type"] = bar_type
    out["ts_event"] = ts_open_raw * mult
    out["ts_init"] = ts_close_raw * mult
    out["open"] = df[1].astype("float64")
    out["high"] = df[2].astype("float64")
    out["low"] = df[3].astype("float64")
    out["close"] = df[4].astype("float64")
    out["volume"] = df[5].astype("float64")
    out["quote_volume"] = df[7].astype("float64") if df.shape[1] > 7 else 0.0
    out["trades_count"] = df[8].astype("int64") if df.shape[1] > 8 else 0
    out["taker_buy_base_volume"] = df[9].astype("float64") if df.shape[1] > 9 else 0.0
    out["taker_buy_quote_volume"] = df[10].astype("float64") if df.shape[1] > 10 else 0.0

    return out

def convert_and_catalog(
    input_dir: Path,
    catalog_root: Path,
    symbol: str = "BTCUSDT",
    market: str = "BINANCE",
    interval_str: str = "1-MINUTE",
    is_perp: bool = True
) -> None:
    """Converts and partitions data into ParquetDataCatalog format."""
    inst_id = f"{symbol}-PERP.{market}" if is_perp else f"{symbol}.{market}"
    bar_type = f"{inst_id}-{interval_str}-LAST-EXTERNAL"

    bar_catalog_dir = catalog_root / "data" / "bar" / bar_type / inst_id
    bar_catalog_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(glob.glob(str(input_dir / "*.csv")))
    if not csv_files:
        print(f"[Warning] No CSV files found in {input_dir}")
        return

    print(f"=== Converting {len(csv_files)} files for {bar_type} ===")
    
    # Process files and group by year
    dfs_by_year = {}
    for f in csv_files:
        try:
            df = parse_binance_kline_csv(Path(f), bar_type)
            if df.empty:
                continue
            # Extract year from ts_event (convert ns to datetime UTC)
            years = pd.to_datetime(df["ts_event"], unit="ns", utc=True).dt.year
            df["year"] = years
            for y, group in df.groupby("year"):
                if y not in dfs_by_year:
                    dfs_by_year[y] = []
                dfs_by_year[y].append(group.drop(columns=["year"]))
        except Exception as e:
            print(f"[Error] Failed to process {f}: {e}")

    # Write each year as partitioned Parquet file
    for year, groups in dfs_by_year.items():
        year_df = pd.concat(groups, axis=0).drop_duplicates(subset=["ts_event"]).sort_values("ts_event")
        year_dir = bar_catalog_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        parquet_file = year_dir / f"{year}_{interval_str.lower()}.parquet"

        table = pa.Table.from_pandas(year_df, schema=NAUTILUS_BAR_SCHEMA, preserve_index=False)
        pq.write_table(
            table,
            parquet_file,
            compression="zstd",
            compression_level=7,
            use_dictionary=True
        )
        print(f"[Wrote Catalog] {parquet_file} ({len(year_df)} bars)")

def main():
    parser = argparse.ArgumentParser(description="Normalize Binance Klines to Nautilus ParquetDataCatalog")
    parser.add_argument("--input-dir", type=str, required=True, help="Path to raw CSV directory")
    parser.add_argument("--catalog-dir", type=str, default="./catalog", help="Root directory of ParquetDataCatalog")
    parser.add_argument("--symbol", type=str, default="BTCUSDT")
    parser.add_argument("--interval", type=str, default="1-MINUTE")
    parser.add_argument("--spot", action="store_true", help="Set flag if spot market, otherwise default is perp")
    args = parser.parse_args()

    convert_and_catalog(
        input_dir=Path(args.input_dir),
        catalog_root=Path(args.catalog_dir),
        symbol=args.symbol,
        interval_str=args.interval,
        is_perp=not args.spot
    )

if __name__ == "__main__":
    main()
