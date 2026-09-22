"""
Nautilus Trader & Quant Backtest Data Ingestion Engine
Fetches OHLCV (1m, 15m, 1h, 4h, 1d) & Order Book data for BTC, ETH, SOL, HYPE, UNI, etc.
Saves natively into Nautilus Parquet Catalog structure and converts to Parquet.
"""

import os
import sys
import time
import datetime
import urllib.request
import gzip
import shutil
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE_DIR = Path(r"\\fnos2\iflow\数据库")
CATALOG_BAR_DIR = BASE_DIR / "catalog" / "data" / "bar"
CATALOG_L2_DIR = BASE_DIR / "catalog" / "data" / "order_book_delta"
RAW_DIR = BASE_DIR / "raw_archive"

BINANCE_VISION_BASE = "https://data.binance.vision/data/futures/um/monthly/klines"
BINANCE_SPOT_VISION_BASE = "https://data.binance.vision/data/spot/monthly/klines"

# Nautilus Bar schema definition
BAR_SCHEMA = pa.schema([
    ("bar_type", pa.string()),
    ("ts_event", pa.int64()),   # nanoseconds
    ("ts_init", pa.int64()),    # nanoseconds
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
    ("quote_volume", pa.float64()),
    ("trades_count", pa.int64())
])

def download_file(url: str, dest_path: Path) -> bool:
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"[Fetch] Downloading: {url}")
        req = urllib.request.Request(
            url, 
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        with urllib.request.urlopen(req, timeout=30) as response, open(dest_path, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
        return True
    except Exception as e:
        print(f"[Fetch] Failed to download {url}: {e}")
        return False

def parse_binance_kline_df(csv_path: Path, bar_type: str) -> pd.DataFrame:
    """
    Parse Binance monthly/daily kline CSV into Nautilus standardized schema.
    Binance format:
    open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_vol, taker_buy_quote_vol, ignore
    """
    try:
        df = pd.read_csv(
            csv_path, 
            header=None,
            compression='zip' if str(csv_path).endswith('.zip') else 'infer'
        )
        # Check if first row is header
        if str(df.iloc[0, 0]).startswith("open_time") or not str(df.iloc[0, 0]).isdigit():
            df = df.iloc[1:]
            
        res = pd.DataFrame()
        res['ts_event'] = df[0].astype('int64') * 1_000_000 # ms to ns
        res['ts_init'] = df[6].astype('int64') * 1_000_000  # close_time ms to ns
        res['bar_type'] = bar_type
        res['open'] = df[1].astype('float64')
        res['high'] = df[2].astype('float64')
        res['low'] = df[3].astype('float64')
        res['close'] = df[4].astype('float64')
        res['volume'] = df[5].astype('float64')
        res['quote_volume'] = df[7].astype('float64')
        res['trades_count'] = df[8].astype('int64')
        
        # Sort and clean
        res = res.sort_values('ts_event').drop_duplicates(subset=['ts_event'])
        return res
    except Exception as e:
        print(f"Error parsing kline {csv_path}: {e}")
        return pd.DataFrame()

def save_to_nautilus_catalog(df: pd.DataFrame, bar_type: str, instrument_id: str, year: int):
    if df.empty:
        return
    out_dir = CATALOG_BAR_DIR / bar_type.replace("/", "_") / instrument_id / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{year}_bars.parquet"
    
    table = pa.Table.from_pandas(df, schema=BAR_SCHEMA, preserve_index=False)
    pq.write_table(table, out_file, compression='zstd')
    print(f"[Catalog] Saved {len(df)} rows to {out_file}")

def download_tardis_sample_l2(exchange: str, symbol: str, date_str: str):
    """
    Download Tardis 1st of month free sample L2 data.
    E.g. date_str = "2024-01-01" -> "2024/01/01"
    """
    year, month, day = date_str.split("-")
    url = f"https://datasets.tardis.dev/v1/{exchange}/incremental_book_L2/{year}/{month}/{day}/{symbol}.csv.gz"
    out_path = RAW_DIR / "tardis" / exchange / symbol / f"{date_str}_incremental_book_L2.csv.gz"
    if out_path.exists():
        print(f"[Tardis] Already exists: {out_path}")
        return out_path
    if download_file(url, out_path):
        return out_path
    return None

if __name__ == "__main__":
    print("Testing Ingestion Engine components...")
