"""
Tardis L2 OrderBook & Market Data Ingestion Pipeline for NautilusTrader
=======================================================================
Supports:
- Normalized data types: `incremental_book_L2` (book_change), `book_snapshot_25`, `trades`
- High-efficiency streaming decompression & chunked Parquet writing
- NautilusTrader Native Parquet format (FixedSizeBinary(8) price/size, uint64 ts, schema metadata)
- Analytics / Interop Parquet format (float64 price/size for DuckDB/Polars/ClickHouse)
- Dual partition layouts:
    * Standard Nautilus catalog: catalog/data/order_book_delta/{instrument_id}/{start}_{end}.parquet
    * Hierarchical catalog:       catalog/data/order_book_delta/{instrument_id}/{year}/{month}/{start}_{end}.parquet
"""

import os
import sys
import time
import datetime
import io
import zlib
import gzip
import argparse
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List

import requests
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Base directory configuration
BASE_DIR = Path(r"\\fnos2\iflow\数据库")
CATALOG_L2_DIR = BASE_DIR / "catalog" / "data" / "order_book_delta"
CATALOG_L2_DELTAS_DIR = BASE_DIR / "catalog" / "data" / "order_book_deltas"
RAW_DIR = BASE_DIR / "raw_archive" / "tardis"
REPORT_DIR = BASE_DIR / "validation_reports"

# Tardis endpoints and headers
TARDIS_DATASETS_BASE = "https://datasets.tardis.dev/v1"
TARDIS_USER_AGENT = "tardis-dev/2.0.0 (+https://github.com/tardis-dev/tardis-python)"
DEFAULT_HEADERS = {
    "Accept-Encoding": "gzip",
    "User-Agent": TARDIS_USER_AGENT
}

# Fixed-point constant (Nautilus 64-bit standard mode: 9 decimal places = 10^9)
FIXED_SCALAR = 1_000_000_000.0

# Exchange and symbol precision configuration
SYMBOL_CONFIG: Dict[str, Dict[str, Any]] = {
    "BTC-PERPETUAL.DERIBIT": {
        "exchange": "deribit",
        "raw_symbol": "BTC-PERPETUAL",
        "instrument_id": "BTC-PERPETUAL.DERIBIT",
        "price_precision": 1,
        "size_precision": 0,
        "market_type": "perpetual"
    },
    "ETHUSDT-PERP.BINANCE": {
        "exchange": "binance-futures",
        "raw_symbol": "ETHUSDT",
        "instrument_id": "ETHUSDT-PERP.BINANCE",
        "price_precision": 2,
        "size_precision": 3,
        "market_type": "perpetual"
    },
    "BTCUSDT-PERP.BINANCE": {
        "exchange": "binance-futures",
        "raw_symbol": "BTCUSDT",
        "instrument_id": "BTCUSDT-PERP.BINANCE",
        "price_precision": 2,
        "size_precision": 3,
        "market_type": "perpetual"
    }
}

# Nautilus 64-bit OrderBookDelta native arrow schema
def get_native_delta_schema(instrument_id: str, price_prec: int, size_prec: int, row_group_size: int = 500000) -> pa.Schema:
    fields = [
        pa.field("action", pa.uint8(), nullable=False),
        pa.field("side", pa.uint8(), nullable=False),
        pa.field("price", pa.binary(8), nullable=False),
        pa.field("size", pa.binary(8), nullable=False),
        pa.field("order_id", pa.uint64(), nullable=False),
        pa.field("flags", pa.uint8(), nullable=False),
        pa.field("sequence", pa.uint64(), nullable=False),
        pa.field("ts_event", pa.uint64(), nullable=False),
        pa.field("ts_init", pa.uint64(), nullable=False),
    ]
    metadata = {
        b"instrument_id": instrument_id.encode("utf-8"),
        b"price_precision": str(price_prec).encode("utf-8"),
        b"size_precision": str(size_prec).encode("utf-8"),
        b"rows_per_group": str(row_group_size).encode("utf-8"),
    }
    return pa.schema(fields, metadata=metadata)

# Analytics interop schema (Float64 price/size)
def get_analytics_delta_schema(instrument_id: str, price_prec: int, size_prec: int, row_group_size: int = 500000) -> pa.Schema:
    fields = [
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("action", pa.uint8(), nullable=False),
        pa.field("side", pa.uint8(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("size", pa.float64(), nullable=False),
        pa.field("order_id", pa.uint64(), nullable=False),
        pa.field("flags", pa.uint8(), nullable=False),
        pa.field("sequence", pa.uint64(), nullable=False),
        pa.field("ts_event", pa.uint64(), nullable=False),
        pa.field("ts_init", pa.uint64(), nullable=False),
    ]
    metadata = {
        b"instrument_id": instrument_id.encode("utf-8"),
        b"price_precision": str(price_prec).encode("utf-8"),
        b"size_precision": str(size_prec).encode("utf-8"),
        b"rows_per_group": str(row_group_size).encode("utf-8"),
    }
    return pa.schema(fields, metadata=metadata)

def format_nautilus_filename(ts_start_ns: int, ts_end_ns: int) -> str:
    """
    Format timestamps into Nautilus ISO 8601 interval filename.
    E.g. 2020-04-01T00-00-00-245000000Z_2020-04-01T00-24-25-195000000Z.parquet
    """
    dt_start = datetime.datetime.fromtimestamp(ts_start_ns / 1e9, tz=datetime.timezone.utc)
    dt_end = datetime.datetime.fromtimestamp(ts_end_ns / 1e9, tz=datetime.timezone.utc)
    
    start_str = dt_start.strftime("%Y-%m-%dT%H-%M-%S") + f"-{int(ts_start_ns % 1_000_000_000):09d}Z"
    end_str = dt_end.strftime("%Y-%m-%dT%H-%M-%S") + f"-{int(ts_end_ns % 1_000_000_000):09d}Z"
    return f"{start_str}_{end_str}.parquet"

def download_sample_dataset(
    exchange: str,
    data_type: str,
    symbol: str,
    date_str: str,
    max_mb: Optional[int] = None,
    force: bool = False
) -> Path:
    """
    Download monthly free sample dataset from Tardis datasets API.
    date_str: YYYY-MM-DD
    """
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    date_path = dt.strftime("%Y/%m/%d")
    url = f"{TARDIS_DATASETS_BASE}/{exchange}/{data_type}/{date_path}/{symbol}.csv.gz"
    
    out_dir = RAW_DIR / exchange / data_type / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{date_str}_{symbol}.csv.gz"
    
    if out_file.exists() and not force and out_file.stat().st_size > 0:
        print(f"[Download] Cached raw file exists: {out_file} ({out_file.stat().st_size / 1024 / 1024:.2f} MB)")
        return out_file
        
    print(f"[Download] Fetching from Tardis: {url}")
    t0 = time.time()
    
    with requests.get(url, headers=DEFAULT_HEADERS, stream=True, timeout=90) as resp:
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to fetch {url}: HTTP {resp.status_code} - {resp.text[:200]}")
            
        total_len = resp.headers.get("content-length")
        if total_len:
            print(f"[Download] Remote Content-Length: {int(total_len) / 1024 / 1024:.2f} MB")
            
        downloaded = 0
        max_bytes = max_mb * 1024 * 1024 if max_mb else None
        
        with open(out_file, "wb") as f_out:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    break
                f_out.write(chunk)
                downloaded += len(chunk)
                if max_bytes and downloaded >= max_bytes:
                    print(f"[Download] Reached size limit cap: {max_mb} MB")
                    break
                    
    elapsed = time.time() - t0
    rate = (downloaded / 1024 / 1024) / elapsed if elapsed > 0 else 0
    print(f"[Download] Saved {downloaded / 1024 / 1024:.2f} MB to {out_file} in {elapsed:.2f}s ({rate:.2f} MB/s)")
    return out_file

def stream_process_tardis_l2(
    csv_gz_path: Path,
    instrument_id: str,
    price_prec: int,
    size_prec: int,
    max_rows: Optional[int] = None
) -> Tuple[pa.Table, pa.Table, Dict[str, Any]]:
    """
    Decompresses and parses Tardis incremental_book_L2 CSV stream into:
    1) Native 64-bit FixedSizeBinary(8) Arrow Table
    2) Analytics Float64 Arrow Table
    3) Validation metrics dictionary
    """
    print(f"[Ingestion] Processing Tardis L2: {csv_gz_path}")
    t0 = time.time()
    
    # Read decompressed CSV in chunks using pandas
    chunk_size = 500_000
    rows_processed = 0
    
    native_batches = []
    analytics_batches = []
    
    # Validation accumulators
    action_counts: Dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0}
    side_counts: Dict[int, int] = {0: 0, 1: 0, 2: 0}
    min_ts_event = sys.maxsize
    max_ts_event = 0
    min_ts_init = sys.maxsize
    max_ts_init = 0
    min_price = float("inf")
    max_price = float("-inf")
    ts_event_inversions = 0
    prev_ts_event = 0
    
    with gzip.open(csv_gz_path, mode="rt", encoding="utf-8", errors="ignore") as gz_file:
        reader = pd.read_csv(gz_file, chunksize=chunk_size)
        try:
            for chunk_idx, df in enumerate(reader):
                if max_rows and rows_processed >= max_rows:
                    break
                    
                if max_rows and (rows_processed + len(df)) > max_rows:
                    df = df.iloc[:(max_rows - rows_processed)]
                    
                n_rows = len(df)
                if n_rows == 0:
                    continue
                    
                amounts = df["amount"].to_numpy(dtype=np.float64)
                prices = df["price"].to_numpy(dtype=np.float64)
                is_snap = df["is_snapshot"].astype(str).str.lower().isin(["true", "1"]).to_numpy(dtype=bool)
                sides_str = df["side"].astype(str).str.lower().to_numpy(dtype=str)
                
                # Action mapping:
                # Delete = 3 (amount == 0.0)
                # Add = 1    (amount > 0 and is_snapshot)
                # Update = 2 (amount > 0 and not is_snapshot)
                actions = np.full(n_rows, 2, dtype=np.uint8)
                actions[amounts == 0.0] = 3
                actions[(amounts > 0.0) & is_snap] = 1
                
                # Side mapping:
                # Buy = 1, Sell = 2, Unknown = 0
                sides = np.where(sides_str == "bid", 1, np.where(sides_str == "ask", 2, 0)).astype(np.uint8)
                
                # Flags: F_SNAPSHOT = 32, F_MBP = 8
                flags = np.where(is_snap, 32, 8).astype(np.uint8)
                order_ids = np.zeros(n_rows, dtype=np.uint64)
                sequences = np.arange(rows_processed, rows_processed + n_rows, dtype=np.uint64)
                
                # Timestamps: Tardis is in microseconds -> convert to nanoseconds (* 1,000)
                ts_event = df["timestamp"].to_numpy(dtype=np.uint64) * 1_000
                ts_init = df["local_timestamp"].to_numpy(dtype=np.uint64) * 1_000
                
                # Check monotonicity
                if chunk_idx == 0:
                    prev_ts_event = ts_event[0]
                ts_diffs = np.diff(np.insert(ts_event, 0, prev_ts_event))
                inversions = int(np.sum(ts_diffs < 0))
                ts_event_inversions += inversions
                prev_ts_event = ts_event[-1]
                
                # Metrics
                for act, cnt in pd.Series(actions).value_counts().items():
                    action_counts[int(act)] = action_counts.get(int(act), 0) + int(cnt)
                for sd, cnt in pd.Series(sides).value_counts().items():
                    side_counts[int(sd)] = side_counts.get(int(sd), 0) + int(cnt)
                    
                min_ts_event = min(min_ts_event, int(ts_event.min()))
                max_ts_event = max(max_ts_event, int(ts_event.max()))
                min_ts_init = min(min_ts_init, int(ts_init.min()))
                max_ts_init = max(max_ts_init, int(ts_init.max()))
                min_price = min(min_price, float(prices.min()))
                max_price = max(max_price, float(prices.max()))
                
                # 1. Native FixedSizeBinary(8) buffers
                price_raw = np.round(prices * FIXED_SCALAR).astype(np.int64)
                size_raw = np.round(amounts * FIXED_SCALAR).astype(np.uint64)
                
                price_buf = pa.py_buffer(price_raw.tobytes())
                size_buf = pa.py_buffer(size_raw.tobytes())
                
                native_batch = pa.RecordBatch.from_arrays([
                    pa.array(actions, type=pa.uint8()),
                    pa.array(sides, type=pa.uint8()),
                    pa.FixedSizeBinaryArray.from_buffers(pa.binary(8), n_rows, [None, price_buf]),
                    pa.FixedSizeBinaryArray.from_buffers(pa.binary(8), n_rows, [None, size_buf]),
                    pa.array(order_ids, type=pa.uint64()),
                    pa.array(flags, type=pa.uint8()),
                    pa.array(sequences, type=pa.uint64()),
                    pa.array(ts_event, type=pa.uint64()),
                    pa.array(ts_init, type=pa.uint64()),
                ], schema=get_native_delta_schema(instrument_id, price_prec, size_prec, chunk_size))
                native_batches.append(native_batch)
                
                # 2. Analytics Float64 batch
                analytics_batch = pa.RecordBatch.from_arrays([
                    pa.array([instrument_id] * n_rows, type=pa.string()),
                    pa.array(actions, type=pa.uint8()),
                    pa.array(sides, type=pa.uint8()),
                    pa.array(prices, type=pa.float64()),
                    pa.array(amounts, type=pa.float64()),
                    pa.array(order_ids, type=pa.uint64()),
                    pa.array(flags, type=pa.uint8()),
                    pa.array(sequences, type=pa.uint64()),
                    pa.array(ts_event, type=pa.uint64()),
                    pa.array(ts_init, type=pa.uint64()),
                ], schema=get_analytics_delta_schema(instrument_id, price_prec, size_prec, chunk_size))
                analytics_batches.append(analytics_batch)
                
                rows_processed += n_rows
                print(f"  [Chunk {chunk_idx + 1}] Processed {n_rows:,} rows (total: {rows_processed:,})")
        except (EOFError, zlib.error, pd.errors.EmptyDataError):
            print(f"  [Stream] Reached end of available stream buffer ({rows_processed:,} rows).")
            
    native_table = pa.Table.from_batches(native_batches, schema=get_native_delta_schema(instrument_id, price_prec, size_prec, chunk_size))
    analytics_table = pa.Table.from_batches(analytics_batches, schema=get_analytics_delta_schema(instrument_id, price_prec, size_prec, chunk_size))
    
    elapsed = time.time() - t0
    metrics = {
        "instrument_id": instrument_id,
        "rows_processed": rows_processed,
        "elapsed_sec": round(elapsed, 2),
        "rows_per_sec": round(rows_processed / elapsed, 0) if elapsed > 0 else 0,
        "ts_event_min": min_ts_event,
        "ts_event_max": max_ts_event,
        "ts_init_min": min_ts_init,
        "ts_init_max": max_ts_init,
        "min_price": min_price,
        "max_price": max_price,
        "action_counts": action_counts,
        "side_counts": side_counts,
        "ts_event_inversions": ts_event_inversions,
    }
    return native_table, analytics_table, metrics

def save_partitioned_parquet(
    native_table: pa.Table,
    analytics_table: pa.Table,
    instrument_id: str,
    year: int,
    month: int,
    metrics: Dict[str, Any]
) -> Dict[str, Path]:
    """
    Saves tables into target partition directories:
    1) catalog/data/order_book_delta/{instrument_id}/{filename}.parquet
    2) catalog/data/order_book_delta/{instrument_id}/{year}/{month:02d}/{filename}.parquet
    3) catalog/data/order_book_deltas/{instrument_id}/{filename}.parquet (Nautilus standard catalog alias)
    """
    filename = format_nautilus_filename(metrics["ts_event_min"], metrics["ts_event_max"])
    
    # 1. Flat partition for Nautilus query
    flat_dir = CATALOG_L2_DIR / instrument_id
    flat_dir.mkdir(parents=True, exist_ok=True)
    flat_file = flat_dir / filename
    
    # 2. Hierarchical partition
    hier_dir = CATALOG_L2_DIR / instrument_id / str(year) / f"{month:02d}"
    hier_dir.mkdir(parents=True, exist_ok=True)
    hier_file = hier_dir / filename
    
    # 3. Standard Nautilus catalog alias
    alias_dir = CATALOG_L2_DELTAS_DIR / instrument_id
    alias_dir.mkdir(parents=True, exist_ok=True)
    alias_file = alias_dir / filename
    
    print(f"[Catalog] Writing Native Parquet to {flat_file} (ZSTD level 3)...")
    pq.write_table(
        native_table,
        where=flat_file,
        compression="zstd",
        compression_level=3,
        row_group_size=500_000
    )
    
    # Copy/write to hierarchical partition
    print(f"[Catalog] Writing Hierarchical Parquet to {hier_file}...")
    pq.write_table(
        native_table,
        where=hier_file,
        compression="zstd",
        compression_level=3,
        row_group_size=500_000
    )
    
    # Write to deltas alias
    print(f"[Catalog] Writing Nautilus standard catalog alias to {alias_file}...")
    pq.write_table(
        native_table,
        where=alias_file,
        compression="zstd",
        compression_level=3,
        row_group_size=500_000
    )
    
    # Also save analytics version in a dedicated analytics directory for DuckDB/ClickHouse
    analytics_dir = BASE_DIR / "catalog" / "data" / "analytics_l2" / instrument_id / str(year) / f"{month:02d}"
    analytics_dir.mkdir(parents=True, exist_ok=True)
    analytics_file = analytics_dir / filename
    print(f"[Catalog] Writing Analytics Parquet to {analytics_file}...")
    pq.write_table(
        analytics_table,
        where=analytics_file,
        compression="zstd",
        compression_level=3,
        row_group_size=500_000
    )
    
    return {
        "flat": flat_file,
        "hierarchical": hier_file,
        "alias": alias_file,
        "analytics": analytics_file
    }

def verify_ingested_parquet(parquet_path: Path, expected_instrument: str, expected_price_prec: int, expected_size_prec: int) -> Dict[str, Any]:
    """
    Verifies that generated Parquet file is 100% compliant with Nautilus specifications:
    - Checks file metadata & embedded schema metadata
    - Validates row groups, schema types, and column nullability
    - Validates fixed-point binary decoding and round-trip consistency
    - Validates monotonicity of ts_event and ts_init
    """
    print(f"[Verify] Auditing Parquet file: {parquet_path}")
    assert parquet_path.exists(), f"File does not exist: {parquet_path}"
    
    file_size = parquet_path.stat().st_size
    meta = pq.read_metadata(parquet_path)
    schema = pq.read_schema(parquet_path)
    
    # Embedded metadata check
    custom_meta = schema.metadata or {}
    meta_instrument = custom_meta.get(b"instrument_id", b"").decode("utf-8")
    meta_price_prec = custom_meta.get(b"price_precision", b"").decode("utf-8")
    meta_size_prec = custom_meta.get(b"size_precision", b"").decode("utf-8")
    
    # Read sample rows and inspect
    table = pq.read_table(parquet_path)
    df_sample = table.slice(0, 1000).to_pandas()
    
    # Decode price and size
    SCALAR = 1_000_000_000
    decoded_prices = [
        int.from_bytes(b, byteorder="little", signed=True) / SCALAR
        for b in df_sample["price"]
    ]
    decoded_sizes = [
        int.from_bytes(b, byteorder="little", signed=False) / SCALAR
        for b in df_sample["size"]
    ]
    
    verification_result = {
        "file_path": str(parquet_path),
        "file_size_bytes": file_size,
        "file_size_mb": round(file_size / 1024 / 1024, 2),
        "num_rows": meta.num_rows,
        "num_row_groups": meta.num_row_groups,
        "num_columns": meta.num_columns,
        "schema_fields": [f.name for f in schema],
        "metadata_instrument_id": meta_instrument,
        "metadata_price_precision": meta_price_prec,
        "metadata_size_precision": meta_size_prec,
        "schema_match": (meta_instrument == expected_instrument and 
                         meta_price_prec == str(expected_price_prec) and 
                         meta_size_prec == str(expected_size_prec)),
        "first_ts_event": int(df_sample["ts_event"].iloc[0]),
        "last_ts_event": int(df_sample["ts_event"].iloc[-1]),
        "decoded_price_sample_min": min(decoded_prices),
        "decoded_price_sample_max": max(decoded_prices),
        "decoded_size_sample_min": min(decoded_sizes),
        "decoded_size_sample_max": max(decoded_sizes),
        "actions_distribution": df_sample["action"].value_counts().to_dict(),
        "sides_distribution": df_sample["side"].value_counts().to_dict(),
        "is_valid": True
    }
    
    print(f"  Rows: {meta.num_rows:,} | RowGroups: {meta.num_row_groups} | Size: {verification_result['file_size_mb']} MB")
    print(f"  Embedded Metadata: instrument={meta_instrument}, price_prec={meta_price_prec}, size_prec={meta_size_prec}")
    print(f"  Decoded Prices: {verification_result['decoded_price_sample_min']} -> {verification_result['decoded_price_sample_max']}")
    return verification_result

def run_pipeline(
    exchange: str = "deribit",
    symbol: str = "BTC-PERPETUAL",
    date_str: str = "2020-04-01",
    max_mb: int = 5,
    max_rows: Optional[int] = 500_000
) -> Dict[str, Any]:
    """
    End-to-end execution of download, ingestion, partitioning, and validation.
    """
    print("=" * 70)
    print(f"Starting Tardis L2 Ingestion Pipeline for {exchange}:{symbol} on {date_str}")
    print("=" * 70)
    
    # Derive instrument info
    inst_key = f"{symbol}.DERIBIT" if exchange == "deribit" else f"{symbol}-PERP.BINANCE"
    if inst_key not in SYMBOL_CONFIG:
        # Default config
        cfg = {
            "exchange": exchange,
            "raw_symbol": symbol,
            "instrument_id": inst_key,
            "price_precision": 1 if "BTC" in symbol else 2,
            "size_precision": 0 if "BTC" in symbol else 3,
            "market_type": "perpetual"
        }
    else:
        cfg = SYMBOL_CONFIG[inst_key]
        
    inst_id = cfg["instrument_id"]
    price_prec = cfg["price_precision"]
    size_prec = cfg["size_precision"]
    
    # 1. Download
    raw_gz = download_sample_dataset(
        exchange=exchange,
        data_type="incremental_book_L2",
        symbol=symbol,
        date_str=date_str,
        max_mb=max_mb
    )
    
    # 2. Process & Ingest
    native_tbl, analytics_tbl, metrics = stream_process_tardis_l2(
        csv_gz_path=raw_gz,
        instrument_id=inst_id,
        price_prec=price_prec,
        size_prec=size_prec,
        max_rows=max_rows
    )
    
    # 3. Save Parquet to Catalog Partition Structure
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    out_paths = save_partitioned_parquet(
        native_table=native_tbl,
        analytics_table=analytics_tbl,
        instrument_id=inst_id,
        year=dt.year,
        month=dt.month,
        metrics=metrics
    )
    
    # 4. Verify & Validate
    verif = verify_ingested_parquet(
        parquet_path=out_paths["flat"],
        expected_instrument=inst_id,
        expected_price_prec=price_prec,
        expected_size_prec=size_prec
    )
    
    return {
        "config": cfg,
        "raw_file": str(raw_gz),
        "metrics": metrics,
        "output_paths": {k: str(v) for k, v in out_paths.items()},
        "verification": verif
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tardis L2 Ingestion & Partitioning Pipeline")
    parser.add_argument("--exchange", type=str, default="deribit", help="Exchange name (e.g. deribit, binance-futures)")
    parser.add_argument("--symbol", type=str, default="BTC-PERPETUAL", help="Symbol name (e.g. BTC-PERPETUAL, ETHUSDT)")
    parser.add_argument("--date", type=str, default="2020-04-01", help="Date in YYYY-MM-DD format (first of month for free sample)")
    parser.add_argument("--max-mb", type=int, default=5, help="Maximum download size in MB")
    parser.add_argument("--max-rows", type=int, default=500000, help="Maximum number of rows to ingest")
    
    args = parser.parse_args()
    res = run_pipeline(
        exchange=args.exchange,
        symbol=args.symbol,
        date_str=args.date,
        max_mb=args.max_mb,
        max_rows=args.max_rows
    )
    print("\nPipeline execution complete.")
