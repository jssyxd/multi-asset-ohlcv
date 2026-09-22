#!/usr/bin/env python3
"""
bybit_futures_trades.py
Downloads futures trade .csv.gz files from public.bybit.com/trading/, verifies
gzip integrity via in-memory decompression (CRC32 + deflate), converts to
Parquet (ZSTD, sorted by timestamp, deduplicated by trdMatchID).
Csv.gz files are never written to disk — only the final .parquet is persisted.

Manifest format (one per symbol):  filename size [true|false]
  - no status (pending)  — file has not been processed yet
  - true                — parquet file successfully created on disk
  - false               — error at any stage (download / gzip / conversion)

Size is recorded in manifest only after a successful (or failed) download.
No pre-scan HEAD requests are made — this avoids hitting rate limits.

On re-run, already processed files (status "true") are skipped by checking
filename in manifest + .parquet existence on disk. No size re-verification.

Download + conversion run in a single thread per file (parallelized across files).
Manifest is updated incrementally after each processed file.
"""
import argparse
import glob
import gzip
import io
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import polars as pl
import requests
from tqdm import tqdm

from bybit_verify_groups import stablecoins

# =============================================================================
# Configuration
# =============================================================================
BASE_URL = "https://public.bybit.com"
TRADING_PATH = "/trading/"
OUTPUT_DIR = "bybit_data/futures/trades"
# If a list — only the specified symbols (Spyder / Jupyter mode)
# If None — DEFAULT_GROUP is used to fetch symbols from the server
SPECIFIC_SYMBOLS: Optional[List[str]] = None  # None = use DEFAULT_GROUP; ["ETHUSDT"] = specific
# Default group for Spyder / Jupyter mode (when SPECIFIC_SYMBOLS is None).
# Set to None to disable auto-fetch (must use --group on CLI).
# Possible values: 'USDT', 'STABLE', 'FUTURES', 'QUARTERLY', 'PERP', 'INVERSE'
DEFAULT_GROUP: Optional[str] = "USDT"
DOWNLOAD_TIMEOUT = 120          # read timeout per file (seconds)
CONNECT_TIMEOUT = 15           # connect timeout (seconds)
MAX_RETRIES = 3
RETRY_DELAY = 5
CONCURRENT_WORKERS = 3        # parallel threads (each: download + convert)
SYMBOL_DELAY = 2.0             # seconds to pause between symbols (rate-limit mitigation)

# Parquet write settings
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
PARQUET_ROW_GROUP_SIZE = 500_000

# Symbol group filters — regex patterns for matching futures symbols on the server.
# Used by CLI --group flags and by DEFAULT_GROUP in Spyder mode.
FUTURES_TRADES_GROUPS = {
    'USDT':      rf'^(?!({"|".join(stablecoins)})USDT$)[A-Z0-9]+USDT$',
    'INVERSE':   r'^[A-Z0-9]+USD$',
    'PERP':      r'^[A-Z0-9]+PERP$',
    'STABLE':    rf'^({"|".join(stablecoins)})USDT$',
    'QUARTERLY': r'^[A-Z0-9]+USD[FHJKMNQUVXZ]\d{2}$',
    'FUTURES':   r'^(?!.*USDT)[A-Z0-9]+-\d{2}[A-Z]{3}\d{2}$',
    'UNSORTED':  r'.*',
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

# =============================================================================
# Bybit trade CSV schema (for Polars)
# =============================================================================
# RPI column is absent in older files (2020). Detected from CSV header at
# read time and included in the column list only when present.
_READ_COLUMNS = [
    "timestamp", "symbol", "side", "size", "price",
    "tickDirection", "trdMatchID", "grossValue", "homeNotional", "foreignNotional",
]
_READ_SCHEMA = {
    "timestamp": pl.Float64,
    "symbol": pl.Utf8,
    "side": pl.Utf8,
    "size": pl.Float64,
    "price": pl.Float64,
    "tickDirection": pl.Utf8,
    "trdMatchID": pl.Utf8,
    "grossValue": pl.Float64,
    "homeNotional": pl.Float64,
    "foreignNotional": pl.Float64,
}
_RPI_SCHEMA = {**_READ_SCHEMA, "RPI": pl.Int32}

TRADE_SCHEMA = {**_READ_SCHEMA, "timestamp": pl.Int64, "RPI": pl.Int32}
TRADE_COLUMNS = list(TRADE_SCHEMA.keys())

# =============================================================================
# HTTP session (per-thread instance for thread safety)
# =============================================================================
_thread_local = threading.local()


def get_session() -> requests.Session:
    """Returns a requests.Session for the current thread (thread-safe)."""
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)
    return _thread_local.session

# =============================================================================
# Helper functions
# =============================================================================
def format_time(seconds: float) -> str:
    """Formats seconds into HH:MM:SS or Xh Ym Zs string."""
    secs = int(seconds)
    hours = secs // 3600
    minutes = (secs % 3600) // 60
    secs_rem = secs % 60
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs_rem:02d}   ({hours}h {minutes}m {secs_rem}s)"
    elif minutes > 0:
        return f"{minutes:02d}:{secs_rem:02d}   ({minutes}m {secs_rem}s)"
    else:
        return f"{secs_rem}s"

def parse_directory_links(html: str) -> List[str]:
    return sorted(re.findall(r'href="([^"]+)/"', html))

def parse_file_links(html: str, symbol: str) -> List[str]:
    """Returns full filenames matching SYMBOL*.csv.gz (includes symbol prefix)."""
    return sorted(re.findall(rf'href="({re.escape(symbol)}[^"]+\.csv\.gz)"', html))

def get_all_symbols() -> List[str]:
    """Fetches all symbol directories from the server (unfiltered)."""
    url = f"{BASE_URL}{TRADING_PATH}"
    resp = get_session().get(url, timeout=30)
    resp.raise_for_status()
    return parse_directory_links(resp.text)

def filter_by_group(all_symbols: List[str], group: str) -> List[str]:
    """Filters symbols by group name using FUTURES_TRADES_GROUPS regex."""
    pattern = FUTURES_TRADES_GROUPS.get(group)
    if pattern is None:
        raise ValueError(f"Unknown group: {group}. Available: {', '.join(FUTURES_TRADES_GROUPS)}")
    return sorted([s for s in all_symbols if re.match(pattern, s)])

def get_remote_files(symbol: str) -> List[str]:
    url = f"{BASE_URL}{TRADING_PATH}{symbol}/"
    resp = get_session().get(url, timeout=30)
    resp.raise_for_status()
    return parse_file_links(resp.text, symbol)


# =============================================================================
# Manifest
# Format:  filename size [true|false]
#   — no status (pending) — file has not been processed yet
#   — true               — parquet file successfully created on disk
#   — false              — error (download / gzip / conversion)
# =============================================================================
def manifest_path(symbol: str) -> str:
    """Returns the manifest file path (does not create directories)."""
    return os.path.join(OUTPUT_DIR, symbol, f"{symbol}_manifest.txt")

def load_manifest(symbol: str) -> Dict[str, Dict]:
    """
    Loads the manifest. Returns {filename: {"size": int, "status": str|None}}.
    Status is "true", "false", or None (pending).
    """
    path = manifest_path(symbol)
    if not os.path.exists(path):
        return {}
    manifest = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 3:
                name, size_str, status = parts[0], parts[1], parts[2]
                manifest[name] = {"size": int(size_str), "status": status}
            elif len(parts) == 2:
                name, size_str = parts
                manifest[name] = {"size": int(size_str), "status": None}
    return manifest

def save_manifest(symbol: str, manifest: Dict[str, Dict]) -> None:
    """Saves the manifest. Line format: filename size [status]."""
    path = manifest_path(symbol)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for name in sorted(manifest):
            entry = manifest[name]
            size = entry["size"]
            status = entry.get("status")
            if status is not None:
                f.write(f"{name} {size} {status}\n")
            else:
                f.write(f"{name} {size}\n")

# =============================================================================
# Utilities
# =============================================================================
def cleanup_tmp_files(directory: str) -> int:
    """Removes .tmp files from directory. Returns count of removed files."""
    removed = 0
    if not os.path.isdir(directory):
        return 0
    for tmp_path in glob.glob(os.path.join(directory, "*.tmp")):
        try:
            os.remove(tmp_path)
            removed += 1
        except OSError:
            pass
    return removed

def _safe_remove(filepath: str) -> None:
    """Removes a file, silently ignoring errors."""
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
    except OSError:
        pass

# =============================================================================
# Download + convert in a single thread (all in RAM, no csv.gz on disk)
# =============================================================================
def download_and_convert(url: str, parquet_path: str, timeout: int = DOWNLOAD_TIMEOUT) -> Tuple[bool, int]:
    """
    Downloads .csv.gz into RAM, verifies gzip integrity via decompression,
    converts to .parquet. Csv.gz is never written to disk.
    Returns (success: bool, downloaded_size: int).
    """
    tmp_path = parquet_path + ".tmp"
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # 1. Download into memory
            resp = get_session().get(url, timeout=(CONNECT_TIMEOUT, timeout))
            resp.raise_for_status()
            gz_bytes = resp.content
            actual_size = len(gz_bytes)
            if actual_size == 0:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                continue

            # 2. Decompress gzip in memory (also verifies CRC32 + deflate integrity)
            try:
                csv_bytes = gzip.decompress(gz_bytes)
            except Exception:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                continue
            del gz_bytes  # free memory

            # 3. Detect RPI in CSV header and read accordingly
            first_nl = csv_bytes.index(b'\n')
            header = csv_bytes[:first_nl].decode('utf-8', errors='replace')
            has_rpi = 'RPI' in header.split(',')

            if has_rpi:
                read_cols = _READ_COLUMNS + ["RPI"]
                read_schema = _RPI_SCHEMA
            else:
                read_cols = _READ_COLUMNS
                read_schema = _READ_SCHEMA

            try:
                df = pl.read_csv(
                    io.BytesIO(csv_bytes),
                    columns=read_cols,
                    schema_overrides=read_schema,
                    infer_schema_length=0,
                    ignore_errors=True,
                )
            except Exception:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                continue
            del csv_bytes  # free memory

            if "timestamp" in df.columns:
                df = df.with_columns(
                    (pl.col("timestamp") * 1_000_000).cast(pl.Int64).alias("timestamp")
                )

            # Add RPI column (missing in older files)
            if "RPI" not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Int32).alias("RPI"))

            if df.is_empty():
                df = pl.DataFrame(schema=TRADE_SCHEMA)
            else:
                df = df.sort("timestamp")
                df = df.unique(subset=["trdMatchID"], keep="first")
                df = df.select(TRADE_COLUMNS)

            # 4. Write parquet to temp file (atomically replace on success)
            try:
                df.write_parquet(
                    tmp_path,
                    compression=PARQUET_COMPRESSION,
                    compression_level=PARQUET_COMPRESSION_LEVEL,
                    row_group_size=PARQUET_ROW_GROUP_SIZE,
                )
            except Exception:
                _safe_remove(tmp_path)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                continue

            # 5. Atomic replace
            os.replace(tmp_path, parquet_path)
            return True, actual_size

        except KeyboardInterrupt:
            _safe_remove(tmp_path)
            raise
        except Exception as e:
            last_error = e
            _safe_remove(tmp_path)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    # All attempts exhausted
    _safe_remove(tmp_path)
    fn_short = url.rsplit("/", 1)[-1]
    print(f"    FAILED {fn_short}: {last_error}")
    return False, 0

# =============================================================================
# Process one symbol
# =============================================================================
def download_symbol(symbol: str, workers: int = CONCURRENT_WORKERS, timeout: int = DOWNLOAD_TIMEOUT) -> Tuple[str, List[str]]:
    """
    Downloads and converts all files for one symbol.
    Returns (status, failed_files) tuple.
    """
    print(f"\n{'='*70}\n{symbol}\n{'='*70}")
    try:
        remote_files = get_remote_files(symbol)
    except Exception as e:
        print(f"  Error fetching file list: {e}")
        return "error", []
    if not remote_files:
        print("  No files on server")
        return "skipped", []

    print(f"  Files on server: {len(remote_files)}")

    symbol_dir = os.path.join(OUTPUT_DIR, symbol)
    os.makedirs(symbol_dir, exist_ok=True)

    # Clean up leftover .tmp files from previous interrupted runs
    tmp_removed = cleanup_tmp_files(symbol_dir)
    if tmp_removed:
        print(f"  Cleaned up {tmp_removed} temp files from previous runs")

    manifest = load_manifest(symbol)

    # ------------------------------------------------------------------
    # First run: populate manifest with all filenames (size=0, pending).
    # ------------------------------------------------------------------
    if not manifest:
        for fn in remote_files:
            manifest[fn] = {"size": 0, "status": None}
        save_manifest(symbol, manifest)
        print(f"  Manifest created: {len(manifest)} files (statuses — pending)")

    # ------------------------------------------------------------------
    # Determine files to process. Check by filename only:
    #   - status "true" in manifest AND .parquet exists → skip
    #   - everything else → download + convert
    # ------------------------------------------------------------------
    to_process = []
    had_any_valid = False

    for fn in remote_files:
        entry = manifest.get(fn)
        entry_status = entry["status"] if entry else None

        # Already true — verify that parquet still exists on disk
        if entry_status == "true":
            parquet_name = fn.replace(".csv.gz", ".parquet")
            parquet_path = os.path.join(symbol_dir, parquet_name)
            if os.path.exists(parquet_path):
                had_any_valid = True
                continue
            # Parquet missing — needs to be re-created

        # Needs download and/or conversion
        to_process.append(fn)

    if not to_process:
        verified_count = sum(1 for e in manifest.values() if e["status"] == "true")
        pending_count = sum(1 for e in manifest.values() if e.get("status") is None)
        false_count = sum(1 for e in manifest.values() if e["status"] == "false")
        print(f"  All files already processed (parquet: {verified_count}, pending: {pending_count}, failed: {false_count})")
        return "skipped", []

    print(f"  Files to process: {len(to_process)}")
    failed, success_files = [], []
    manifest_lock = threading.Lock()

    def task(fn):
        url = f"{BASE_URL}{TRADING_PATH}{symbol}/{fn}"
        parquet_name = fn.replace(".csv.gz", ".parquet")
        parquet_path = os.path.join(symbol_dir, parquet_name)
        success, actual_size = download_and_convert(url, parquet_path, timeout=timeout)
        return (fn, success, actual_size)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(task, fn): fn for fn in to_process}
        with tqdm(total=len(to_process), desc="  Processing", unit="file", colour="cyan") as pbar:
            for future in as_completed(futures):
                fn = futures[future]
                fn_result, success, actual_size = future.result()
                with manifest_lock:
                    if success:
                        success_files.append(fn_result)
                        manifest[fn_result] = {"size": actual_size, "status": "true"}
                    else:
                        failed.append(fn)
                        manifest[fn] = {"size": actual_size, "status": "false"}
                    save_manifest(symbol, manifest)
                pbar.update(1)

    if failed and not success_files:
        print(f"  Failed to process all {len(failed)} files.")
        return "error", failed

    print(f"  Successfully processed: {len(success_files)} files.")
    if failed:
        print(f"  Warning: {len(failed)} files failed. Re-run to retry.")
    return ("new" if not had_any_valid else "updated"), failed

# =============================================================================
# CLI and entry point
# =============================================================================
_GROUP_CHOICES = [k for k in FUTURES_TRADES_GROUPS if k != 'UNSORTED']

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download Bybit futures trades (.csv.gz) → convert to Parquet → bybit_data/futures/trades/{symbol}/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
python bybit_futures_trades.py --usdt
python bybit_futures_trades.py --inverse
python bybit_futures_trades.py --stable --workers 5
python bybit_futures_trades.py --symbols ETHUSDT SOLUSDT
python bybit_futures_trades.py --group PERP
No arguments (Spyder): uses SPECIFIC_SYMBOLS or DEFAULT_GROUP from script configuration.
""",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--usdt", action="store_true",
        help="All USDT perpetual futures",
    )
    group.add_argument(
        "--stable", action="store_true",
        help="Stablecoin pairs (stablecoins vs USDT)",
    )
    group.add_argument(
        "--futures", action="store_true",
        help="Delivery futures (BTC-28FEB26, ETH-01MAR24, etc.)",
    )
    group.add_argument(
        "--quarterly", action="store_true",
        help="Quarterly futures (BTC/ETHUSDH26, etc.)",
    )
    group.add_argument(
        "--perp", action="store_true",
        help="PERP contracts",
    )
    group.add_argument(
        "--inverse", action="store_true",
        help="Inverse contracts (BTCUSD, etc.)",
    )
    group.add_argument(
        "--group", choices=_GROUP_CHOICES, metavar="NAME",
        help=f"Select symbol group: {', '.join(_GROUP_CHOICES)}",
    )
    group.add_argument(
        "--symbols", nargs="+", metavar="SYM",
        help="List of specific symbols (e.g. ETHUSDT SOLUSDT BTCUSD)",
    )
    parser.add_argument(
        "--workers", type=int, default=None,
        help=f"Parallel worker threads (default: {CONCURRENT_WORKERS})",
    )
    parser.add_argument(
        "--timeout", type=int, default=None,
        help=f"Download timeout per file in seconds (default: {DOWNLOAD_TIMEOUT})",
    )
    return parser

# Map CLI flag attribute names → group names
_FLAG_TO_GROUP = {
    'usdt': 'USDT',
    'stable': 'STABLE',
    'futures': 'FUTURES',
    'quarterly': 'QUARTERLY',
    'perp': 'PERP',
    'inverse': 'INVERSE',
}

def main(symbols_override: Optional[List[str]] = None):
    workers = CONCURRENT_WORKERS
    timeout = DOWNLOAD_TIMEOUT
    cli_symbols = None
    cli_group = None

    if len(sys.argv) > 1:
        parser = build_parser()
        args = parser.parse_args()
        if args.workers is not None:
            workers = args.workers
        if args.timeout is not None:
            timeout = args.timeout
        if args.symbols:
            cli_symbols = [s.upper() for s in args.symbols]
        elif args.group:
            cli_group = args.group
        else:
            # Check shorthand flags (--usdt, --stable, etc.)
            for attr, group_name in _FLAG_TO_GROUP.items():
                if getattr(args, attr, False):
                    cli_group = group_name
                    break

    # Determine target: CLI > symbols_override > SPECIFIC_SYMBOLS > DEFAULT_GROUP
    if cli_symbols is not None:
        symbols = sorted(cli_symbols)
        print(f"Using specified list: {len(symbols)} symbols")
    elif symbols_override is not None:
        symbols = sorted(s.upper() for s in symbols_override)
        print(f"Using override list: {len(symbols)} symbols")
    elif SPECIFIC_SYMBOLS is not None:
        symbols = sorted(s.upper() for s in SPECIFIC_SYMBOLS)
        print(f"Using SPECIFIC_SYMBOLS: {len(symbols)} symbols")
    else:
        group = cli_group or DEFAULT_GROUP
        if group is None:
            print("Error: no group specified. Set DEFAULT_GROUP or use --group/--symbols on CLI.")
            return
        print(f"Fetching symbols for group [{group}]...")
        try:
            all_symbols = get_all_symbols()
            symbols = filter_by_group(all_symbols, group)
        except ValueError as e:
            print(f"Error: {e}")
            return
        except Exception as e:
            print(f"Error fetching symbol list: {e}")
            return
        print(f"Found {len(symbols)} symbols in group [{group}]")

    results = {"new": [], "updated": [], "skipped": [], "error": []}
    partial_failures: Dict[str, List[str]] = {}
    t0 = time.time()
    for i, sym in enumerate(symbols, 1):
        # Rate-limit mitigation: pause between symbols
        if i > 1:
            time.sleep(SYMBOL_DELAY)
        print(f"\n[{i}/{len(symbols)}] {sym}")
        status, failed_files = download_symbol(sym, workers=workers, timeout=timeout)
        results[status].append(sym)
        if failed_files:
            partial_failures[sym] = failed_files

    elapsed = time.time() - t0
    print(f"\n{'═'*70}\nSUMMARY\n{'═'*70}")
    print(f"  Elapsed time:       {format_time(elapsed)}")
    print(f"  Total symbols:      {len(symbols)}")
    print(f"  ● Processed (new):  {len(results['new'])}")
    print(f"  ● Updated:          {len(results['updated'])}")
    print(f"  ● Unchanged:        {len(results['skipped'])}")
    print(f"  ● Errors:           {len(results['error'])}")
    if results['error']:
        print(f"  Error symbols: {', '.join(results['error'])}")
    if partial_failures:
        total_failed = sum(len(v) for v in partial_failures.values())
        print(f"  ● Partial failures: {len(partial_failures)} symbols, {total_failed} files")
        print(f"{'─'*70}")
        for sym, flist in sorted(partial_failures.items()):
            print(f"    {sym}: {len(flist)} files")
    print(f"{'═'*70}")

if __name__ == "__main__":
    main()
