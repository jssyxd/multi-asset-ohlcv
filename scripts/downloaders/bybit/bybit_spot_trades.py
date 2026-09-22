#!/usr/bin/env python3
"""
bybit_spot_trades.py
Downloads spot trade .csv.gz files from public.bybit.com/spot/, verifies
gzip integrity via in-memory decompression (CRC32 + deflate), converts to
Parquet (ZSTD, sorted by timestamp, deduplicated by id).
Csv.gz files are never written to disk — only the final .parquet is persisted.

Spot CSV columns:
  New files: id, timestamp, price, volume, side, rpi
  Old files: id, timestamp, price, volume, side

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

from bybit_verify_groups import stablecoins, fiatcoins

# =============================================================================
# Configuration
# =============================================================================
BASE_URL = "https://public.bybit.com"
TRADING_PATH = "/spot/"
OUTPUT_DIR = "bybit_data/spot/trades"
# If a list — only the specified symbols (Spyder / Jupyter mode)
# If None — DEFAULT_GROUP is used to fetch symbols from the server
SPECIFIC_SYMBOLS: Optional[List[str]] = None  # None = use DEFAULT_GROUP; ["ETHUSDT"] = specific
# Default group for Spyder / Jupyter mode (when SPECIFIC_SYMBOLS is None).
# Set to None to disable auto-fetch (must use --group on CLI).
# Possible values: 'USDT', 'STABLE', 'USDC', 'OTHER', 'FIAT', 'CRYPTO', 'LEVERAGED'
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

# Symbol group filters — regex patterns for matching spot symbols on the server.
# Groups are evaluated in priority order: first match wins (no overlaps).
# Used by CLI --group flags and by DEFAULT_GROUP in Spyder mode.
SPOT_TRADES_GROUPS = {
    'LEVERAGED': r'^[A-Z0-9]+\d[LS]USDT$',
    'STABLE':    rf'^({"|".join(stablecoins)})USDT$',
    'USDT':      r'^[A-Z0-9]+USDT$',
    'FIAT':      rf'^[A-Z0-9]+({"|".join(fiatcoins)})$',
    'USDC':      r'^[A-Z0-9]+USDC$',
    'INVERSE':   None,
    'OTHER':     rf'^[A-Z0-9]+({"|".join(stablecoins)})$',
    'CRYPTO':    None,
    'UNSORTED':  r'.*',
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

# =============================================================================
# Spot trade CSV schema (for Polars)
# =============================================================================
# New files:  id, timestamp, price, volume, side, rpi
# Old files:  id, timestamp, price, volume, side          (rpi is absent)
#
# We read ALL columns present in the CSV (no `columns=` filter), so the rpi
# column is read when it exists. The rpi column is then added back as None
# for older files where it is missing. This avoids the previous bug where
# `columns=_READ_COLUMNS` excluded rpi from being read at all, causing the
# `if "rpi" not in df.columns` branch to always trigger and save None.
_READ_SCHEMA = {
    "id": pl.Utf8,
    "timestamp": pl.Int64,
    "price": pl.Float64,
    "volume": pl.Float64,
    "side": pl.Utf8,
    "rpi": pl.Int32,
}

SPOT_SCHEMA = _READ_SCHEMA  # read schema == output schema
SPOT_COLUMNS = list(SPOT_SCHEMA.keys())

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

def _compute_basecoins(all_symbols: List[str]) -> set:
    """Extracts base currencies from USDT pairs, excluding stablecoins."""
    usdt_pat = SPOT_TRADES_GROUPS['USDT']
    return {sym[:-4] for sym in all_symbols
            if sym.endswith('USDT') and re.match(usdt_pat, sym)
            and sym[:-4] not in stablecoins}

def filter_by_group(all_symbols: List[str], group: str) -> List[str]:
    """Filters symbols by group name using SPOT_TRADES_GROUPS regex."""
    basecoins = _compute_basecoins(all_symbols)
    if group == 'INVERSE':
        if not basecoins:
            return []
        pattern = rf'^({"|".join(basecoins)})USD$'
        return sorted([s for s in all_symbols if re.match(pattern, s)])
    if group == 'CRYPTO':
        if not basecoins:
            return []
        pattern = rf'^({"|".join(basecoins)})+({"|".join(basecoins)})$'
        return sorted([s for s in all_symbols if re.match(pattern, s)])
    pattern = SPOT_TRADES_GROUPS.get(group)
    if pattern is None:
        raise ValueError(f"Unknown group: {group}. Available: {', '.join(SPOT_TRADES_GROUPS)}")
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

            # 3. Convert CSV bytes → DataFrame → Parquet
            try:
                df = pl.read_csv(
                    io.BytesIO(csv_bytes),
                    schema_overrides=_READ_SCHEMA,
                    infer_schema_length=0,
                    ignore_errors=True,
                    truncate_ragged_lines=True,
                )
            except Exception:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                continue
            del csv_bytes  # free memory

            # Add rpi column (missing in older files)
            if "rpi" not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Int32).alias("rpi"))

            if df.is_empty():
                df = pl.DataFrame(schema=SPOT_SCHEMA)
            else:
                df = df.sort("timestamp")
                df = df.unique(subset=["id"], keep="first")
                df = df.select(SPOT_COLUMNS)

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

    # ------------------------------------------------------------------
    # Separate daily files (SYMBOL_YYYY-MM-DD.csv.gz) from monthly
    # aggregation files (SYMBOL-YYYY-MM.csv.gz). Only daily files are
    # downloaded — monthly files contain the same data aggregated and
    # are marked "skip" in the manifest.
    # ------------------------------------------------------------------
    _MONTHLY_RE = re.compile(rf"^{re.escape(symbol)}-\d{{4}}-\d{{2}}\.csv\.gz$")
    daily_files = [fn for fn in remote_files if not _MONTHLY_RE.match(fn)]
    monthly_files = [fn for fn in remote_files if _MONTHLY_RE.match(fn)]

    if monthly_files:
        print(f"  Skipped (monthly): {len(monthly_files)} files")

    manifest = load_manifest(symbol)

    # ------------------------------------------------------------------
    # First run: populate manifest with all filenames.
    # Daily files — pending. Monthly files — skip.
    # ------------------------------------------------------------------
    if not manifest:
        for fn in daily_files:
            manifest[fn] = {"size": 0, "status": None}
        for fn in monthly_files:
            manifest[fn] = {"size": 0, "status": "skip"}
        save_manifest(symbol, manifest)
        print(f"  Manifest created: {len(manifest)} files (daily — pending, monthly — skip)")

    # ------------------------------------------------------------------
    # Determine files to process. Check by filename only:
    #   - status "true" in manifest AND .parquet exists → skip
    #   - status "skip" → not downloaded (monthly aggregation)
    #   - everything else → download + convert
    # ------------------------------------------------------------------
    to_process = []
    had_any_valid = False

    for fn in daily_files:
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
        skip_count = sum(1 for e in manifest.values() if e["status"] == "skip")
        print(f"  All files already processed (parquet: {verified_count}, pending: {pending_count}, failed: {false_count}, skipped: {skip_count})")
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
_GROUP_CHOICES = [k for k in SPOT_TRADES_GROUPS if k != 'UNSORTED']

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download Bybit spot trades (.csv.gz) → convert to Parquet → bybit_data/spot/trades/{symbol}/",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
python bybit_spot_trades.py --usdt
python bybit_spot_trades.py --usdc
python bybit_spot_trades.py --other
python bybit_spot_trades.py --fiat --workers 5
python bybit_spot_trades.py --symbols ETHUSDT SOLUSDT
python bybit_spot_trades.py --group CRYPTO
No arguments (Spyder): uses SPECIFIC_SYMBOLS or DEFAULT_GROUP from script configuration.
""",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--usdt", action="store_true",
        help="All USDT spot pairs",
    )
    group.add_argument(
        "--usdc", action="store_true",
        help="All USDC spot pairs",
    )
    group.add_argument(
        "--stable", action="store_true",
        help="Stablecoin pairs (stablecoins vs USDT)",
    )
    group.add_argument(
        "--other", action="store_true",
        help="Other stablecoins (DAI, RLUSD, USDE, USDQ, USDR, USD1, XUSD)",
    )
    group.add_argument(
        "--fiat", action="store_true",
        help="Fiat pairs (EUR, BRL, ARS, TRY, GBP, etc.)",
    )
    group.add_argument(
        "--crypto", action="store_true",
        help="Crypto-quoted pairs (BTC, ETH, SOL, MNT, BNB)",
    )
    group.add_argument(
        "--inverse", action="store_true",
        help="Inverse pairs (coinUSD)",
    )
    group.add_argument(
        "--leveraged", action="store_true",
        help="Leveraged tokens (e.g. ADA2LUSDT, BTC3SUSDT)",
    )
    group.add_argument(
        "--group", choices=_GROUP_CHOICES, metavar="NAME",
        help=f"Select symbol group: {', '.join(_GROUP_CHOICES)}",
    )
    group.add_argument(
        "--symbols", nargs="+", metavar="SYM",
        help="List of specific symbols (e.g. ETHUSDT SOLUSDT AAVEUSDC)",
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
    'usdc': 'USDC',
    'other': 'OTHER',
    'fiat': 'FIAT',
    'crypto': 'CRYPTO',
    'inverse': 'INVERSE',
    'leveraged': 'LEVERAGED',
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
