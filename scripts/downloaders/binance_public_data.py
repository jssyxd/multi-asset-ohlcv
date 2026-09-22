"""
Binance Public Data Downloader for NautilusTrader
Downloads official historical data (Klines, aggTrades, fundingRate, metrics)
from data.binance.vision (AWS S3 binance-public-data) without API keys.
"""

import os
import sys
import argparse
import urllib.request
import zipfile
from pathlib import Path
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://data.binance.vision/data"

def build_download_url(
    market: str, # "futures/um" or "spot"
    data_type: str, # "klines", "aggTrades", "fundingRate", "metrics"
    frequency: str, # "monthly" or "daily"
    symbol: str,
    period_str: str, # e.g. "2024-01" or "2024-01-01"
    interval: Optional[str] = "1m"
) -> str:
    """Builds official Binance Data Vision URL."""
    if data_type == "klines":
        filename = f"{symbol}-{interval}-{period_str}.zip"
        return f"{BASE_URL}/{market}/{frequency}/klines/{symbol}/{interval}/{filename}"
    elif data_type == "aggTrades":
        filename = f"{symbol}-aggTrades-{period_str}.zip"
        return f"{BASE_URL}/{market}/{frequency}/aggTrades/{symbol}/{filename}"
    elif data_type == "fundingRate":
        filename = f"{symbol}-fundingRate-{period_str}.zip"
        return f"{BASE_URL}/{market}/{frequency}/fundingRate/{symbol}/{filename}"
    elif data_type == "metrics":
        filename = f"{symbol}-metrics-{period_str}.zip"
        return f"{BASE_URL}/{market}/daily/metrics/{symbol}/{filename}"
    else:
        raise ValueError(f"Unsupported data_type: {data_type}")

def download_file(url: str, output_dir: Path, extract: bool = True) -> Optional[Path]:
    """Downloads a single archive and extracts CSV."""
    filename = url.split("/")[-1]
    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / filename
    
    csv_name = filename.replace(".zip", ".csv")
    csv_path = output_dir / csv_name
    if csv_path.exists():
        print(f"[Exists] {csv_path.name}")
        return csv_path

    try:
        print(f"[Downloading] {url}")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as response, open(zip_path, "wb") as out:
            out.write(response.read())

        if extract and zipfile.is_zipfile(zip_path):
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(output_dir)
            zip_path.unlink() # remove zip after extraction
            print(f"[Extracted] {csv_name}")
            return csv_path
        return zip_path
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"[Not Found] 404: {url}")
        else:
            print(f"[HTTP Error] {e.code} for {url}")
        return None
    except Exception as e:
        print(f"[Error] Failed to download {url}: {e}")
        return None

def download_batch(
    market: str,
    data_type: str,
    frequency: str,
    symbol: str,
    periods: List[str],
    output_dir: Path,
    interval: str = "1m",
    max_workers: int = 4
) -> List[Path]:
    """Downloads multiple periods concurrently."""
    urls = [
        build_download_url(market, data_type, frequency, symbol, p, interval)
        for p in periods
    ]
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_file, url, output_dir, True) for url in urls]
        for f in futures:
            res = f.result()
            if res:
                results.append(res)
    return results

def main():
    parser = argparse.ArgumentParser(description="Binance Public Data Downloader for NautilusTrader")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading symbol, e.g. BTCUSDT")
    parser.add_argument("--market", type=str, default="futures/um", choices=["futures/um", "futures/cm", "spot"])
    parser.add_argument("--type", type=str, default="klines", choices=["klines", "aggTrades", "fundingRate", "metrics"])
    parser.add_argument("--interval", type=str, default="1m", help="Kline interval, e.g. 1m, 5m, 1h")
    parser.add_argument("--frequency", type=str, default="monthly", choices=["monthly", "daily"])
    parser.add_argument("--months", nargs="+", default=["2024-01", "2024-02"], help="Months list e.g. 2024-01 2024-02")
    parser.add_argument("--output-dir", type=str, default="./raw_archive")
    args = parser.parse_args()

    out_path = Path(args.output_dir) / args.symbol / args.type
    print(f"=== Downloading Binance Public Data: {args.symbol} ({args.type}) ===")
    download_batch(
        market=args.market,
        data_type=args.type,
        frequency=args.frequency,
        symbol=args.symbol,
        periods=args.months,
        output_dir=out_path,
        interval=args.interval
    )
    print("Download task completed.")

if __name__ == "__main__":
    main()
