"""
One-click Data Bundle Downloader from GitHub Releases
Downloads pre-packaged NautilusTrader ParquetDataCatalog bundles
(BTCUSDT, ETHUSDT, SOLUSDT with Taker Volumes & 1m bars).
"""

import os
import sys
import zipfile
import urllib.request
from pathlib import Path

RELEASE_URL = "https://github.com/jssyxd/nautilus-crypto-datacatalog/releases/download/v1.0.0/NautilusTrader_Crypto_Catalog_2024_Q1_Bundle.zip"

def download_and_extract(url: str = RELEASE_URL, output_dir: str = "."):
    dest_path = Path(output_dir)
    dest_path.mkdir(parents=True, exist_ok=True)
    zip_target = dest_path / "nautilus_catalog_bundle.zip"

    print(f"=== Downloading Pre-built Nautilus Parquet Catalog Bundle ===")
    print(f"Source URL: {url}")
    print(f"Target: {zip_target}")

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp, open(zip_target, "wb") as f:
        length = resp.getheader("content-length")
        total_size = int(length) if length else None
        downloaded = 0
        block_size = 64 * 1024
        while True:
            buffer = resp.read(block_size)
            if not buffer:
                break
            downloaded += len(buffer)
            f.write(buffer)
            if total_size:
                percent = downloaded * 100 / total_size
                print(f"\rProgress: {downloaded / (1024*1024):.1f}MB / {total_size / (1024*1024):.1f}MB ({percent:.1f}%)", end="")

    print("\n[Download Complete] Extracting into catalog...")
    with zipfile.ZipFile(zip_target, "r") as zf:
        zf.extractall(dest_path)
    zip_target.unlink()
    print("[Extraction Complete] Nautilus ParquetDataCatalog is ready in ./catalog")

if __name__ == "__main__":
    download_and_extract()
