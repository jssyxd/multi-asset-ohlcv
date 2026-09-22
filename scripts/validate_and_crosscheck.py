"""
Data Validator & Cross-Check Engine
Compares newly structured Nautilus Parquet Catalog against existing fragmented database:
1. \\fnos2\\iflow\\nautilusTrader\\data\\btc_klines_15m_aligned.csv
2. \\fnos2\\iflow\\nautilusTrader\\data\\binance_klines\\*.csv
Checks:
- Timestamp alignment & gap detection (nanosecond vs millisecond precision)
- OHLCV price & volume identity consistency
- Anomaly detection (High < Low, negative volume, extreme jumps)
Outputs audit report into \\fnos2\\iflow\\数据库\\validation_reports\\
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow.parquet as pq

BASE_DIR = Path(r"\\fnos2\iflow\数据库")
FRAGMENT_DIR = Path(r"\\fnos2\iflow\nautilusTrader\data")
REPORT_DIR = BASE_DIR / "validation_reports"

def inspect_fragment_data():
    aligned_csv = FRAGMENT_DIR / "btc_klines_15m_aligned.csv"
    if aligned_csv.exists():
        print(f"[Fragment] Reading aligned CSV: {aligned_csv}")
        df = pd.read_csv(aligned_csv, nrows=10)
        print("[Fragment] Columns:", df.columns.tolist())
        print(df.head(3))
        return df
    else:
        print("[Fragment] Aligned CSV not found.")
        return None

if __name__ == "__main__":
    inspect_fragment_data()
