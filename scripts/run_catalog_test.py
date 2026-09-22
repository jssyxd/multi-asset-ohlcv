import sys
import time
import os
from pathlib import Path

print("Step 1: Check Python environment", flush=True)
print("Python:", sys.executable, sys.version, flush=True)

import nautilus_trader
print("NautilusTrader version:", nautilus_trader.__version__, flush=True)

cat_path = Path("/mnt/fnos2_iflow/数据库/catalog")
print(f"Catalog path: {cat_path}, exists: {cat_path.exists()}", flush=True)

from nautilus_trader.persistence.catalog import ParquetDataCatalog

print("Step 2: Initializing ParquetDataCatalog...", flush=True)
catalog = ParquetDataCatalog(str(cat_path))
print("Catalog instance:", catalog, flush=True)

print("Step 3: Querying instruments...", flush=True)
insts = catalog.instruments()
print(f"Instruments ({len(insts)}):", insts, flush=True)

from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

print("Data types:", catalog.list_data_types())

bt_str = "BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"
print(f"Querying bars for {bt_str}...")
try:
    bar_type = BarType.from_str(bt_str)
    print("BarType parsed:", bar_type)
    bars = catalog.bars(bar_types=[bar_type])
    print("Result type:", type(bars))
    print("Loaded count:", len(bars) if hasattr(bars, "__len__") else "generator/iterator")
    if hasattr(bars, "__len__") and len(bars) > 0:
        print("First bar:", bars[0])
        print("Last bar:", bars[-1])
except Exception as e:
    print("Exception loading bars:", e)
    import traceback
    traceback.print_exc()

print("Done Step 1-5 successfully!", flush=True)
