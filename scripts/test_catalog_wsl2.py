import sys
import time
from pathlib import Path
import nautilus_trader
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

print("=== [WSL2 Nautilus Smoke Test] ===")
print("NautilusTrader version:", nautilus_trader.__version__)
catalog_path = Path("/mnt/fnos2_iflow/数据库/catalog")
print(f"Catalog Path: {catalog_path} (exists: {catalog_path.exists()})")

catalog = ParquetDataCatalog(str(catalog_path))
print("ParquetDataCatalog initialized successfully.")

# Check instruments
instruments = catalog.instruments()
print(f"Total Instruments in Catalog: {len(instruments)}")
for inst in instruments:
    print(f" - Instrument: {inst.id}")

# Check bars
try:
    bar_types = catalog.bar_types()
    print(f"Available BarTypes ({len(bar_types)}):")
    for bt in bar_types:
        print(f" - {bt}")
except Exception as e:
    print("Error querying bar_types:", e)

# Test loading bars
t0 = time.perf_counter()
bars = catalog.bars()
t1 = time.perf_counter()
print(f"Loaded {len(bars)} bars in {t1 - t0:.4f} seconds ({len(bars)/(t1-t0 if t1>t0 else 1):.1f} bars/sec)")
if len(bars) > 0:
    print(f"First bar: ts_event={bars[0].ts_event}, close={bars[0].close}")
    print(f"Last bar:  ts_event={bars[-1].ts_event}, close={bars[-1].close}")

print("Catalog inspection completed.")
