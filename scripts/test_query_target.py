import time
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import Bar

catalog = ParquetDataCatalog("/mnt/fnos2_iflow/数据库/catalog")
target_files = [
    "/mnt/fnos2_iflow/数据库/catalog/data/bar/BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL/BTCUSDT-PERP.BINANCE/2026/2026_bars.parquet"
]

print(f"Reading bars via catalog.query(Bar, files={target_files})...")
t0 = time.perf_counter()
bars = catalog.query(Bar, files=target_files)
t1 = time.perf_counter()

print(f"Successfully loaded {len(bars)} bars in {t1 - t0:.4f}s")
print(f"Throughput: {len(bars) / (t1 - t0):.1f} bars/sec")
if len(bars) > 0:
    print("First bar:", bars[0])
    print("Last bar: ", bars[-1])
