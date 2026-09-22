import time
import pyarrow.parquet as pq
from pathlib import Path

p = Path("/mnt/fnos2_iflow/数据库/catalog/data/bar/BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL/BTCUSDT-PERP.BINANCE/2026/2026_bars.parquet")
print("File exists:", p.exists(), "Size:", p.stat().st_size)

t0 = time.perf_counter()
table = pq.read_table(str(p))
t1 = time.perf_counter()

print(f"Read {len(table)} rows in {t1 - t0:.4f}s")
print("Schema:")
print(table.schema)
print("Sample head:")
print(table.slice(0, 5).to_pydict())
