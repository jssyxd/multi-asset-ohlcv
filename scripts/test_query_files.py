import time
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import Bar

catalog = ParquetDataCatalog("/mnt/fnos2_iflow/数据库/catalog")
bt_str = "BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL"

print("1. Testing get_file_list_from_data_cls...")
files = catalog.get_file_list_from_data_cls(Bar, [bt_str])
print(f"Discovered {len(files)} files.")
for f in files[:5]:
    print(" -", f)

print("\n2. Testing catalog.query with files parameter...")
t0 = time.perf_counter()
res = catalog.query(Bar, identifiers=[bt_str], files=files[:1])
t1 = time.perf_counter()
print(f"Loaded {len(res)} bars in {t1 - t0:.4f}s from 1 file.")
if len(res) > 0:
    print("First bar:", res[0])
    print("Last bar:", res[-1])
