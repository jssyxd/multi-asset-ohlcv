import time
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import Bar

catalog = ParquetDataCatalog("/mnt/fnos2_iflow/数据库/catalog")
print("Calling get_file_list_from_data_cls(Bar)...")
t0 = time.perf_counter()
files = catalog.get_file_list_from_data_cls(Bar)
t1 = time.perf_counter()
print(f"Discovered {len(files)} files in {t1 - t0:.4f}s")
print("First 10 files:")
for f in files[:10]:
    print(" -", f)
