import time
from pathlib import Path
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import BarType

catalog = ParquetDataCatalog("/mnt/fnos2_iflow/数据库/catalog")
bar_type = BarType.from_str("BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL")

print("Checking show_query_paths:")
paths = catalog.show_query_paths(
    data_cls=None,
    bar_types=[bar_type],
)
print("Query paths:", paths)

print("Calling get_file_list_from_data_cls...")
files = catalog.get_file_list_from_data_cls(
    data_cls=None,
    bar_types=[bar_type],
)
print(f"Discovered {len(files)} files:", files[:5])
