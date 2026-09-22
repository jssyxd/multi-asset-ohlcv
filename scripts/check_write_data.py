import time
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.model.data import Bar

catalog = ParquetDataCatalog("/mnt/fnos2_iflow/数据库/catalog")

# Let's inspect how catalog.write_data writes bars and metadata
print("Checking catalog.write_data signature:")
import inspect
print(inspect.signature(catalog.write_data))
