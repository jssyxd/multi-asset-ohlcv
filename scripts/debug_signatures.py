import inspect
from nautilus_trader.persistence.catalog import ParquetDataCatalog

print("catalog.bars signature:")
print(inspect.signature(ParquetDataCatalog.bars))

print("\ncatalog.query signature:")
print(inspect.signature(ParquetDataCatalog.query))
