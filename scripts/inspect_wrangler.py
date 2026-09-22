import inspect
import pyarrow.parquet as pq
from nautilus_trader.persistence.wranglers_v2 import BarDataWranglerV2
from nautilus_trader.model.data import Bar

print("BarDataWranglerV2 signature:")
print(inspect.signature(BarDataWranglerV2.__init__))

print("\nfrom_schema implementation:")
print(inspect.getsource(BarDataWranglerV2.from_schema))
