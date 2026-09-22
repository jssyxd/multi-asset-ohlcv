import inspect
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer
from nautilus_trader.persistence.wranglers_v2 import BarDataWranglerV2

print("ArrowSerializer.serialize signature:")
print(inspect.signature(ArrowSerializer.serialize))

print("BarDataWranglerV2.build_schema:")
if hasattr(BarDataWranglerV2, "build_schema"):
    print(inspect.getsource(BarDataWranglerV2.build_schema))
