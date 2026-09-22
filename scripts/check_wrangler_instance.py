from nautilus_trader.persistence.wranglers_v2 import BarDataWranglerV2
import inspect

wrangler = BarDataWranglerV2(
    bar_type="BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL",
    price_precision=2,
    size_precision=5
)
print("wrangler attributes and methods:")
for m in dir(wrangler):
    if not m.startswith("_"):
        print(" -", m)

if hasattr(wrangler, "schema"):
    print("wrangler.schema:", wrangler.schema)
    print("wrangler.schema.metadata:", wrangler.schema.metadata)
