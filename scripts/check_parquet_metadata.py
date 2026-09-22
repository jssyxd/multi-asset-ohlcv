import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

p = Path("/mnt/fnos2_iflow/数据库/catalog/data/bar/BTCUSDT-PERP.BINANCE-15-MINUTE-LAST-EXTERNAL/BTCUSDT-PERP.BINANCE/2026/2026_bars.parquet")
schema = pq.read_schema(str(p))
print("Current metadata keys:")
for k, v in (schema.metadata or {}).items():
    print(" -", k, v[:60] if len(v) > 60 else v)
