"""
NautilusTrader Event-Driven Backtest Example using ParquetDataCatalog
Demonstrates loading partitioned bars from the catalog and executing a strategy.
"""

from decimal import Decimal
from pathlib import Path
import pandas as pd

# Notice: Optional import of nautilus_trader if installed in runtime environment
try:
    from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
    from nautilus_trader.model.data import BarType, BarSpecification
    from nautilus_trader.model.enums import BarAggregation, PriceType, AggregationSource
    from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
    from nautilus_trader.config import BacktestVenueConfig, BacktestEngineConfig
    from nautilus_trader.backtest.node import BacktestNode
    NAUTILUS_AVAILABLE = True
except ImportError:
    NAUTILUS_AVAILABLE = False

def run_sample_catalog_query(catalog_path: str = "./catalog"):
    """Demonstrates standard ParquetDataCatalog access."""
    print(f"=== NautilusTrader ParquetDataCatalog Demonstration ===")
    if not NAUTILUS_AVAILABLE:
        print("[Notice] nautilus_trader package not installed in active environment.")
        print("To install: pip install nautilus_trader")
        print("Displaying standard ParquetDataCatalog directory layout expectations:")
        print(f"Catalog Root: {Path(catalog_path).resolve()}")
        print("Partition Pattern: data/bar/{bar_type}/{instrument_id}/{year}.parquet")
        return

    catalog = ParquetDataCatalog(catalog_path)
    instrument_id = InstrumentId(Symbol("BTCUSDT-PERP"), Venue("BINANCE"))
    bar_type = BarType(
        instrument_id=instrument_id,
        bar_spec=BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
        aggregation_source=AggregationSource.EXTERNAL,
    )
    print(f"Querying catalog for BarType: {bar_type}")
    try:
        bars = catalog.bars([bar_type])
        print(f"Successfully loaded {len(bars)} bars from ParquetDataCatalog.")
    except Exception as e:
        print(f"Catalog query: {e}")

if __name__ == "__main__":
    run_sample_catalog_query()
