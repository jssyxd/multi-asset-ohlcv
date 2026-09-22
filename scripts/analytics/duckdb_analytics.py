"""
DuckDB Analytics Engine for NautilusTrader ParquetDataCatalog
Demonstrates ultra-fast zero-copy querying, dynamic bar rollups,
ASOF JOINs (1m bars + funding rates + open interest), and Cumulative Volume Delta (CVD).
"""

import sys
from pathlib import Path
from typing import Optional
import duckdb

class NautilusDuckDBAnalytics:
    def __init__(self, catalog_dir: str = "./catalog/data"):
        self.catalog_dir = Path(catalog_dir)
        self.con = duckdb.connect(database=":memory:")
        self.con.execute("PRAGMA threads=4;")
        self.con.execute("PRAGMA memory_limit='4GB';")

    def query_bars(self, parquet_glob: str, start_ns: Optional[int] = None, end_ns: Optional[int] = None):
        """Zero-copy predicate pushdown scan on Parquet bars."""
        where_clauses = []
        if start_ns:
            where_clauses.append(f"ts_event >= {start_ns}")
        if end_ns:
            where_clauses.append(f"ts_event <= {end_ns}")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        query = f"""
        SELECT 
            bar_type,
            to_timestamp(ts_event / 1e9) as dt_utc,
            ts_event,
            open, high, low, close, volume,
            taker_buy_base_volume,
            (volume - taker_buy_base_volume) as taker_sell_base_volume,
            (taker_buy_base_volume - (volume - taker_buy_base_volume)) as volume_delta
        FROM read_parquet('{parquet_glob}')
        {where_sql}
        ORDER BY ts_event
        """
        return self.con.execute(query).fetchdf()

    def dynamic_resample_rollup(self, parquet_glob: str, timeframe_minutes: int = 15):
        """
        Dynamically resamples 1-minute bars into any higher timeframe using DuckDB,
        strictly maintaining OHLCV and Order Flow (CVD) conservation laws.
        """
        query = f"""
        WITH raw AS (
            SELECT 
                bar_type,
                ts_event,
                time_bucket(INTERVAL '{timeframe_minutes} minutes', to_timestamp(ts_event / 1e9)) as bucket_dt,
                open, high, low, close, volume, quote_volume, trades_count,
                taker_buy_base_volume
            FROM read_parquet('{parquet_glob}')
        )
        SELECT 
            bucket_dt,
            FIRST(open ORDER BY ts_event) as open,
            MAX(high) as high,
            MIN(low) as low,
            LAST(close ORDER BY ts_event) as close,
            ROUND(SUM(volume), 6) as volume,
            ROUND(SUM(quote_volume), 2) as quote_volume,
            SUM(trades_count) as trades_count,
            ROUND(SUM(taker_buy_base_volume), 6) as taker_buy_volume,
            ROUND(SUM(2 * taker_buy_base_volume - volume), 6) as cumulative_volume_delta
        FROM raw
        GROUP BY bucket_dt
        ORDER BY bucket_dt
        """
        return self.con.execute(query).fetchdf()

    def asof_join_funding_and_oi(self, bars_glob: str, funding_glob: str, oi_glob: str):
        """
        Demonstrates DuckDB native ASOF JOIN:
        Matches each 1m/15m bar with the most recent Funding Rate (8h) and Open Interest (5m)
        strictly eliminating look-ahead bias for accurate perpetual backtesting.
        """
        query = f"""
        WITH bars AS (
            SELECT 
                ts_event,
                to_timestamp(ts_event / 1e9) as dt,
                close, volume
            FROM read_parquet('{bars_glob}')
        ),
        funding AS (
            SELECT 
                ts_event as funding_ts,
                funding_rate
            FROM read_parquet('{funding_glob}')
        ),
        oi AS (
            SELECT 
                ts_event as oi_ts,
                open_interest,
                open_interest_usd
            FROM read_parquet('{oi_glob}')
        )
        SELECT 
            b.dt,
            b.close,
            b.volume,
            f.funding_rate,
            o.open_interest,
            o.open_interest_usd
        FROM bars b
        ASOF LEFT JOIN funding f ON b.ts_event >= f.funding_ts
        ASOF LEFT JOIN oi o ON b.ts_event >= o.oi_ts
        ORDER BY b.ts_event
        """
        return self.con.execute(query).fetchdf()

if __name__ == "__main__":
    print("Nautilus DuckDB Analytics Engine initialized.")
    print("Ready for high-throughput zero-copy Parquet analytics.")
