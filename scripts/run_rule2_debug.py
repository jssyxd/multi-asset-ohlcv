import duckdb

con = duckdb.connect()
parquet_pattern = "//fnos2/iflow/数据库/catalog/data/bar/*/*/*/*.parquet"

q = f"""
SELECT 
    COUNT(*) as total_bars,
    COUNT(CASE WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 THEN 1 END) as non_positive_price_count,
    COUNT(CASE WHEN high < GREATEST(open, close) THEN 1 END) as invalid_high_count,
    COUNT(CASE WHEN low > LEAST(open, close) THEN 1 END) as invalid_low_count,
    COUNT(CASE WHEN high < low THEN 1 END) as high_less_than_low_count,
    COUNT(CASE WHEN volume < 0 OR quote_volume < 0 OR trades_count < 0 THEN 1 END) as negative_volume_count,
    COUNT(CASE WHEN volume = 0 AND (open != high OR high != low OR low != close) THEN 1 END) as invalid_zero_vol_bars,
    COUNT(CASE WHEN volume > 0 AND quote_volume > 0 AND (quote_volume / volume < low * 0.9999 OR quote_volume / volume > high * 1.0001) THEN 1 END) as vwap_envelope_violations
FROM read_parquet('{parquet_pattern}');
"""
res = con.execute(q).fetchone()
names = [
    "total_bars", "non_positive_price_count", "invalid_high_count", "invalid_low_count",
    "high_less_than_low_count", "negative_volume_count", "invalid_zero_vol_bars", "vwap_envelope_violations"
]
for n, v in zip(names, res):
    print(f"{n}: {v}")
