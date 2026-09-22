import duckdb
from pathlib import Path

con = duckdb.connect()
df = con.execute("""
    SELECT filename, count(*) as cnt
    FROM read_parquet('//fnos2/iflow/数据库/catalog/data/bar/*/*/*/*.parquet', filename=true)
    GROUP BY filename
    ORDER BY filename
""").df()

for idx, row in df.iterrows():
    print(f"{row['filename']} -> {row['cnt']}")
