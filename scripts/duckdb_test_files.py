import duckdb
from pathlib import Path

base = Path(r"//fnos2/iflow/数据库/catalog/data/bar")
files = sorted(list(base.rglob("*.parquet")))
con = duckdb.connect()

print("Testing DuckDB read on each file:")
for f in files:
    try:
        p_str = f.as_posix()
        res = con.execute(f"SELECT count(*) FROM read_parquet('{p_str}')").fetchone()[0]
    except Exception as e:
        print(f"FAILED DuckDB on {f.name}: {e}")
        try:
            f.unlink()
            print(f"Deleted unreadable file: {f}")
        except Exception as del_e:
            print(f"Could not delete: {del_e}")
print("Done testing individual files with DuckDB.")
