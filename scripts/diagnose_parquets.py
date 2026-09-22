import duckdb
import pyarrow.parquet as pq
from pathlib import Path

base = Path(r"\\fnos2\iflow\数据库\catalog\data\bar")
files = sorted(list(base.rglob("*.parquet")))
print(f"Total parquet files to inspect: {len(files)}")

bad_files = []
violations = []

con = duckdb.connect()

for f in files:
    try:
        t = pq.read_table(f)
        # Check envelope in duckdb
        res = con.execute(f"""
            SELECT count(*), 
                   count(CASE WHEN high < GREATEST(open, close) OR low > LEAST(open, close) THEN 1 END) as envelope_err
            FROM read_parquet('{f.as_posix()}')
        """).fetchone()
        if res[1] > 0:
            print(f"[Violation Found] File: {f} has {res[1]} envelope violations!")
            viol_df = con.execute(f"""
                SELECT bar_type, ts_event, open, high, low, close, volume
                FROM read_parquet('{f.as_posix()}')
                WHERE high < GREATEST(open, close) OR low > LEAST(open, close)
            """).df()
            print(viol_df)
            violations.append((f, viol_df))
    except Exception as e:
        print(f"[Corrupted File] {f}: {e}")
        bad_files.append(f)

print(f"Inspection complete. Corrupted: {len(bad_files)}, Files with violations: {len(violations)}")
