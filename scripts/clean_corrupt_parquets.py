import pyarrow.parquet as pq
from pathlib import Path

base = Path(r"//fnos2/iflow/数据库/catalog/data/bar")
files = sorted(list(base.rglob("*.parquet")))
print(f"Scanning {len(files)} parquet files for structural integrity...")

corrupt_files = []
valid_files = 0

for f in files:
    try:
        t = pq.read_table(f)
        valid_files += 1
    except Exception as e:
        print(f"Corrupt/truncated file detected: {f} -> {e}")
        corrupt_files.append(f)

print(f"Summary: {valid_files} valid, {len(corrupt_files)} corrupt.")

for c in corrupt_files:
    try:
        c.unlink()
        print(f"Removed corrupt file: {c}")
    except Exception as e:
        print(f"Failed removing {c}: {e}")
