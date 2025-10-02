#!/usr/bin/env python3
import os
import sys
import argparse
import duckdb

from suppression_tools.src.outliers import cube_outliers


def default_store_glob() -> str:
    here = os.getcwd()
    return os.path.join(here, "duckdb_partitioned_store", "**", "*.parquet")


def get_min_max_dates(ds_glob: str) -> tuple[str, str]:
    con = duckdb.connect()
    try:
        q = f"SELECT MIN(the_date) AS min_date, MAX(the_date) AS max_date FROM parquet_scan('{ds_glob}')"
        result = con.execute(q).fetchone()
        if result and result[0] and result[1]:
            return str(result[0]), str(result[1])
        return '2020-01-01', '2030-01-01' # Fallback
    finally:
        con.close()

def build_win_cube(
    store_glob: str,
    ds: str,
    mover_ind: str,
    output_csv: str,
    start_date: str | None = None,
    end_date: str | None = None,
    only_outliers: bool = True,
) -> int:
    try:
        import duckdb
    except Exception:
        print("[ERROR] duckdb not installed. Use uv to install: uv pip install --python .venv duckdb pandas numpy", file=sys.stderr)
        return 2

    try:
        print(f"DEBUG: start_date={start_date}, end_date={end_date}, only_outliers={only_outliers}")
        df = cube_outliers(store_glob, ds, mover_ind, start_date, end_date, window=14, z_nat=2.5, z_pair=2.0, only_outliers=only_outliers)
        os.makedirs(os.path.dirname(os.path.expanduser(output_csv)), exist_ok=True)
        df.to_csv(os.path.expanduser(output_csv), index=False)
        print(f"[INFO] Wrote cube: {output_csv} ({len(df):,} rows)")
        return 0
    except Exception as e:
        print(f"[ERROR] Failed to build cube: {e}", file=sys.stderr)
        return 1


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Build national + pair outlier cube to CSV")
    p.add_argument("--store", default=default_store_glob(), help="Parquet glob for partitioned store")
    p.add_argument("--ds", default="gamoshi", help="Dataset id (ds)")
    p.add_argument("--mover-ind", choices=["True","False"], required=True, help="Mover indicator filter")
    p.add_argument("--start-date", default=None, help="Optional start date (YYYY-MM-DD)")
    p.add_argument("--end-date", default=None, help="Optional end date (YYYY-MM-DD)")
    p.add_argument("--all-rows", action="store_true", help="Do not filter to national outlier days (produces a very large cube)")
    p.add_argument("-o", "--output", required=True, help="Output CSV path")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    _start_date = args.start_date
    _end_date = args.end_date

    if _start_date is None or _end_date is None:
        global_min_date, global_max_date = get_min_max_dates(args.store)
        if _start_date is None:
            _start_date = global_min_date
        if _end_date is None:
            _end_date = global_max_date

    return build_win_cube(
        args.store,
        args.ds,
        args.mover_ind,
        args.output,
        start_date=_start_date,
        end_date=_end_date,
        only_outliers=(not args.all_rows),
    )


if __name__ == "__main__":
    sys.exit(main())
