#!/usr/bin/env python3
import os
import sys
import argparse

from suppression_tools.src.outliers import cube_outliers


def default_store_glob() -> str:
    here = os.getcwd()
    return os.path.join(here, "duckdb_partitioned_store", "**", "*.parquet")


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
    return build_win_cube(
        args.store,
        args.ds,
        args.mover_ind,
        args.output,
        start_date=args.start_date,
        end_date=args.end_date,
        only_outliers=(not args.all_rows),
    )


if __name__ == "__main__":
    sys.exit(main())
