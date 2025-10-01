#!/usr/bin/env python3
"""
Lightweight smoke tests for the suppression tools project.

Checks:
- Partitioned store exists and is readable
- base_national_series returns a frame with expected columns for a tiny window
- scan_base_outliers executes and returns the expected columns (may be empty)
- Optional: build and validate mover cube CSV (opt-in via --with-cube)

Usage:
  uv run --python .venv python tools/smoke_test_dashboard.py [--with-cube]
"""
import argparse
import glob
import os
import sys
from datetime import timedelta

import duckdb
import pandas as pd

from suppression_tools.src.plan import base_national_series, scan_base_outliers


def find_store_glob() -> str:
    here = os.getcwd()
    return os.path.join(here, 'duckdb_partitioned_store', '**', '*.parquet')


def pick_small_window(con: duckdb.DuckDBPyConnection, store_glob: str, ds: str) -> tuple[str, str]:
    q = f"SELECT MIN(CAST(the_date AS DATE)) AS mn, MAX(CAST(the_date AS DATE)) AS mx FROM parquet_scan('{store_glob}') WHERE ds = '{ds.replace("'","''")}'"
    df = con.execute(q).df()
    if df.empty or pd.isna(df['mn'][0]) or pd.isna(df['mx'][0]):
        raise RuntimeError('No data available in store for ds=' + ds)
    start = pd.to_datetime(df['mn'][0]).date()
    end = min(pd.to_datetime(df['mx'][0]).date(), start + timedelta(days=7))
    return (start.isoformat(), end.isoformat())


def pick_winners(con: duckdb.DuckDBPyConnection, store_glob: str, ds: str, limit: int = 3) -> list[str]:
    q = f"""
    WITH ds AS (SELECT * FROM parquet_scan('{store_glob}') WHERE ds = '{ds.replace("'","''")}'),
         agg AS (
           SELECT winner, SUM(adjusted_wins) AS w FROM ds GROUP BY 1
         )
    SELECT winner FROM agg ORDER BY w DESC NULLS LAST LIMIT {int(limit)}
    """
    df = con.execute(q).df()
    return [str(x) for x in df['winner'].dropna().tolist()]


def ensure_cube(store_glob: str, ds: str, mover_ind: str, out_csv: str) -> None:
    if os.path.exists(out_csv):
        return
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    cmd = [sys.executable, os.path.join(os.getcwd(), 'build_win_cube.py'), '--store', store_glob, '--ds', ds, '--mover-ind', mover_ind, '-o', out_csv]
    import subprocess
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"cube build failed: {res.stderr or res.stdout}")


def validate_cube(out_csv: str) -> None:
    df = pd.read_csv(out_csv, nrows=5)
    required = ['the_date','winner','loser','dma_name','pair_wins_current','nat_share_current','nat_outlier_pos']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise AssertionError('cube missing columns: ' + ', '.join(missing))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--ds', default='gamoshi')
    ap.add_argument('--mover-ind', default='True', choices=['True','False'])
    ap.add_argument('--with-cube', action='store_true', help='Also build and validate mover cube CSV')
    args = ap.parse_args(argv)

    store_glob = find_store_glob()
    matches = glob.glob(store_glob, recursive=True)
    if not matches:
        print('[FAIL] No parquet files found in', store_glob)
        return 2

    con = duckdb.connect()
    try:
        start, end = pick_small_window(con, store_glob, args.ds)
        winners = pick_winners(con, store_glob, args.ds, limit=3)
    finally:
        con.close()
    if not winners:
        print('[FAIL] Could not find any winners in store')
        return 2

    # base_national_series
    base_df = base_national_series(store_glob, args.ds, args.mover_ind, winners, start, end)
    if base_df.empty or 'win_share' not in base_df.columns:
        print('[FAIL] base_national_series returned empty or missing win_share column')
        return 2
    print('[OK] base_national_series rows:', len(base_df))

    # scan_base_outliers (may be empty for tiny windows; just check columns)
    out_df = scan_base_outliers(store_glob, args.ds, args.mover_ind, start, end, window=14, z_thresh=2.5)
    if not set(['the_date','winner']).issubset(out_df.columns):
        print('[FAIL] scan_base_outliers missing required columns')
        return 2
    print('[OK] scan_base_outliers rows:', len(out_df))

    if args.with_cube:
        cube_path = os.path.join(os.getcwd(), 'current_run_duckdb', 'win_cube_mover.csv')
        ensure_cube(store_glob, args.ds, args.mover_ind, cube_path)
        validate_cube(cube_path)
        print('[OK] cube built and validated at', cube_path)

    print('[SUCCESS] Smoke tests passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

