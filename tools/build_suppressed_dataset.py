#!/usr/bin/env python3
import os
import glob
import argparse
import shutil
import duckdb
import pandas as pd


def pick_suppression_files(supp_glob: str, max_files: int = 5) -> list[str]:
    files = [p for p in glob.glob(os.path.expanduser(supp_glob)) if os.path.isfile(p)]
    files = [p for p in files if not os.path.basename(os.path.dirname(p)).lower().startswith('processed')]
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[:max_files]


def load_suppressions(files: list[str]) -> pd.DataFrame:
    frames = []
    for p in files:
        try:
            df = pd.read_csv(p)
            frames.append(df)
        except Exception:
            pass
    if not frames:
        return pd.DataFrame(columns=['date','winner','mover_ind','loser','dma_name','remove_units'])
    sup = pd.concat(frames, ignore_index=True)
    # normalize columns
    if 'date' not in sup.columns and 'the_date' in sup.columns:
        sup['date'] = sup['the_date']
    for col in ['date','winner','mover_ind','loser','dma_name','remove_units']:
        if col not in sup.columns:
            sup[col] = None
    sup['date'] = pd.to_datetime(sup['date'], errors='coerce').dt.date
    sup['remove_units'] = pd.to_numeric(sup['remove_units'], errors='coerce').fillna(0).astype(int)
    # coerce mover_ind to bool
    sup['mover_ind'] = sup['mover_ind'].apply(lambda v: True if str(v).strip().lower() in ('true','1') else (False if str(v).strip().lower() in ('false','0') else None))
    # drop invalid rows
    sup = sup.dropna(subset=['date','winner','loser','dma_name','mover_ind'])
    return sup[['date','winner','mover_ind','loser','dma_name','remove_units']]


def build_suppressed(store_glob: str, sup_df: pd.DataFrame, out_dir: str, min_wins: float = 1.0, partition_by: tuple[str, ...] = ("ds","p_mover_ind","year","month","day","the_date")) -> str:
    os.makedirs(out_dir, exist_ok=True)
    con = duckdb.connect()
    try:
        # Speed up
        try:
            thr = os.cpu_count() or 4
            con.execute(f"PRAGMA threads={thr}")
        except Exception:
            pass
        con.register('sup_df', sup_df)
        q = f"""
        WITH sup AS (
          SELECT CAST(date AS DATE) AS d,
                 winner,
                 mover_ind::BOOLEAN AS mover_ind,
                 loser,
                 dma_name,
                 CAST(remove_units AS BIGINT) AS rm
          FROM sup_df
        ), sup_agg AS (
          SELECT d, winner, mover_ind, loser, dma_name, SUM(rm) AS rm
          FROM sup GROUP BY 1,2,3,4,5
        ), ds AS (
          SELECT * FROM parquet_scan('{store_glob}')
        ), cand AS (
          SELECT ds.element_id,
                 CAST(ds.the_date AS DATE) AS d,
                 ds.winner, ds.mover_ind, ds.loser, ds.dma_name,
                 ds.adjusted_wins::DOUBLE AS adjusted_wins,
                 s.rm,
                 SUM(ds.adjusted_wins) OVER (
                   PARTITION BY CAST(ds.the_date AS DATE), ds.winner, ds.mover_ind, ds.loser, ds.dma_name
                   ORDER BY ds.adjusted_wins DESC, ds.element_id
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                 ) AS csum
          FROM ds
          JOIN sup_agg s
            ON CAST(ds.the_date AS DATE) = s.d
           AND ds.winner = s.winner
           AND ds.mover_ind = s.mover_ind
           AND ds.loser = s.loser
           AND ds.dma_name = s.dma_name
          WHERE ds.adjusted_wins >= {min_wins}
        ), to_drop AS (
          SELECT element_id FROM cand WHERE csum <= rm
        ), result AS (
          SELECT ds.*,
                 CAST(strftime('%Y', ds.the_date) AS VARCHAR) AS year,
                 CAST(strftime('%m', ds.the_date) AS VARCHAR) AS month,
                 CAST(strftime('%d', ds.the_date) AS VARCHAR) AS day
          FROM ds
          LEFT JOIN to_drop t ON t.element_id = ds.element_id
          WHERE t.element_id IS NULL
            AND ds.the_date IS NOT NULL
            AND ds.ds IS NOT NULL
            AND ds.mover_ind IS NOT NULL
        )
        SELECT * FROM result
        """
        # Write partitioned by requested columns (default: ds, mover_ind, year, month, day, the_date)
        inner = q.strip()
        if inner.endswith(';'):
            inner = inner[:-1]
        part_cols = ", ".join(partition_by)
        # Allow overwriting existing partition files to simplify iterative runs
        con.execute(f"COPY ({inner}) TO '{out_dir}' (FORMAT PARQUET, PARTITION_BY ({part_cols}), OVERWRITE)" )
    finally:
        con.close()
    return out_dir


def main():
    ap = argparse.ArgumentParser(description='Build suppressed dataset by anti-joining top-down removed rows (no partial rows)')
    ap.add_argument('--store-glob', default=os.path.expanduser('~/codebase-comparison/duckdb_partitioned_store/**/*.parquet'))
    ap.add_argument('--suppressions-glob', default=os.path.expanduser('~/codebase-comparison/suppression_tools/suppressions/*.csv'))
    ap.add_argument('--max-files', type=int, default=5)
    ap.add_argument('--output-dir', default=os.path.expanduser('~/codebase-comparison/duckdb_partitioned_store_suppressed'))
    ap.add_argument('--processed-dir', default=os.path.expanduser('~/codebase-comparison/suppression_tools/suppressions/processed'))
    ap.add_argument('--clean', action='store_true', help='Delete contents of the output directory before writing')
    ap.add_argument('--min-wins', type=float, default=1.0)
    args = ap.parse_args()

    files = pick_suppression_files(args.suppressions_glob, max_files=args.max_files)
    if not files:
        print('No suppression files found matching glob.')
        return
    sup_df = load_suppressions(files)
    if sup_df.empty:
        print('Suppression files did not contain valid rows; aborting.')
        return

    # Clean output directory if requested
    if args.clean and os.path.isdir(args.output_dir):
        for root, dirs, files in os.walk(args.output_dir, topdown=False):
            for name in files:
                try:
                    os.remove(os.path.join(root, name))
                except Exception:
                    pass
            for name in dirs:
                try:
                    os.rmdir(os.path.join(root, name))
                except Exception:
                    pass

    out_path = build_suppressed(args.store_glob, sup_df, args.output_dir, min_wins=args.min_wins, partition_by=("ds","p_mover_ind","year","month","day","the_date"))
    print('Wrote suppressed dataset to:', out_path)

    # Move processed files
    os.makedirs(args.processed_dir, exist_ok=True)
    for p in files:
        dst = os.path.join(args.processed_dir, os.path.basename(p))
        try:
            shutil.move(p, dst)
        except Exception:
            pass
    print('Moved processed files to:', args.processed_dir)


if __name__ == '__main__':
    main()
