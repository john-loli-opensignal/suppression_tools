#!/usr/bin/env python3
import os
import sys
import argparse


def default_store_glob() -> str:
    here = os.getcwd()
    local = os.path.join(here, "duckdb_partitioned_store", "**", "*.parquet")
    home = os.path.expanduser("~/codebase-comparison/duckdb_partitioned_store/**/*.parquet")
    return local if os.path.exists(os.path.join(here, "duckdb_partitioned_store")) else home


def build_win_cube(store_glob: str, ds: str, mover_ind: str, output_csv: str) -> int:
    try:
        import duckdb
    except Exception:
        print("[ERROR] duckdb not installed. Use uv to install: uv pip install --python .venv duckdb pandas numpy", file=sys.stderr)
        return 2
    mi = 'TRUE' if str(mover_ind) == 'True' else 'FALSE'
    ds_q = str(ds).replace("'", "''")
    q = f"""
    WITH base AS (
      SELECT * FROM parquet_scan('{store_glob}')
    ), filt AS (
      SELECT CAST(the_date AS DATE) AS the_date, ds, mover_ind, winner, loser, dma_name,
             adjusted_wins, adjusted_losses
      FROM base
      WHERE ds = '{ds_q}' AND mover_ind = {mi}
    ), typed AS (
      SELECT *, CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                     WHEN strftime('%w', the_date)='0' THEN 'Sun'
                     ELSE 'Weekday' END AS day_type
      FROM filt
    ), market AS (
      SELECT the_date, SUM(adjusted_wins) AS market_total_wins
      FROM typed GROUP BY 1
    ), nat AS (
      SELECT the_date, winner, SUM(adjusted_wins) AS nat_total_wins
      FROM typed GROUP BY 1,2
    ), nat_metrics AS (
      SELECT n.the_date, n.winner, n.nat_total_wins, m.market_total_wins,
             n.nat_total_wins / NULLIF(m.market_total_wins, 0) AS nat_share_current,
             avg(n.nat_total_wins / NULLIF(m.market_total_wins, 0)) OVER (
               PARTITION BY n.winner,
                 CASE WHEN strftime('%w', n.the_date)='6' THEN 'Sat'
                      WHEN strftime('%w', n.the_date)='0' THEN 'Sun'
                      ELSE 'Weekday' END
               ORDER BY n.the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS nat_mu_share,
             stddev_samp(n.nat_total_wins / NULLIF(m.market_total_wins, 0)) OVER (
               PARTITION BY n.winner,
                 CASE WHEN strftime('%w', n.the_date)='6' THEN 'Sat'
                      WHEN strftime('%w', n.the_date)='0' THEN 'Sun'
                      ELSE 'Weekday' END
               ORDER BY n.the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS nat_sigma_share,
             14 AS nat_mu_window
      FROM nat n JOIN market m USING (the_date)
    ), nat_scored AS (
      SELECT *, CASE WHEN nat_sigma_share>0 THEN (nat_share_current - nat_mu_share)/NULLIF(nat_sigma_share,0) ELSE 0 END AS nat_zscore,
             CASE WHEN nat_sigma_share>0 THEN (nat_share_current - nat_mu_share)/NULLIF(nat_sigma_share,0) ELSE 0 END > 2.5 AS nat_outlier_pos
      FROM nat_metrics
    ), pair AS (
      SELECT the_date, winner, loser, dma_name, SUM(adjusted_wins) AS pair_wins_current
      FROM typed GROUP BY 1,2,3,4
    ), pair_metrics AS (
      SELECT p.*,
             avg(pair_wins_current) OVER (
               PARTITION BY winner, loser, dma_name,
                 CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                      WHEN strftime('%w', the_date)='0' THEN 'Sun'
                      ELSE 'Weekday' END
               ORDER BY the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS pair_mu_wins,
             stddev_samp(pair_wins_current) OVER (
               PARTITION BY winner, loser, dma_name,
                 CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                      WHEN strftime('%w', the_date)='0' THEN 'Sun'
                      ELSE 'Weekday' END
               ORDER BY the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS pair_sigma_wins,
             14 AS pair_mu_window
      FROM pair p
    ), pair_scored AS (
      SELECT *,
        CASE WHEN pair_sigma_wins>0 THEN (pair_wins_current - pair_mu_wins)/NULLIF(pair_sigma_wins,0) ELSE 0 END AS pair_z,
        (pair_mu_wins IS NOT NULL AND pair_wins_current > 1.3*pair_mu_wins) AS pct_outlier_pos,
        CASE WHEN pair_sigma_wins>0 THEN (pair_wins_current - pair_mu_wins)/NULLIF(pair_sigma_wins,0) ELSE 0 END > 2.0 AS pair_outlier_pos,
        COALESCE(pair_mu_wins, 0) < 2.0 AS rare_pair,
        (pair_mu_wins IS NULL OR pair_mu_wins = 0) AS new_pair
      FROM pair_metrics
    )
    SELECT ps.the_date, ps.winner, ps.loser, ps.dma_name,
           ps.pair_wins_current, ps.pair_mu_wins, ps.pair_sigma_wins, ps.pair_mu_window, ps.pair_z,
           ps.pair_outlier_pos, ps.pct_outlier_pos, ps.rare_pair, ps.new_pair,
           nm.nat_total_wins, nm.market_total_wins AS nat_market_wins, nm.nat_share_current, nm.nat_mu_share, nm.nat_sigma_share, nm.nat_mu_window,
           ns.nat_zscore, ns.nat_outlier_pos
    FROM pair_scored ps
    JOIN nat_metrics nm ON nm.the_date = ps.the_date AND nm.winner = ps.winner
    JOIN nat_scored ns   ON ns.the_date = ps.the_date AND ns.winner = ps.winner
    ORDER BY 1,2,3,4;
    """
    con = duckdb.connect()
    try:
        df = con.execute(q).df()
        os.makedirs(os.path.dirname(os.path.expanduser(output_csv)), exist_ok=True)
        df.to_csv(os.path.expanduser(output_csv), index=False)
        print(f"[INFO] Wrote cube: {output_csv} ({len(df):,} rows)")
        return 0
    except Exception as e:
        print(f"[ERROR] Failed to build cube: {e}", file=sys.stderr)
        return 1
    finally:
        con.close()


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Build national + pair outlier cube to CSV")
    p.add_argument("--store", default=default_store_glob(), help="Parquet glob for partitioned store")
    p.add_argument("--ds", default="gamoshi", help="Dataset id (ds)")
    p.add_argument("--mover-ind", choices=["True","False"], required=True, help="Mover indicator filter")
    p.add_argument("-o", "--output", required=True, help="Output CSV path")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    return build_win_cube(args.store, args.ds, args.mover_ind, args.output)


if __name__ == "__main__":
    sys.exit(main())

