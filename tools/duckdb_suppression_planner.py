#!/usr/bin/env python3
import os
import argparse
import duckdb
import os
import pandas as pd


def run(store_glob: str, dates: list[str], ds: str = 'gamoshi', mover_ind: str = 'False', winner: str = 'AT&T', out_csv: str | None = None):
    dates_csv = ",".join([f"DATE '{d}'" for d in dates])
    con = duckdb.connect()
    try:
        thr = os.cpu_count() or 4
        con.execute(f"PRAGMA threads={thr}")
    except Exception:
        pass
    q = f"""
    -- Base filtered dataset
    WITH base AS (
        SELECT CAST(the_date AS DATE) AS the_date,
               winner,
               loser,
               dma_name,
               adjusted_wins::DOUBLE AS wins
        FROM parquet_scan('{store_glob}')
        WHERE ds = '{ds}' AND mover_ind = {mover_ind.upper()}
    ), att AS (
        SELECT * FROM base WHERE winner = '{winner.replace("'","''")}'
    ), market AS (
        SELECT the_date, SUM(wins) AS T FROM base GROUP BY 1
    ), att_daily AS (
        SELECT the_date, SUM(wins) AS W FROM att GROUP BY 1
    ), nat AS (
        SELECT m.the_date,
               a.W,
               m.T,
               a.W / NULLIF(m.T, 0) AS share,
               CASE WHEN strftime('%w', m.the_date)='6' THEN 'Sat'
                    WHEN strftime('%w', m.the_date)='0' THEN 'Sun'
                    ELSE 'Weekday' END AS day_type
        FROM market m
        JOIN att_daily a USING (the_date)
    ), nat_roll AS (
        SELECT the_date, W, T, share, day_type,
               COUNT(*) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS c28,
               AVG(share) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS mu28,
               STDDEV_SAMP(share) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS sd28,
               COUNT(*) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS c14,
               AVG(share) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS mu14,
               STDDEV_SAMP(share) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS sd14,
               AVG(share) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS mu_all,
               STDDEV_SAMP(share) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS sd_all
        FROM nat
    ), nat_target AS (
        SELECT the_date, W, T, share, day_type,
               CASE WHEN c28 >= 28 THEN 28 WHEN c14 >= 14 THEN 14 ELSE 0 END AS mu_window,
               CASE WHEN c28 >= 28 THEN mu28 WHEN c14 >= 14 THEN mu14 ELSE mu_all END AS mu_target,
               CASE WHEN c28 >= 28 THEN sd28 WHEN c14 >= 14 THEN sd14 ELSE sd_all END AS sd_target
        FROM nat_roll
    ), nat_need AS (
        SELECT the_date, W, T, share, day_type, mu_target, sd_target, mu_window,
               CAST(CEIL(GREATEST((W - mu_target*T) / NULLIF(1 - mu_target, 0), 0)) AS BIGINT) AS target_remove
        FROM nat_target
        WHERE the_date IN ({dates_csv})
    ), att_dma_daily AS (
        SELECT the_date, loser, dma_name, SUM(wins) AS wins_day
        FROM att
        GROUP BY 1,2,3
    ), pair_roll AS (
        SELECT d.the_date, d.loser, d.dma_name, d.wins_day,
               COUNT(*) OVER (PARTITION BY d.loser, d.dma_name ORDER BY d.the_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS pc28,
               AVG(d.wins_day) OVER (PARTITION BY d.loser, d.dma_name ORDER BY d.the_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS pmu28,
               STDDEV_SAMP(d.wins_day) OVER (PARTITION BY d.loser, d.dma_name ORDER BY d.the_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS ps28,
               COUNT(*) OVER (PARTITION BY d.loser, d.dma_name ORDER BY d.the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS pc14,
               AVG(d.wins_day) OVER (PARTITION BY d.loser, d.dma_name ORDER BY d.the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS pmu14,
               STDDEV_SAMP(d.wins_day) OVER (PARTITION BY d.loser, d.dma_name ORDER BY d.the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS ps14,
               AVG(d.wins_day) OVER (PARTITION BY d.loser, d.dma_name ORDER BY d.the_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS pmu_all,
               STDDEV_SAMP(d.wins_day) OVER (PARTITION BY d.loser, d.dma_name ORDER BY d.the_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS ps_all
        FROM att_dma_daily d
    ), pair_stats AS (
        SELECT the_date, loser, dma_name, wins_day,
               CASE WHEN pc28 >= 28 THEN 28 WHEN pc14 >= 14 THEN 14 ELSE 0 END AS mu_window_pair,
               CASE WHEN pc28 >= 28 THEN pmu28 WHEN pc14 >= 14 THEN pmu14 ELSE pmu_all END AS mu_pair,
               CASE WHEN pc28 >= 28 THEN ps28 WHEN pc14 >= 14 THEN ps14 ELSE ps_all END AS sigma_pair
        FROM pair_roll
    ), stage1_candidates AS (
        SELECT p.the_date, p.loser, p.dma_name, p.wins_day,
               COALESCE(mu_pair, 0) AS mu_pair,
               COALESCE(sigma_pair, 0) AS sigma_pair,
               CASE WHEN COALESCE(sigma_pair,0) > 0 THEN (p.wins_day - mu_pair) / sigma_pair
                    WHEN p.wins_day > COALESCE(mu_pair,0) THEN 1e9 ELSE 0 END AS z_pair,
               CAST(CEIL(GREATEST(p.wins_day - COALESCE(mu_pair,0), 0)) AS BIGINT) AS rm_pair
        FROM pair_stats p
        WHERE p.the_date IN ({dates_csv})
    ), stage1_ranked AS (
        SELECT c.*, n.target_remove,
               SUM(rm_pair) OVER (PARTITION BY c.the_date ORDER BY z_pair DESC, wins_day DESC, loser, dma_name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_prev
        FROM stage1_candidates c
        JOIN nat_need n USING (the_date)
        WHERE z_pair > 1.5 AND rm_pair > 0 AND n.target_remove > 0
    ), stage1 AS (
        SELECT the_date, loser, dma_name, wins_day, mu_pair, sigma_pair, z_pair, rm_pair,
               CASE WHEN cum_prev >= target_remove THEN 0
                    ELSE LEAST(rm_pair, target_remove - cum_prev) END AS rm1
        FROM stage1_ranked
    ), stage1_sum AS (
        SELECT the_date, SUM(rm1) AS removed1
        FROM stage1
        GROUP BY 1
    ), remaining AS (
        SELECT n.the_date, n.mu_target, n.target_remove,
               GREATEST(n.target_remove - COALESCE(s.removed1, 0), 0) AS remaining_need
        FROM nat_need n
        LEFT JOIN stage1_sum s USING (the_date)
    ), capacities AS (
        SELECT d.the_date, d.loser, d.dma_name,
               d.wins_day - COALESCE(s.rm1, 0) AS cap
        FROM att_dma_daily d
        LEFT JOIN stage1 s USING (the_date, loser, dma_name)
        WHERE d.the_date IN ({dates_csv})
    ), stage2_ranked AS (
        SELECT c.*, r.remaining_need,
               SUM(cap) OVER (PARTITION BY c.the_date ORDER BY cap DESC, loser, dma_name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cum_prev
        FROM capacities c
        JOIN remaining r USING (the_date)
        WHERE r.remaining_need > 0 AND cap > 0
    ), stage2 AS (
        SELECT the_date, loser, dma_name, cap,
               CASE WHEN cum_prev >= remaining_need THEN 0
                    ELSE LEAST(cap, remaining_need - cum_prev) END AS rm2
        FROM stage2_ranked
    ), plan AS (
        SELECT s.the_date AS date,
                '{winner.replace("'","''")}' AS winner,
                {mover_ind.upper()} AS mover_ind,
               s.loser,
               s.dma_name,
               s.rm1 AS remove_units,
                'auto' AS stage,
               n.share AS nat_share_current,
               n.mu_target AS nat_mu_share,
               n.sd_target AS nat_sigma_share,
               n.mu_window AS nat_mu_window,
               CASE WHEN n.sd_target > 0 THEN (n.share - n.mu_target) / n.sd_target ELSE NULL END AS nat_zscore
              , ps1.wins_day AS pair_wins_current,
              ps1.mu_pair AS pair_mu_wins,
              ps1.sigma_pair AS pair_sigma_wins,
              ps1.mu_window_pair AS pair_mu_window,
              CASE WHEN ps1.sigma_pair > 0 THEN (ps1.wins_day - ps1.mu_pair) / ps1.sigma_pair ELSE NULL END AS pair_z
        FROM stage1 s
        JOIN nat_need n ON n.the_date = s.the_date
        LEFT JOIN pair_stats ps1 ON ps1.the_date = s.the_date AND ps1.loser = s.loser AND ps1.dma_name = s.dma_name
        WHERE s.rm1 > 0
        UNION ALL
        SELECT s.the_date AS date,
                '{winner.replace("'","''")}' AS winner,
                {mover_ind.upper()} AS mover_ind,
               s.loser,
               s.dma_name,
               s.rm2 AS remove_units,
                'distributed' AS stage,
               n.share AS nat_share_current,
               n.mu_target AS nat_mu_share,
               n.sd_target AS nat_sigma_share,
               n.mu_window AS nat_mu_window,
               CASE WHEN n.sd_target > 0 THEN (n.share - n.mu_target) / n.sd_target ELSE NULL END AS nat_zscore
              , ps2.wins_day AS pair_wins_current,
              ps2.mu_pair AS pair_mu_wins,
              ps2.sigma_pair AS pair_sigma_wins,
              ps2.mu_window_pair AS pair_mu_window,
              CASE WHEN ps2.sigma_pair > 0 THEN (ps2.wins_day - ps2.mu_pair) / ps2.sigma_pair ELSE NULL END AS pair_z
        FROM stage2 s
        JOIN nat_need n ON n.the_date = s.the_date
        LEFT JOIN pair_stats ps2 ON ps2.the_date = s.the_date AND ps2.loser = s.loser AND ps2.dma_name = s.dma_name
        WHERE s.rm2 > 0
    ), summary AS (
        SELECT r.the_date,
               '{winner.replace("'","''")}' AS winner,
               r.mu_target,
               r.target_remove,
               COALESCE(s1.removed1, 0) AS removed_auto,
               COALESCE(s2.removed2, 0) AS removed_distributed,
               COALESCE(s1.removed1, 0) + COALESCE(s2.removed2, 0) AS removed_total
        FROM remaining r
        LEFT JOIN (SELECT the_date, SUM(remove_units) AS removed1 FROM plan WHERE stage='auto' GROUP BY 1) s1 USING (the_date)
        LEFT JOIN (SELECT the_date, SUM(remove_units) AS removed2 FROM plan WHERE stage='distributed' GROUP BY 1) s2 USING (the_date)
    )
    SELECT * FROM plan ORDER BY date, stage, loser, dma_name;
    """
    plan_df = con.execute(q).df()
    if out_csv:
        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        plan_df.to_csv(out_csv, index=False)
        print('Wrote suppression plan CSV:', out_csv, 'rows:', len(plan_df))

    # Also print summary
    summary_q = f"""
    WITH base AS (
        SELECT CAST(the_date AS DATE) AS the_date,
               winner,
               loser,
               dma_name,
               adjusted_wins::DOUBLE AS wins
        FROM parquet_scan('{store_glob}')
        WHERE ds = '{ds}' AND mover_ind = {mover_ind.upper()}
    ), att AS (
        SELECT * FROM base WHERE winner = '{winner.replace("'","''")}'
    ), market AS (
        SELECT the_date, SUM(wins) AS T FROM base GROUP BY 1
    ), att_daily AS (
        SELECT the_date, SUM(wins) AS W FROM att GROUP BY 1
    ), nat AS (
        SELECT m.the_date, a.W, m.T, a.W/NULLIF(m.T,0) AS share,
               CASE WHEN strftime('%w', m.the_date)='6' THEN 'Sat'
                    WHEN strftime('%w', m.the_date)='0' THEN 'Sun'
                    ELSE 'Weekday' END AS day_type
        FROM market m JOIN att_daily a USING (the_date)
    ), nat_roll AS (
        SELECT the_date, W, T, share, day_type,
               COUNT(*) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS c28,
               AVG(share) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS mu28,
               COUNT(*) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS c14,
               AVG(share) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS mu14,
               AVG(share) OVER (PARTITION BY day_type ORDER BY the_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS mu_all
        FROM nat
    ), nat_target AS (
        SELECT the_date, W, T, share, day_type,
               CASE WHEN c28 >= 28 THEN mu28 WHEN c14 >= 14 THEN mu14 ELSE mu_all END AS mu_target
        FROM nat_roll
    ), nat_need AS (
        SELECT the_date, W, T, share, day_type, mu_target,
               CAST(CEIL(GREATEST((W - mu_target*T) / NULLIF(1 - mu_target, 0), 0)) AS BIGINT) AS target_remove
        FROM nat_target
        WHERE the_date IN ({dates_csv})
    ), p AS (
        SELECT * FROM read_csv_auto('{out_csv}')
    ), agg AS (
        SELECT the_date, SUM(CASE WHEN stage='auto' THEN remove_units ELSE 0 END) AS removed_auto,
               SUM(CASE WHEN stage='distributed' THEN remove_units ELSE 0 END) AS removed_distributed
        FROM p GROUP BY 1
    )
    SELECT n.the_date, '{winner.replace("'","''")}' AS winner, n.mu_target, n.target_remove,
           COALESCE(agg.removed_auto,0) AS removed_auto,
           COALESCE(agg.removed_distributed,0) AS removed_distributed,
           COALESCE(agg.removed_auto,0) + COALESCE(agg.removed_distributed,0) AS removed_total
    FROM nat_need n
    LEFT JOIN agg USING (the_date)
    ORDER BY 1;
    """
    if out_csv:
        summary_df = con.execute(summary_q).df()
        print('\nSummary by date:')
        if not summary_df.empty:
            print(summary_df.to_string(index=False))
        else:
            print('No rows.')
    con.close()
    return plan_df


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='DuckDB-only suppression planner for AT&T on selected dates')
    ap.add_argument('--store-dir', default=os.path.join(os.getcwd(), 'duckdb_partitioned_store'))
    ap.add_argument('--ds', default='gamoshi')
    ap.add_argument('--mover-ind', default='False', choices=['True','False'])
    ap.add_argument('--winner', default='AT&T')
    ap.add_argument('--dates', nargs='+', default=['2025-08-14','2025-08-15','2025-08-16','2025-08-17'])
    ap.add_argument('-o', '--out', default=os.path.join(os.getcwd(), 'suppressions', 'att_duckdb_plan_aug14_17.csv'))
    args = ap.parse_args()
    store_glob = args.store_dir if args.store_dir.endswith('.parquet') else os.path.join(args.store_dir, '**', '*.parquet')
    run(store_glob, args.dates, ds=args.ds, mover_ind=args.mover_ind, winner=args.winner, out_csv=args.out)
