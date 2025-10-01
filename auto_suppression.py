#!/usr/bin/env python3
import os
import sys
import math
import argparse
from datetime import date, datetime
from typing import List, Tuple

import duckdb
import pandas as pd


def get_store_glob(store_dir: str) -> str:
    if os.path.isdir(store_dir):
        return os.path.join(store_dir, '**', '*.parquet')
    if store_dir.endswith('.parquet'):
        return store_dir
    return os.path.join(store_dir, '*.parquet')


def load_dataset(ds_glob: str, ds: str, mover_ind: str) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        conds = [f"ds = '{ds}'"] if ds else []
        if mover_ind in ('True','False'):
            conds.append(f"mover_ind = {mover_ind.upper()}")
        where = f"WHERE {' AND '.join(conds)}" if conds else ''
        q = f"""
        SELECT CAST(the_date AS DATE) AS d, winner, loser, dma_name, mover_ind,
               adjusted_wins::DOUBLE AS wins
        FROM parquet_scan('{ds_glob}')
        {where}
        """
        return duckdb.query(q).to_df()
    finally:
        con.close()


def compute_national_share(df: pd.DataFrame) -> pd.DataFrame:
    tot = df.groupby('d', as_index=False)['wins'].sum().rename(columns={'wins': 'T'})
    wtot = df.groupby(['d', 'winner'], as_index=False)['wins'].sum().rename(columns={'wins': 'W'})
    daily = wtot.merge(tot, on='d', how='left')
    daily['share'] = daily['W'] / daily['T'].replace({0: pd.NA})
    return daily


def national_z_windowed(daily: pd.DataFrame, window: int) -> pd.DataFrame:
    df = daily.copy()
    df['day_type'] = pd.to_datetime(df['d']).dt.dayofweek.map(lambda x: 'Sat' if x == 6 else ('Sun' if x == 0 else 'Weekday'))
    df = df.sort_values(['winner', 'day_type', 'd'])

    def add_roll(g: pd.DataFrame):
        s = g['share']
        mu = s.rolling(window=window, min_periods=2).mean()
        sig = s.rolling(window=window, min_periods=2).std(ddof=1)
        z = (s - mu) / sig
        g = g.copy()
        g['mu_roll'] = mu
        g['sigma_roll'] = sig
        g['z_roll'] = z
        return g

    out = df.groupby(['winner', 'day_type'], group_keys=False).apply(add_roll)
    return out[['d', 'winner', 'share', 'mu_roll', 'sigma_roll', 'z_roll', 'day_type']]


def required_remove_to_z(W: float, T: float, mu: float, sigma: float, z_thresh: float) -> int:
    if sigma is None or not (sigma > 0):
        return 0
    target = mu + (z_thresh - 0.01) * sigma
    if target >= 1.0:
        return 0
    need = max(0.0, (W - target * T) / (1.0 - target))
    return int(math.ceil(need))


def monthly_group_stats(df: pd.DataFrame, d: pd.Timestamp, winner: str, loser: str, mover_flag) -> pd.DataFrame:
    day_df = df[(df['d'] == d) & (df['winner'] == winner) & (df['loser'] == loser) & (df['mover_ind'] == mover_flag)]
    if day_df.empty:
        return pd.DataFrame(columns=['dma_name', 'wins', 'base_mean', 'base_std', 'z'])
    mon = pd.Period(d, freq='M')
    mon_df = df[(pd.PeriodIndex(pd.to_datetime(df['d']), freq='M') == mon) & (df['winner'] == winner) & (df['loser'] == loser) & (df['mover_ind'] == mover_flag)]
    per_day = mon_df.groupby(['dma_name', 'd'], as_index=False)['wins'].sum().rename(columns={'wins': 'wins_day'})
    base = per_day[per_day['d'] != d].groupby('dma_name', as_index=False).agg(base_mean=('wins_day', 'mean'), base_std=('wins_day', 'std'))
    today = day_df.groupby('dma_name', as_index=False)['wins'].sum()
    frame = today.merge(base, on='dma_name', how='left').fillna({'base_mean': 0.0, 'base_std': 0.0})

    def zfun(r):
        if r['base_std'] and r['base_std'] > 0:
            return (r['wins'] - r['base_mean']) / r['base_std']
        return float('inf') if (r['wins'] > r['base_mean']) else 0.0

    frame['z'] = frame.apply(zfun, axis=1)
    return frame


def auto_suppress_for_day(df: pd.DataFrame, d: pd.Timestamp, winner: str, pair_z_thresh: float) -> pd.DataFrame:
    plan_rows = []
    day_w = df[(df['d'] == d) & (df['winner'] == winner)]
    if day_w.empty:
        return pd.DataFrame(columns=['d', 'winner', 'loser', 'mover_ind', 'dma_name', 'rm'])
    for mover_flag in sorted(day_w['mover_ind'].unique()):
        losers = sorted(day_w[day_w['mover_ind'] == mover_flag]['loser'].unique())
        for loser in losers:
            frame = monthly_group_stats(df, d, winner, loser, mover_flag)
            if frame.empty:
                continue
            for _, r in frame.sort_values('wins', ascending=False).iterrows():
                wins = int(round(r['wins']))
                mu = float(r['base_mean']); z = float(r['z'])
                rm = 0
                if z > pair_z_thresh:
                    if mu < 5 and wins > mu:
                        rm = wins
                    elif wins > mu:
                        rm = int(max(0, math.ceil(wins - mu)))
                if rm > 0:
                    plan_rows.append({'d': d, 'winner': winner, 'loser': loser, 'mover_ind': mover_flag,
                                      'dma_name': r['dma_name'], 'rm': rm})
    return pd.DataFrame(plan_rows)


def apply_plan(df: pd.DataFrame, plan: pd.DataFrame) -> pd.DataFrame:
    if plan is None or plan.empty:
        return df.copy()
    key_cols = ['d', 'winner', 'loser', 'mover_ind', 'dma_name']
    plan2 = plan.groupby(key_cols, as_index=False)['rm'].sum().copy()
    if not pd.api.types.is_datetime64_any_dtype(plan2['d']):
        plan2['d'] = pd.to_datetime(plan2['d'])
    df2 = df.copy()
    grp = df2.groupby(key_cols, as_index=False)['wins'].sum().rename(columns={'wins': 'group_wins'})
    df2 = df2.merge(grp, on=key_cols, how='left')
    df2 = df2.merge(plan2, on=key_cols, how='left')
    df2['rm'] = df2['rm'].fillna(0.0)
    df2['rm_row'] = df2.apply(lambda r: min(r['wins'], r['rm'] * (r['wins'] / r['group_wins']) if r['group_wins'] else 0.0), axis=1)
    df2['wins'] = (df2['wins'] - df2['rm_row']).clip(lower=0)
    return df2.drop(columns=['group_wins', 'rm', 'rm_row'])


def distributed_fill(df_after_auto: pd.DataFrame, d: pd.Timestamp, winner: str, need: int) -> pd.DataFrame:
    day = df_after_auto[(df_after_auto['d'] == d) & (df_after_auto['winner'] == winner)].copy()
    agg = day.groupby(['loser', 'mover_ind', 'dma_name'], as_index=False)['wins'].sum()
    agg = agg[agg['wins'] > 0].copy()
    if agg.empty or need <= 0:
        return pd.DataFrame(columns=['d', 'winner', 'loser', 'mover_ind', 'dma_name', 'rm'])
    m = len(agg)
    base = need // m
    alloc = agg['wins'].clip(upper=base).astype(int)
    allocated = int(alloc.sum())
    remaining = need - allocated
    if remaining > 0:
        agg['residual'] = (agg['wins'] - alloc).astype(int)
        ordered = agg.sort_values(['residual', 'wins'], ascending=[False, False]).index.tolist()
        give = [i for i in ordered if agg.loc[i, 'residual'] > 0][:remaining]
        if give:
            locs = [agg.index.get_loc(i) for i in give]
            alloc.iloc[locs] += 1
    plan = agg.copy()
    plan['rm'] = alloc
    plan['d'] = d
    plan['winner'] = winner
    return plan[['d', 'winner', 'loser', 'mover_ind', 'dma_name', 'rm']]


def main():
    ap = argparse.ArgumentParser(description='Auto suppression planner (national outliers → granular removals)')
    ap.add_argument('--store-dir', default=os.path.expanduser('~/codebase-comparison/duckdb_partitioned_store'))
    ap.add_argument('--ds', default='gamoshi')
    ap.add_argument('--mover-ind', default='False', choices=['True', 'False'])
    ap.add_argument('--start', default='2025-08-01')
    ap.add_argument('--end', default='2025-08-31')
    ap.add_argument('--window', type=int, default=14)
    ap.add_argument('--nat-z', type=float, default=2.5)
    ap.add_argument('--pair-z', type=float, default=1.5)
    ap.add_argument('--out', default=os.path.expanduser('~/codebase-comparison/suppression_tools/suppressions/auto_suppression_aug_gamoshi_mover0.csv'))
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    ds_glob = get_store_glob(args.store_dir)
    df = load_dataset(ds_glob, args.ds, args.mover_ind)
    if df.empty:
        print('No data found for the selected filters.')
        sys.exit(1)

    # Filter display window for targets
    start_ts = pd.to_datetime(args.start)
    end_ts = pd.to_datetime(args.end)

    # Compute national rolling z
    nat = compute_national_share(df)
    nat_win = nat[(nat['d'] >= start_ts) & (nat['d'] <= end_ts)].copy()
    nat_all = national_z_windowed(nat, window=args.window)
    nat_targets = nat_all[(nat_all['d'].between(start_ts, end_ts)) & (nat_all['z_roll'] >= args.nat_z)]

    plan_rows = []
    for _, r in nat_targets.iterrows():
        d = pd.Timestamp(r['d']); w = r['winner']
        # compute required removal using rolling mu/sigma on that date
        cur = nat[(nat['d'] == d) & (nat['winner'] == w)]
        if cur.empty:
            continue
        W = float(cur['W'].iloc[0]); T = float(cur['T'].iloc[0]); s = float(cur['share'].iloc[0])
        zrow = nat_all[(nat_all['d'] == d) & (nat_all['winner'] == w)].tail(1)
        mu = float(zrow['mu_roll'].iloc[0]) if not zrow.empty else 0.0
        sigma = float(zrow['sigma_roll'].iloc[0]) if not zrow.empty else 0.0
        need = required_remove_to_z(W, T, mu, sigma, args.nat_z)
        # Auto suppress natural granular outliers first
        auto = auto_suppress_for_day(df, d, w, args.pair_z)
        sim_auto = apply_plan(df, auto)
        # Remaining needed after auto
        cur_after = compute_national_share(sim_auto)
        cur_row = cur_after[(cur_after['d'] == d) & (cur_after['winner'] == w)]
        if not cur_row.empty and sigma > 0:
            s_after = float(cur_row['share'].iloc[0])
            z_after = (s_after - mu) / sigma
            if z_after >= args.nat_z:
                # compute new required removal from current W,T
                W2 = float(cur_row['W'].iloc[0]); T2 = float(cur_row['T'].iloc[0])
                need2 = required_remove_to_z(W2, T2, mu, sigma, args.nat_z)
                dist = distributed_fill(sim_auto, d, w, need2)
            else:
                dist = pd.DataFrame(columns=['d', 'winner', 'loser', 'mover_ind', 'dma_name', 'rm'])
        else:
            dist = pd.DataFrame(columns=['d', 'winner', 'loser', 'mover_ind', 'dma_name', 'rm'])
        if not auto.empty:
            plan_rows.append(auto)
        if not dist.empty:
            plan_rows.append(dist)

    plan_all = pd.concat(plan_rows, ignore_index=True) if plan_rows else pd.DataFrame(columns=['d', 'winner', 'loser', 'mover_ind', 'dma_name', 'rm'])
    if plan_all.empty:
        print('No suppression needed for the selected window and thresholds.')
        sys.exit(0)

    # Aggregate per key and write CSV
    out_df = plan_all.groupby(['d', 'winner', 'loser', 'mover_ind', 'dma_name'], as_index=False)['rm'].sum().copy()
    out_df = out_df.rename(columns={'d': 'date', 'winner': 'winner', 'loser': 'loser', 'dma_name': 'dma_name', 'mover_ind': 'mover_ind', 'rm': 'remove_units'})
    out_df['date'] = pd.to_datetime(out_df['date']).dt.date
    out_df = out_df[['date', 'winner', 'loser', 'dma_name', 'mover_ind', 'remove_units']]
    out_df.to_csv(args.out, index=False)
    print('Wrote suppression plan to:', args.out)
    print('Rows:', len(out_df))

    # Quick validation summary: per-date z_after
    sim_after = apply_plan(df, plan_all)
    nat_after = compute_national_share(sim_after)
    rows = []
    for _, r in nat_targets.iterrows():
        d = pd.Timestamp(r['d']); w = r['winner']
        mu = float(r['mu_roll']); sigma = float(r['sigma_roll']) if r['sigma_roll'] is not None else 0.0
        base_row = nat[(nat['d'] == d) & (nat['winner'] == w)]
        after_row = nat_after[(nat_after['d'] == d) & (nat_after['winner'] == w)]
        if base_row.empty or after_row.empty or sigma <= 0:
            continue
        z_before = (float(base_row['share'].iloc[0]) - mu) / sigma
        z_after = (float(after_row['share'].iloc[0]) - mu) / sigma
        rows.append({'date': d.date(), 'winner': w, 'z_before': z_before, 'z_after': z_after, 'met': z_after < args.nat_z})
    if rows:
        print('Validation (national z before → after):')
        for r in rows:
            print(r)


if __name__ == '__main__':
    main()
