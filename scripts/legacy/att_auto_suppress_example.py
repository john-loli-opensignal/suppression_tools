#!/usr/bin/env python3
import os
import math
import argparse
from datetime import datetime
from typing import Iterable

import duckdb
import pandas as pd


def get_store_glob(store_dir: str) -> str:
    if os.path.isdir(store_dir):
        return os.path.join(store_dir, '**', '*.parquet')
    if store_dir.endswith('.parquet'):
        return store_dir
    return os.path.join(store_dir, '*.parquet')


def load_filtered(ds_glob: str, ds: str, mover_ind: str) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        where = f"WHERE ds = '{ds}' AND mover_ind = {mover_ind.upper()}"
        q = f"""
        SELECT CAST(the_date AS DATE) AS d,
               winner,
               loser,
               dma_name,
               mover_ind,
               adjusted_wins::DOUBLE AS wins
        FROM parquet_scan('{ds_glob}')
        {where}
        """
        return con.execute(q).fetchdf()
    finally:
        con.close()


def compute_national_share(df: pd.DataFrame) -> pd.DataFrame:
    tot = df.groupby('d', as_index=False)['wins'].sum().rename(columns={'wins': 'T'})
    wtot = df.groupby(['d', 'winner'], as_index=False)['wins'].sum().rename(columns={'wins': 'W'})
    daily = wtot.merge(tot, on='d', how='left')
    daily['share'] = daily['W'] / daily['T'].replace({0: pd.NA})
    return daily


def dow_of(ts) -> int:
    return pd.Timestamp(ts).dayofweek


def rolling_mean_std(series: pd.Series, window: int) -> tuple[float, float]:
    if len(series) == 0:
        return 0.0, 0.0
    s = series.tail(window)
    mu = float(s.mean()) if len(s) > 0 else 0.0
    sigma = float(s.std(ddof=1)) if len(s) >= 3 else 0.0
    return mu, sigma


def national_target_mu(daily: pd.DataFrame, d, min_same_dow_weekend=10) -> float:
    w = daily['winner'].iloc[0]
    hist = daily[(daily['winner'] == w) & (daily['d'] < pd.Timestamp(d))].copy()
    if hist.empty:
        return 0.0
    is_weekend = dow_of(d) in (5, 6, 0) and (dow_of(d) in (6, 0))  # treat Sat/Sun specially
    base = hist[hist['d'].apply(lambda x: dow_of(x) == dow_of(d))]
    if len(base) >= 28:
        mu, _ = rolling_mean_std(base['share'], 28)
        return mu
    if len(base) >= (14 if not is_weekend else min_same_dow_weekend):
        mu, _ = rolling_mean_std(base['share'], 14)
        return mu
    # fallback: use all history
    return float(hist['share'].mean())


def removal_to_target(W: float, T: float, mu_target: float) -> int:
    if mu_target <= 0 or mu_target >= 1:
        return 0
    need = (W - mu_target * T) / (1.0 - mu_target)
    return int(max(0, math.ceil(need)))


def pair_dma_stats(df: pd.DataFrame, d, loser: str, dma_name: str) -> tuple[float, float, float]:
    # returns (wins_today, mu_pair, sigma_pair) using DOW-partitioned rolling 28/14 fallback
    day_df = df[(df['d'] == pd.Timestamp(d)) & (df['loser'] == loser) & (df['dma_name'] == dma_name)]
    wins_today = float(day_df['wins'].sum()) if not day_df.empty else 0.0
    hist = df[(df['d'] < pd.Timestamp(d)) & (df['loser'] == loser) & (df['dma_name'] == dma_name)]
    if hist.empty:
        return wins_today, 0.0, 0.0
    base = hist[hist['d'].apply(lambda x: dow_of(x) == dow_of(d))]
    if len(base) >= 28:
        mu, sigma = rolling_mean_std(base['wins'], 28)
        return wins_today, mu, sigma
    if len(base) >= 14:
        mu, sigma = rolling_mean_std(base['wins'], 14)
        return wins_today, mu, sigma
    # fallback to all history
    mu = float(hist['wins'].mean())
    sigma = float(hist['wins'].std(ddof=1)) if len(hist) >= 3 else 0.0
    return wins_today, mu, sigma


def stage1_auto_outliers(df: pd.DataFrame, d, z_thresh_pair=1.5, remaining_need=None) -> pd.DataFrame:
    # compute DMA-level outliers and remove excess down to mu, capped by remaining_need
    day = df[df['d'] == pd.Timestamp(d)].copy()
    agg = day.groupby(['loser', 'dma_name'], as_index=False)['wins'].sum().rename(columns={'wins': 'wins_today'})
    rows = []
    for _, r in agg.iterrows():
        loser = r['loser']; dma = r['dma_name']; wins = float(r['wins_today'])
        w_today, mu, sigma = pair_dma_stats(df, d, loser, dma)
        if sigma > 0:
            z = (w_today - mu) / sigma
        else:
            z = float('inf') if w_today > mu and mu > 0 else (float('inf') if (w_today > 0 and mu == 0) else 0.0)
        excess = max(0.0, w_today - mu)
        rm = int(math.ceil(excess)) if z > z_thresh_pair else 0
        if rm > 0:
            rows.append({'d': pd.Timestamp(d), 'loser': loser, 'dma_name': dma, 'w0': int(round(w_today)), 'mu': mu, 'sigma': sigma, 'z': z, 'rm': rm})
    plan = pd.DataFrame(rows).sort_values(['z', 'w0'], ascending=[False, False]) if rows else pd.DataFrame(columns=['d','loser','dma_name','w0','mu','sigma','z','rm'])
    if plan.empty:
        return plan
    # Cap by remaining_need
    if isinstance(remaining_need, (int, float)) and remaining_need > 0:
        total = int(plan['rm'].sum())
        if total > remaining_need:
            # reduce from bottom of list (lowest z)
            need = int(remaining_need)
            new = []
            for _, r in plan.iterrows():
                take = min(int(r['rm']), max(0, need))
                if take > 0:
                    new.append({**r, 'rm': int(take)})
                    need -= take
                if need <= 0:
                    break
            plan = pd.DataFrame(new)
    return plan


def apply_plan(df: pd.DataFrame, plan: pd.DataFrame) -> pd.DataFrame:
    if plan is None or plan.empty:
        return df.copy()
    key_cols = ['d', 'winner', 'loser', 'dma_name']
    plan2 = plan.groupby(['d', 'loser', 'dma_name'], as_index=False)['rm'].sum().copy()
    if not pd.api.types.is_datetime64_any_dtype(plan2['d']):
        plan2['d'] = pd.to_datetime(plan2['d'])
    df2 = df.copy()
    grp = df2.groupby(key_cols, as_index=False)['wins'].sum().rename(columns={'wins': 'group_wins'})
    df2 = df2.merge(grp, on=key_cols, how='left')
    df2 = df2.merge(plan2, on=['d', 'loser', 'dma_name'], how='left')
    df2['rm'] = df2['rm'].fillna(0.0)
    df2['rm_row'] = df2.apply(lambda r: min(r['wins'], r['rm'] * (r['wins'] / r['group_wins']) if r['group_wins'] else 0.0), axis=1)
    df2['wins'] = (df2['wins'] - df2['rm_row']).clip(lower=0)
    return df2.drop(columns=['group_wins', 'rm', 'rm_row'])


def distributed_fill(df_after: pd.DataFrame, d, remaining_need: int, sort_by_impact: bool = True) -> pd.DataFrame:
    # Allocate remaining_need across (loser,dma) by impact desc (fallback to wins desc)
    if remaining_need is None or remaining_need <= 0:
        return pd.DataFrame(columns=['d','loser','dma_name','rm','stage'])
    day = df_after[df_after['d'] == pd.Timestamp(d)].copy()
    agg_cols = ['loser', 'dma_name']
    agg = day.groupby(agg_cols, as_index=False)['wins'].sum().rename(columns={'wins': 'cap'})
    if agg.empty:
        return pd.DataFrame(columns=['d','loser','dma_name','rm','stage'])
    # If impact exists in source rows, aggregate mean impact
    use_impact = False
    if 'impact' in day.columns and sort_by_impact:
        imp = day.groupby(agg_cols, as_index=False)['impact'].mean()
        agg = agg.merge(imp, on=agg_cols, how='left')
        use_impact = True
    # Base equal allocation up to capacity
    m = len(agg)
    base = int(remaining_need) // m
    alloc = agg['cap'].clip(upper=base).astype(int)
    allocated = int(alloc.sum())
    rem = int(remaining_need) - allocated
    if rem > 0:
        if use_impact:
            agg['_key'] = list(zip(agg['impact'].fillna(0), agg['cap']))
        else:
            agg['_key'] = list(zip(agg['cap'], agg['cap']))
        # Order by impact desc then capacity desc
        idx = agg.sort_values('_key', ascending=False).index.tolist()
        # Give +1 across top rem rows that still have residual capacity
        agg['residual'] = (agg['cap'] - alloc).astype(int)
        give = [i for i in idx if agg.loc[i, 'residual'] > 0][:rem]
        if give:
            locs = [agg.index.get_loc(i) for i in give]
            alloc.iloc[locs] += 1
    plan = agg.copy()
    plan['rm'] = alloc
    plan['d'] = pd.Timestamp(d)
    plan['stage'] = 'distributed'
    return plan[['d', 'loser', 'dma_name', 'rm', 'stage']]


def run(ds_dir: str, dates: Iterable[str], winner: str = 'AT&T', ds: str = 'gamoshi', mover_ind: str = 'False', out_csv: str | None = None):
    ds_glob = get_store_glob(ds_dir)
    df = load_filtered(ds_glob, ds, mover_ind)
    if df.empty:
        print('No data for given filters.')
        return
    df['d'] = pd.to_datetime(df['d'])
    # Compute national daily (all winners)
    nat = compute_national_share(df)
    # Winner-specific slice for granular actions
    df = df[df['winner'] == winner].copy()
    # Build plan
    plan_rows = []
    summaries = []
    for d_str in dates:
        d = pd.Timestamp(d_str)
        # National target based on DOW (28 if possible else 14 else all)
        mu_nat = national_target_mu(nat, d)
        cur = nat[(nat['d'] == d) & (nat['winner'] == winner)]
        if cur.empty:
            continue
        W = float(cur['W'].iloc[0]); T = float(cur['T'].iloc[0])
        target_remove = removal_to_target(W, T, mu_nat)
        # Stage 1: DMA outliers >1.5z
        stg1 = stage1_auto_outliers(df, d, z_thresh_pair=1.5, remaining_need=target_remove)
        df_after = apply_plan(df, stg1)
        removed1 = int(stg1['rm'].sum()) if not stg1.empty else 0
        # Recompute remaining need vs nat target
        nat_after1 = compute_national_share(df_after)
        cur1 = nat_after1[(nat_after1['d'] == d) & (nat_after1['winner'] == winner)]
        if not cur1.empty:
            W1 = float(cur1['W'].iloc[0]); T1 = float(cur1['T'].iloc[0])
            rem_need = removal_to_target(W1, T1, mu_nat)
        else:
            rem_need = 0
        # Stage 2: distributed by impact (fallback to wins)
        stg2 = distributed_fill(df_after, d, rem_need, sort_by_impact=True)
        df_after2 = apply_plan(df_after, stg2)
        removed2 = int(stg2['rm'].sum()) if not stg2.empty else 0
        # Record
        if not stg1.empty:
            tmp = stg1.copy(); tmp['stage'] = 'auto'
            tmp['winner'] = winner
            plan_rows.append(tmp[['d','winner','loser','dma_name','rm','stage']])
        if not stg2.empty:
            tmp2 = stg2.copy(); tmp2['winner'] = winner
            plan_rows.append(tmp2[['d','winner','loser','dma_name','rm','stage']])
        # Summaries
        summaries.append({
            'date': d.date(),
            'winner': winner,
            'mu_nat': mu_nat,
            'removed_auto': removed1,
            'removed_distributed': removed2,
            'removed_total': removed1 + removed2,
            'target_remove_initial': target_remove,
        })
    plan_all = pd.concat(plan_rows, ignore_index=True) if plan_rows else pd.DataFrame(columns=['d','winner','loser','dma_name','rm','stage'])
    if out_csv:
        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        out = plan_all.copy()
        out = out.rename(columns={'d':'date'})
        out['date'] = pd.to_datetime(out['date']).dt.date
        out.to_csv(out_csv, index=False)
        print('Wrote plan to:', out_csv, 'rows:', len(out))
    # Print summaries
    summ = pd.DataFrame(summaries)
    print('\nSummary by date:')
    if not summ.empty:
        print(summ.to_string(index=False))
    else:
        print('No rows.')
    # Breakdown by loser/dma
    if not plan_all.empty:
        br = plan_all.groupby(['loser','dma_name','stage'], as_index=False)['rm'].sum().sort_values('rm', ascending=False)
        print('\nBreakdown by DMA and competitor:')
        print(br.head(40).to_string(index=False))
    else:
        print('\nNo removals planned.')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='AT&T auto suppression for specific dates')
    ap.add_argument('--store-dir', default=os.path.join(os.getcwd(), 'duckdb_partitioned_store'))
    ap.add_argument('--ds', default='gamoshi')
    ap.add_argument('--mover-ind', default='False', choices=['True','False'])
    ap.add_argument('--winner', default='AT&T')
    ap.add_argument('--dates', nargs='+', default=['2025-08-14','2025-08-15','2025-08-16','2025-08-17'])
    ap.add_argument('-o', '--out', default=os.path.join(os.getcwd(), 'suppressions', 'att_aug14_17_plan.csv'))
    args = ap.parse_args()
    run(args.store_dir, args.dates, winner=args.winner, ds=args.ds, mover_ind=args.mover_ind, out_csv=args.out)
