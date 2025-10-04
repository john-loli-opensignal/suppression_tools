#!/usr/bin/env python3
"""
Z-Score Based Distribution Suppression with Census Block Targeting

This script implements an enhanced suppression algorithm that combines:
1. Historical 2-stage distribution (targeted + equalized)
2. Z-score based outlier detection at DMA-pair level
3. Census block level surgical removal
4. First appearance detection
5. Market-aware need calculation

Usage:
    uv run scripts/zscore_distribution_suppression.py --ds gamoshi --dates 2025-06-19 2025-08-15-2025-08-18
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict, Tuple
import pandas as pd
import numpy as np
import json

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools import db


def parse_date_arg(date_str: str) -> List[date]:
    """Parse date argument which can be single date or range."""
    if '-' in date_str and len(date_str.split('-')) > 3:
        # Range like 2025-08-15-2025-08-18
        parts = date_str.split('-')
        start = date(int(parts[0]), int(parts[1]), int(parts[2]))
        end = date(int(parts[3]), int(parts[4]), int(parts[5]))
        dates = []
        current = start
        while current <= end:
            dates.append(current)
            current = date.fromordinal(current.toordinal() + 1)
        return dates
    else:
        # Single date
        dt = datetime.strptime(date_str, '%Y-%m-%d').date()
        return [dt]


def get_national_outliers(
    ds: str,
    mover_ind: bool,
    start_date: date,
    end_date: date,
    z_thresh: float = 2.5,
    db_path: str = 'data/databases/duck_suppression.db'
) -> pd.DataFrame:
    """
    Get national level outliers using DOW-partitioned z-scores.
    
    Returns DataFrame with columns:
    - the_date
    - winner
    - mover_ind
    - total_wins_current
    - total_wins_mu
    - total_wins_sigma
    - win_share_current
    - win_share_mu
    - z_score
    - dow (day of week)
    """
    sql = f"""
    WITH daily AS (
        SELECT
            the_date,
            winner,
            mover_ind,
            total_wins,
            EXTRACT(dow FROM the_date) as dow,
            CAST(strftime('%Y-%m-%d', the_date) AS DATE) as the_date_date
        FROM national_daily
        WHERE ds = '{ds}'
          AND mover_ind = {mover_ind}
          AND the_date BETWEEN '{start_date}' AND '{end_date}'
    ),
    market_totals AS (
        SELECT
            the_date,
            SUM(total_wins) as market_total
        FROM daily
        GROUP BY the_date
    ),
    with_shares AS (
        SELECT
            d.*,
            m.market_total,
            CAST(d.total_wins AS DOUBLE) / NULLIF(m.market_total, 0) as win_share
        FROM daily d
        JOIN market_totals m ON d.the_date = m.the_date
    ),
    stats AS (
        SELECT
            winner,
            dow,
            AVG(total_wins) as mu_wins,
            STDDEV_POP(total_wins) as sigma_wins,
            AVG(win_share) as mu_share,
            STDDEV_POP(win_share) as sigma_share,
            COUNT(*) as n_obs
        FROM with_shares
        WHERE the_date < (SELECT MAX(the_date) FROM with_shares)  -- Exclude current for baseline
        GROUP BY winner, dow
        HAVING COUNT(*) >= 4  -- Need at least 4 observations per DOW
    )
    SELECT
        ws.the_date,
        ws.winner,
        ws.mover_ind,
        ws.total_wins as total_wins_current,
        ws.market_total as market_total_current,
        ws.win_share as win_share_current,
        st.mu_wins as total_wins_mu,
        st.sigma_wins as total_wins_sigma,
        st.mu_share as win_share_mu,
        st.sigma_share as win_share_sigma,
        st.n_obs as baseline_window,
        ws.dow,
        (ws.total_wins - st.mu_wins) / NULLIF(st.sigma_wins, 0) as z_score_wins,
        (ws.win_share - st.mu_share) / NULLIF(st.sigma_share, 0) as z_score_share
    FROM with_shares ws
    JOIN stats st ON ws.winner = st.winner AND ws.dow = st.dow
    WHERE (ws.total_wins - st.mu_wins) / NULLIF(st.sigma_wins, 0) > {z_thresh}
    ORDER BY the_date, z_score_wins DESC
    """
    
    return db.query(sql, db_path)


def calculate_need(
    current_wins: float,
    market_total: float,
    baseline_share: float
) -> int:
    """
    Calculate how many wins need to be removed to get back to baseline share.
    
    Uses market-aware formula:
    If we remove X from winner, market becomes (market_total - X)
    We want: (current_wins - X) / (market_total - X) = baseline_share
    Solving: X = (current_wins - baseline_share * market_total) / (1 - baseline_share)
    """
    if baseline_share >= 1.0 or baseline_share <= 0:
        return 0
    
    target_wins = baseline_share * market_total
    excess = current_wins - target_wins
    
    if excess <= 0:
        return 0
    
    # Market-aware calculation
    need = (current_wins - baseline_share * market_total) / (1 - baseline_share)
    
    return int(np.ceil(max(need, 0)))


def get_pair_level_data(
    ds: str,
    mover_ind: bool,
    the_date: date,
    winner: str,
    db_path: str = 'data/databases/duck_suppression.db'
) -> pd.DataFrame:
    """
    Get pair-level (winner-loser-DMA) data for a specific date and winner.
    
    Includes:
    - Current day metrics
    - Historical baselines (DOW-partitioned)
    - Z-scores
    - First appearance flag
    """
    # Determine table name
    mover_str = 'mover' if mover_ind else 'non_mover'
    table = f"{ds}_win_{mover_str}_cube"
    
    dow = the_date.isoweekday() % 7  # Convert to 0=Sunday
    
    sql = f"""
    WITH current_day AS (
        SELECT
            the_date,
            winner,
            loser,
            state,
            dma_name,
            total_wins as pair_wins_current,
            record_count
        FROM {table}
        WHERE the_date = '{the_date}'
          AND winner = '{winner}'
    ),
    historical AS (
        SELECT
            winner,
            loser,
            state,
            dma_name,
            AVG(total_wins) as pair_mu_wins,
            STDDEV_POP(total_wins) as pair_sigma_wins,
            MIN(the_date) as first_date,
            MAX(the_date) as last_date,
            COUNT(*) as pair_window
        FROM {table}
        WHERE the_date < '{the_date}'
          AND winner = '{winner}'
          AND EXTRACT(dow FROM the_date) = {dow}
        GROUP BY winner, loser, state, dma_name
    ),
    dma_totals AS (
        SELECT
            dma_name,
            SUM(pair_wins_current) as dma_wins_current
        FROM current_day
        GROUP BY dma_name
    )
    SELECT
        cd.the_date,
        cd.winner,
        cd.loser,
        cd.state,
        cd.dma_name,
        cd.pair_wins_current,
        cd.record_count,
        COALESCE(h.pair_mu_wins, 0) as pair_mu_wins,
        COALESCE(h.pair_sigma_wins, 0) as pair_sigma_wins,
        COALESCE(h.pair_window, 0) as pair_window,
        COALESCE(h.first_date, cd.the_date) as pair_first_date,
        CASE WHEN h.first_date IS NULL THEN TRUE ELSE FALSE END as is_first_appearance,
        CASE WHEN COALESCE(h.pair_mu_wins, 0) < 5 THEN TRUE ELSE FALSE END as is_rare_pair,
        (cd.pair_wins_current - COALESCE(h.pair_mu_wins, 0)) / NULLIF(COALESCE(h.pair_sigma_wins, 1), 0) as pair_z,
        CASE 
            WHEN COALESCE(h.pair_mu_wins, 0) > 0 
            THEN (cd.pair_wins_current - h.pair_mu_wins) / h.pair_mu_wins 
            ELSE NULL 
        END as pct_change,
        dt.dma_wins_current,
        cd.pair_wins_current / NULLIF(dt.dma_wins_current, 0) as pair_share_current
    FROM current_day cd
    LEFT JOIN historical h ON cd.loser = h.loser AND cd.state = h.state AND cd.dma_name = h.dma_name
    JOIN dma_totals dt ON cd.dma_name = dt.dma_name
    ORDER BY pair_z DESC NULLS LAST
    """
    
    return db.query(sql, db_path)


def get_census_block_data(
    ds: str,
    mover_ind: bool,
    the_date: date,
    winner: str,
    loser: str,
    state: str,
    dma_name: str,
    db_path: str = 'data/databases/duck_suppression.db'
) -> pd.DataFrame:
    """
    Get census block level data for surgical targeting.
    """
    mover_str = 'mover' if mover_ind else 'non_mover'
    table = f"{ds}_win_{mover_str}_census_cube"
    
    dow = the_date.isoweekday() % 7
    
    sql = f"""
    WITH current_blocks AS (
        SELECT
            the_date,
            winner,
            loser,
            state,
            dma_name,
            census_blockid,
            total_wins as cb_wins_current,
            record_count as cb_record_count
        FROM {table}
        WHERE the_date = '{the_date}'
          AND winner = '{winner}'
          AND loser = '{loser}'
          AND state = '{state}'
          AND dma_name = '{dma_name}'
    ),
    historical_blocks AS (
        SELECT
            census_blockid,
            AVG(total_wins) as cb_mu_wins,
            STDDEV_POP(total_wins) as cb_sigma_wins,
            MIN(the_date) as cb_first_date,
            COUNT(*) as cb_window
        FROM {table}
        WHERE the_date < '{the_date}'
          AND winner = '{winner}'
          AND loser = '{loser}'
          AND state = '{state}'
          AND dma_name = '{dma_name}'
          AND EXTRACT(dow FROM the_date) = {dow}
        GROUP BY census_blockid
    )
    SELECT
        cb.the_date,
        cb.winner,
        cb.loser,
        cb.state,
        cb.dma_name,
        cb.census_blockid,
        cb.cb_wins_current,
        cb.cb_record_count,
        COALESCE(hb.cb_mu_wins, 0) as cb_mu_wins,
        COALESCE(hb.cb_sigma_wins, 0) as cb_sigma_wins,
        COALESCE(hb.cb_window, 0) as cb_window,
        COALESCE(hb.cb_first_date, cb.the_date) as cb_first_date,
        CASE WHEN hb.cb_first_date IS NULL THEN TRUE ELSE FALSE END as cb_is_first_appearance,
        (cb.cb_wins_current - COALESCE(hb.cb_mu_wins, 0)) / NULLIF(COALESCE(hb.cb_sigma_wins, 1), 0) as cb_z,
        CASE 
            WHEN COALESCE(hb.cb_mu_wins, 0) > 0 
            THEN (cb.cb_wins_current - hb.cb_mu_wins) / hb.cb_mu_wins 
            ELSE NULL 
        END as cb_pct_change
    FROM current_blocks cb
    LEFT JOIN historical_blocks hb ON cb.census_blockid = hb.census_blockid
    ORDER BY cb_z DESC NULLS LAST
    """
    
    return db.query(sql, db_path)


def stage1_targeted_removal(
    pairs: pd.DataFrame,
    need: int,
    z_thresh: float = 2.5,
    pct_thresh: float = 0.30,
    min_volume: int = 5
) -> Tuple[pd.DataFrame, int]:
    """
    Stage 1: Targeted removal from outlier pairs.
    
    Triggers:
    - Z-score based (pair_z > threshold)
    - 30% jump (pct_change > 0.30)
    - Rare pairs (pair_mu_wins < 5)
    - First appearance
    
    Returns:
    - DataFrame with removal plan
    - Remaining need
    """
    # Filter to outlier pairs
    auto = pairs[
        (pairs['pair_z'] > z_thresh) |
        (pairs['pct_change'] > pct_thresh) |
        (pairs['is_rare_pair'] == True) |
        (pairs['is_first_appearance'] == True)
    ].copy()
    
    # Enforce minimum current volume
    auto = auto[auto['pair_wins_current'] > min_volume]
    
    if auto.empty:
        return auto, need
    
    # Calculate removal amount
    # If baseline < 5 (rare/new): remove ALL
    # Else: remove EXCESS (current - baseline)
    auto['remove_all'] = auto['pair_mu_wins'] < 5.0
    auto['rm_excess'] = np.ceil(np.maximum(0, auto['pair_wins_current'] - auto['pair_mu_wins']))
    auto['rm_pair'] = np.where(
        auto['remove_all'],
        np.ceil(auto['pair_wins_current']),
        auto['rm_excess']
    ).astype(int)
    
    # Sort by priority (z-score, then current wins)
    auto = auto.sort_values(['pair_z', 'pair_wins_current'], ascending=[False, False])
    
    # Apply budget constraint
    auto['cum'] = auto['rm_pair'].cumsum()
    auto['rm_stage1'] = np.where(
        auto['cum'] <= need,
        auto['rm_pair'],
        np.maximum(0, need - auto['cum'].shift(fill_value=0))
    ).astype(int)
    
    auto = auto[auto['rm_stage1'] > 0]
    
    remaining_need = int(max(0, need - auto['rm_stage1'].sum()))
    
    return auto, remaining_need


def stage2_equalized_distribution(
    pairs: pd.DataFrame,
    stage1_indices: pd.Index,
    need: int
) -> pd.DataFrame:
    """
    Stage 2: Distribute remaining need evenly across all pairs.
    
    Each pair-DMA gets floor(need/m), then remaining distributed
    one-by-one to pairs with highest residual capacity.
    """
    if need <= 0:
        return pd.DataFrame()
    
    # Get pairs not already targeted in stage1
    remaining_pairs = pairs[~pairs.index.isin(stage1_indices)].copy()
    
    if remaining_pairs.empty:
        return remaining_pairs
    
    m = len(remaining_pairs)
    base = need // m
    
    # Each pair gets base amount (capped at their volume)
    remaining_pairs['rm_base'] = np.minimum(remaining_pairs['pair_wins_current'], base).astype(int)
    
    # Calculate remaining after base distribution
    still_remaining = int(need - remaining_pairs['rm_base'].sum())
    
    # Calculate residual capacity
    remaining_pairs['residual'] = (remaining_pairs['pair_wins_current'] - remaining_pairs['rm_base']).astype(int)
    
    # Sort by residual capacity and volume
    remaining_pairs = remaining_pairs.sort_values(
        ['residual', 'pair_wins_current'],
        ascending=[False, False]
    ).reset_index(drop=True)
    
    # Distribute remaining one-by-one
    remaining_pairs['extra'] = 0
    if still_remaining > 0:
        eligible = remaining_pairs.index[remaining_pairs['residual'] > 0][:still_remaining]
        remaining_pairs.loc[eligible, 'extra'] = 1
    
    remaining_pairs['rm_stage2'] = (remaining_pairs['rm_base'] + remaining_pairs['extra']).astype(int)
    remaining_pairs = remaining_pairs[remaining_pairs['rm_stage2'] > 0]
    
    return remaining_pairs


def census_block_surgical_targeting(
    stage1: pd.DataFrame,
    ds: str,
    mover_ind: bool,
    the_date: date,
    db_path: str
) -> List[Dict]:
    """
    For each pair-DMA in stage1, use census blocks to surgically target removal.
    
    Returns list of census block level suppression records.
    """
    cb_records = []
    
    for _, pair in stage1.iterrows():
        target_removal = pair['rm_stage1']
        
        # Get census blocks for this pair-DMA
        cbs = get_census_block_data(
            ds=ds,
            mover_ind=mover_ind,
            the_date=the_date,
            winner=pair['winner'],
            loser=pair['loser'],
            state=pair['state'],
            dma_name=pair['dma_name'],
            db_path=db_path
        )
        
        if cbs.empty:
            # Fallback: remove at pair-DMA level
            cb_records.append({
                'the_date': str(the_date),
                'winner': pair['winner'],
                'loser': pair['loser'],
                'state': pair['state'],
                'dma_name': pair['dma_name'],
                'census_blockid': None,
                'remove_units': target_removal,
                'stage': 'stage1_pair_level',
                'pair_z': pair['pair_z'],
                'pair_wins_current': pair['pair_wins_current'],
                'pair_mu_wins': pair['pair_mu_wins'],
                'is_first_appearance': pair['is_first_appearance']
            })
            continue
        
        # Sort census blocks by z-score (highest first)
        cbs = cbs.sort_values('cb_z', ascending=False)
        
        removed_so_far = 0
        for _, cb in cbs.iterrows():
            if removed_so_far >= target_removal:
                break
            
            # How much to remove from this block
            cb_removal = min(cb['cb_wins_current'], target_removal - removed_so_far)
            
            cb_records.append({
                'the_date': str(the_date),
                'winner': pair['winner'],
                'loser': pair['loser'],
                'state': pair['state'],
                'dma_name': pair['dma_name'],
                'census_blockid': cb['census_blockid'],
                'remove_units': cb_removal,
                'stage': 'stage1_census_block',
                'pair_z': pair['pair_z'],
                'cb_z': cb['cb_z'],
                'cb_wins_current': cb['cb_wins_current'],
                'cb_mu_wins': cb['cb_mu_wins'],
                'cb_is_first_appearance': cb['cb_is_first_appearance']
            })
            
            removed_so_far += cb_removal
    
    return cb_records


def build_suppression_plan(
    ds: str,
    mover_ind: bool,
    dates: List[date],
    z_thresh: float = 2.5,
    db_path: str = 'data/databases/duck_suppression.db'
) -> pd.DataFrame:
    """
    Build comprehensive suppression plan for given dates.
    
    Process:
    1. Detect national outliers
    2. For each outlier, calculate need
    3. Get pair-level data
    4. Stage 1: Targeted removal
    5. Stage 2: Equalized distribution
    6. Census block surgical targeting for stage1
    
    Returns DataFrame with all suppression records.
    """
    start_date = min(dates)
    end_date = max(dates)
    
    print(f"\n{'='*70}")
    print(f"Building Suppression Plan")
    print(f"Dataset: {ds} | Mover: {mover_ind}")
    print(f"Dates: {start_date} to {end_date}")
    print(f"{'='*70}\n")
    
    # Step 1: Get national outliers
    print("Step 1: Detecting national outliers...")
    outliers = get_national_outliers(ds, mover_ind, start_date, end_date, z_thresh, db_path)
    
    if outliers.empty:
        print("No national outliers detected.")
        return pd.DataFrame()
    
    print(f"Found {len(outliers)} national outliers")
    print(outliers[['the_date', 'winner', 'z_score_wins', 'win_share_current', 'win_share_mu']].to_string(index=False))
    
    all_records = []
    
    # Step 2: Process each outlier
    for _, outlier in outliers.iterrows():
        the_date = outlier['the_date'].date() if hasattr(outlier['the_date'], 'date') else outlier['the_date']
        winner = outlier['winner']
        
        print(f"\n{'-'*70}")
        print(f"Processing: {the_date} | {winner}")
        print(f"{'-'*70}")
        
        # Calculate need
        need = calculate_need(
            current_wins=outlier['total_wins_current'],
            market_total=outlier['market_total_current'],
            baseline_share=outlier['win_share_mu']
        )
        
        print(f"  Current wins: {outlier['total_wins_current']:.0f}")
        print(f"  Baseline share: {outlier['win_share_mu']:.4f}")
        print(f"  Current share: {outlier['win_share_current']:.4f}")
        print(f"  Need to remove: {need} wins")
        
        if need == 0:
            print("  No removal needed")
            continue
        
        # Get pair-level data
        print("  Fetching pair-level data...")
        pairs = get_pair_level_data(ds, mover_ind, the_date, winner, db_path)
        
        if pairs.empty:
            print("  No pair data found")
            continue
        
        print(f"  Found {len(pairs)} pair-DMA combinations")
        
        # Stage 1: Targeted removal
        print("  Stage 1: Targeted removal...")
        stage1, need_after_stage1 = stage1_targeted_removal(pairs, need, z_thresh)
        
        if not stage1.empty:
            print(f"    Targeted {len(stage1)} pairs for removal")
            print(f"    Total stage1 removal: {stage1['rm_stage1'].sum()} wins")
            print(f"    Remaining need: {need_after_stage1} wins")
            
            # Census block surgical targeting for stage1
            print("  Performing census block surgical targeting...")
            cb_records = census_block_surgical_targeting(stage1, ds, mover_ind, the_date, db_path)
            all_records.extend(cb_records)
        else:
            need_after_stage1 = need
            print("    No pairs qualified for stage1")
        
        # Stage 2: Equalized distribution
        if need_after_stage1 > 0:
            print(f"  Stage 2: Distributing remaining {need_after_stage1} wins...")
            stage2 = stage2_equalized_distribution(pairs, stage1.index if not stage1.empty else pd.Index([]), need_after_stage1)
            
            if not stage2.empty:
                print(f"    Distributed across {len(stage2)} pairs")
                print(f"    Total stage2 removal: {stage2['rm_stage2'].sum()} wins")
                
                # Add stage2 records
                for _, pair in stage2.iterrows():
                    all_records.append({
                        'the_date': str(the_date),
                        'winner': pair['winner'],
                        'loser': pair['loser'],
                        'state': pair['state'],
                        'dma_name': pair['dma_name'],
                        'census_blockid': None,
                        'remove_units': pair['rm_stage2'],
                        'stage': 'stage2_distributed',
                        'pair_z': pair['pair_z'],
                        'pair_wins_current': pair['pair_wins_current'],
                        'pair_mu_wins': pair['pair_mu_wins']
                    })
    
    plan_df = pd.DataFrame(all_records)
    
    print(f"\n{'='*70}")
    print(f"Suppression Plan Summary")
    print(f"{'='*70}")
    print(f"Total records: {len(plan_df)}")
    if not plan_df.empty:
        print(f"Total removal: {plan_df['remove_units'].sum()} wins")
        print(f"\nBy stage:")
        print(plan_df.groupby('stage')['remove_units'].agg(['count', 'sum']).to_string())
    print(f"{'='*70}\n")
    
    return plan_df


def main():
    parser = argparse.ArgumentParser(description='Z-Score Based Distribution Suppression')
    parser.add_argument('--ds', required=True, help='Dataset name (e.g., gamoshi)')
    parser.add_argument('--dates', nargs='+', required=True, help='Dates to process (e.g., 2025-06-19 2025-08-15-2025-08-18)')
    parser.add_argument('--mover-ind', type=lambda x: x.lower() == 'true', default=True, help='Mover indicator (true/false)')
    parser.add_argument('--z-thresh', type=float, default=2.5, help='Z-score threshold')
    parser.add_argument('--db', default='data/databases/duck_suppression.db', help='Database path')
    parser.add_argument('--output', default='analysis_results/suppression_plan.json', help='Output file')
    
    args = parser.parse_args()
    
    # Parse dates
    all_dates = []
    for date_arg in args.dates:
        all_dates.extend(parse_date_arg(date_arg))
    
    all_dates = sorted(set(all_dates))
    
    print(f"Processing {len(all_dates)} dates: {all_dates[0]} to {all_dates[-1]}")
    
    # Build plan
    plan = build_suppression_plan(
        ds=args.ds,
        mover_ind=args.mover_ind,
        dates=all_dates,
        z_thresh=args.z_thresh,
        db_path=args.db
    )
    
    # Save plan
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if not plan.empty:
        # Save as JSON
        plan_json = plan.to_dict(orient='records')
        with open(output_path, 'w') as f:
            json.dump(plan_json, f, indent=2)
        
        # Also save as CSV for easier viewing
        csv_path = output_path.with_suffix('.csv')
        plan.to_csv(csv_path, index=False)
        
        print(f"\nSuppression plan saved to:")
        print(f"  - {output_path}")
        print(f"  - {csv_path}")
    else:
        print("\nNo suppressions needed")


if __name__ == '__main__':
    main()
