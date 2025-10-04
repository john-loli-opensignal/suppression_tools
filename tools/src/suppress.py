"""
Suppression planning and distribution logic.

Implements the 2-stage distribution approach:
- Stage 1: Targeted auto-suppression (outliers, new pairs, rare pairs, 30% jumps)
- Stage 2: Equalized distribution of remaining excess

Uses database cubes for fast query performance.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Optional

from tools import db
from tools.src import outliers


def calculate_suppression_need(
    current_wins: float,
    market_total: float,
    historical_share: float
) -> int:
    """
    Calculate how many wins need to be removed to restore historical share.
    
    Formula accounts for market dynamics:
    - If we remove X from winner, market total becomes (T - X)
    - We want: (W - X) / (T - X) = mu
    - Solving for X: X = (W - mu*T) / (1 - mu)
    
    Args:
        current_wins: Current total wins for winner
        market_total: Current market total wins
        historical_share: Historical average share (0 to 1)
    
    Returns:
        Number of wins to remove (integer, >= 0)
    """
    W = float(current_wins)
    T = float(market_total)
    mu = float(historical_share)
    
    # Avoid division by zero or negative values
    if mu >= 1.0 or mu < 0:
        mu = max(0.001, min(0.999, mu))
    
    # Calculate need
    numerator = W - mu * T
    denominator = 1.0 - mu
    need = numerator / denominator if denominator > 1e-12 else 0
    
    return int(np.ceil(max(0, need)))


def detect_first_appearances(
    db_path: str,
    ds: str,
    mover_ind: bool,
    the_date: str,
    winner: str,
    lookback_days: int = 90
) -> pd.DataFrame:
    """
    Detect pair-DMA combinations that have never appeared before (or very recently).
    
    Args:
        db_path: Path to database
        ds: Dataset name
        mover_ind: True for movers, False for non-movers
        the_date: Date to check (YYYY-MM-DD)
        winner: Winner name
        lookback_days: How many days to look back (default: 90)
    
    Returns:
        DataFrame with columns: winner, loser, dma_name, first_appearance (bool)
    """
    table_suffix = "mover" if mover_ind else "non_mover"
    table_name = f"{ds}_win_{table_suffix}_cube"
    
    sql = f"""
    WITH current_pairs AS (
        SELECT DISTINCT winner, loser, dma_name
        FROM {table_name}
        WHERE the_date = DATE '{the_date}'
          AND winner = '{winner.replace("'", "''")}'
    ), historical_pairs AS (
        SELECT DISTINCT winner, loser, dma_name
        FROM {table_name}
        WHERE the_date BETWEEN DATE '{the_date}' - INTERVAL '{lookback_days} days'
                          AND DATE '{the_date}' - INTERVAL '1 day'
          AND winner = '{winner.replace("'", "''")}'
    )
    SELECT 
        c.winner,
        c.loser,
        c.dma_name,
        CASE WHEN h.winner IS NULL THEN TRUE ELSE FALSE END AS first_appearance
    FROM current_pairs c
    LEFT JOIN historical_pairs h 
        ON c.winner = h.winner 
        AND c.loser = h.loser 
        AND c.dma_name = h.dma_name
    """
    
    return db.query(sql, db_path)


def get_pair_dma_details(
    db_path: str,
    ds: str,
    mover_ind: bool,
    the_date: str,
    winner: str,
    window: int = 14
) -> pd.DataFrame:
    """
    Get detailed pair-DMA statistics for a specific winner/date.
    
    Includes:
    - Current wins
    - Historical baseline (DOW-adjusted)
    - Z-scores
    - Percentage changes
    
    Args:
        db_path: Path to database
        ds: Dataset name
        mover_ind: True for movers, False for non-movers
        the_date: Date (YYYY-MM-DD)
        winner: Winner name
        window: Lookback window for statistics
    
    Returns:
        DataFrame with pair-DMA level statistics
    """
    table_suffix = "mover" if mover_ind else "non_mover"
    table_name = f"{ds}_win_{table_suffix}_cube"
    
    sql = f"""
    WITH base AS (
        SELECT 
            the_date,
            winner,
            loser,
            dma_name,
            state,
            total_wins AS pair_wins_current,
            CASE 
                WHEN strftime('%w', the_date) IN ('0', '6') THEN 'Weekend'
                ELSE 'Weekday'
            END AS day_type
        FROM {table_name}
        WHERE winner = '{winner.replace("'", "''")}'
    ), current AS (
        SELECT *
        FROM base
        WHERE the_date = DATE '{the_date}'
    ), historical AS (
        SELECT 
            c.winner,
            c.loser,
            c.dma_name,
            AVG(h.pair_wins_current) AS pair_mu_wins,
            STDDEV_SAMP(h.pair_wins_current) AS pair_sigma_wins,
            COUNT(*) AS pair_mu_window
        FROM current c
        LEFT JOIN base h 
            ON c.winner = h.winner 
            AND c.loser = h.loser 
            AND c.dma_name = h.dma_name
            AND c.day_type = h.day_type
            AND h.the_date < DATE '{the_date}'
        WHERE h.the_date >= DATE '{the_date}' - INTERVAL '{window * 7} days'
           OR h.the_date IS NULL
        GROUP BY 1, 2, 3
    )
    SELECT 
        c.winner,
        c.loser,
        c.dma_name,
        c.state,
        c.pair_wins_current,
        COALESCE(h.pair_mu_wins, 0) AS pair_mu_wins,
        COALESCE(h.pair_sigma_wins, 0) AS pair_sigma_wins,
        COALESCE(h.pair_mu_window, 0) AS pair_mu_window,
        CASE 
            WHEN h.pair_sigma_wins > 0 AND h.pair_mu_window > 1
            THEN (c.pair_wins_current - h.pair_mu_wins) / h.pair_sigma_wins
            ELSE 0
        END AS pair_z,
        CASE 
            WHEN h.pair_mu_wins > 0
            THEN (c.pair_wins_current - h.pair_mu_wins) / h.pair_mu_wins
            ELSE 0
        END AS pct_change
    FROM current c
    LEFT JOIN historical h 
        ON c.winner = h.winner 
        AND c.loser = h.loser 
        AND c.dma_name = h.dma_name
    """
    
    return db.query(sql, db_path)


def build_suppression_plan(
    db_path: str,
    ds: str,
    mover_ind: bool,
    the_date: str,
    winner: str,
    removal_target: int,
    z_thresh: float = 2.0,
    pct_thresh: float = 0.30,
    rare_thresh: float = 5.0,
    min_volume: float = 5.0,
    window: int = 14,
    lookback_days: int = 90
) -> pd.DataFrame:
    """
    Build a 2-stage suppression plan for a specific winner/date outlier.
    
    Stage 1: Targeted auto-suppression
    - High z-scores (pair_z > threshold)
    - Large percentage jumps (> 30%)
    - Rare pairs (historical baseline < 5)
    - First appearances (never seen before)
    
    Stage 2: Equalized distribution
    - Distribute remaining removal evenly across all pairs
    - Prioritize pairs with higher residual capacity
    
    Args:
        db_path: Path to database
        ds: Dataset name
        mover_ind: True for movers, False for non-movers
        the_date: Date (YYYY-MM-DD)
        winner: Winner name
        removal_target: Total wins to remove
        z_thresh: Z-score threshold for auto-suppression
        pct_thresh: Percentage change threshold (0.30 = 30%)
        rare_thresh: Baseline threshold for "rare" pairs
        min_volume: Minimum current volume to consider for removal
        window: Lookback window for statistics
        lookback_days: Days to look back for first appearance detection
    
    Returns:
        DataFrame with suppression plan:
        - date, winner, loser, dma_name, state
        - remove_units: how many wins to remove
        - stage: 'auto' or 'distributed'
        - reason: why this pair was selected
        - pair_wins_current, pair_mu_wins, pair_z, pct_change
    """
    # Get pair-DMA details
    pairs = get_pair_dma_details(db_path, ds, mover_ind, the_date, winner, window)
    
    if pairs.empty:
        return pd.DataFrame()
    
    # Detect first appearances
    first_app = detect_first_appearances(db_path, ds, mover_ind, the_date, winner, lookback_days)
    
    # Merge first appearance flag
    pairs = pairs.merge(
        first_app[['loser', 'dma_name', 'first_appearance']],
        on=['loser', 'dma_name'],
        how='left'
    )
    pairs['first_appearance'] = pairs['first_appearance'].fillna(False)
    
    # Filter to minimum volume
    pairs = pairs[pairs['pair_wins_current'] >= min_volume].copy()
    
    if pairs.empty:
        return pd.DataFrame()
    
    # ========== Stage 1: Targeted Auto-Suppression ==========
    
    # Identify outlier pairs
    pairs['pair_outlier'] = pairs['pair_z'] > z_thresh
    pairs['pct_outlier'] = pairs['pct_change'] > pct_thresh
    pairs['rare_pair'] = pairs['pair_mu_wins'] < rare_thresh
    
    # Select auto-suppression candidates
    auto = pairs[
        pairs['pair_outlier'] | 
        pairs['pct_outlier'] | 
        pairs['rare_pair'] | 
        pairs['first_appearance']
    ].copy()
    
    if not auto.empty:
        # Calculate removal amounts
        # If rare/new (baseline < rare_thresh), remove ALL
        # Otherwise, remove EXCESS (current - baseline)
        auto['rm_pair'] = np.where(
            auto['pair_mu_wins'] < rare_thresh,
            np.ceil(auto['pair_wins_current']),  # Remove all
            np.ceil(np.maximum(0, auto['pair_wins_current'] - auto['pair_mu_wins']))  # Remove excess
        ).astype(int)
        
        # Sort by priority: z-score desc, then current volume desc
        auto = auto.sort_values(['pair_z', 'pair_wins_current'], ascending=[False, False])
        
        # Apply budget constraint: cumulative removal up to target
        auto['cum'] = auto['rm_pair'].cumsum()
        auto['rm1'] = np.where(
            auto['cum'] <= removal_target,
            auto['rm_pair'],
            np.maximum(0, removal_target - auto['cum'].shift(fill_value=0))
        ).astype(int)
        
        # Keep only pairs with removal > 0
        auto = auto[auto['rm1'] > 0].copy()
        auto['stage'] = 'auto'
        auto['remove_units'] = auto['rm1']
        
        # Build reasons
        reasons = []
        for _, row in auto.iterrows():
            r = []
            if row['pair_outlier']:
                r.append(f"z-score={row['pair_z']:.2f}")
            if row['pct_outlier']:
                r.append(f"jump={row['pct_change']*100:.1f}%")
            if row['rare_pair']:
                r.append(f"rare (baseline={row['pair_mu_wins']:.1f})")
            if row['first_appearance']:
                r.append("first appearance")
            reasons.append(", ".join(r))
        auto['reason'] = reasons
        
        # Calculate remaining need
        need_after = max(0, removal_target - int(auto['rm1'].sum()))
    else:
        auto = pd.DataFrame()
        need_after = removal_target
    
    # ========== Stage 2: Equalized Distribution ==========
    
    distributed = pd.DataFrame()
    
    if need_after > 0:
        # Get pairs not already in auto stage
        if not auto.empty:
            remaining_pairs = pairs[
                ~pairs.set_index(['loser', 'dma_name']).index.isin(
                    auto.set_index(['loser', 'dma_name']).index
                )
            ].copy()
        else:
            remaining_pairs = pairs.copy()
        
        if not remaining_pairs.empty:
            m = len(remaining_pairs)
            base_removal = need_after // m  # Floor division
            
            # Each pair gets base amount (capped at their current volume)
            remaining_pairs['rm_base'] = np.minimum(
                remaining_pairs['pair_wins_current'], 
                base_removal
            ).astype(int)
            
            # Calculate residual capacity
            remaining_pairs['residual'] = (
                remaining_pairs['pair_wins_current'] - remaining_pairs['rm_base']
            ).astype(int)
            
            # Distribute remaining units to pairs with highest residual
            still_needed = need_after - int(remaining_pairs['rm_base'].sum())
            
            if still_needed > 0:
                # Sort by residual capacity and volume
                remaining_pairs = remaining_pairs.sort_values(
                    ['residual', 'pair_wins_current'], 
                    ascending=[False, False]
                ).reset_index(drop=True)
                
                remaining_pairs['extra'] = 0
                idx = remaining_pairs.index[remaining_pairs['residual'] > 0][:still_needed]
                remaining_pairs.loc[idx, 'extra'] = 1
            else:
                remaining_pairs['extra'] = 0
            
            remaining_pairs['rm2'] = (remaining_pairs['rm_base'] + remaining_pairs['extra']).astype(int)
            
            # Keep only pairs with removal > 0
            distributed = remaining_pairs[remaining_pairs['rm2'] > 0].copy()
            distributed['stage'] = 'distributed'
            distributed['remove_units'] = distributed['rm2']
            distributed['reason'] = 'equalized distribution'
    
    # ========== Combine Plans ==========
    
    plan_columns = [
        'winner', 'loser', 'dma_name', 'state',
        'remove_units', 'stage', 'reason',
        'pair_wins_current', 'pair_mu_wins', 'pair_sigma_wins',
        'pair_z', 'pct_change', 'first_appearance'
    ]
    
    plan_parts = []
    
    if not auto.empty:
        plan_parts.append(auto[plan_columns])
    
    if not distributed.empty:
        plan_parts.append(distributed[plan_columns])
    
    if plan_parts:
        plan = pd.concat(plan_parts, ignore_index=True)
        plan['date'] = the_date
        plan['mover_ind'] = mover_ind
        plan['ds'] = ds
        return plan
    else:
        return pd.DataFrame()


def build_full_suppression_plan(
    db_path: str,
    ds: str,
    mover_ind: bool,
    start_date: str,
    end_date: str,
    window: int = 14,
    z_nat: float = 2.5,
    z_pair: float = 2.0,
    pct_thresh: float = 0.30,
    rare_thresh: float = 5.0,
    min_volume: float = 5.0,
    lookback_days: int = 90
) -> pd.DataFrame:
    """
    Build a complete suppression plan for date range.
    
    1. Detect national-level outliers (winner/date combos)
    2. For each outlier, calculate removal target
    3. Build 2-stage distribution plan
    
    Args:
        db_path: Path to database
        ds: Dataset name
        mover_ind: True for movers, False for non-movers
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        window: Lookback window for statistics
        z_nat: Z-score threshold for national outlier detection
        z_pair: Z-score threshold for pair-level auto-suppression
        pct_thresh: Percentage change threshold
        rare_thresh: Baseline threshold for rare pairs
        min_volume: Minimum volume for consideration
        lookback_days: Days to look back for first appearances
    
    Returns:
        Complete suppression plan DataFrame
    """
    # Step 1: Detect national outliers
    nat_outliers = outliers.national_outliers(
        db_path=db_path,
        ds=ds,
        mover_ind=mover_ind,
        start_date=start_date,
        end_date=end_date,
        window=window,
        z_thresh=z_nat
    )
    
    if nat_outliers.empty:
        print("[INFO] No national outliers detected")
        return pd.DataFrame()
    
    print(f"[INFO] Detected {len(nat_outliers)} national outliers")
    
    # Step 2: Build plans for each outlier
    plans = []
    
    for idx, row in nat_outliers.iterrows():
        the_date = str(row['the_date'])
        winner = row['winner']
        
        # Calculate removal target
        current_wins = float(row.get('nat_total_wins', 0))
        market_total = float(row.get('market_total_wins', 0))
        historical_share = float(row.get('nat_mu_share', 0))
        
        removal_target = calculate_suppression_need(
            current_wins=current_wins,
            market_total=market_total,
            historical_share=historical_share
        )
        
        if removal_target == 0:
            print(f"[INFO] {the_date} {winner}: No removal needed (target=0)")
            continue
        
        print(f"[INFO] {the_date} {winner}: Target removal = {removal_target:,} wins")
        
        # Build suppression plan
        plan = build_suppression_plan(
            db_path=db_path,
            ds=ds,
            mover_ind=mover_ind,
            the_date=the_date,
            winner=winner,
            removal_target=removal_target,
            z_thresh=z_pair,
            pct_thresh=pct_thresh,
            rare_thresh=rare_thresh,
            min_volume=min_volume,
            window=window,
            lookback_days=lookback_days
        )
        
        if not plan.empty:
            # Add national-level context
            plan['nat_total_wins'] = current_wins
            plan['market_total_wins'] = market_total
            plan['nat_mu_share'] = historical_share
            plan['nat_share_current'] = current_wins / market_total if market_total > 0 else 0
            plan['removal_target'] = removal_target
            plan['removal_actual'] = plan['remove_units'].sum()
            
            plans.append(plan)
            
            print(f"  → Stage 1 (auto): {plan[plan['stage']=='auto']['remove_units'].sum():,} wins from {len(plan[plan['stage']=='auto'])} pairs")
            print(f"  → Stage 2 (dist): {plan[plan['stage']=='distributed']['remove_units'].sum():,} wins from {len(plan[plan['stage']=='distributed'])} pairs")
    
    if plans:
        full_plan = pd.concat(plans, ignore_index=True)
        print(f"\n[SUCCESS] Built suppression plan:")
        print(f"  - Total outliers: {len(nat_outliers)}")
        print(f"  - Plans created: {len(plans)}")
        print(f"  - Total removals: {full_plan['remove_units'].sum():,} wins")
        print(f"  - Auto stage: {full_plan[full_plan['stage']=='auto']['remove_units'].sum():,} wins")
        print(f"  - Distributed: {full_plan[full_plan['stage']=='distributed']['remove_units'].sum():,} wins")
        return full_plan
    else:
        print("[WARNING] No suppression plans could be created")
        return pd.DataFrame()
