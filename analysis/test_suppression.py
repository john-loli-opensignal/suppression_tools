#!/usr/bin/env python3
"""
Test the 2-stage suppression algorithm with database cubes.

Usage:
    uv run test_suppression.py --ds gamoshi --start 2025-06-19 --end 2025-06-19
    uv run test_suppression.py --ds gamoshi --start 2025-08-15 --end 2025-08-18
"""

import argparse
from pathlib import Path

import pandas as pd

from tools import db
from tools.src import suppress


def get_default_db() -> str:
    """Get default database path."""
    return str(Path(__file__).parent / "data" / "databases" / "duck_suppression.db")


def parse_args():
    p = argparse.ArgumentParser(description="Test 2-stage suppression algorithm")
    p.add_argument("--db", default=get_default_db(), help="Database path")
    p.add_argument("--ds", required=True, help="Dataset name (e.g., gamoshi)")
    p.add_argument("--mover", action="store_true", help="Use mover data (default: non-mover)")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--window", type=int, default=14, help="DOW window size (default: 14)")
    p.add_argument("--z-nat", type=float, default=2.5, help="National z-score threshold (default: 2.5)")
    p.add_argument("--z-pair", type=float, default=2.0, help="Pair z-score threshold (default: 2.0)")
    p.add_argument("--pct", type=float, default=0.30, help="Percentage jump threshold (default: 0.30)")
    p.add_argument("--rare", type=float, default=5.0, help="Rare pair threshold (default: 5.0)")
    p.add_argument("--min-vol", type=float, default=5.0, help="Minimum volume (default: 5.0)")
    p.add_argument("--lookback", type=int, default=90, help="Lookback days for first appearance (default: 90)")
    p.add_argument("-o", "--output", help="Output CSV path (optional)")
    return p.parse_args()


def main():
    args = parse_args()
    
    print("=" * 70)
    print("Testing 2-Stage Suppression Algorithm")
    print("=" * 70)
    print(f"Database: {args.db}")
    print(f"Dataset: {args.ds}")
    print(f"Mover: {args.mover}")
    print(f"Date range: {args.start} to {args.end}")
    print(f"National z-threshold: {args.z_nat}")
    print(f"Pair z-threshold: {args.z_pair}")
    print(f"Percentage threshold: {args.pct * 100:.0f}%")
    print(f"Rare threshold: {args.rare}")
    print(f"Minimum volume: {args.min_vol}")
    print(f"Lookback days: {args.lookback}")
    print()
    
    # Build suppression plan
    plan = suppress.build_full_suppression_plan(
        db_path=args.db,
        ds=args.ds,
        mover_ind=args.mover,
        start_date=args.start,
        end_date=args.end,
        window=args.window,
        z_nat=args.z_nat,
        z_pair=args.z_pair,
        pct_thresh=args.pct,
        rare_thresh=args.rare,
        min_volume=args.min_vol,
        lookback_days=args.lookback
    )
    
    if plan.empty:
        print("\n[INFO] No suppression plan generated (no outliers detected)")
        return 0
    
    # Print summary
    print("\n" + "=" * 70)
    print("Suppression Plan Summary")
    print("=" * 70)
    
    # By date
    print("\nBy Date:")
    date_summary = plan.groupby('date').agg({
        'remove_units': 'sum',
        'winner': 'nunique',
        'loser': 'nunique',
        'dma_name': 'nunique'
    }).rename(columns={
        'remove_units': 'Total Removals',
        'winner': 'Unique Winners',
        'loser': 'Unique Losers',
        'dma_name': 'Unique DMAs'
    })
    print(date_summary.to_string())
    
    # By winner
    print("\nBy Winner:")
    winner_summary = plan.groupby('winner').agg({
        'remove_units': 'sum',
        'date': 'nunique',
        'loser': 'nunique',
        'dma_name': 'nunique'
    }).rename(columns={
        'remove_units': 'Total Removals',
        'date': 'Dates',
        'loser': 'Unique Losers',
        'dma_name': 'Unique DMAs'
    }).sort_values('Total Removals', ascending=False)
    print(winner_summary.to_string())
    
    # By stage
    print("\nBy Stage:")
    stage_summary = plan.groupby('stage').agg({
        'remove_units': ['sum', 'count', 'mean']
    })
    stage_summary.columns = ['Total Removals', 'Count', 'Avg per Pair']
    print(stage_summary.to_string())
    
    # Reason breakdown (auto stage only)
    if not plan[plan['stage'] == 'auto'].empty:
        print("\nAuto-Suppression Reasons:")
        # Parse reasons
        auto_plan = plan[plan['stage'] == 'auto'].copy()
        reason_counts = {}
        for reason_str in auto_plan['reason']:
            for reason in str(reason_str).split(', '):
                reason_key = reason.split('=')[0] if '=' in reason else reason
                reason_counts[reason_key] = reason_counts.get(reason_key, 0) + 1
        
        reason_df = pd.DataFrame(list(reason_counts.items()), columns=['Reason', 'Count'])
        reason_df = reason_df.sort_values('Count', ascending=False)
        print(reason_df.to_string(index=False))
    
    # Top removals
    print("\nTop 10 Pair-DMA Removals:")
    top_removals = plan.nlargest(10, 'remove_units')[
        ['date', 'winner', 'loser', 'dma_name', 'remove_units', 'stage', 'pair_wins_current', 'pair_mu_wins']
    ]
    print(top_removals.to_string(index=False))
    
    # Save to CSV if requested
    if args.output:
        plan.to_csv(args.output, index=False)
        print(f"\n[SUCCESS] Saved suppression plan to: {args.output}")
        print(f"Total rows: {len(plan):,}")
    
    return 0


if __name__ == "__main__":
    exit(main())
