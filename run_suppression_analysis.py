#!/usr/bin/env python3
"""
Comprehensive suppression analysis script that recreates the CSV approach using DB cubes.

Uses the 2-stage distribution algorithm:
1. Stage 1: Targeted auto-suppression (z-score, 30% jump, rare, first appearance)
2. Stage 2: Equalized distribution across remaining pairs

Generates before/after visualizations with proper overlay (solid vs dashed lines).
"""

import argparse
import os
import sys
from datetime import datetime
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Add tools to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'tools'))

from tools import db
from tools.src import metrics, outliers, suppress


def get_national_timeseries(
    db_path: str,
    ds: str,
    mover_ind: bool,
    start_date: str,
    end_date: str,
    winners: list = None
) -> pd.DataFrame:
    """Get national win share time series for winners."""
    table_suffix = "mover" if mover_ind else "non_mover"
    table_name = f"{ds}_win_{table_suffix}_cube"
    
    winner_filter = ""
    if winners:
        winner_list = "', '".join([w.replace("'", "''") for w in winners])
        winner_filter = f"AND winner IN ('{winner_list}')"
    
    sql = f"""
    WITH market AS (
        SELECT 
            the_date,
            SUM(total_wins) AS market_total_wins
        FROM {table_name}
        WHERE the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        GROUP BY the_date
    ), selected AS (
        SELECT 
            the_date,
            winner,
            SUM(total_wins) AS total_wins
        FROM {table_name}
        WHERE the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
            {winner_filter}
        GROUP BY the_date, winner
    )
    SELECT 
        s.the_date,
        s.winner,
        s.total_wins,
        m.market_total_wins,
        s.total_wins / NULLIF(m.market_total_wins, 0) AS win_share
    FROM selected s
    JOIN market m USING (the_date)
    ORDER BY s.the_date, s.winner
    """
    
    return db.query(sql, db_path)


def apply_suppression_to_data(
    db_path: str,
    ds: str,
    mover_ind: bool,
    start_date: str,
    end_date: str,
    suppression_plan: pd.DataFrame
) -> pd.DataFrame:
    """
    Apply suppression plan to raw data and return modified dataset.
    
    The plan contains: date, winner, loser, dma_name, remove_units
    We subtract remove_units from the corresponding pair-DMA-date combination.
    """
    if suppression_plan.empty:
        print("[WARNING] Empty suppression plan, returning original data")
        return get_national_timeseries(db_path, ds, mover_ind, start_date, end_date)
    
    table_suffix = "mover" if mover_ind else "non_mover"
    table_name = f"{ds}_win_{table_suffix}_cube"
    
    # Create temporary table with suppression amounts
    print("[INFO] Creating temporary suppression table...")
    
    # Build the plan as VALUES clause
    plan_values = []
    for _, row in suppression_plan.iterrows():
        date_str = row['date']
        winner = row['winner'].replace("'", "''")
        loser = row['loser'].replace("'", "''")
        dma = row['dma_name'].replace("'", "''")
        remove = int(row['remove_units'])
        plan_values.append(f"(DATE '{date_str}', '{winner}', '{loser}', '{dma}', {remove})")
    
    values_clause = ",\n        ".join(plan_values)
    
    sql = f"""
    WITH suppression_plan AS (
        SELECT * FROM (VALUES
            {values_clause}
        ) AS t(the_date, winner, loser, dma_name, remove_units)
    ), suppressed_cube AS (
        SELECT 
            c.the_date,
            c.winner,
            c.loser,
            c.dma_name,
            c.state,
            GREATEST(0, c.total_wins - COALESCE(s.remove_units, 0)) AS total_wins_suppressed
        FROM {table_name} c
        LEFT JOIN suppression_plan s
            ON c.the_date = s.the_date
            AND c.winner = s.winner
            AND c.loser = s.loser
            AND c.dma_name = s.dma_name
        WHERE c.the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    ), market AS (
        SELECT 
            the_date,
            SUM(total_wins_suppressed) AS market_total_wins
        FROM suppressed_cube
        GROUP BY the_date
    ), selected AS (
        SELECT 
            the_date,
            winner,
            SUM(total_wins_suppressed) AS total_wins
        FROM suppressed_cube
        GROUP BY the_date, winner
    )
    SELECT 
        s.the_date,
        s.winner,
        s.total_wins,
        m.market_total_wins,
        s.total_wins / NULLIF(m.market_total_wins, 0) AS win_share
    FROM selected s
    JOIN market m USING (the_date)
    ORDER BY s.the_date, s.winner
    """
    
    print("[INFO] Executing suppression query...")
    return db.query(sql, db_path)


def create_before_after_visualization(
    base_data: pd.DataFrame,
    suppressed_data: pd.DataFrame,
    outliers: pd.DataFrame,
    title: str = "Win Share: Before vs After Suppression"
) -> go.Figure:
    """
    Create visualization with:
    - Solid lines for original data (base)
    - Dashed lines for suppressed data (overlaid underneath)
    - Yellow stars for outlier markers
    """
    fig = go.Figure()
    
    # Get all unique winners
    all_winners = sorted(set(base_data['winner'].unique()) | set(suppressed_data['winner'].unique()))
    
    # Define color palette
    colors = [
        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
    ]
    color_map = {w: colors[i % len(colors)] for i, w in enumerate(all_winners)}
    
    # First, add suppressed data (dashed, underneath)
    for winner in all_winners:
        supp_winner = suppressed_data[suppressed_data['winner'] == winner]
        if not supp_winner.empty:
            fig.add_trace(go.Scatter(
                x=supp_winner['the_date'],
                y=supp_winner['win_share'],
                mode='lines',
                name=f"{winner} (suppressed)",
                line=dict(
                    color=color_map[winner],
                    width=2,
                    dash='dash'
                ),
                showlegend=True,
                legendgroup=winner
            ))
    
    # Then, add base data (solid, on top)
    for winner in all_winners:
        base_winner = base_data[base_data['winner'] == winner]
        if not base_winner.empty:
            fig.add_trace(go.Scatter(
                x=base_winner['the_date'],
                y=base_winner['win_share'],
                mode='lines',
                name=winner,
                line=dict(
                    color=color_map[winner],
                    width=3
                ),
                showlegend=True,
                legendgroup=winner
            ))
    
    # Add outlier markers (yellow stars)
    if not outliers.empty:
        for _, outlier in outliers.iterrows():
            # Get the y-value from base data
            base_point = base_data[
                (base_data['the_date'] == outlier['the_date']) &
                (base_data['winner'] == outlier['winner'])
            ]
            
            if not base_point.empty:
                y_val = base_point['win_share'].values[0]
                winner_name = outlier['winner']
                
                fig.add_trace(go.Scatter(
                    x=[outlier['the_date']],
                    y=[y_val],
                    mode='markers',
                    marker=dict(
                        symbol='star',
                        size=16,
                        color='yellow',
                        line=dict(color='orange', width=2)
                    ),
                    name='Outlier',
                    showlegend=False,
                    legendgroup=winner_name,
                    hovertext=f"{winner_name} - Outlier<br>Date: {outlier['the_date']}<br>Win Share: {y_val:.4f}"
                ))
    
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=20)
        ),
        xaxis_title="Date",
        yaxis_title="Win Share",
        hovermode='closest',
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        ),
        width=1400,
        height=700,
        margin=dict(l=60, r=200, t=80, b=60)
    )
    
    return fig


def generate_summary_stats(
    base_data: pd.DataFrame,
    suppressed_data: pd.DataFrame,
    outliers: pd.DataFrame,
    suppression_plan: pd.DataFrame
) -> dict:
    """Generate summary statistics for the analysis."""
    
    # Calculate total impact
    base_total = base_data['total_wins'].sum()
    suppressed_total = suppressed_data['total_wins'].sum()
    total_removed = base_total - suppressed_total
    
    # Per-winner impact
    winner_impact = []
    for winner in base_data['winner'].unique():
        base_wins = base_data[base_data['winner'] == winner]['total_wins'].sum()
        supp_wins = suppressed_data[suppressed_data['winner'] == winner]['total_wins'].sum()
        removed = base_wins - supp_wins
        
        if removed > 0:
            winner_impact.append({
                'winner': winner,
                'base_wins': int(base_wins),
                'suppressed_wins': int(supp_wins),
                'removed': int(removed),
                'pct_removed': removed / base_wins * 100 if base_wins > 0 else 0
            })
    
    winner_impact_df = pd.DataFrame(winner_impact).sort_values('removed', ascending=False)
    
    # Stage breakdown
    stage_stats = {}
    if not suppression_plan.empty:
        stage_stats = {
            'auto': {
                'count': len(suppression_plan[suppression_plan['stage'] == 'auto']),
                'total_removed': int(suppression_plan[suppression_plan['stage'] == 'auto']['remove_units'].sum())
            },
            'distributed': {
                'count': len(suppression_plan[suppression_plan['stage'] == 'distributed']),
                'total_removed': int(suppression_plan[suppression_plan['stage'] == 'distributed']['remove_units'].sum())
            }
        }
    
    # Outlier reasons breakdown
    reason_stats = {}
    if not suppression_plan.empty and 'reason' in suppression_plan.columns:
        reason_counts = {}
        for reason in suppression_plan[suppression_plan['stage'] == 'auto']['reason']:
            for r in reason.split(', '):
                reason_counts[r] = reason_counts.get(r, 0) + 1
        reason_stats = reason_counts
    
    return {
        'total_base_wins': int(base_total),
        'total_suppressed_wins': int(suppressed_total),
        'total_removed': int(total_removed),
        'pct_removed': total_removed / base_total * 100 if base_total > 0 else 0,
        'outlier_count': len(outliers),
        'plan_entries': len(suppression_plan),
        'winner_impact': winner_impact_df,
        'stage_stats': stage_stats,
        'reason_stats': reason_stats
    }


def generate_markdown_report(
    stats: dict,
    suppression_plan: pd.DataFrame,
    outliers: pd.DataFrame,
    output_file: str
):
    """Generate comprehensive markdown report."""
    
    report = []
    report.append("# Suppression Analysis Report")
    report.append("")
    report.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Executive Summary
    report.append("## Executive Summary")
    report.append("")
    report.append(f"- **Total Wins (Base):** {stats['total_base_wins']:,}")
    report.append(f"- **Total Wins (Suppressed):** {stats['total_suppressed_wins']:,}")
    report.append(f"- **Total Removed:** {stats['total_removed']:,} ({stats['pct_removed']:.2f}%)")
    report.append(f"- **Outliers Detected:** {stats['outlier_count']}")
    report.append(f"- **Suppression Plan Entries:** {stats['plan_entries']}")
    report.append("")
    
    # Stage Breakdown
    if stats['stage_stats']:
        report.append("## Suppression Stage Breakdown")
        report.append("")
        report.append("### Stage 1: Targeted Auto-Suppression")
        auto = stats['stage_stats'].get('auto', {})
        report.append(f"- **Pair-DMA combinations:** {auto.get('count', 0)}")
        report.append(f"- **Wins removed:** {auto.get('total_removed', 0):,}")
        report.append("")
        report.append("**Triggers:**")
        for reason, count in stats['reason_stats'].items():
            report.append(f"- {reason}: {count} occurrences")
        report.append("")
        
        report.append("### Stage 2: Equalized Distribution")
        dist = stats['stage_stats'].get('distributed', {})
        report.append(f"- **Pair-DMA combinations:** {dist.get('count', 0)}")
        report.append(f"- **Wins removed:** {dist.get('total_removed', 0):,}")
        report.append("")
    
    # Winner Impact
    report.append("## Per-Winner Impact")
    report.append("")
    if not stats['winner_impact'].empty:
        report.append("| Winner | Base Wins | Suppressed Wins | Removed | % Removed |")
        report.append("|--------|-----------|-----------------|---------|-----------|")
        for _, row in stats['winner_impact'].iterrows():
            report.append(f"| {row['winner']} | {row['base_wins']:,} | {row['suppressed_wins']:,} | {row['removed']:,} | {row['pct_removed']:.2f}% |")
        report.append("")
    
    # Outlier Details
    report.append("## Detected Outliers")
    report.append("")
    if not outliers.empty:
        report.append("| Date | Winner | Current Share | Historical Share | Z-Score |")
        report.append("|------|--------|---------------|------------------|---------|")
        for _, row in outliers.head(20).iterrows():
            curr_share = row.get('nat_share_current', 0)
            hist_share = row.get('nat_mu_share', 0)
            z = row.get('nat_z', 0)
            report.append(f"| {row['the_date']} | {row['winner']} | {curr_share:.4f} | {hist_share:.4f} | {z:.2f} |")
        if len(outliers) > 20:
            report.append(f"| ... | ... | ... | ... | ... |")
            report.append(f"*Showing first 20 of {len(outliers)} outliers*")
        report.append("")
    
    # Top Suppression Actions
    report.append("## Top Suppression Actions (Auto Stage)")
    report.append("")
    if not suppression_plan.empty:
        auto_plan = suppression_plan[suppression_plan['stage'] == 'auto'].sort_values(
            'remove_units', ascending=False
        ).head(20)
        
        if not auto_plan.empty:
            report.append("| Date | Winner | Loser | DMA | Removed | Current | Baseline | Z-Score | Reason |")
            report.append("|------|--------|-------|-----|---------|---------|----------|---------|--------|")
            for _, row in auto_plan.iterrows():
                report.append(
                    f"| {row['date']} | {row['winner']} | {row['loser']} | "
                    f"{row['dma_name'][:20]}... | {row['remove_units']} | "
                    f"{row['pair_wins_current']:.0f} | {row['pair_mu_wins']:.1f} | "
                    f"{row['pair_z']:.2f} | {row['reason'][:50]}... |"
                )
            report.append("")
    
    # Visualization
    report.append("## Visualization")
    report.append("")
    report.append("The visualization shows:")
    report.append("- **Solid lines**: Original data (before suppression)")
    report.append("- **Dashed lines**: Suppressed data (after suppression)")
    report.append("- **Yellow stars**: Detected outliers")
    report.append("")
    report.append("![Win Share Before vs After](./suppression_visualization.html)")
    report.append("")
    
    # Methodology
    report.append("## Methodology")
    report.append("")
    report.append("### 2-Stage Distribution Algorithm")
    report.append("")
    report.append("**Stage 1: Targeted Auto-Suppression**")
    report.append("- Identifies outlier pair-DMA combinations based on:")
    report.append("  - High z-scores (statistical outliers)")
    report.append("  - Large percentage jumps (>30% increase)")
    report.append("  - Rare pairs (historical baseline < 5)")
    report.append("  - First appearances (never seen before)")
    report.append("- Removes excess wins (current - baseline) or all wins for new/rare pairs")
    report.append("- Prioritizes highest z-score pairs first")
    report.append("")
    report.append("**Stage 2: Equalized Distribution**")
    report.append("- If Stage 1 doesn't remove enough, distributes remaining evenly")
    report.append("- Each pair-DMA gets base amount (floor division)")
    report.append("- Remaining units distributed to pairs with highest residual capacity")
    report.append("")
    report.append("### Removal Target Calculation")
    report.append("")
    report.append("Formula: `need = (W - μ×T) / (1 - μ)`")
    report.append("")
    report.append("Where:")
    report.append("- W = Current total wins for winner")
    report.append("- T = Current market total wins")
    report.append("- μ = Historical average share")
    report.append("")
    report.append("This accounts for market dynamics: removing wins from winner also shrinks market total.")
    report.append("")
    
    # Write report
    with open(output_file, 'w') as f:
        f.write('\n'.join(report))
    
    print(f"[SUCCESS] Report written to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive suppression analysis using DB cubes"
    )
    parser.add_argument('--db', default='data/databases/duck_suppression.db', help='Path to database')
    parser.add_argument('--ds', default='gamoshi', help='Dataset name')
    parser.add_argument('--mover-ind', choices=['True', 'False'], default='True', help='Mover indicator')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--window', type=int, default=14, help='Lookback window for statistics')
    parser.add_argument('--z-nat', type=float, default=2.5, help='Z-score threshold for national outliers')
    parser.add_argument('--z-pair', type=float, default=2.0, help='Z-score threshold for pair outliers')
    parser.add_argument('--pct-thresh', type=float, default=0.30, help='Percentage change threshold')
    parser.add_argument('--rare-thresh', type=float, default=5.0, help='Baseline threshold for rare pairs')
    parser.add_argument('--min-volume', type=float, default=5.0, help='Minimum volume for consideration')
    parser.add_argument('--lookback-days', type=int, default=90, help='Days to look back for first appearances')
    parser.add_argument('--output-dir', default='analysis_results/suppression', help='Output directory')
    parser.add_argument('--winners', nargs='+', help='Specific winners to analyze (default: all)')
    
    args = parser.parse_args()
    
    # Convert mover_ind to boolean
    mover_ind = args.mover_ind == 'True'
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    print("=" * 80)
    print("COMPREHENSIVE SUPPRESSION ANALYSIS")
    print("=" * 80)
    print(f"Dataset: {args.ds}")
    print(f"Mover: {mover_ind}")
    print(f"Date Range: {args.start_date} to {args.end_date}")
    print(f"Database: {args.db}")
    print("=" * 80)
    print()
    
    # Step 1: Get base data
    print("[1/6] Loading base data...")
    base_data = get_national_timeseries(
        args.db, args.ds, mover_ind, args.start_date, args.end_date, args.winners
    )
    print(f"  → Loaded {len(base_data)} records")
    print()
    
    # Step 2: Build suppression plan
    print("[2/6] Building suppression plan...")
    suppression_plan = suppress.build_full_suppression_plan(
        db_path=args.db,
        ds=args.ds,
        mover_ind=mover_ind,
        start_date=args.start_date,
        end_date=args.end_date,
        window=args.window,
        z_nat=args.z_nat,
        z_pair=args.z_pair,
        pct_thresh=args.pct_thresh,
        rare_thresh=args.rare_thresh,
        min_volume=args.min_volume,
        lookback_days=args.lookback_days
    )
    print()
    
    if suppression_plan.empty:
        print("[WARNING] No suppressions needed!")
        return
    
    # Step 3: Get outliers for visualization
    print("[3/6] Getting national outliers...")
    nat_outliers = outliers.national_outliers(
        db_path=args.db,
        ds=args.ds,
        mover_ind=mover_ind,
        start_date=args.start_date,
        end_date=args.end_date,
        window=args.window,
        z_thresh=args.z_nat
    )
    print(f"  → Found {len(nat_outliers)} national outliers")
    print()
    
    # Step 4: Apply suppression
    print("[4/6] Applying suppression to data...")
    suppressed_data = apply_suppression_to_data(
        args.db, args.ds, mover_ind, args.start_date, args.end_date, suppression_plan
    )
    print(f"  → Generated {len(suppressed_data)} suppressed records")
    print()
    
    # Step 5: Generate visualization
    print("[5/6] Creating visualization...")
    fig = create_before_after_visualization(
        base_data, suppressed_data, nat_outliers,
        title=f"Win Share Analysis: {args.ds} ({'Mover' if mover_ind else 'Non-Mover'})"
    )
    
    viz_file = os.path.join(args.output_dir, 'suppression_visualization.html')
    fig.write_html(viz_file)
    print(f"  → Saved to {viz_file}")
    print()
    
    # Step 6: Generate report
    print("[6/6] Generating report...")
    stats = generate_summary_stats(base_data, suppressed_data, nat_outliers, suppression_plan)
    
    report_file = os.path.join(args.output_dir, 'SUPPRESSION_ANALYSIS.md')
    generate_markdown_report(stats, suppression_plan, nat_outliers, report_file)
    
    # Save data files
    plan_file = os.path.join(args.output_dir, 'suppression_plan.csv')
    suppression_plan.to_csv(plan_file, index=False)
    print(f"  → Saved plan to {plan_file}")
    
    base_file = os.path.join(args.output_dir, 'base_data.csv')
    base_data.to_csv(base_file, index=False)
    print(f"  → Saved base data to {base_file}")
    
    supp_file = os.path.join(args.output_dir, 'suppressed_data.csv')
    suppressed_data.to_csv(supp_file, index=False)
    print(f"  → Saved suppressed data to {supp_file}")
    
    print()
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"Total Removed: {stats['total_removed']:,} wins ({stats['pct_removed']:.2f}%)")
    print(f"Outliers: {stats['outlier_count']}")
    print(f"Plan Entries: {stats['plan_entries']}")
    print(f"Output Directory: {args.output_dir}")
    print("=" * 80)


if __name__ == '__main__':
    main()
