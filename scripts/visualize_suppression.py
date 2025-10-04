#!/usr/bin/env python3
"""
Visualize Suppression Results with Before/After Overlay

Creates visualizations showing:
- Base series (solid lines)
- Suppressed series (dashed lines) 
- Outlier markers (yellow stars)
- Side-by-side metrics comparison

Usage:
    uv run scripts/visualize_suppression.py --plan analysis_results/zscore_suppression_plan.json --ds gamoshi
"""

import argparse
import sys
import json
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools import db


def load_suppression_plan(plan_path: str) -> pd.DataFrame:
    """Load suppression plan from JSON."""
    with open(plan_path) as f:
        plan = json.load(f)
    
    df = pd.DataFrame(plan)
    df['the_date'] = pd.to_datetime(df['the_date'])
    return df


def get_national_timeseries(
    ds: str,
    mover_ind: bool,
    start_date: date,
    end_date: date,
    db_path: str = 'data/databases/duck_suppression.db'
) -> pd.DataFrame:
    """Get national timeseries data."""
    sql = f"""
    WITH daily AS (
        SELECT
            the_date,
            winner,
            total_wins
        FROM national_daily
        WHERE ds = '{ds}'
          AND mover_ind = {mover_ind}
          AND the_date BETWEEN '{start_date}' AND '{end_date}'
    ),
    market AS (
        SELECT
            the_date,
            SUM(total_wins) as market_total
        FROM daily
        GROUP BY the_date
    )
    SELECT
        d.the_date,
        d.winner,
        d.total_wins,
        m.market_total,
        CAST(d.total_wins AS DOUBLE) / NULLIF(m.market_total, 0) as win_share
    FROM daily d
    JOIN market m ON d.the_date = m.the_date
    ORDER BY d.the_date, d.winner
    """
    
    return db.query(sql, db_path)


def apply_suppression_plan(
    base_data: pd.DataFrame,
    plan: pd.DataFrame
) -> pd.DataFrame:
    """
    Apply suppression plan to base data.
    
    Returns suppressed timeseries with recalculated win_shares.
    """
    # Aggregate plan by date and winner
    plan_agg = plan.groupby(['the_date', 'winner'], as_index=False)['remove_units'].sum()
    plan_agg.columns = ['the_date', 'winner', 'total_removal']
    
    # Merge with base data
    suppressed = base_data.merge(plan_agg, on=['the_date', 'winner'], how='left')
    suppressed['total_removal'] = suppressed['total_removal'].fillna(0)
    
    # Subtract removals
    suppressed['total_wins_suppressed'] = np.maximum(0, suppressed['total_wins'] - suppressed['total_removal'])
    
    # Recalculate market totals and win_shares
    market_suppressed = suppressed.groupby('the_date', as_index=False)['total_wins_suppressed'].sum()
    market_suppressed.columns = ['the_date', 'market_total_suppressed']
    
    suppressed = suppressed.merge(market_suppressed, on='the_date')
    suppressed['win_share_suppressed'] = suppressed['total_wins_suppressed'] / suppressed['market_total_suppressed']
    
    return suppressed


def create_comparison_plot(
    base_data: pd.DataFrame,
    suppressed_data: pd.DataFrame,
    plan: pd.DataFrame,
    title: str = "Win Share: Before vs After Suppression"
) -> go.Figure:
    """
    Create comparison plot with base (solid) and suppressed (dashed) overlaid.
    
    Base series on bottom (solid), suppressed on top (dashed) so differences are visible.
    """
    fig = go.Figure()
    
    # Get carriers that were suppressed
    suppressed_carriers = plan['winner'].unique()
    all_carriers = base_data['winner'].unique()
    
    # Sort carriers by total wins (descending) for better legend order
    carrier_totals = base_data.groupby('winner')['total_wins'].sum().sort_values(ascending=False)
    carriers_sorted = carrier_totals.index.tolist()
    
    # Color palette
    colors = {}
    color_palette = [
        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
    ]
    
    for i, carrier in enumerate(carriers_sorted[:10]):  # Top 10 carriers get distinct colors
        colors[carrier] = color_palette[i % len(color_palette)]
    
    # Add suppressed series FIRST (as dashed, layer below)
    for carrier in carriers_sorted:
        if carrier in suppressed_carriers:
            df_supp = suppressed_data[suppressed_data['winner'] == carrier].sort_values('the_date')
            
            fig.add_trace(go.Scatter(
                x=df_supp['the_date'],
                y=df_supp['win_share_suppressed'],
                mode='lines',
                name=f"{carrier} (suppressed)",
                line=dict(
                    dash='dash',
                    width=2,
                    color=colors.get(carrier, '#888888')
                ),
                showlegend=True,
                legendgroup=carrier,
                opacity=0.7
            ))
    
    # Add base series (solid, layer on top)
    for carrier in carriers_sorted:
        df_base = base_data[base_data['winner'] == carrier].sort_values('the_date')
        
        fig.add_trace(go.Scatter(
            x=df_base['the_date'],
            y=df_base['win_share'],
            mode='lines',
            name=carrier,
            line=dict(
                dash='solid',
                width=2.5,
                color=colors.get(carrier, '#888888')
            ),
            showlegend=True,
            legendgroup=carrier
        ))
    
    # Add outlier markers (yellow stars)
    outliers = plan.groupby(['the_date', 'winner'], as_index=False).first()
    
    for _, outlier in outliers.iterrows():
        # Get y-value from base data
        match = base_data[
            (base_data['the_date'] == outlier['the_date']) &
            (base_data['winner'] == outlier['winner'])
        ]
        
        if not match.empty:
            y_val = match['win_share'].values[0]
            
            fig.add_trace(go.Scatter(
                x=[outlier['the_date']],
                y=[y_val],
                mode='markers',
                name='Outlier',
                marker=dict(
                    symbol='star',
                    size=14,
                    color='gold',
                    line=dict(width=1, color='orange')
                ),
                showlegend=False,
                hovertemplate=f"<b>{outlier['winner']}</b><br>" +
                             f"Date: {outlier['the_date'].strftime('%Y-%m-%d')}<br>" +
                             f"Win Share: {y_val:.4f}<br>" +
                             "<extra></extra>"
            ))
    
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Win Share",
        hovermode='closest',
        width=1400,
        height=800,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=1.01
        )
    )
    
    return fig


def create_metrics_table(
    base_data: pd.DataFrame,
    suppressed_data: pd.DataFrame,
    plan: pd.DataFrame
) -> pd.DataFrame:
    """Create summary metrics table comparing before and after."""
    
    suppressed_carriers = plan['winner'].unique()
    
    metrics = []
    
    for carrier in suppressed_carriers:
        base_carrier = base_data[base_data['winner'] == carrier]
        supp_carrier = suppressed_data[suppressed_data['winner'] == carrier]
        plan_carrier = plan[plan['winner'] == carrier]
        
        # Dates with outliers
        outlier_dates = plan_carrier['the_date'].nunique()
        
        # Total removal
        total_removal = plan_carrier['remove_units'].sum()
        
        # Average win share before/after
        avg_share_before = base_carrier['win_share'].mean()
        avg_share_after = supp_carrier['win_share_suppressed'].mean()
        
        # Max win share before/after
        max_share_before = base_carrier['win_share'].max()
        max_share_after = supp_carrier['win_share_suppressed'].max()
        
        metrics.append({
            'Carrier': carrier,
            'Outlier Dates': outlier_dates,
            'Total Removal': int(total_removal),
            'Avg Share Before': f"{avg_share_before:.4f}",
            'Avg Share After': f"{avg_share_after:.4f}",
            'Delta Avg': f"{(avg_share_after - avg_share_before):.4f}",
            'Max Share Before': f"{max_share_before:.4f}",
            'Max Share After': f"{max_share_after:.4f}",
            'Delta Max': f"{(max_share_after - max_share_before):.4f}"
        })
    
    return pd.DataFrame(metrics).sort_values('Total Removal', ascending=False)


def create_detailed_analysis(
    plan: pd.DataFrame,
    output_dir: str
):
    """Create detailed analysis markdown."""
    
    md = []
    md.append("# Suppression Analysis Results")
    md.append("")
    md.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    md.append("")
    
    # Overall summary
    md.append("## Overall Summary")
    md.append("")
    md.append(f"- **Total Records:** {len(plan):,}")
    md.append(f"- **Total Wins Removed:** {int(plan['remove_units'].sum()):,}")
    md.append(f"- **Date Range:** {plan['the_date'].min().strftime('%Y-%m-%d')} to {plan['the_date'].max().strftime('%Y-%m-%d')}")
    md.append(f"- **Carriers Affected:** {plan['winner'].nunique()}")
    md.append("")
    
    # By stage
    md.append("## Removal by Stage")
    md.append("")
    stage_stats = plan.groupby('stage').agg({
        'remove_units': ['count', 'sum'],
        'winner': 'nunique'
    }).round(0)
    stage_stats.columns = ['Records', 'Total Removal', 'Unique Carriers']
    md.append("| Stage | Records | Total Removal | Unique Carriers |")
    md.append("|-------|---------|---------------|-----------------|")
    for stage, row in stage_stats.iterrows():
        md.append(f"| {stage} | {int(row['Records'])} | {int(row['Total Removal'])} | {int(row['Unique Carriers'])} |")
    md.append("")
    
    # Top carriers
    md.append("## Top 20 Carriers by Removal")
    md.append("")
    carrier_stats = plan.groupby('winner').agg({
        'remove_units': 'sum',
        'the_date': 'nunique',
        'stage': lambda x: (x == 'stage1_census_block').sum() + (x == 'stage1_pair_level').sum()
    }).round(0)
    carrier_stats.columns = ['Total Removal', 'Outlier Dates', 'Stage1 Records']
    carrier_stats = carrier_stats.sort_values('Total Removal', ascending=False).head(20)
    md.append("| Carrier | Total Removal | Outlier Dates | Stage1 Records |")
    md.append("|---------|---------------|---------------|----------------|")
    for carrier, row in carrier_stats.iterrows():
        md.append(f"| {carrier} | {int(row['Total Removal'])} | {int(row['Outlier Dates'])} | {int(row['Stage1 Records'])} |")
    md.append("")
    
    # Stage 1 details
    stage1 = plan[plan['stage'].str.contains('stage1')]
    if not stage1.empty:
        md.append("## Stage 1 Targeted Removal Details")
        md.append("")
        md.append(f"Stage 1 targeted {len(stage1):,} records for removal based on outlier triggers:")
        md.append("")
        
        # Count by trigger type
        first_appear = stage1[stage1.get('is_first_appearance', False) == True] if 'is_first_appearance' in stage1.columns else pd.DataFrame()
        md.append(f"- **First Appearance:** {len(first_appear):,} records")
        
        high_z = stage1[stage1.get('pair_z', 0) > 2.5] if 'pair_z' in stage1.columns else pd.DataFrame()
        md.append(f"- **High Z-Score (>2.5):** {len(high_z):,} records")
        md.append("")
        
        if 'pair_z' in stage1.columns:
            md.append("### Top 10 Highest Z-Scores in Stage 1")
            md.append("")
            top_z = stage1.nlargest(10, 'pair_z')[['the_date', 'winner', 'loser', 'dma_name', 'pair_z', 'remove_units']]
            md.append("| Date | Winner | Loser | DMA | Z-Score | Removal |")
            md.append("|------|--------|-------|-----|---------|---------|")
            for _, row in top_z.iterrows():
                date_str = row['the_date'].strftime('%Y-%m-%d')
                md.append(f"| {date_str} | {row['winner']} | {row['loser']} | {row['dma_name']} | {row['pair_z']:.2f} | {int(row['remove_units'])} |")
            md.append("")
    
    # Save markdown
    output_path = Path(output_dir) / 'ZSCORE_SUPPRESSION_ANALYSIS.md'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write('\n'.join(md))
    
    print(f"\nDetailed analysis saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Visualize Suppression Results')
    parser.add_argument('--plan', required=True, help='Path to suppression plan JSON')
    parser.add_argument('--ds', required=True, help='Dataset name')
    parser.add_argument('--mover-ind', type=lambda x: x.lower() == 'true', default=True, help='Mover indicator')
    parser.add_argument('--db', default='data/databases/duck_suppression.db', help='Database path')
    parser.add_argument('--output', default='analysis_results/suppression_visualization.html', help='Output HTML file')
    
    args = parser.parse_args()
    
    print("Loading suppression plan...")
    plan = load_suppression_plan(args.plan)
    
    print(f"Plan contains {len(plan)} records")
    print(f"Date range: {plan['the_date'].min()} to {plan['the_date'].max()}")
    
    # Get base data
    print("Fetching base timeseries...")
    start_date = plan['the_date'].min().date()
    end_date = plan['the_date'].max().date()
    
    base_data = get_national_timeseries(args.ds, args.mover_ind, start_date, end_date, args.db)
    
    print(f"Base data: {len(base_data)} records")
    
    # Apply suppression
    print("Applying suppression plan...")
    suppressed_data = apply_suppression_plan(base_data, plan)
    
    # Create visualization
    print("Creating visualization...")
    fig = create_comparison_plot(base_data, suppressed_data, plan)
    
    # Save HTML
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    fig.write_html(str(output_path))
    print(f"\nVisualization saved to: {output_path}")
    
    # Create metrics table
    print("\nGenerating metrics table...")
    metrics = create_metrics_table(base_data, suppressed_data, plan)
    
    metrics_path = output_path.with_name('suppression_metrics.csv')
    metrics.to_csv(metrics_path, index=False)
    print(f"Metrics saved to: {metrics_path}")
    
    print("\nTop 10 Carriers by Removal:")
    print(metrics.head(10).to_string(index=False))
    
    # Create detailed analysis
    print("\nGenerating detailed analysis...")
    create_detailed_analysis(plan, str(output_path.parent))
    
    print("\n" + "="*70)
    print("Suppression visualization complete!")
    print("="*70)


if __name__ == '__main__':
    main()
