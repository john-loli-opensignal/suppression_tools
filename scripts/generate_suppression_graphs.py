#!/usr/bin/env python3
"""
Generate before/after suppression visualization graphs.

Shows:
1. Win share time series with before (solid) and after (dashed) overlays
2. Z-scores and pair outliers by date with current vs avg
3. Suppressed record counts by date with current vs avg
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import duckdb

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tools import db
from tools.src import metrics

DB_PATH = project_root / "data" / "databases" / "duck_suppression.db"
ANALYSIS_DIR = project_root / "analysis_results" / "suppression"
GRAPHS_DIR = ANALYSIS_DIR / "graphs"
GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

TARGET_DATES = [
    "2025-06-19",
    "2025-08-15",
    "2025-08-16",
    "2025-08-17",
    "2025-08-18",
]

DS = "gamoshi"


def load_json(filename):
    """Load JSON file from analysis data directory."""
    filepath = ANALYSIS_DIR / "data" / filename
    if not filepath.exists():
        print(f"[WARNING] File not found: {filepath}")
        return None
    with open(filepath, "r") as f:
        return json.load(f)


def get_win_share_data(mover_ind, start_date, end_date):
    """
    Get win share time series data from database.
    
    Returns DataFrame with columns: the_date, winner, total_wins
    """
    table_name = f"{DS}_win_{'mover' if mover_ind else 'non_mover'}_cube"
    
    sql = f"""
    SELECT 
        the_date,
        winner,
        SUM(total_wins) as total_wins
    FROM {table_name}
    WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY the_date, winner
    ORDER BY the_date, winner
    """
    
    return db.query(sql, str(DB_PATH))


def apply_suppressions(df, suppression_data):
    """
    Apply suppressions to win share data.
    
    Returns modified DataFrame with suppressed wins removed.
    """
    if not suppression_data:
        return df
    
    df_suppressed = df.copy()
    
    # Build suppression index for fast lookup
    # suppression_data is a list of census blocks with suppression flags
    suppressions = {}
    for block in suppression_data:
        if block.get('should_suppress', False):
            date = block['the_date']
            winner = block['winner']
            wins = block.get('current_wins', 0)
            
            key = (date, winner)
            if key not in suppressions:
                suppressions[key] = 0
            suppressions[key] += wins
    
    # Apply suppressions
    for idx, row in df_suppressed.iterrows():
        key = (str(row['the_date']), row['winner'])
        if key in suppressions:
            suppressed_wins = suppressions[key]
            df_suppressed.at[idx, 'total_wins'] = max(0, row['total_wins'] - suppressed_wins)
    
    return df_suppressed


def calculate_win_share(df):
    """Calculate win share percentage for each carrier by date."""
    # Remove any existing daily_total and win_share columns
    df = df[[c for c in df.columns if c not in ['daily_total', 'win_share']]]
    
    # Calculate daily totals
    daily_totals = df.groupby('the_date')['total_wins'].sum().reset_index()
    daily_totals.columns = ['the_date', 'daily_total']
    
    # Merge and calculate share
    df = df.merge(daily_totals, on='the_date')
    df['win_share'] = (df['total_wins'] / df['daily_total']) * 100
    
    return df


def plot_win_share_overlay(mover_ind):
    """
    Create win share time series with before/after overlay.
    
    Solid lines = before suppression (on top)
    Dashed lines = after suppression (underneath)
    """
    print(f"\n[INFO] Generating win share overlay for mover_ind={mover_ind}")
    
    # Load suppression data
    suppression_file = f"census_block_suppression_mover_{mover_ind}.json"
    suppression_data = load_json(suppression_file)
    
    if not suppression_data:
        print(f"[WARNING] No suppression data found for mover_ind={mover_ind}")
        return
    
    # Determine date range (30 days before first target to 7 days after last target)
    start_date = (datetime.strptime(TARGET_DATES[0], "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = (datetime.strptime(TARGET_DATES[-1], "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
    
    # Get before data
    df_before = get_win_share_data(mover_ind, start_date, end_date)
    df_before = calculate_win_share(df_before)
    
    # Get after data (with suppressions applied)
    df_after = apply_suppressions(df_before.copy(), suppression_data)
    df_after = calculate_win_share(df_after)
    
    # Get top 10 carriers by average win share
    top_carriers = (
        df_before.groupby('winner')['win_share']
        .mean()
        .sort_values(ascending=False)
        .head(10)
        .index.tolist()
    )
    
    # Create figure
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Color palette
    colors = plt.cm.tab10(range(10))
    
    # Plot each carrier - AFTER first (dashed, underneath)
    for i, carrier in enumerate(top_carriers):
        carrier_data_after = df_after[df_after['winner'] == carrier].sort_values('the_date')
        if not carrier_data_after.empty:
            ax.plot(
                pd.to_datetime(carrier_data_after['the_date']),
                carrier_data_after['win_share'],
                label=f"{carrier} (after)",
                color=colors[i],
                linestyle='--',
                linewidth=2,
                alpha=0.7,
                zorder=1
            )
    
    # Plot each carrier - BEFORE second (solid, on top)
    for i, carrier in enumerate(top_carriers):
        carrier_data_before = df_before[df_before['winner'] == carrier].sort_values('the_date')
        if not carrier_data_before.empty:
            ax.plot(
                pd.to_datetime(carrier_data_before['the_date']),
                carrier_data_before['win_share'],
                label=f"{carrier} (before)",
                color=colors[i],
                linestyle='-',
                linewidth=2.5,
                alpha=0.9,
                zorder=2
            )
    
    # Highlight target dates
    for target_date in TARGET_DATES:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        ax.axvline(target_dt, color='red', linestyle=':', alpha=0.5, linewidth=1.5, zorder=0)
    
    # Format
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Win Share (%)', fontsize=12, fontweight='bold')
    ax.set_title(
        f'Win Share Over Time: {"Movers" if mover_ind else "Non-Movers"}\n'
        f'Before (solid) vs After (dashed) Suppression',
        fontsize=14,
        fontweight='bold'
    )
    ax.grid(True, alpha=0.3)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    
    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    
    # Save
    output_file = GRAPHS_DIR / f"win_share_overlay_mover_{mover_ind}.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"[SUCCESS] Saved: {output_file}")
    plt.close()


def plot_outlier_metrics(mover_ind):
    """
    Plot outlier metrics by date:
    - Z-scores (current vs avg)
    - Pair outlier counts
    - Suppressed records
    """
    print(f"\n[INFO] Generating outlier metrics for mover_ind={mover_ind}")
    
    # Load data
    national_data = load_json(f"national_outliers_mover_{mover_ind}.json")
    pair_data = load_json(f"pair_outliers_mover_{mover_ind}.json")
    suppression_data = load_json(f"census_block_suppression_mover_{mover_ind}.json")
    
    if not all([national_data, pair_data, suppression_data]):
        print(f"[WARNING] Missing data files for mover_ind={mover_ind}")
        return
    
    # Aggregate by date
    date_metrics = {}
    
    # National outliers
    for outlier in national_data:
        date = outlier['the_date']
        if date not in date_metrics:
            date_metrics[date] = {
                'max_zscore': 0,
                'avg_zscore': 0,
                'zscore_count': 0,
                'pair_outliers': 0,
                'suppressed_records': 0,
                'suppressed_wins': 0,
            }
        z = abs(outlier.get('z', 0))
        date_metrics[date]['max_zscore'] = max(date_metrics[date]['max_zscore'], z)
        date_metrics[date]['avg_zscore'] += z
        date_metrics[date]['zscore_count'] += 1
    
    # Calculate averages
    for date in date_metrics:
        if date_metrics[date]['zscore_count'] > 0:
            date_metrics[date]['avg_zscore'] /= date_metrics[date]['zscore_count']
    
    # Pair outliers
    for pair in pair_data:
        date = pair['the_date']
        if date in date_metrics:
            date_metrics[date]['pair_outliers'] += 1
    
    # Suppressions - suppression_data is a list of blocks
    for block in suppression_data:
        if block.get('should_suppress', False):
            date = block['the_date']
            if date in date_metrics:
                date_metrics[date]['suppressed_records'] += 1
                date_metrics[date]['suppressed_wins'] += block.get('current_wins', 0)
    
    # Convert to DataFrame
    df = pd.DataFrame.from_dict(date_metrics, orient='index')
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    
    # Filter to target dates
    target_dates_dt = [datetime.strptime(d, "%Y-%m-%d") for d in TARGET_DATES]
    df = df[df.index.isin(target_dates_dt)]
    
    if df.empty:
        print(f"[WARNING] No metrics for target dates")
        return
    
    # Create subplots
    fig, axes = plt.subplots(3, 1, figsize=(14, 12))
    
    # Plot 1: Z-scores
    ax = axes[0]
    x = range(len(df))
    width = 0.35
    
    ax.bar([i - width/2 for i in x], df['max_zscore'], width, label='Max Z-Score', alpha=0.8, color='red')
    ax.bar([i + width/2 for i in x], df['avg_zscore'], width, label='Avg Z-Score', alpha=0.8, color='orange')
    ax.axhline(y=2.5, color='gray', linestyle='--', label='Threshold (2.5)', alpha=0.7)
    
    ax.set_ylabel('Z-Score', fontsize=11, fontweight='bold')
    ax.set_title(f'Z-Score Outliers by Date: {"Movers" if mover_ind else "Non-Movers"}', fontsize=12, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in df.index], rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    # Plot 2: Pair outliers
    ax = axes[1]
    ax.bar(x, df['pair_outliers'], color='steelblue', alpha=0.8)
    ax.set_ylabel('Count', fontsize=11, fontweight='bold')
    ax.set_title('H2H Pair Outliers Detected', fontsize=12, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in df.index], rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add count labels
    for i, v in enumerate(df['pair_outliers']):
        ax.text(i, v + max(df['pair_outliers']) * 0.02, str(int(v)), ha='center', va='bottom', fontsize=9)
    
    # Plot 3: Suppressions
    ax = axes[2]
    ax.bar([i - width/2 for i in x], df['suppressed_records'], width, label='Records Suppressed', alpha=0.8, color='darkred')
    ax.bar([i + width/2 for i in x], df['suppressed_wins'], width, label='Wins Suppressed', alpha=0.8, color='salmon')
    
    ax.set_ylabel('Count', fontsize=11, fontweight='bold')
    ax.set_xlabel('Date', fontsize=11, fontweight='bold')
    ax.set_title('Census Blocks & Wins Suppressed', fontsize=12, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([d.strftime('%Y-%m-%d') for d in df.index], rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    # Save
    output_file = GRAPHS_DIR / f"outlier_metrics_mover_{mover_ind}.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"[SUCCESS] Saved: {output_file}")
    plt.close()


def plot_carrier_comparison(mover_ind):
    """
    Plot top outlier carriers showing current vs historical average.
    """
    print(f"\n[INFO] Generating carrier comparison for mover_ind={mover_ind}")
    
    # Load data
    national_data = load_json(f"national_outliers_mover_{mover_ind}.json")
    
    if not national_data:
        print(f"[WARNING] No national outliers data")
        return
    
    # Get top 15 outliers by z-score
    top_outliers = sorted(national_data, key=lambda x: abs(x.get('z', 0)), reverse=True)[:15]
    
    # We need to query the database to get current and historical averages
    # Since we only have z-scores in the JSON, we'll need to calculate from data
    # For now, we'll create a simplified version showing the z-scores
    
    carriers = []
    dates = []
    zscores = []
    
    for outlier in top_outliers:
        carriers.append(f"{outlier['winner']}\n{outlier['the_date']}")
        dates.append(outlier['the_date'])
        zscores.append(abs(outlier.get('z', 0)))
    
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))
    
    x = range(len(carriers))
    
    # Plot bars
    colors = ['red' if z > 5 else 'orange' if z > 3 else 'yellow' for z in zscores]
    bars = ax.bar(x, zscores, alpha=0.8, color=colors)
    
    # Add threshold line
    ax.axhline(y=2.5, color='gray', linestyle='--', label='Threshold (2.5)', alpha=0.7, linewidth=2)
    
    # Add z-score as text
    for i, z in enumerate(zscores):
        ax.text(i, z + 0.3, f'{z:.1f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    ax.set_ylabel('Z-Score', fontsize=11, fontweight='bold')
    ax.set_xlabel('Carrier & Date', fontsize=11, fontweight='bold')
    ax.set_title(
        f'Top 15 National Outliers by Z-Score: {"Movers" if mover_ind else "Non-Movers"}',
        fontsize=12,
        fontweight='bold'
    )
    ax.set_xticks(x)
    ax.set_xticklabels(carriers, rotation=45, ha='right', fontsize=8)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add color legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='red', alpha=0.8, label='Extreme (z > 5)'),
        Patch(facecolor='orange', alpha=0.8, label='High (3 < z ≤ 5)'),
        Patch(facecolor='yellow', alpha=0.8, label='Medium (z ≤ 3)'),
    ]
    ax.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    
    # Save
    output_file = GRAPHS_DIR / f"carrier_zscore_mover_{mover_ind}.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"[SUCCESS] Saved: {output_file}")
    plt.close()


def main():
    print("=" * 70)
    print("Generating Suppression Visualization Graphs")
    print("=" * 70)
    
    for mover_ind in [True, False]:
        print(f"\n{'='*70}")
        print(f"Processing mover_ind={mover_ind}")
        print(f"{'='*70}")
        
        plot_win_share_overlay(mover_ind)
        plot_outlier_metrics(mover_ind)
        plot_carrier_comparison(mover_ind)
    
    print("\n" + "=" * 70)
    print(f"All graphs saved to: {GRAPHS_DIR}")
    print("=" * 70)


if __name__ == "__main__":
    main()
