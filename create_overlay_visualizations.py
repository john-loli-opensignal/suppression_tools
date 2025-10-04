#!/usr/bin/env python3
"""
Create before/after overlay visualizations for suppression analysis.
Before data shown as solid lines, after data as dashed lines.
"""

import json
import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

# Paths
DB_PATH = "duck_suppression.db"
RESULTS_DIR = Path("suppression_analysis_results")
GRAPHS_DIR = RESULTS_DIR / "graphs"
DATA_DIR = RESULTS_DIR / "data"

GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

# Target dates to highlight
TARGET_DATES = [
    "2025-06-19",
    "2025-08-15", 
    "2025-08-16",
    "2025-08-17",
    "2025-08-18"
]

def load_suppression_data(mover_ind):
    """Load census block suppression data"""
    json_path = DATA_DIR / f"census_block_suppression_mover_{mover_ind}.json"
    if not json_path.exists():
        return []
    with open(json_path) as f:
        return json.load(f)

def create_overlay_timeseries(mover_ind, top_n=10):
    """Create overlay time series showing before/after with dashed lines"""
    
    print(f"\n[INFO] Creating overlay visualization for mover_ind={mover_ind}")
    
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # Get suppression list
    suppression_data = load_suppression_data(mover_ind)
    
    # Create temp table with suppressions
    if suppression_data:
        suppressions_df = []
        for item in suppression_data:
            for block in item.get("blocks_to_suppress", []):
                suppressions_df.append({
                    "the_date": item["date"],
                    "winner": item["winner"],
                    "loser": item["loser"],
                    "census_blockid": block["census_blockid"]
                })
        
        if suppressions_df:
            supp_df = pd.DataFrame(suppressions_df)
            con.register("suppressions", supp_df)
    
    # Get cube table name
    table_name = f"gamoshi_win_{'mover' if mover_ind else 'non_mover'}_cube"
    
    # Query: Get before data (all wins)
    before_sql = f"""
    SELECT 
        the_date,
        winner,
        SUM(total_wins) as wins
    FROM {table_name}
    WHERE the_date BETWEEN '2025-05-15' AND '2025-09-04'
    GROUP BY the_date, winner
    ORDER BY the_date, winner
    """
    
    before_df = con.execute(before_sql).df()
    
    # Query: Get after data (excluding suppressions)
    if suppression_data and suppressions_df:
        after_sql = f"""
        WITH raw AS (
            SELECT 
                c.the_date,
                c.winner,
                c.loser,
                c.census_blockid,
                c.total_wins
            FROM {table_name.replace('_cube', '_census_block_cube')} c
            WHERE c.the_date BETWEEN '2025-05-15' AND '2025-09-04'
        ),
        filtered AS (
            SELECT *
            FROM raw r
            WHERE NOT EXISTS (
                SELECT 1 FROM suppressions s
                WHERE s.the_date = r.the_date
                  AND s.winner = r.winner
                  AND s.loser = r.loser
                  AND s.census_blockid = r.census_blockid
            )
        )
        SELECT 
            the_date,
            winner,
            SUM(total_wins) as wins
        FROM filtered
        GROUP BY the_date, winner
        ORDER BY the_date, winner
        """
        after_df = con.execute(after_sql).df()
    else:
        after_df = before_df.copy()
    
    con.close()
    
    # Calculate win shares
    before_totals = before_df.groupby('the_date')['wins'].sum().to_dict()
    after_totals = after_df.groupby('the_date')['wins'].sum().to_dict()
    
    before_df['win_share'] = before_df.apply(
        lambda row: (row['wins'] / before_totals.get(row['the_date'], 1)) * 100, axis=1
    )
    after_df['win_share'] = after_df.apply(
        lambda row: (row['wins'] / after_totals.get(row['the_date'], 1)) * 100, axis=1
    )
    
    # Get top carriers by total win share
    carrier_totals = before_df.groupby('winner')['win_share'].sum().sort_values(ascending=False)
    top_carriers = carrier_totals.head(top_n).index.tolist()
    
    print(f"[INFO] Top {top_n} carriers: {top_carriers[:5]}...")
    
    # Filter to top carriers
    before_plot = before_df[before_df['winner'].isin(top_carriers)]
    after_plot = after_df[after_df['winner'].isin(top_carriers)]
    
    # Convert dates
    before_plot['date_obj'] = pd.to_datetime(before_plot['the_date'])
    after_plot['date_obj'] = pd.to_datetime(after_plot['the_date'])
    
    # Create figure
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Color palette
    colors = plt.cm.tab10(np.linspace(0, 1, len(top_carriers)))
    
    # Plot each carrier
    for idx, carrier in enumerate(top_carriers):
        color = colors[idx]
        
        # Before data (solid line)
        carrier_before = before_plot[before_plot['winner'] == carrier]
        if not carrier_before.empty:
            ax.plot(
                carrier_before['date_obj'], 
                carrier_before['win_share'],
                color=color,
                linewidth=2,
                label=f"{carrier} (Before)",
                alpha=0.7
            )
        
        # After data (dashed line)
        carrier_after = after_plot[after_plot['winner'] == carrier]
        if not carrier_after.empty:
            ax.plot(
                carrier_after['date_obj'],
                carrier_after['win_share'],
                color=color,
                linewidth=2,
                linestyle='--',
                label=f"{carrier} (After)",
                alpha=0.9
            )
    
    # Highlight target dates
    for target_date in TARGET_DATES:
        target_dt = pd.to_datetime(target_date)
        ax.axvline(x=target_dt, color='red', linestyle=':', alpha=0.5, linewidth=1.5)
        ax.text(target_dt, ax.get_ylim()[1] * 0.95, target_date, 
                rotation=90, verticalalignment='top', fontsize=8, color='red')
    
    # Formatting
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Win Share (%)', fontsize=12)
    ax.set_title(
        f'Win Share Before/After Suppression - {"Movers" if mover_ind else "Non-Movers"}\n'
        f'Solid = Before, Dashed = After | Red lines = Target dates',
        fontsize=14, fontweight='bold'
    )
    ax.grid(True, alpha=0.3)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9, ncol=2)
    
    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    
    # Save
    output_path = GRAPHS_DIR / f"overlay_timeseries_mover_{mover_ind}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"[SUCCESS] Saved: {output_path}")
    plt.close()

def create_target_dates_overlay(mover_ind, top_n=10):
    """Create bar chart comparison for target dates with before/after side-by-side"""
    
    print(f"\n[INFO] Creating target dates overlay for mover_ind={mover_ind}")
    
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # Get suppression list
    suppression_data = load_suppression_data(mover_ind)
    
    # Create temp table with suppressions
    if suppression_data:
        suppressions_df = []
        for item in suppression_data:
            for block in item.get("blocks_to_suppress", []):
                suppressions_df.append({
                    "the_date": item["date"],
                    "winner": item["winner"],
                    "loser": item["loser"],
                    "census_blockid": block["census_blockid"]
                })
        
        if suppressions_df:
            supp_df = pd.DataFrame(suppressions_df)
            con.register("suppressions", supp_df)
    
    # Get cube table name
    table_name = f"gamoshi_win_{'mover' if mover_ind else 'non_mover'}_cube"
    
    # Query before data
    date_list = "', '".join(TARGET_DATES)
    before_sql = f"""
    SELECT 
        the_date,
        winner,
        SUM(total_wins) as wins
    FROM {table_name}
    WHERE the_date IN ('{date_list}')
    GROUP BY the_date, winner
    """
    
    before_df = con.execute(before_sql).df()
    
    # Query after data
    if suppression_data and suppressions_df:
        after_sql = f"""
        WITH raw AS (
            SELECT 
                c.the_date,
                c.winner,
                c.loser,
                c.census_blockid,
                c.total_wins
            FROM {table_name.replace('_cube', '_census_block_cube')} c
            WHERE c.the_date IN ('{date_list}')
        ),
        filtered AS (
            SELECT *
            FROM raw r
            WHERE NOT EXISTS (
                SELECT 1 FROM suppressions s
                WHERE s.the_date = r.the_date
                  AND s.winner = r.winner
                  AND s.loser = r.loser
                  AND s.census_blockid = r.census_blockid
            )
        )
        SELECT 
            the_date,
            winner,
            SUM(total_wins) as wins
        FROM filtered
        GROUP BY the_date, winner
        """
        after_df = con.execute(after_sql).df()
    else:
        after_df = before_df.copy()
    
    con.close()
    
    # Calculate win shares
    before_totals = before_df.groupby('the_date')['wins'].sum().to_dict()
    after_totals = after_df.groupby('the_date')['wins'].sum().to_dict()
    
    before_df['win_share'] = before_df.apply(
        lambda row: (row['wins'] / before_totals.get(row['the_date'], 1)) * 100, axis=1
    )
    after_df['win_share'] = after_df.apply(
        lambda row: (row['wins'] / after_totals.get(row['the_date'], 1)) * 100, axis=1
    )
    
    # Get top carriers overall
    carrier_totals = before_df.groupby('winner')['win_share'].sum().sort_values(ascending=False)
    top_carriers = carrier_totals.head(top_n).index.tolist()
    
    # Create subplots
    fig, axes = plt.subplots(len(TARGET_DATES), 1, figsize=(14, 4 * len(TARGET_DATES)))
    if len(TARGET_DATES) == 1:
        axes = [axes]
    
    for idx, target_date in enumerate(TARGET_DATES):
        ax = axes[idx]
        
        # Filter data for this date
        before_date = before_df[before_df['the_date'] == target_date]
        after_date = after_df[after_df['the_date'] == target_date]
        
        # Get top carriers for this date
        date_carriers = before_date.nlargest(top_n, 'win_share')['winner'].tolist()
        
        # Combine with overall top carriers
        carriers_to_plot = list(dict.fromkeys(top_carriers[:5] + date_carriers[:5]))[:top_n]
        
        # Prepare data
        x_pos = np.arange(len(carriers_to_plot))
        width = 0.35
        
        before_values = []
        after_values = []
        
        for carrier in carriers_to_plot:
            before_val = before_date[before_date['winner'] == carrier]['win_share'].values
            after_val = after_date[after_date['winner'] == carrier]['win_share'].values
            before_values.append(before_val[0] if len(before_val) > 0 else 0)
            after_values.append(after_val[0] if len(after_val) > 0 else 0)
        
        # Plot bars
        ax.bar(x_pos - width/2, before_values, width, label='Before', alpha=0.7, color='steelblue')
        ax.bar(x_pos + width/2, after_values, width, label='After', alpha=0.7, color='coral')
        
        # Formatting
        ax.set_ylabel('Win Share (%)', fontsize=11)
        ax.set_title(f'{target_date}', fontsize=12, fontweight='bold')
        ax.set_xticks(x_pos)
        ax.set_xticklabels(carriers_to_plot, rotation=45, ha='right', fontsize=9)
        ax.legend(fontsize=10)
        ax.grid(axis='y', alpha=0.3)
    
    plt.suptitle(
        f'Target Dates Win Share Comparison - {"Movers" if mover_ind else "Non-Movers"}\n'
        f'Blue = Before Suppression, Orange = After Suppression',
        fontsize=14, fontweight='bold', y=0.995
    )
    plt.tight_layout()
    
    # Save
    output_path = GRAPHS_DIR / f"overlay_target_dates_mover_{mover_ind}.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"[SUCCESS] Saved: {output_path}")
    plt.close()

if __name__ == "__main__":
    
    print("=" * 70)
    print("Creating Overlay Visualizations")
    print("=" * 70)
    
    # Create visualizations for both segments
    for mover_ind in [True, False]:
        create_overlay_timeseries(mover_ind, top_n=10)
        create_target_dates_overlay(mover_ind, top_n=10)
    
    print("\n" + "=" * 70)
    print("✅ All visualizations created successfully!")
    print("=" * 70)
