#!/usr/bin/env python3
"""
Create before/after visualizations to validate suppression effectiveness.
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from datetime import datetime
import json

DB_PATH = "data/databases/duck_suppression.db"
OUTPUT_DIR = Path("analysis/top50_suppression/visualizations")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_top50_carriers():
    """Load top 50 carriers from context"""
    with open('.agent_context.json', 'r') as f:
        context = json.load(f)
    return context['top_50_carriers'][:10]  # Use top 10 for cleaner visualizations

def get_national_timeseries(con, table_name, carriers, start_date='2025-06-01', end_date='2025-09-04'):
    """Get national win share timeseries for carriers"""
    carrier_list = "', '".join(carriers)
    
    sql = f"""
    WITH daily_totals AS (
        SELECT 
            the_date,
            winner,
            SUM(total_wins) as total_wins
        FROM {table_name}
        WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY the_date, winner
    ),
    daily_sums AS (
        SELECT 
            the_date,
            SUM(total_wins) as day_total
        FROM daily_totals
        GROUP BY the_date
    )
    SELECT 
        dt.the_date,
        dt.winner,
        dt.total_wins,
        dt.total_wins * 100.0 / ds.day_total as win_share
    FROM daily_totals dt
    JOIN daily_sums ds ON dt.the_date = ds.the_date
    WHERE dt.winner IN ('{carrier_list}')
    ORDER BY dt.the_date, dt.total_wins DESC
    """
    
    return con.execute(sql).df()

def plot_national_comparison(before_df, after_df, carriers, output_path):
    """Plot before/after comparison for national win shares"""
    
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Plot each carrier
    for carrier in carriers:
        # Before (solid line)
        carrier_before = before_df[before_df['winner'] == carrier].sort_values('the_date')
        if len(carrier_before) > 0:
            ax.plot(carrier_before['the_date'], carrier_before['win_share'], 
                   label=f'{carrier}', linewidth=2, alpha=0.8)
        
        # After (dashed line)
        carrier_after = after_df[after_df['winner'] == carrier].sort_values('the_date')
        if len(carrier_after) > 0:
            ax.plot(carrier_after['the_date'], carrier_after['win_share'], 
                   linestyle='--', linewidth=2, alpha=0.6)
    
    # Formatting
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Win Share (%)', fontsize=12, fontweight='bold')
    ax.set_title('National Win Share: Before (━━) vs After Suppression (- - -)', 
                 fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=10)
    ax.grid(True, alpha=0.3)
    
    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  ✅ Saved: {output_path}")

def get_h2h_timeseries(con, table_name, winner, loser, start_date='2025-06-01', end_date='2025-09-04'):
    """Get H2H timeseries for a specific matchup"""
    
    sql = f"""
    SELECT 
        the_date,
        winner,
        loser,
        SUM(total_wins) as h2h_wins
    FROM {table_name}
    WHERE winner = '{winner}'
        AND loser = '{loser}'
        AND the_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY the_date, winner, loser
    ORDER BY the_date
    """
    
    return con.execute(sql).df()

def plot_h2h_comparison(con, carriers, output_dir):
    """Plot before/after for top H2H matchups"""
    
    # Get top 5 H2H matchups by total wins
    sql = f"""
    WITH top_matchups AS (
        SELECT 
            winner,
            loser,
            SUM(total_wins) as total_wins
        FROM gamoshi_win_mover_cube
        WHERE the_date >= '2025-06-01'
        GROUP BY winner, loser
        ORDER BY total_wins DESC
        LIMIT 5
    )
    SELECT winner, loser FROM top_matchups
    """
    
    matchups = con.execute(sql).df()
    
    for idx, row in matchups.iterrows():
        winner = row['winner']
        loser = row['loser']
        
        # Get before/after data
        before = get_h2h_timeseries(con, 'gamoshi_win_mover_cube', winner, loser)
        after = get_h2h_timeseries(con, 'gamoshi_win_mover_round3', winner, loser)
        
        # Plot
        fig, ax = plt.subplots(figsize=(14, 6))
        
        if len(before) > 0:
            ax.plot(before['the_date'], before['h2h_wins'], 
                   label='Before Suppression', linewidth=2.5, alpha=0.9, color='#e74c3c')
        
        if len(after) > 0:
            ax.plot(after['the_date'], after['h2h_wins'], 
                   label='After Suppression', linestyle='--', linewidth=2.5, alpha=0.9, color='#27ae60')
        
        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel('H2H Wins', fontsize=12, fontweight='bold')
        ax.set_title(f'H2H: {winner} vs {loser}\nBefore (━━) vs After Suppression (- - -)', 
                     fontsize=13, fontweight='bold', pad=15)
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)
        
        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        plt.xticks(rotation=45, ha='right')
        
        plt.tight_layout()
        output_path = output_dir / f"h2h_{winner}_vs_{loser}.png"
        output_path = Path(str(output_path).replace(' ', '_').replace('/', '_'))
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  ✅ Saved: {output_path}")

def main():
    print("=" * 80)
    print("CREATING BEFORE/AFTER VISUALIZATIONS")
    print("=" * 80)
    print()
    
    con = duckdb.connect(DB_PATH)
    top10_carriers = load_top50_carriers()
    
    print(f"📊 Generating national win share comparison...")
    before = get_national_timeseries(con, 'gamoshi_win_mover_cube', top10_carriers)
    after = get_national_timeseries(con, 'gamoshi_win_mover_round3', top10_carriers)
    
    plot_national_comparison(
        before, after, top10_carriers,
        OUTPUT_DIR / 'national_winshare_comparison.png'
    )
    
    print(f"\n📊 Generating H2H comparisons for top matchups...")
    plot_h2h_comparison(con, top10_carriers, OUTPUT_DIR)
    
    print(f"\n✅ All visualizations saved to {OUTPUT_DIR}")
    
    con.close()

if __name__ == "__main__":
    main()
