#!/usr/bin/env python3
"""
Validate suppression results with before/after visualizations
"""
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
import json

DB_PATH = "/home/jloli/codebase-comparison/suppression_tools/data/databases/duck_suppression.db"
OUTPUT_DIR = Path("analysis/validation")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def get_national_shares(con, mover_ind, apply_suppression=False):
    """Get national win shares over time"""
    
    suppression_filter = ""
    if apply_suppression:
        if mover_ind:
            # Mover view has is_outlier_any
            suppression_filter = """
            AND NOT (
                is_outlier_any = true 
                OR is_first_appearance = true
            )
            """
        else:
            # Non-mover view uses zscore and pct_change
            suppression_filter = """
            AND NOT (
                (ABS(zscore) > 1.5 AND current_wins >= 10)
                OR is_first_appearance = true
                OR (pct_change > 0.30 AND current_wins >= 10)
            )
            """
    
    query = f"""
    WITH daily_totals AS (
        SELECT 
            the_date,
            winner,
            SUM(current_wins) as wins
        FROM gamoshi_win_{'mover' if mover_ind else 'non_mover'}_rolling
        WHERE 1=1 {suppression_filter}
        GROUP BY the_date, winner
    ),
    date_totals AS (
        SELECT 
            the_date,
            SUM(wins) as total_wins
        FROM daily_totals
        GROUP BY the_date
    )
    SELECT 
        dt.the_date,
        dt.winner,
        dt.wins,
        date_totals.total_wins,
        (dt.wins * 100.0 / NULLIF(date_totals.total_wins, 0)) as win_share
    FROM daily_totals dt
    JOIN date_totals ON dt.the_date = date_totals.the_date
    WHERE dt.the_date >= '2025-06-01'
    ORDER BY dt.the_date, dt.winner
    """
    
    return con.execute(query).df()

def get_h2h_shares(con, winner, loser, mover_ind, apply_suppression=False):
    """Get head-to-head win shares over time"""
    
    suppression_filter = ""
    if apply_suppression:
        if mover_ind:
            # Mover view has is_outlier_any
            suppression_filter = """
            AND NOT (
                is_outlier_any = true 
                OR is_first_appearance = true
            )
            """
        else:
            # Non-mover view uses zscore and pct_change
            suppression_filter = """
            AND NOT (
                (ABS(zscore) > 1.5 AND current_wins >= 10)
                OR is_first_appearance = true
                OR (pct_change > 0.30 AND current_wins >= 10)
            )
            """
    
    query = f"""
    SELECT 
        the_date,
        SUM(CASE WHEN winner = '{winner}' THEN current_wins ELSE 0 END) as winner_wins,
        SUM(CASE WHEN winner = '{loser}' THEN current_wins ELSE 0 END) as loser_wins,
        SUM(current_wins) as total_wins,
        (SUM(CASE WHEN winner = '{winner}' THEN current_wins ELSE 0 END) * 100.0 / 
         NULLIF(SUM(current_wins), 0)) as winner_share
    FROM gamoshi_win_{'mover' if mover_ind else 'non_mover'}_rolling
    WHERE (winner = '{winner}' AND loser = '{loser}')
       OR (winner = '{loser}' AND loser = '{winner}')
       {suppression_filter}
       AND the_date >= '2025-06-01'
    GROUP BY the_date
    ORDER BY the_date
    """
    
    return con.execute(query).df()

def plot_national_comparison(before_df, after_df, mover_ind, top_n=5):
    """Plot national win share before/after suppression"""
    
    # Get top N carriers by total wins
    top_carriers = (before_df.groupby('winner')['wins'].sum()
                    .nlargest(top_n).index.tolist())
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    for carrier in top_carriers:
        # Before (solid line)
        carrier_before = before_df[before_df['winner'] == carrier]
        ax.plot(pd.to_datetime(carrier_before['the_date']), 
                carrier_before['win_share'],
                label=f'{carrier} (Original)',
                linewidth=2,
                alpha=0.8)
        
        # After (dashed line)
        carrier_after = after_df[after_df['winner'] == carrier]
        ax.plot(pd.to_datetime(carrier_after['the_date']), 
                carrier_after['win_share'],
                label=f'{carrier} (Suppressed)',
                linestyle='--',
                linewidth=2,
                alpha=0.8)
    
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Win Share (%)', fontsize=12, fontweight='bold')
    ax.set_title(f'National Win Share - {"Mover" if mover_ind else "Non-Mover"}\nBefore vs After Suppression',
                 fontsize=14, fontweight='bold')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    filename = OUTPUT_DIR / f"national_comparison_{'mover' if mover_ind else 'non_mover'}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    
    return filename

def plot_h2h_comparison(before_df, after_df, winner, loser, mover_ind):
    """Plot H2H win share before/after suppression"""
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Before (solid line)
    ax.plot(pd.to_datetime(before_df['the_date']), 
            before_df['winner_share'],
            label=f'{winner} Share (Original)',
            linewidth=2,
            color='#1f77b4',
            alpha=0.8)
    
    # After (dashed line)
    ax.plot(pd.to_datetime(after_df['the_date']), 
            after_df['winner_share'],
            label=f'{winner} Share (Suppressed)',
            linestyle='--',
            linewidth=2,
            color='#1f77b4',
            alpha=0.8)
    
    ax.axhline(y=50, color='gray', linestyle=':', alpha=0.5, label='50% line')
    
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Win Share (%)', fontsize=12, fontweight='bold')
    ax.set_title(f'H2H: {winner} vs {loser} - {"Mover" if mover_ind else "Non-Mover"}\nBefore vs After Suppression',
                 fontsize=14, fontweight='bold')
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    filename = OUTPUT_DIR / f"h2h_{winner}_vs_{loser}_{'mover' if mover_ind else 'non_mover'}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    
    return filename

def calculate_metrics(before_df, after_df):
    """Calculate comparison metrics"""
    metrics = {}
    
    # Volatility (std dev of win shares)
    for carrier in before_df['winner'].unique():
        before_vol = before_df[before_df['winner'] == carrier]['win_share'].std()
        after_vol = after_df[after_df['winner'] == carrier]['win_share'].std()
        
        if before_vol > 0:
            reduction = ((before_vol - after_vol) / before_vol) * 100
            metrics[carrier] = {
                'before_volatility': before_vol,
                'after_volatility': after_vol,
                'reduction_pct': reduction
            }
    
    return metrics

def main():
    print("\n" + "="*70)
    print("VALIDATION: Before vs After Suppression")
    print("="*70 + "\n")
    
    con = duckdb.connect(DB_PATH, read_only=True)
    
    results = {
        'national_mover': {},
        'national_non_mover': {},
        'h2h_examples': []
    }
    
    # 1. National Mover Analysis
    print("[1/4] Analyzing National Mover Win Shares...")
    before_mover = get_national_shares(con, mover_ind=True, apply_suppression=False)
    after_mover = get_national_shares(con, mover_ind=True, apply_suppression=True)
    
    plot_file = plot_national_comparison(before_mover, after_mover, mover_ind=True)
    metrics_mover = calculate_metrics(before_mover, after_mover)
    
    results['national_mover']['plot'] = str(plot_file)
    results['national_mover']['metrics'] = metrics_mover
    
    print(f"  ✓ Created: {plot_file}")
    print(f"  ✓ Analyzed {len(metrics_mover)} carriers")
    
    # 2. National Non-Mover Analysis
    print("\n[2/4] Analyzing National Non-Mover Win Shares...")
    before_non_mover = get_national_shares(con, mover_ind=False, apply_suppression=False)
    after_non_mover = get_national_shares(con, mover_ind=False, apply_suppression=True)
    
    plot_file = plot_national_comparison(before_non_mover, after_non_mover, mover_ind=False)
    metrics_non_mover = calculate_metrics(before_non_mover, after_non_mover)
    
    results['national_non_mover']['plot'] = str(plot_file)
    results['national_non_mover']['metrics'] = metrics_non_mover
    
    print(f"  ✓ Created: {plot_file}")
    print(f"  ✓ Analyzed {len(metrics_non_mover)} carriers")
    
    # 3. H2H Examples (top problematic pairs from Round 1)
    print("\n[3/4] Analyzing Top H2H Matchups...")
    h2h_pairs = [
        ('AT&T Mobility', 'Verizon Wireless', True),
        ('AT&T Mobility', 'T-Mobile', True),
        ('Verizon Wireless', 'T-Mobile', False),
    ]
    
    for winner, loser, mover_ind in h2h_pairs:
        print(f"  Analyzing: {winner} vs {loser} ({'Mover' if mover_ind else 'Non-Mover'})")
        
        before_h2h = get_h2h_shares(con, winner, loser, mover_ind, apply_suppression=False)
        after_h2h = get_h2h_shares(con, winner, loser, mover_ind, apply_suppression=True)
        
        if len(before_h2h) > 0 and len(after_h2h) > 0:
            plot_file = plot_h2h_comparison(before_h2h, after_h2h, winner, loser, mover_ind)
            
            before_vol = before_h2h['winner_share'].std()
            after_vol = after_h2h['winner_share'].std()
            reduction = ((before_vol - after_vol) / before_vol) * 100 if before_vol > 0 else 0
            
            results['h2h_examples'].append({
                'pair': f"{winner} vs {loser}",
                'mover_ind': mover_ind,
                'plot': str(plot_file),
                'before_volatility': before_vol,
                'after_volatility': after_vol,
                'reduction_pct': reduction
            })
            
            print(f"    ✓ Volatility: {before_vol:.2f}% → {after_vol:.2f}% ({reduction:+.1f}% change)")
    
    # 4. Save results
    print("\n[4/4] Saving validation results...")
    results_file = OUTPUT_DIR / "validation_results.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"  ✓ Saved to: {results_file}")
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY: Top Improvements")
    print("="*70)
    
    print("\n📊 National Mover - Top 5 Volatility Reductions:")
    sorted_mover = sorted(metrics_mover.items(), 
                          key=lambda x: x[1]['reduction_pct'], 
                          reverse=True)[:5]
    for carrier, m in sorted_mover:
        print(f"  {carrier:30s}: {m['before_volatility']:6.2f}% → {m['after_volatility']:6.2f}% "
              f"({m['reduction_pct']:+6.1f}%)")
    
    print("\n📊 National Non-Mover - Top 5 Volatility Reductions:")
    sorted_non_mover = sorted(metrics_non_mover.items(), 
                               key=lambda x: x[1]['reduction_pct'], 
                               reverse=True)[:5]
    for carrier, m in sorted_non_mover:
        print(f"  {carrier:30s}: {m['before_volatility']:6.2f}% → {m['after_volatility']:6.2f}% "
              f"({m['reduction_pct']:+6.1f}%)")
    
    print("\n📊 H2H Matchups:")
    for h2h in results['h2h_examples']:
        print(f"  {h2h['pair']:40s}: {h2h['before_volatility']:6.2f}% → {h2h['after_volatility']:6.2f}% "
              f"({h2h['reduction_pct']:+6.1f}%)")
    
    print("\n" + "="*70)
    print(f"✅ Validation complete! Visualizations saved to: {OUTPUT_DIR}")
    print("="*70 + "\n")
    
    con.close()

if __name__ == "__main__":
    main()
