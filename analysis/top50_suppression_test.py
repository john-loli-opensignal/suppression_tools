#!/usr/bin/env python3
"""
Test suppression effectiveness on top 50 carriers by total wins.
Runs 3 rounds of suppression and validates results at national and H2H levels.
"""

import duckdb
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

DB_PATH = "data/databases/duck_suppression.db"
OUTPUT_DIR = Path("analysis/top50_suppression")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Verify database exists
assert Path(DB_PATH).exists(), f"Database not found at {DB_PATH}. Check .agent_context.json!"

def load_top50_carriers():
    """Load top 50 carriers from context"""
    with open('.agent_context.json', 'r') as f:
        context = json.load(f)
    return context['top_50_carriers']

def get_national_shares(con, table_name, top50_carriers):
    """Get national win shares for top 50 carriers"""
    carrier_list = "', '".join(top50_carriers)
    
    sql = f"""
    SELECT 
        the_date,
        winner,
        SUM(total_wins) as total_wins,
        SUM(total_wins) * 100.0 / SUM(SUM(total_wins)) OVER (PARTITION BY the_date) as win_share
    FROM {table_name}
    WHERE winner IN ('{carrier_list}')
    GROUP BY the_date, winner
    ORDER BY the_date, total_wins DESC
    """
    
    return con.execute(sql).df()

def get_h2h_shares(con, table_name, top50_carriers):
    """Get H2H win shares for top 50 carriers"""
    carrier_list = "', '".join(top50_carriers)
    
    sql = f"""
    WITH h2h_totals AS (
        SELECT 
            the_date,
            winner,
            loser,
            SUM(total_wins) as h2h_wins
        FROM {table_name}
        WHERE winner IN ('{carrier_list}')
            AND loser IN ('{carrier_list}')
        GROUP BY the_date, winner, loser
        HAVING h2h_wins >= 10
    )
    SELECT 
        the_date,
        winner,
        loser,
        h2h_wins,
        h2h_wins * 100.0 / NULLIF(h2h_wins, 0) as h2h_share
    FROM h2h_totals
    ORDER BY the_date, h2h_wins DESC
    """
    
    return con.execute(sql).df()

def detect_outliers_from_rolling_view(con, view_name, top50_carriers, min_current_wins=10):
    """Detect outliers using the rolling metrics view"""
    carrier_list = "', '".join(top50_carriers)
    
    sql = f"""
    SELECT 
        the_date,
        state,
        dma_name,
        winner,
        loser,
        total_wins as current_wins,
        avg_wins_28d as rolling_avg,
        stddev_wins_28d as rolling_stddev,
        zscore as z_score,
        pct_change as pct_increase,
        is_first_appearance,
        is_outlier,
        CASE 
            WHEN is_first_appearance THEN 'First Appearance'
            WHEN ABS(zscore) > 1.5 THEN 'Z-Score > 1.5'
            WHEN pct_change > 30 THEN 'Pct Increase > 30%'
            ELSE 'Other'
        END as outlier_reason
    FROM {view_name}
    WHERE winner IN ('{carrier_list}')
        AND loser IN ('{carrier_list}')
        AND is_outlier = true
        AND total_wins >= {min_current_wins}
    ORDER BY the_date, ABS(zscore) DESC
    """
    
    return con.execute(sql).df()

def calculate_suppression_impact(outliers_df):
    """Calculate how much to suppress from each outlier"""
    suppressions = []
    
    for _, row in outliers_df.iterrows():
        # Only suppress the excess above rolling average
        excess = row['current_wins'] - row['rolling_avg']
        if excess > 0:
            suppressions.append({
                'the_date': row['the_date'],
                'state': row['state'],
                'dma_name': row['dma_name'],
                'winner': row['winner'],
                'loser': row['loser'],
                'suppress_amount': excess,
                'reason': row['outlier_reason']
            })
    
    return pd.DataFrame(suppressions)

def apply_suppressions(con, base_table, suppressions_df, output_table):
    """Apply suppressions to create a cleaned table"""
    
    # Create temp table with suppressions
    con.execute("DROP TABLE IF EXISTS temp_suppressions")
    con.execute("""
        CREATE TEMP TABLE temp_suppressions AS 
        SELECT * FROM suppressions_df
    """)
    
    # Create output table with suppressions applied
    con.execute(f"DROP TABLE IF EXISTS {output_table}")
    con.execute(f"""
        CREATE TABLE {output_table} AS
        SELECT 
            c.the_date,
            c.year,
            c.month,
            c.day,
            c.day_of_week,
            c.state,
            c.dma,
            c.dma_name,
            c.winner,
            c.loser,
            GREATEST(c.total_wins - COALESCE(s.suppress_amount, 0), 0) as total_wins,
            c.record_count
        FROM {base_table} c
        LEFT JOIN temp_suppressions s 
            ON c.the_date = s.the_date
            AND c.state = s.state
            AND c.dma_name = s.dma_name
            AND c.winner = s.winner
            AND c.loser = s.loser
        WHERE c.total_wins - COALESCE(s.suppress_amount, 0) > 0
    """)
    
    rows = con.execute(f"SELECT COUNT(*) as cnt FROM {output_table}").fetchone()[0]
    return rows

def count_anomalies(df, metric_col='win_share', z_threshold=1.5):
    """Count anomalies in a dataframe using z-score"""
    if len(df) == 0:
        return 0
    
    # Group by entity (winner or winner+loser) and calculate rolling stats
    if 'loser' in df.columns:
        # H2H data
        entity_col = df['winner'] + ' vs ' + df['loser']
    else:
        # National data
        entity_col = df['winner']
    
    df = df.copy()
    df['entity'] = entity_col
    
    anomaly_count = 0
    for entity in df['entity'].unique():
        entity_df = df[df['entity'] == entity].sort_values('the_date')
        
        if len(entity_df) < 4:
            continue
        
        # Calculate rolling metrics
        entity_df['rolling_mean'] = entity_df[metric_col].rolling(window=4, min_periods=1).mean()
        entity_df['rolling_std'] = entity_df[metric_col].rolling(window=4, min_periods=1).std()
        
        # Z-score
        entity_df['z_score'] = (entity_df[metric_col] - entity_df['rolling_mean']) / entity_df['rolling_std'].replace(0, 1)
        
        anomaly_count += (entity_df['z_score'].abs() > z_threshold).sum()
    
    return anomaly_count

def main():
    print("=" * 80)
    print("TOP 50 CARRIER SUPPRESSION TEST - 3 ROUNDS")
    print("=" * 80)
    print(f"Database: {DB_PATH}")
    print(f"Output: {OUTPUT_DIR}")
    print()
    
    top50_carriers = load_top50_carriers()
    print(f"Loaded {len(top50_carriers)} top carriers")
    print()
    
    con = duckdb.connect(DB_PATH)
    
    results = {
        'rounds': [],
        'timestamp': datetime.now().isoformat()
    }
    
    # Start with original mover cube
    current_table = "gamoshi_win_mover_cube"
    
    for round_num in range(1, 4):
        print(f"\n{'='*80}")
        print(f"ROUND {round_num}")
        print(f"{'='*80}\n")
        
        # Get baseline metrics BEFORE suppression
        print(f"📊 Analyzing {current_table}...")
        
        # National shares
        national_before = get_national_shares(con, current_table, top50_carriers)
        print(f"  National data points: {len(national_before)}")
        
        # H2H shares
        h2h_before = get_h2h_shares(con, current_table, top50_carriers)
        print(f"  H2H data points: {len(h2h_before)}")
        
        # Count anomalies before
        national_anomalies_before = count_anomalies(national_before, 'win_share')
        h2h_anomalies_before = count_anomalies(h2h_before, 'h2h_share')
        
        print(f"  🚨 National anomalies (before): {national_anomalies_before}")
        print(f"  🚨 H2H anomalies (before): {h2h_anomalies_before}")
        
        # Detect outliers using rolling view
        print(f"\n🔍 Detecting outliers...")
        outliers = detect_outliers_from_rolling_view(
            con, 
            'gamoshi_win_mover_rolling',
            top50_carriers,
            min_current_wins=10
        )
        
        print(f"  Found {len(outliers)} outliers")
        
        if len(outliers) == 0:
            print("  ✅ No outliers found! Suppression complete.")
            break
        
        # Show top outliers
        print(f"\n  Top 10 outliers by impact:")
        top_outliers = outliers.nlargest(10, 'current_wins')[
            ['the_date', 'winner', 'loser', 'dma_name', 'current_wins', 'rolling_avg', 'z_score', 'outlier_reason']
        ]
        print(top_outliers.to_string(index=False))
        
        # Calculate suppressions
        print(f"\n💉 Calculating suppression amounts...")
        suppressions = calculate_suppression_impact(outliers)
        total_suppressed = suppressions['suppress_amount'].sum()
        print(f"  Total wins to suppress: {total_suppressed:,.0f}")
        
        # Apply suppressions
        next_table = f"gamoshi_win_mover_round{round_num}"
        print(f"\n✂️  Applying suppressions to create {next_table}...")
        
        rows = apply_suppressions(con, current_table, suppressions, next_table)
        print(f"  Created table with {rows:,} rows")
        
        # Get metrics AFTER suppression
        print(f"\n📊 Analyzing {next_table} (after suppression)...")
        national_after = get_national_shares(con, next_table, top50_carriers)
        h2h_after = get_h2h_shares(con, next_table, top50_carriers)
        
        national_anomalies_after = count_anomalies(national_after, 'win_share')
        h2h_anomalies_after = count_anomalies(h2h_after, 'h2h_share')
        
        print(f"  🚨 National anomalies (after): {national_anomalies_after}")
        print(f"  🚨 H2H anomalies (after): {h2h_anomalies_after}")
        
        # Calculate improvement
        national_improvement = national_anomalies_before - national_anomalies_after
        h2h_improvement = h2h_anomalies_before - h2h_anomalies_after
        
        print(f"\n  📈 Improvement:")
        print(f"     National: {national_improvement} anomalies removed ({national_improvement/max(1,national_anomalies_before)*100:.1f}%)")
        print(f"     H2H: {h2h_improvement} anomalies removed ({h2h_improvement/max(1,h2h_anomalies_before)*100:.1f}%)")
        
        # Store results
        round_result = {
            'round': round_num,
            'outliers_detected': len(outliers),
            'total_suppressed': float(total_suppressed),
            'national_anomalies_before': int(national_anomalies_before),
            'national_anomalies_after': int(national_anomalies_after),
            'h2h_anomalies_before': int(h2h_anomalies_before),
            'h2h_anomalies_after': int(h2h_anomalies_after),
            'national_improvement': int(national_improvement),
            'h2h_improvement': int(h2h_improvement)
        }
        results['rounds'].append(round_result)
        
        # Save intermediate results
        outliers.to_csv(OUTPUT_DIR / f"round{round_num}_outliers.csv", index=False)
        suppressions.to_csv(OUTPUT_DIR / f"round{round_num}_suppressions.csv", index=False)
        national_before.to_csv(OUTPUT_DIR / f"round{round_num}_national_before.csv", index=False)
        national_after.to_csv(OUTPUT_DIR / f"round{round_num}_national_after.csv", index=False)
        h2h_before.to_csv(OUTPUT_DIR / f"round{round_num}_h2h_before.csv", index=False)
        h2h_after.to_csv(OUTPUT_DIR / f"round{round_num}_h2h_after.csv", index=False)
        
        # Move to next round
        current_table = next_table
    
    # Save final results
    with open(OUTPUT_DIR / 'results_summary.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}\n")
    
    for r in results['rounds']:
        print(f"Round {r['round']}:")
        print(f"  Outliers detected: {r['outliers_detected']}")
        print(f"  Wins suppressed: {r['total_suppressed']:,.0f}")
        print(f"  National anomalies: {r['national_anomalies_before']} → {r['national_anomalies_after']} ({r['national_improvement']} removed)")
        print(f"  H2H anomalies: {r['h2h_anomalies_before']} → {r['h2h_anomalies_after']} ({r['h2h_improvement']} removed)")
        print()
    
    print(f"✅ All results saved to {OUTPUT_DIR}")
    print()
    
    con.close()

if __name__ == "__main__":
    main()
