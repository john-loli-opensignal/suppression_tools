#!/usr/bin/env python3
"""
Comprehensive Hierarchical Outlier Analysis
Analyzes outliers from June 1 through end of dataset
Focus: National -> H2H National -> State -> State H2H -> DMA pairs
"""

import duckdb
from pathlib import Path
import pandas as pd
from datetime import datetime

# CRITICAL: Use correct database path
DB_PATH = "/home/jloli/codebase-comparison/suppression_tools/data/databases/duck_suppression.db"
assert Path(DB_PATH).exists(), f"Database not found at {DB_PATH} - DO NOT CREATE ALTERNATE DBs!"

def analyze_national_shares(con, start_date='2025-06-01'):
    """
    Level 1: National carrier shares (ds, mover_ind)
    Shows which carriers have the most suspicious patterns
    """
    print("\n" + "="*80)
    print("LEVEL 1: NATIONAL CARRIER SHARES OUTLIERS")
    print("="*80)
    
    sql = f"""
    WITH national_daily AS (
        SELECT 
            the_date,
            winner,
            SUM(current_wins) as daily_wins
        FROM gamoshi_win_mover_rolling
        WHERE the_date >= '{start_date}'
        GROUP BY the_date, winner
    ),
    national_totals AS (
        SELECT 
            the_date,
            SUM(daily_wins) as total_daily_wins
        FROM national_daily
        GROUP BY the_date
    ),
    national_shares AS (
        SELECT 
            nd.the_date,
            nd.winner,
            nd.daily_wins,
            nt.total_daily_wins,
            (nd.daily_wins::DOUBLE / nt.total_daily_wins * 100) as win_share_pct
        FROM national_daily nd
        JOIN national_totals nt ON nd.the_date = nt.the_date
    )
    SELECT 
        the_date,
        winner,
        daily_wins,
        win_share_pct,
        LAG(win_share_pct, 1) OVER (PARTITION BY winner ORDER BY the_date) as prev_share,
        win_share_pct - LAG(win_share_pct, 1) OVER (PARTITION BY winner ORDER BY the_date) as share_change
    FROM national_shares
    ORDER BY the_date, winner
    """
    
    df = con.execute(sql).df()
    
    # Find top outliers by absolute share change
    df['abs_share_change'] = df['share_change'].abs()
    outliers = df[df['abs_share_change'] > 2.0].sort_values('abs_share_change', ascending=False)
    
    print(f"\nTop 20 National Share Changes (>2% absolute change):")
    print(outliers[['the_date', 'winner', 'win_share_pct', 'prev_share', 'share_change', 'daily_wins']].head(20).to_string())
    
    return outliers

def analyze_h2h_national(con, start_date='2025-06-01'):
    """
    Level 2: National H2H pairs (winner, loser)
    Shows which head-to-head matchups are most suspicious
    """
    print("\n" + "="*80)
    print("LEVEL 2: NATIONAL H2H PAIR OUTLIERS")
    print("="*80)
    
    sql = f"""
    SELECT 
        the_date,
        winner,
        loser,
        dma_name,
        state,
        current_wins,
        avg_wins_28d as rolling_avg_28d,
        stddev_wins_28d as rolling_stddev_28d,
        z_score_28d as z_score,
        pct_change_28d as pct_change,
        is_first_appearance,
        is_outlier_28d as is_outlier
    FROM gamoshi_win_mover_rolling
    WHERE the_date >= '{start_date}'
        AND is_outlier_any = true
        AND current_wins >= 10
    ORDER BY ABS(z_score_28d) DESC, pct_change_28d DESC
    LIMIT 100
    """
    
    df = con.execute(sql).df()
    
    # Aggregate to national H2H level (sum across all DMAs)
    h2h_national = df.groupby(['the_date', 'winner', 'loser']).agg({
        'current_wins': 'sum',
        'rolling_avg_28d': 'sum',
        'z_score': 'mean',
        'pct_change': 'mean',
        'is_first_appearance': 'max'
    }).reset_index()
    
    h2h_national['impact'] = h2h_national['current_wins'] - h2h_national['rolling_avg_28d']
    h2h_national = h2h_national.sort_values('impact', ascending=False)
    
    print(f"\nTop 20 H2H National Pairs by IMPACT:")
    print(h2h_national[['the_date', 'winner', 'loser', 'current_wins', 'rolling_avg_28d', 'impact', 'z_score', 'pct_change']].head(20).to_string())
    
    return h2h_national

def analyze_state_shares(con, start_date='2025-06-01'):
    """
    Level 3: State-level carrier shares
    Shows geographical patterns of outliers
    """
    print("\n" + "="*80)
    print("LEVEL 3: STATE CARRIER SHARES OUTLIERS")
    print("="*80)
    
    sql = f"""
    WITH state_daily AS (
        SELECT 
            the_date,
            state,
            winner,
            SUM(current_wins) as state_wins
        FROM gamoshi_win_mover_rolling
        WHERE the_date >= '{start_date}'
        GROUP BY the_date, state, winner
    ),
    state_totals AS (
        SELECT 
            the_date,
            state,
            SUM(state_wins) as total_state_wins
        FROM state_daily
        GROUP BY the_date, state
    ),
    state_shares AS (
        SELECT 
            sd.the_date,
            sd.state,
            sd.winner,
            sd.state_wins,
            st.total_state_wins,
            (sd.state_wins::DOUBLE / st.total_state_wins * 100) as win_share_pct
        FROM state_daily sd
        JOIN state_totals st ON sd.the_date = st.the_date AND sd.state = st.state
    )
    SELECT 
        the_date,
        state,
        winner,
        state_wins,
        win_share_pct,
        LAG(win_share_pct, 1) OVER (PARTITION BY state, winner ORDER BY the_date) as prev_share,
        win_share_pct - LAG(win_share_pct, 1) OVER (PARTITION BY state, winner ORDER BY the_date) as share_change
    FROM state_shares
    ORDER BY the_date, state, winner
    """
    
    df = con.execute(sql).df()
    
    df['abs_share_change'] = df['share_change'].abs()
    outliers = df[df['abs_share_change'] > 5.0].sort_values('abs_share_change', ascending=False)
    
    print(f"\nTop 20 State Share Changes (>5% absolute change):")
    print(outliers[['the_date', 'state', 'winner', 'win_share_pct', 'prev_share', 'share_change', 'state_wins']].head(20).to_string())
    
    return outliers

def analyze_state_h2h(con, start_date='2025-06-01'):
    """
    Level 4: State-level H2H pairs
    Shows which state-level matchups are problematic
    """
    print("\n" + "="*80)
    print("LEVEL 4: STATE H2H PAIR OUTLIERS")
    print("="*80)
    
    sql = f"""
    WITH state_h2h AS (
        SELECT 
            the_date,
            state,
            winner,
            loser,
            SUM(current_wins) as total_wins,
            AVG(avg_wins_28d) as avg_baseline,
            AVG(z_score_28d) as avg_z_score,
            MAX(is_first_appearance) as is_first_appearance
        FROM gamoshi_win_mover_rolling
        WHERE the_date >= '{start_date}'
            AND is_outlier_any = true
            AND current_wins >= 10
        GROUP BY the_date, state, winner, loser
    )
    SELECT 
        the_date,
        state,
        winner,
        loser,
        total_wins,
        avg_baseline,
        (total_wins - avg_baseline) as impact,
        avg_z_score,
        is_first_appearance
    FROM state_h2h
    WHERE ABS(total_wins - avg_baseline) > 50
    ORDER BY impact DESC
    LIMIT 50
    """
    
    df = con.execute(sql).df()
    
    print(f"\nTop 20 State H2H Pairs by IMPACT (>50 win difference):")
    print(df[['the_date', 'state', 'winner', 'loser', 'total_wins', 'avg_baseline', 'impact', 'avg_z_score']].head(20).to_string())
    
    return df

def analyze_dma_pairs(con, start_date='2025-06-01'):
    """
    Level 5: DMA-level carrier pairs (finest granularity for suppression)
    Shows exact DMA-winner-loser combinations to suppress
    """
    print("\n" + "="*80)
    print("LEVEL 5: DMA CARRIER PAIR OUTLIERS (SUPPRESSION TARGETS)")
    print("="*80)
    
    sql = f"""
    SELECT 
        the_date,
        state,
        dma_name,
        winner,
        loser,
        current_wins,
        avg_wins_28d as rolling_avg_28d,
        stddev_wins_28d as rolling_stddev_28d,
        z_score_28d as z_score,
        pct_change_28d as pct_change,
        is_first_appearance,
        (current_wins - avg_wins_28d) as impact,
        dayofweek(the_date) as dow
    FROM gamoshi_win_mover_rolling
    WHERE the_date >= '{start_date}'
        AND is_outlier_any = true
        AND current_wins >= 10
    ORDER BY ABS(current_wins - avg_wins_28d) DESC
    LIMIT 100
    """
    
    df = con.execute(sql).df()
    
    print(f"\nTop 20 DMA Pairs by IMPACT (current - baseline):")
    print(df[['the_date', 'state', 'dma_name', 'winner', 'loser', 'current_wins', 'rolling_avg_28d', 'impact', 'z_score', 'is_first_appearance']].head(20).to_string())
    
    # Count by outlier type
    print("\n\nOutlier Type Distribution:")
    print(f"First Appearances: {df['is_first_appearance'].sum()}")
    print(f"Z-Score outliers (z > 1.5): {(df['z_score'].abs() > 1.5).sum()}")
    print(f"High percentage change: {(df['pct_change'] > 30).sum()}")
    
    return df

def generate_summary_stats(con, start_date='2025-06-01'):
    """Generate overall summary statistics"""
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    
    # Total outliers by date
    sql = f"""
    SELECT 
        the_date,
        COUNT(*) as num_outliers,
        SUM(current_wins) as total_outlier_wins,
        SUM(avg_wins_28d) as total_baseline_wins,
        SUM(current_wins - avg_wins_28d) as total_impact,
        dayname(the_date) as dow
    FROM gamoshi_win_mover_rolling
    WHERE the_date >= '{start_date}'
        AND is_outlier_any = true
        AND current_wins >= 10
    GROUP BY the_date
    ORDER BY the_date
    """
    
    df = con.execute(sql).df()
    
    print("\nOutliers by Date:")
    print(df.to_string())
    
    print(f"\n\nTotal outlier records: {df['num_outliers'].sum()}")
    print(f"Total impact (excess wins): {df['total_impact'].sum():.0f}")
    print(f"Average daily outliers: {df['num_outliers'].mean():.1f}")
    
    return df

def main():
    print(f"Connecting to database: {DB_PATH}")
    print(f"Analysis period: 2025-06-01 through end of data\n")
    
    con = duckdb.connect(DB_PATH)
    
    # Verify tables exist
    tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema='main' 
        AND table_name LIKE '%gamoshi%'
        ORDER BY table_name
    """).df()
    print("Available tables:")
    print(tables.to_string())
    
    # Run hierarchical analysis
    try:
        # Summary first
        summary = generate_summary_stats(con)
        
        # Level 1: National shares
        national_outliers = analyze_national_shares(con)
        
        # Level 2: H2H National
        h2h_national = analyze_h2h_national(con)
        
        # Level 3: State shares
        state_outliers = analyze_state_shares(con)
        
        # Level 4: State H2H
        state_h2h = analyze_state_h2h(con)
        
        # Level 5: DMA pairs (suppression targets)
        dma_pairs = analyze_dma_pairs(con)
        
        print("\n" + "="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)
        print("\nKey Findings:")
        print(f"- Total outlier DMA-pair-date combinations: {len(dma_pairs)}")
        print(f"- Unique dates with outliers: {summary['the_date'].nunique()}")
        print(f"- Unique carriers involved: {dma_pairs['winner'].nunique()}")
        print(f"- Unique DMAs affected: {dma_pairs['dma_name'].nunique()}")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        con.close()

if __name__ == "__main__":
    main()
