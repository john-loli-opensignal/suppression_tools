#!/usr/bin/env python3
"""
Create rolling metric views for outlier detection.

This script creates views that calculate rolling statistics (mean, stddev, z-scores)
for win/loss cubes, enabling efficient outlier detection without modifying base cubes.
"""

import argparse
import duckdb
from pathlib import Path


def create_gamoshi_win_mover_rolling_view(con, z_threshold=1.5, pct_threshold=0.30):
    """
    Create a view with rolling metrics for gamoshi_win_mover_cube.
    
    Rolling windows are calculated over preceding dates with the SAME day_of_week:
    - 28-day window (~4 occurrences of same DOW)
    - 14-day window (~2 occurrences of same DOW)
    
    Outlier flags:
    - Z-score based: current value is > z_threshold standard deviations from mean
    - Percentage based: current value is > pct_threshold above the mean
    - First appearance: new winner-loser pair in this DMA
    - Rare pair: pair has appeared < 4 times in the DMA before this date
    
    Args:
        con: DuckDB connection
        z_threshold: Z-score threshold for outlier detection (default 1.5)
        pct_threshold: Percentage threshold for outlier detection (default 0.30)
    """
    
    print(f"[INFO] Creating gamoshi_win_mover_rolling view...")
    print(f"       Z-score threshold: {z_threshold}")
    print(f"       Percentage threshold: {pct_threshold * 100}%")
    
    sql = f"""
    CREATE OR REPLACE VIEW gamoshi_win_mover_rolling AS
    WITH base AS (
        SELECT
            the_date,
            year,
            month,
            day,
            day_of_week,
            winner,
            loser,
            dma,
            dma_name,
            state,
            total_wins,
            record_count
        FROM gamoshi_win_mover_cube
    ),
    -- Calculate historical metrics for each pair in each DMA
    pair_history AS (
        SELECT
            b1.the_date,
            b1.day_of_week,
            b1.state,
            b1.dma,
            b1.dma_name,
            b1.winner,
            b1.loser,
            b1.total_wins as current_wins,
            b1.record_count as current_records,
            
            -- Count how many times this pair appeared before (same DOW)
            COUNT(b2.the_date) as historical_count_same_dow,
            
            -- 28-day rolling window (same DOW)
            AVG(CASE 
                WHEN b2.the_date < b1.the_date 
                AND b2.the_date >= b1.the_date - INTERVAL '28 days'
                AND b2.day_of_week = b1.day_of_week
                THEN b2.total_wins 
            END) as avg_wins_28d,
            
            STDDEV_POP(CASE 
                WHEN b2.the_date < b1.the_date 
                AND b2.the_date >= b1.the_date - INTERVAL '28 days'
                AND b2.day_of_week = b1.day_of_week
                THEN b2.total_wins 
            END) as stddev_wins_28d,
            
            COUNT(CASE 
                WHEN b2.the_date < b1.the_date 
                AND b2.the_date >= b1.the_date - INTERVAL '28 days'
                AND b2.day_of_week = b1.day_of_week
                THEN 1 
            END) as sample_count_28d,
            
            -- 14-day rolling window (same DOW)
            AVG(CASE 
                WHEN b2.the_date < b1.the_date 
                AND b2.the_date >= b1.the_date - INTERVAL '14 days'
                AND b2.day_of_week = b1.day_of_week
                THEN b2.total_wins 
            END) as avg_wins_14d,
            
            STDDEV_POP(CASE 
                WHEN b2.the_date < b1.the_date 
                AND b2.the_date >= b1.the_date - INTERVAL '14 days'
                AND b2.day_of_week = b1.day_of_week
                THEN b2.total_wins 
            END) as stddev_wins_14d,
            
            COUNT(CASE 
                WHEN b2.the_date < b1.the_date 
                AND b2.the_date >= b1.the_date - INTERVAL '14 days'
                AND b2.day_of_week = b1.day_of_week
                THEN 1 
            END) as sample_count_14d
            
        FROM base b1
        LEFT JOIN base b2 
            ON b1.state = b2.state
            AND b1.dma = b2.dma
            AND b1.winner = b2.winner
            AND b1.loser = b2.loser
            AND b2.the_date <= b1.the_date
            AND b2.day_of_week = b1.day_of_week
        GROUP BY 
            b1.the_date, b1.day_of_week, b1.state, b1.dma, b1.dma_name,
            b1.winner, b1.loser, b1.total_wins, b1.record_count
    ),
    -- Calculate z-scores and flags
    metrics AS (
        SELECT
            *,
            -- Z-scores (28-day)
            CASE 
                WHEN stddev_wins_28d > 0 AND stddev_wins_28d IS NOT NULL
                THEN (current_wins - avg_wins_28d) / stddev_wins_28d
                ELSE NULL
            END as z_score_28d,
            
            -- Percentage change (28-day)
            CASE 
                WHEN avg_wins_28d > 0 AND avg_wins_28d IS NOT NULL
                THEN (current_wins - avg_wins_28d) / avg_wins_28d
                ELSE NULL
            END as pct_change_28d,
            
            -- Z-scores (14-day)
            CASE 
                WHEN stddev_wins_14d > 0 AND stddev_wins_14d IS NOT NULL
                THEN (current_wins - avg_wins_14d) / stddev_wins_14d
                ELSE NULL
            END as z_score_14d,
            
            -- Percentage change (14-day)
            CASE 
                WHEN avg_wins_14d > 0 AND avg_wins_14d IS NOT NULL
                THEN (current_wins - avg_wins_14d) / avg_wins_14d
                ELSE NULL
            END as pct_change_14d,
            
            -- First appearance flag (excluding current date)
            CASE WHEN historical_count_same_dow = 1 THEN TRUE ELSE FALSE END as is_first_appearance,
            
            -- Rare pair flag (appeared < 4 times before)
            CASE WHEN historical_count_same_dow < 5 THEN TRUE ELSE FALSE END as is_rare_pair
            
        FROM pair_history
    )
    SELECT
        *,
        -- Outlier flags (28-day window, requiring at least 4 samples)
        CASE 
            WHEN sample_count_28d >= 4 AND (
                (z_score_28d > {z_threshold}) OR 
                (pct_change_28d > {pct_threshold})
            ) THEN TRUE
            ELSE FALSE
        END as is_outlier_28d,
        
        -- Outlier flags (14-day window, requiring at least 2 samples)
        CASE 
            WHEN sample_count_14d >= 2 AND (
                (z_score_14d > {z_threshold}) OR 
                (pct_change_14d > {pct_threshold})
            ) THEN TRUE
            ELSE FALSE
        END as is_outlier_14d,
        
        -- Combined outlier flag (any method)
        CASE 
            WHEN (sample_count_28d >= 4 AND (z_score_28d > {z_threshold} OR pct_change_28d > {pct_threshold}))
                OR (sample_count_14d >= 2 AND (z_score_14d > {z_threshold} OR pct_change_14d > {pct_threshold}))
            THEN TRUE
            ELSE FALSE
        END as is_outlier_any
        
    FROM metrics
    """
    
    con.execute(sql)
    
    # Validate the view
    stats = con.execute("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT the_date) as num_dates,
            SUM(CASE WHEN is_first_appearance THEN 1 ELSE 0 END) as first_appearances,
            SUM(CASE WHEN is_rare_pair THEN 1 ELSE 0 END) as rare_pairs,
            SUM(CASE WHEN is_outlier_28d THEN 1 ELSE 0 END) as outliers_28d,
            SUM(CASE WHEN is_outlier_14d THEN 1 ELSE 0 END) as outliers_14d,
            SUM(CASE WHEN is_outlier_any THEN 1 ELSE 0 END) as outliers_any,
            MIN(the_date) as min_date,
            MAX(the_date) as max_date
        FROM gamoshi_win_mover_rolling
    """).df()
    
    print("[SUCCESS] View created successfully!")
    print(f"\nView Statistics:")
    print(f"  Total rows: {stats['total_rows'].iloc[0]:,}")
    print(f"  Date range: {stats['min_date'].iloc[0]} to {stats['max_date'].iloc[0]}")
    print(f"  First appearances: {stats['first_appearances'].iloc[0]:,}")
    print(f"  Rare pairs (< 5 appearances): {stats['rare_pairs'].iloc[0]:,}")
    print(f"  Outliers (28d): {stats['outliers_28d'].iloc[0]:,}")
    print(f"  Outliers (14d): {stats['outliers_14d'].iloc[0]:,}")
    print(f"  Outliers (any): {stats['outliers_any'].iloc[0]:,}")
    
    # Show sample outliers
    print("\n[INFO] Sample outliers from 2025-06-19:")
    sample = con.execute("""
        SELECT 
            the_date,
            state,
            dma_name,
            winner,
            loser,
            current_wins,
            avg_wins_28d,
            z_score_28d,
            pct_change_28d,
            is_first_appearance,
            is_rare_pair,
            is_outlier_28d
        FROM gamoshi_win_mover_rolling
        WHERE the_date = '2025-06-19'
        AND is_outlier_28d = TRUE
        ORDER BY z_score_28d DESC
        LIMIT 5
    """).df()
    print(sample.to_string())


def main():
    parser = argparse.ArgumentParser(
        description="Create rolling metric views for outlier detection"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="data/databases/duck_suppression.db",
        help="Path to DuckDB database"
    )
    parser.add_argument(
        "--z-threshold",
        type=float,
        default=1.5,
        help="Z-score threshold for outlier detection (default: 1.5)"
    )
    parser.add_argument(
        "--pct-threshold",
        type=float,
        default=0.30,
        help="Percentage threshold for outlier detection (default: 0.30)"
    )
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[ERROR] Database not found: {db_path}")
        return 1
    
    print(f"[INFO] Connecting to database: {db_path}")
    con = duckdb.connect(str(db_path))
    
    try:
        create_gamoshi_win_mover_rolling_view(
            con, 
            z_threshold=args.z_threshold,
            pct_threshold=args.pct_threshold
        )
        print("\n[SUCCESS] All views created successfully!")
        return 0
        
    except Exception as e:
        print(f"[ERROR] Failed to create views: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        con.close()


if __name__ == "__main__":
    exit(main())
