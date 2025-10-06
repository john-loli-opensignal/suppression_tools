#!/usr/bin/env python3
"""
Compare 3-category DOW (Sat/Sun/Weekday) vs 7-day DOW for outlier detection
"""
import duckdb
import pandas as pd
from datetime import datetime

DB_PATH = "data/databases/duck_suppression.db"

def test_3_category_approach(start_date, end_date, z_threshold=2.5, window_days=28):
    """
    3-category approach: Weekday, Saturday, Sunday
    Similar to carrier_dashboard_duckdb.py
    """
    con = duckdb.connect(DB_PATH, read_only=True)
    
    sql = f"""
    WITH filtered_cube AS (
        SELECT 
            the_date,
            day_of_week,
            winner,
            loser,
            dma,
            dma_name,
            state,
            total_wins
        FROM gamoshi_win_mover_cube
        WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
    ),
    dma_daily AS (
        SELECT 
            the_date,
            day_of_week,
            CASE 
                WHEN day_of_week = 6 THEN 'Sat'
                WHEN day_of_week = 0 THEN 'Sun'
                ELSE 'Weekday'
            END as day_type,
            winner,
            loser,
            dma,
            dma_name,
            state,
            SUM(total_wins) as total_wins
        FROM filtered_cube
        GROUP BY the_date, day_of_week, winner, loser, dma, dma_name, state
    ),
    rolling_metrics AS (
        SELECT 
            d.*,
            AVG(total_wins) OVER (
                PARTITION BY winner, loser, dma, day_type
                ORDER BY the_date
                ROWS BETWEEN {window_days-1} PRECEDING AND 1 PRECEDING
            ) as avg_wins,
            STDDEV(total_wins) OVER (
                PARTITION BY winner, loser, dma, day_type
                ORDER BY the_date
                ROWS BETWEEN {window_days-1} PRECEDING AND 1 PRECEDING
            ) as stddev_wins,
            COUNT(*) OVER (
                PARTITION BY winner, loser, dma, day_type
                ORDER BY the_date
                ROWS BETWEEN {window_days-1} PRECEDING AND 1 PRECEDING
            ) as n_periods
        FROM dma_daily d
    )
    SELECT 
        the_date,
        winner,
        loser,
        dma_name,
        day_type,
        total_wins,
        avg_wins,
        stddev_wins,
        n_periods,
        CASE 
            WHEN stddev_wins IS NULL OR stddev_wins = 0 THEN NULL
            ELSE (total_wins - avg_wins) / stddev_wins
        END as zscore
    FROM rolling_metrics
    WHERE avg_wins IS NOT NULL
        AND stddev_wins IS NOT NULL
        AND stddev_wins > 0
        AND ABS((total_wins - avg_wins) / stddev_wins) > {z_threshold}
    ORDER BY the_date, winner
    """
    
    result = con.execute(sql).df()
    con.close()
    return result


def test_7_day_approach(start_date, end_date, z_threshold=2.5, window_days=28):
    """
    7-day approach: Each day of week tracked separately
    Current main.py/rolling view approach
    """
    con = duckdb.connect(DB_PATH, read_only=True)
    
    sql = f"""
    WITH filtered_cube AS (
        SELECT 
            the_date,
            day_of_week,
            winner,
            loser,
            dma,
            dma_name,
            state,
            total_wins
        FROM gamoshi_win_mover_cube
        WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
    ),
    dma_daily AS (
        SELECT 
            the_date,
            day_of_week,
            winner,
            loser,
            dma,
            dma_name,
            state,
            SUM(total_wins) as total_wins
        FROM filtered_cube
        GROUP BY the_date, day_of_week, winner, loser, dma, dma_name, state
    ),
    rolling_metrics AS (
        SELECT 
            d.*,
            AVG(total_wins) OVER (
                PARTITION BY winner, loser, dma, day_of_week
                ORDER BY the_date
                ROWS BETWEEN {window_days-1} PRECEDING AND 1 PRECEDING
            ) as avg_wins,
            STDDEV(total_wins) OVER (
                PARTITION BY winner, loser, dma, day_of_week
                ORDER BY the_date
                ROWS BETWEEN {window_days-1} PRECEDING AND 1 PRECEDING
            ) as stddev_wins,
            COUNT(*) OVER (
                PARTITION BY winner, loser, dma, day_of_week
                ORDER BY the_date
                ROWS BETWEEN {window_days-1} PRECEDING AND 1 PRECEDING
            ) as n_periods
        FROM dma_daily d
    )
    SELECT 
        the_date,
        winner,
        loser,
        dma_name,
        day_of_week,
        total_wins,
        avg_wins,
        stddev_wins,
        n_periods,
        CASE 
            WHEN stddev_wins IS NULL OR stddev_wins = 0 THEN NULL
            ELSE (total_wins - avg_wins) / stddev_wins
        END as zscore
    FROM rolling_metrics
    WHERE avg_wins IS NOT NULL
        AND stddev_wins IS NOT NULL
        AND stddev_wins > 0
        AND ABS((total_wins - avg_wins) / stddev_wins) > {z_threshold}
    ORDER BY the_date, winner
    """
    
    result = con.execute(sql).df()
    con.close()
    return result


def compare_national_outliers(start_date, end_date, z_threshold=2.5, window_days=28):
    """
    Compare national-level outlier detection with both approaches
    """
    print(f"\n{'='*80}")
    print(f"COMPARING DOW APPROACHES: {start_date} to {end_date}")
    print(f"Z-Score Threshold: {z_threshold}, Window: {window_days} days")
    print(f"{'='*80}\n")
    
    # Test 3-category approach
    print("Running 3-Category Approach (Sat/Sun/Weekday)...")
    three_cat = test_3_category_approach(start_date, end_date, z_threshold, window_days)
    
    # Test 7-day approach
    print("Running 7-Day Approach (Each DOW separate)...")
    seven_day = test_7_day_approach(start_date, end_date, z_threshold, window_days)
    
    print(f"\n{'='*80}")
    print("RESULTS COMPARISON")
    print(f"{'='*80}\n")
    
    print(f"3-Category Approach:")
    print(f"  Total DMA-level outliers: {len(three_cat)}")
    print(f"  Unique dates: {three_cat['the_date'].nunique()}")
    print(f"  Unique carriers: {three_cat['winner'].nunique()}")
    
    print(f"\n7-Day Approach:")
    print(f"  Total DMA-level outliers: {len(seven_day)}")
    print(f"  Unique dates: {seven_day['the_date'].nunique()}")
    print(f"  Unique carriers: {seven_day['winner'].nunique()}")
    
    # Show specific carriers with outliers
    print(f"\n{'='*80}")
    print("CARRIER COMPARISON")
    print(f"{'='*80}\n")
    
    three_cat_carriers = set(three_cat['winner'].unique())
    seven_day_carriers = set(seven_day['winner'].unique())
    
    print(f"Carriers with outliers in 3-Category: {sorted(three_cat_carriers)[:10]}")
    print(f"Carriers with outliers in 7-Day: {sorted(seven_day_carriers)[:10]}")
    
    only_three = three_cat_carriers - seven_day_carriers
    only_seven = seven_day_carriers - three_cat_carriers
    both = three_cat_carriers & seven_day_carriers
    
    print(f"\nOnly in 3-Category: {sorted(only_three) if only_three else 'None'}")
    print(f"Only in 7-Day: {sorted(only_seven) if only_seven else 'None'}")
    print(f"In Both: {len(both)} carriers")
    
    # Check specific dates that user mentioned
    print(f"\n{'='*80}")
    print("DETAILED EXAMPLE: AT&T Outliers")
    print(f"{'='*80}\n")
    
    att_three = three_cat[three_cat['winner'] == 'AT&T']
    att_seven = seven_day[seven_day['winner'] == 'AT&T']
    
    print(f"3-Category approach found {len(att_three)} AT&T outliers")
    if len(att_three) > 0:
        print(att_three[['the_date', 'dma_name', 'day_type', 'total_wins', 'avg_wins', 'zscore']].head(10).to_string(index=False))
    
    print(f"\n7-Day approach found {len(att_seven)} AT&T outliers")
    if len(att_seven) > 0:
        print(att_seven[['the_date', 'dma_name', 'day_of_week', 'total_wins', 'avg_wins', 'zscore']].head(10).to_string(index=False))
    
    return three_cat, seven_day


if __name__ == "__main__":
    # Test with the same parameters as carrier_dashboard
    start_date = "2025-06-01"
    end_date = "2025-09-04"
    
    three_cat, seven_day = compare_national_outliers(start_date, end_date, z_threshold=2.5, window_days=28)
    
    # Save results for analysis
    three_cat.to_csv("analysis/outliers_3_category.csv", index=False)
    seven_day.to_csv("analysis/outliers_7_day.csv", index=False)
    
    print(f"\n{'='*80}")
    print("Results saved to:")
    print("  - analysis/outliers_3_category.csv")
    print("  - analysis/outliers_7_day.csv")
    print(f"{'='*80}\n")
