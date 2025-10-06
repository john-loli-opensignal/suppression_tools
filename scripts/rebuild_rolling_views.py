#!/usr/bin/env python3
"""
Rebuild rolling views with correct DOW-aware tiered lookback logic.

DOW Encoding (DuckDB standard):
- 0 = Sunday (weekend)
- 1 = Monday (weekday)
- 2 = Tuesday (weekday)
- 3 = Wednesday (weekday)
- 4 = Thursday (weekday)
- 5 = Friday (weekday)
- 6 = Saturday (weekend)

Tiered Windows:
- Weekdays (1-5): Requires 4+ periods minimum, prefers 28d, falls back to 14d, then 4d
- Weekends (0, 6): Requires 2+ periods minimum (more lenient), same fallback order
"""

import duckdb
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tools import db as db_tools


def create_rolling_view_sql(ds: str, metric_type: str, mover_type: str) -> str:
    """
    Generate SQL for DOW-aware rolling metrics view.
    
    Args:
        ds: Dataset name (e.g., 'gamoshi')
        metric_type: 'win' or 'loss'
        mover_type: 'mover' or 'non_mover'
    
    Returns:
        SQL CREATE OR REPLACE VIEW statement
    """
    cube_table = f"{ds}_{metric_type}_{mover_type}_cube"
    view_name = f"{ds}_{metric_type}_{mover_type}_rolling"
    metric_col = f"total_{metric_type}s"
    carrier_col = "winner" if metric_type == "win" else "loser"
    
    sql = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    WITH top_carriers AS (
        -- Top 50 carriers by total metric across all time
        SELECT {carrier_col}
        FROM {cube_table}
        GROUP BY {carrier_col}
        ORDER BY SUM({metric_col}) DESC
        LIMIT 50
    ),
    filtered_cube AS (
        -- Filter to top carriers only
        SELECT c.*
        FROM {cube_table} c
        INNER JOIN top_carriers tc ON c.{carrier_col} = tc.{carrier_col}
    ),
    dma_daily AS (
        -- Aggregate to DMA-carrier-pair-day level
        SELECT 
            the_date,
            day_of_week,
            winner,
            loser,
            dma,
            dma_name,
            state,
            SUM({metric_col}) AS total_{metric_type}s,
            SUM(record_count) AS record_count
        FROM filtered_cube
        GROUP BY the_date, day_of_week, winner, loser, dma, dma_name, state
    ),
    rolling_metrics AS (
        -- Calculate rolling metrics for 28d, 14d, and 4d windows
        SELECT 
            d.*,
            -- 28-day window (4 weeks = 4 periods of same DOW)
            AVG(total_{metric_type}s) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 27 PRECEDING AND 1 PRECEDING
            ) AS avg_{metric_type}s_28d,
            STDDEV(total_{metric_type}s) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 27 PRECEDING AND 1 PRECEDING
            ) AS stddev_{metric_type}s_28d,
            COUNT(*) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 27 PRECEDING AND 1 PRECEDING
            ) AS record_count_28,
            
            -- 14-day window (2 weeks = 2 periods of same DOW)
            AVG(total_{metric_type}s) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
            ) AS avg_{metric_type}s_14d,
            STDDEV(total_{metric_type}s) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
            ) AS stddev_{metric_type}s_14d,
            COUNT(*) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
            ) AS record_count_14,
            
            -- 4-day window (minimum for weekdays)
            AVG(total_{metric_type}s) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_{metric_type}s_4d,
            STDDEV(total_{metric_type}s) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS stddev_{metric_type}s_4d,
            COUNT(*) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS record_count_4,
            
            -- Track first appearance
            ROW_NUMBER() OVER (
                PARTITION BY winner, loser, dma 
                ORDER BY the_date
            ) AS appearance_rank
        FROM dma_daily d
    ),
    tiered_selection AS (
        -- Select best available window based on DOW and data availability
        SELECT 
            *,
            -- Select window tier
            CASE 
                -- Weekdays (Monday-Friday = 1-5): Need 4+ periods minimum
                WHEN day_of_week BETWEEN 1 AND 5 THEN
                    CASE 
                        WHEN record_count_28 >= 4 THEN 28
                        WHEN record_count_14 >= 4 THEN 14
                        WHEN record_count_4 >= 4 THEN 4
                        ELSE NULL
                    END
                -- Weekends (Sunday=0, Saturday=6): Need 2+ periods minimum (more lenient)
                ELSE
                    CASE 
                        WHEN record_count_28 >= 2 THEN 28
                        WHEN record_count_14 >= 2 THEN 14
                        WHEN record_count_4 >= 2 THEN 4
                        ELSE NULL
                    END
            END AS selected_window,
            
            -- Select avg metric based on selected window
            CASE 
                WHEN day_of_week BETWEEN 1 AND 5 THEN
                    CASE 
                        WHEN record_count_28 >= 4 THEN avg_{metric_type}s_28d
                        WHEN record_count_14 >= 4 THEN avg_{metric_type}s_14d
                        WHEN record_count_4 >= 4 THEN avg_{metric_type}s_4d
                        ELSE NULL
                    END
                ELSE
                    CASE 
                        WHEN record_count_28 >= 2 THEN avg_{metric_type}s_28d
                        WHEN record_count_14 >= 2 THEN avg_{metric_type}s_14d
                        WHEN record_count_4 >= 2 THEN avg_{metric_type}s_4d
                        ELSE NULL
                    END
            END AS avg_{metric_type}s,
            
            -- Select stddev based on selected window
            CASE 
                WHEN day_of_week BETWEEN 1 AND 5 THEN
                    CASE 
                        WHEN record_count_28 >= 4 THEN stddev_{metric_type}s_28d
                        WHEN record_count_14 >= 4 THEN stddev_{metric_type}s_14d
                        WHEN record_count_4 >= 4 THEN stddev_{metric_type}s_4d
                        ELSE NULL
                    END
                ELSE
                    CASE 
                        WHEN record_count_28 >= 2 THEN stddev_{metric_type}s_28d
                        WHEN record_count_14 >= 2 THEN stddev_{metric_type}s_14d
                        WHEN record_count_4 >= 2 THEN stddev_{metric_type}s_4d
                        ELSE NULL
                    END
            END AS stddev_{metric_type}s,
            
            -- Number of periods used (not days!)
            CASE 
                WHEN day_of_week BETWEEN 1 AND 5 THEN
                    CASE 
                        WHEN record_count_28 >= 4 THEN record_count_28
                        WHEN record_count_14 >= 4 THEN record_count_14
                        WHEN record_count_4 >= 4 THEN record_count_4
                        ELSE NULL
                    END
                ELSE
                    CASE 
                        WHEN record_count_28 >= 2 THEN record_count_28
                        WHEN record_count_14 >= 2 THEN record_count_14
                        WHEN record_count_4 >= 2 THEN record_count_4
                        ELSE NULL
                    END
            END AS n_periods
        FROM rolling_metrics
    )
    SELECT 
        the_date,
        day_of_week,
        winner,
        loser,
        dma,
        dma_name,
        state,
        total_{metric_type}s,
        record_count,
        avg_{metric_type}s,
        stddev_{metric_type}s,
        n_periods,
        selected_window,
        
        -- Z-score (handle NULL stddev)
        CASE 
            WHEN stddev_{metric_type}s IS NULL OR stddev_{metric_type}s = 0 THEN NULL
            ELSE (total_{metric_type}s - avg_{metric_type}s) / stddev_{metric_type}s
        END AS zscore,
        
        -- Percent change
        CASE 
            WHEN avg_{metric_type}s IS NULL OR avg_{metric_type}s = 0 THEN NULL
            ELSE ((total_{metric_type}s - avg_{metric_type}s) / avg_{metric_type}s) * 100
        END AS pct_change,
        
        -- First appearance flag (first 4 occurrences at DMA level)
        (appearance_rank <= 4) AS is_first_appearance,
        
        -- Outlier flag (z-score > 1.5 OR first appearance with insufficient history)
        (
            (zscore IS NOT NULL AND ABS(zscore) > 1.5) 
            OR (appearance_rank <= 4 AND n_periods IS NULL)
        ) AS is_outlier,
        
        appearance_rank
    FROM tiered_selection
    """
    
    return sql


def rebuild_rolling_views(db_path: str, dataset: str = 'gamoshi'):
    """Rebuild all rolling views for a dataset."""
    assert db_path.endswith('data/databases/duck_suppression.db'), \
        f"❌ CRITICAL ERROR: Wrong database path: {db_path}\n" \
        f"   Expected: data/databases/duck_suppression.db\n" \
        f"   This error exists to prevent accidental database proliferation."
    
    con = duckdb.connect(db_path)
    
    views_to_create = [
        ('win', 'mover'),
        ('win', 'non_mover'),
        # Add loss views later if needed
    ]
    
    print(f"🔄 Rebuilding rolling views for dataset: {dataset}")
    print(f"   Database: {db_path}\n")
    
    for metric_type, mover_type in views_to_create:
        view_name = f"{dataset}_{metric_type}_{mover_type}_rolling"
        print(f"[INFO] Creating view: {view_name}")
        
        sql = create_rolling_view_sql(dataset, metric_type, mover_type)
        
        try:
            con.execute(sql)
            
            # Validate view
            stats = con.execute(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN avg_{metric_type}s IS NULL THEN 1 ELSE 0 END) as null_avg,
                    SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) as outliers,
                    MIN(the_date) as min_date,
                    MAX(the_date) as max_date
                FROM {view_name}
            """).fetchone()
            
            total, nulls, outliers, min_date, max_date = stats
            null_pct = (nulls / total * 100) if total > 0 else 0
            
            print(f"   ✅ Created successfully")
            print(f"      - Total rows: {total:,}")
            print(f"      - NULL avg_{metric_type}s: {nulls:,} ({null_pct:.1f}%)")
            print(f"      - Outliers flagged: {outliers:,}")
            print(f"      - Date range: {min_date} to {max_date}\n")
            
        except Exception as e:
            print(f"   ❌ Failed: {e}\n")
            raise
    
    con.close()
    
    print("✅ All rolling views rebuilt successfully!")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Rebuild rolling views with correct DOW logic')
    parser.add_argument('--db', default=None, help='Database path')
    parser.add_argument('--ds', default='gamoshi', help='Dataset name')
    
    args = parser.parse_args()
    
    db_path = args.db if args.db else db_tools.get_default_db_path()
    
    rebuild_rolling_views(db_path, args.ds)
