"""
Fix rolling view to use tiered thresholds: 28d preferred, fall back to 14d, then 4d.
This prevents NULLs in rolling metrics by using the best available window.
"""

import duckdb
import sys
from pathlib import Path

def get_db_path():
    """Return canonical database path."""
    db_path = Path(__file__).parent.parent / 'data' / 'databases' / 'duck_suppression.db'
    assert db_path.exists(), f"Database not found: {db_path}"
    assert str(db_path).endswith('data/databases/duck_suppression.db'), f"Wrong DB: {db_path}"
    return str(db_path)

def create_tiered_rolling_view(con, ds='gamoshi', mover_ind='mover'):
    """
    Create rolling view with tiered windows:
    - Try 28 preceding (needs 4+ for min stats)
    - Fall back to 14 preceding (needs 4+ for min stats)
    - Fall back to 4 preceding (needs 4 for min stats)
    - Weekend gets more lenient (needs 2+ minimum)
    """
    
    table_name = f"{ds}_win_{mover_ind}_cube"
    view_name = f"{ds}_win_{mover_ind}_rolling"
    
    print(f"[INFO] Creating tiered rolling view: {view_name}")
    
    # Drop existing view
    con.execute(f"DROP VIEW IF EXISTS {view_name}")
    
    sql = f"""
    CREATE VIEW {view_name} AS
    WITH top_carriers AS (
        -- Top 50 carriers by total wins (entire time series)
        SELECT winner
        FROM {table_name}
        GROUP BY winner
        ORDER BY SUM(total_wins) DESC
        LIMIT 50
    ),
    filtered_cube AS (
        SELECT c.*
        FROM {table_name} c
        INNER JOIN top_carriers tc ON c.winner = tc.winner
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
            SUM(total_wins) AS total_wins,
            SUM(record_count) AS record_count
        FROM filtered_cube
        GROUP BY the_date, day_of_week, winner, loser, dma, dma_name, state
    ),
    rolling_metrics AS (
        SELECT 
            d.*,
            
            -- Compute metrics for 28d window (preferred)
            AVG(total_wins) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 27 PRECEDING AND 1 PRECEDING
            ) AS avg_wins_28d,
            STDDEV(total_wins) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 27 PRECEDING AND 1 PRECEDING
            ) AS stddev_wins_28d,
            COUNT(*) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 27 PRECEDING AND 1 PRECEDING
            ) AS record_count_28,
            
            -- Compute metrics for 14d window (fallback)
            AVG(total_wins) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
            ) AS avg_wins_14d,
            STDDEV(total_wins) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
            ) AS stddev_wins_14d,
            COUNT(*) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
            ) AS record_count_14,
            
            -- Compute metrics for 4d window (last resort)
            AVG(total_wins) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_wins_4d,
            STDDEV(total_wins) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS stddev_wins_4d,
            COUNT(*) OVER (
                PARTITION BY winner, loser, dma, day_of_week 
                ORDER BY the_date 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS record_count_4,
            
            -- First appearance tracking
            ROW_NUMBER() OVER (PARTITION BY winner, loser, dma ORDER BY the_date) AS appearance_rank
        FROM dma_daily d
    ),
    tiered_selection AS (
        SELECT
            *,
            -- Choose best available window (28d preferred, then 14d, then 4d)
            -- Weekday needs 4+ periods minimum, Weekend needs 2+ (more lenient)
            CASE 
                -- Weekday logic (Mon-Fri = day_of_week 0-4)
                WHEN day_of_week <= 4 THEN
                    CASE
                        WHEN record_count_28 >= 4 THEN 28
                        WHEN record_count_14 >= 4 THEN 14
                        WHEN record_count_4 >= 4 THEN 4
                        ELSE NULL
                    END
                -- Weekend logic (Sat-Sun = day_of_week 5-6)  
                ELSE
                    CASE
                        WHEN record_count_28 >= 2 THEN 28
                        WHEN record_count_14 >= 2 THEN 14
                        WHEN record_count_4 >= 2 THEN 4
                        ELSE NULL
                    END
            END AS selected_window,
            
            -- Select avg_wins based on chosen window
            CASE 
                WHEN day_of_week <= 4 THEN
                    CASE
                        WHEN record_count_28 >= 4 THEN avg_wins_28d
                        WHEN record_count_14 >= 4 THEN avg_wins_14d
                        WHEN record_count_4 >= 4 THEN avg_wins_4d
                        ELSE NULL
                    END
                ELSE
                    CASE
                        WHEN record_count_28 >= 2 THEN avg_wins_28d
                        WHEN record_count_14 >= 2 THEN avg_wins_14d
                        WHEN record_count_4 >= 2 THEN avg_wins_4d
                        ELSE NULL
                    END
            END AS avg_wins,
            
            -- Select stddev based on chosen window
            CASE 
                WHEN day_of_week <= 4 THEN
                    CASE
                        WHEN record_count_28 >= 4 THEN stddev_wins_28d
                        WHEN record_count_14 >= 4 THEN stddev_wins_14d
                        WHEN record_count_4 >= 4 THEN stddev_wins_4d
                        ELSE NULL
                    END
                ELSE
                    CASE
                        WHEN record_count_28 >= 2 THEN stddev_wins_28d
                        WHEN record_count_14 >= 2 THEN stddev_wins_14d
                        WHEN record_count_4 >= 2 THEN stddev_wins_4d
                        ELSE NULL
                    END
            END AS stddev_wins,
            
            -- Select record count based on chosen window
            CASE 
                WHEN day_of_week <= 4 THEN
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
        total_wins,
        record_count,
        
        -- Tiered rolling metrics
        avg_wins,
        stddev_wins,
        n_periods,
        selected_window,
        
        -- Computed outlier metrics
        CASE 
            WHEN n_periods IS NOT NULL AND stddev_wins > 0 
            THEN (total_wins - avg_wins) / stddev_wins
            ELSE NULL 
        END AS zscore,
        
        CASE 
            WHEN n_periods IS NOT NULL AND avg_wins > 0 
            THEN ((total_wins - avg_wins) / avg_wins) * 100
            ELSE NULL 
        END AS pct_change,
        
        -- First appearance flag
        (appearance_rank = 1) AS is_first_appearance,
        appearance_rank,
        
        -- Outlier flag (configurable thresholds)
        CASE
            -- Ignore low volume
            WHEN total_wins < 10 THEN FALSE
            
            -- First appearance with volume
            WHEN appearance_rank = 1 AND total_wins >= 10 THEN TRUE
            
            -- Z-score or percentage outlier
            WHEN n_periods IS NOT NULL THEN
                (stddev_wins > 0 AND ABS((total_wins - avg_wins) / stddev_wins) > 1.5)
                OR (avg_wins > 0 AND ABS((total_wins - avg_wins) / avg_wins) > 0.30)
            
            ELSE FALSE
        END AS is_outlier
        
    FROM tiered_selection
    """
    
    con.execute(sql)
    print(f"[SUCCESS] Created tiered rolling view: {view_name}")
    
    # Validate
    stats = con.execute(f"""
        SELECT 
            selected_window,
            COUNT(*) as record_count,
            COUNT(CASE WHEN avg_wins IS NOT NULL THEN 1 END) as has_avg,
            COUNT(CASE WHEN avg_wins IS NULL THEN 1 END) as null_avg
        FROM {view_name}
        GROUP BY selected_window
        ORDER BY selected_window
    """).df()
    
    print("\nWindow Distribution:")
    print(stats.to_string())
    
    return stats


def main():
    db_path = get_db_path()
    print(f"Database: {db_path}\n")
    
    con = duckdb.connect(db_path)
    
    try:
        # Create tiered views for mover and non_mover
        print("=" * 80)
        create_tiered_rolling_view(con, ds='gamoshi', mover_ind='mover')
        print()
        
        print("=" * 80)
        create_tiered_rolling_view(con, ds='gamoshi', mover_ind='non_mover')
        print()
        
        # Sample query to show it works
        print("=" * 80)
        print("Sample outliers from June 19, 2025 (gamoshi mover):")
        print("=" * 80)
        sample = con.execute("""
            SELECT 
                the_date,
                winner,
                loser,
                dma_name,
                total_wins,
                ROUND(avg_wins, 1) as avg_wins,
                n_periods,
                selected_window,
                ROUND(zscore, 2) as zscore,
                ROUND(pct_change, 1) as pct_change,
                is_first_appearance,
                is_outlier
            FROM gamoshi_win_mover_rolling
            WHERE the_date = '2025-06-19'
              AND is_outlier = TRUE
            ORDER BY ABS(total_wins - avg_wins) DESC
            LIMIT 15
        """).df()
        print(sample.to_string())
        
    finally:
        con.close()
    
    print("\n[SUCCESS] Tiered rolling views created!")


if __name__ == "__main__":
    main()
