"""Plan building and outlier detection using database cubes."""
from typing import List, Optional
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import tools.db as db


def get_top_n_carriers(
    ds: str, 
    mover_ind: bool, 
    n: int = 50,
    min_share_pct: float = 0.0,
    db_path: Optional[str] = None
) -> List[str]:
    """Get top N carriers by total wins over entire time series.
    
    Args:
        ds: Dataset name (e.g., 'gamoshi')
        mover_ind: True for movers, False for non-movers
        n: Number of top carriers to return
        min_share_pct: Minimum overall share % (0.0 = no filter)
        db_path: Path to database (uses default if None)
        
    Returns:
        List of carrier names
    """
    if db_path is None:
        db_path = db.get_default_db_path()
    
    # Assert correct database path
    assert db_path.endswith('data/databases/duck_suppression.db'), \
        f"ERROR: Wrong database path: {db_path}. Must use data/databases/duck_suppression.db"
    
    cube_table = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"
    
    # Apply share filter if specified
    if min_share_pct > 0:
        sql = f"""
            WITH total_wins AS (
                SELECT 
                    winner,
                    SUM(total_wins) as total
                FROM {cube_table}
                GROUP BY winner
            ),
            market_total AS (
                SELECT SUM(total) as market_total
                FROM total_wins
            ),
            with_share AS (
                SELECT 
                    t.winner,
                    t.total,
                    t.total * 100.0 / m.market_total as overall_share_pct
                FROM total_wins t
                CROSS JOIN market_total m
            )
            SELECT winner
            FROM with_share
            WHERE overall_share_pct >= {min_share_pct}
            ORDER BY total DESC
            LIMIT {n}
        """
    else:
        sql = f"""
            SELECT winner, SUM(total_wins) as total
            FROM {cube_table}
            GROUP BY winner
            ORDER BY total DESC
            LIMIT {n}
        """
    df = db.query(sql, db_path)
    return df['winner'].tolist()


def base_national_series(
    ds: str,
    mover_ind: bool,
    winners: List[str],
    start_date: str,
    end_date: str,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """Get national win share time series from database cubes.
    
    Args:
        ds: Dataset name
        mover_ind: True for movers, False for non-movers
        winners: List of winner names to include
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        db_path: Path to database
        
    Returns:
        DataFrame with columns: the_date, winner, total_wins, market_total, win_share
    """
    if db_path is None:
        db_path = db.get_default_db_path()
    
    # Assert correct database path
    assert db_path.endswith('data/databases/duck_suppression.db'), \
        f"ERROR: Wrong database path: {db_path}. Must use data/databases/duck_suppression.db"
    
    cube_table = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"
    
    # Build winner filter
    winners_str = ','.join([f"'{w}'" for w in winners]) if winners else "''"
    
    sql = f"""
        WITH agg AS (
            SELECT 
                the_date,
                winner,
                SUM(total_wins) as total_wins
            FROM {cube_table}
            WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
                AND winner IN ({winners_str})
            GROUP BY the_date, winner
        ),
        market AS (
            SELECT 
                the_date,
                SUM(total_wins) as market_total
            FROM agg
            GROUP BY the_date
        )
        SELECT 
            a.the_date,
            a.winner,
            a.total_wins,
            m.market_total,
            a.total_wins * 1.0 / NULLIF(m.market_total, 0) as win_share
        FROM agg a
        JOIN market m ON a.the_date = m.the_date
        ORDER BY a.the_date, a.winner
    """
    return db.query(sql, db_path)


def scan_base_outliers(
    ds: str,
    mover_ind: bool,
    start_date: str,
    end_date: str,
    z_threshold: float = 2.5,
    top_n: int = 50,
    min_share_pct: float = 0.0,
    egregious_threshold: int = 40,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """Scan for national-level outliers using tiered rolling windows.
    
    Computes rolling metrics from the beginning of the time series using tiered logic:
    - Try 28d window (needs 4+ DOW samples for weekdays, 2+ for weekends)
    - Fall back to 14d window (needs 4+ DOW samples for weekdays, 2+ for weekends)
    - Fall back to 4d window (minimum threshold)
    - Then filter results to the graph window (start_date to end_date)
    
    Focuses on top N carriers (with optional min share %), but flags egregious outliers outside top N.
    
    Args:
        ds: Dataset name
        mover_ind: True for movers, False for non-movers
        start_date: Start date for graph window
        end_date: End date for graph window
        z_threshold: Z-score threshold for outlier detection
        top_n: Number of top carriers to focus on
        min_share_pct: Minimum overall share % (0.0 = no filter)
        egregious_threshold: Impact threshold for non-top-N carriers
        db_path: Path to database
        
    Returns:
        DataFrame with columns: the_date, winner, nat_z_score, impact, selected_window
    """
    if db_path is None:
        db_path = db.get_default_db_path()
    
    # Assert correct database path
    assert db_path.endswith('data/databases/duck_suppression.db'), \
        f"ERROR: Wrong database path: {db_path}. Must use data/databases/duck_suppression.db"
    
    # Get top N carriers (with optional share filter)
    top_carriers = get_top_n_carriers(ds, mover_ind, top_n, min_share_pct, db_path)
    top_carriers_str = ','.join([f"'{c}'" for c in top_carriers])
    
    cube_table = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"
    
    # National aggregation with tiered rolling windows
    # Key insight: Calculate rolling metrics over ENTIRE series, then filter to window at the end
    sql = f"""
        WITH national_daily AS (
            SELECT 
                the_date,
                DAYOFWEEK(the_date) as dow,  -- 1=Sunday, 7=Saturday
                winner,
                SUM(total_wins) as nat_total_wins,
                SUM(SUM(total_wins)) OVER (PARTITION BY the_date) as nat_market_wins
            FROM {cube_table}
            GROUP BY the_date, winner
        ),
        -- Self-join approach for DOW-partitioned rolling windows
        with_history AS (
            SELECT 
                curr.the_date,
                curr.dow,
                curr.winner,
                curr.nat_total_wins,
                curr.nat_market_wins,
                hist.the_date as hist_date,
                hist.nat_total_wins as hist_wins,
                DATEDIFF('day', hist.the_date, curr.the_date) as days_back
            FROM national_daily curr
            LEFT JOIN national_daily hist
                ON curr.winner = hist.winner
                AND curr.dow = hist.dow
                AND hist.the_date < curr.the_date
        ),
        -- Calculate 28d, 14d, and 4d rolling metrics
        with_rolling AS (
            SELECT 
                the_date,
                dow,
                winner,
                nat_total_wins,
                nat_market_wins,
                
                -- 28-day window metrics
                AVG(CASE WHEN days_back <= 28 THEN hist_wins END) as avg_28d,
                STDDEV(CASE WHEN days_back <= 28 THEN hist_wins END) as std_28d,
                COUNT(CASE WHEN days_back <= 28 THEN 1 END) as n_28d,
                
                -- 14-day window metrics
                AVG(CASE WHEN days_back <= 14 THEN hist_wins END) as avg_14d,
                STDDEV(CASE WHEN days_back <= 14 THEN hist_wins END) as std_14d,
                COUNT(CASE WHEN days_back <= 14 THEN 1 END) as n_14d,
                
                -- 4-day window metrics
                AVG(CASE WHEN days_back <= 4 THEN hist_wins END) as avg_4d,
                STDDEV(CASE WHEN days_back <= 4 THEN hist_wins END) as std_4d,
                COUNT(CASE WHEN days_back <= 4 THEN 1 END) as n_4d
            FROM with_history
            GROUP BY the_date, dow, winner, nat_total_wins, nat_market_wins
        ),
        -- Tiered selection: Choose best available window based on DOW and sample count
        tiered_metrics AS (
            SELECT 
                the_date,
                dow,
                winner,
                nat_total_wins,
                nat_market_wins,
                -- Weekday needs 4+ samples, weekend needs 2+
                CASE 
                    -- Try 28d first
                    WHEN dow IN (1, 7) AND n_28d >= 2 THEN avg_28d  -- Weekend
                    WHEN dow NOT IN (1, 7) AND n_28d >= 4 THEN avg_28d  -- Weekday
                    -- Fall back to 14d
                    WHEN dow IN (1, 7) AND n_14d >= 2 THEN avg_14d
                    WHEN dow NOT IN (1, 7) AND n_14d >= 4 THEN avg_14d
                    -- Fall back to 4d
                    WHEN dow IN (1, 7) AND n_4d >= 2 THEN avg_4d
                    WHEN dow NOT IN (1, 7) AND n_4d >= 4 THEN avg_4d
                    ELSE NULL
                END as nat_mu_wins,
                CASE 
                    WHEN dow IN (1, 7) AND n_28d >= 2 THEN std_28d
                    WHEN dow NOT IN (1, 7) AND n_28d >= 4 THEN std_28d
                    WHEN dow IN (1, 7) AND n_14d >= 2 THEN std_14d
                    WHEN dow NOT IN (1, 7) AND n_14d >= 4 THEN std_14d
                    WHEN dow IN (1, 7) AND n_4d >= 2 THEN std_4d
                    WHEN dow NOT IN (1, 7) AND n_4d >= 4 THEN std_4d
                    ELSE NULL
                END as nat_sigma_wins,
                CASE 
                    WHEN dow IN (1, 7) AND n_28d >= 2 THEN n_28d
                    WHEN dow NOT IN (1, 7) AND n_28d >= 4 THEN n_28d
                    WHEN dow IN (1, 7) AND n_14d >= 2 THEN n_14d
                    WHEN dow NOT IN (1, 7) AND n_14d >= 4 THEN n_14d
                    WHEN dow IN (1, 7) AND n_4d >= 2 THEN n_4d
                    WHEN dow NOT IN (1, 7) AND n_4d >= 4 THEN n_4d
                    ELSE NULL
                END as n_periods,
                CASE 
                    WHEN dow IN (1, 7) AND n_28d >= 2 THEN 28
                    WHEN dow NOT IN (1, 7) AND n_28d >= 4 THEN 28
                    WHEN dow IN (1, 7) AND n_14d >= 2 THEN 14
                    WHEN dow NOT IN (1, 7) AND n_14d >= 4 THEN 14
                    WHEN dow IN (1, 7) AND n_4d >= 2 THEN 4
                    WHEN dow NOT IN (1, 7) AND n_4d >= 4 THEN 4
                    ELSE NULL
                END as selected_window
            FROM with_rolling
        ),
        with_zscore AS (
            SELECT 
                the_date,
                dow,
                winner,
                nat_total_wins,
                nat_market_wins,
                nat_mu_wins,
                nat_sigma_wins,
                n_periods,
                selected_window,
                nat_total_wins * 1.0 / NULLIF(nat_market_wins, 0) as nat_share_current,
                CASE 
                    WHEN nat_sigma_wins > 0 AND nat_sigma_wins IS NOT NULL 
                         AND nat_mu_wins IS NOT NULL AND NOT isnan(nat_mu_wins) 
                         AND NOT isnan(nat_sigma_wins) THEN 
                        (nat_total_wins - nat_mu_wins) / NULLIF(nat_sigma_wins, 0)
                    ELSE 0 
                END as nat_z_score,
                CASE 
                    WHEN nat_mu_wins IS NOT NULL AND NOT isnan(nat_mu_wins) THEN 
                        CAST(ROUND(nat_total_wins - nat_mu_wins) AS INTEGER)
                    ELSE 0
                END as impact
            FROM tiered_metrics
        )
        SELECT 
            the_date,
            winner,
            nat_z_score,
            impact,
            nat_total_wins,
            nat_mu_wins,
            nat_share_current,
            n_periods,
            selected_window,
            dow as day_of_week
        FROM with_zscore
        WHERE the_date BETWEEN '{start_date}' AND '{end_date}'  -- Filter to graph window at the end
            AND selected_window IS NOT NULL  -- Only include dates with valid rolling metrics
            AND (
                -- Top N carriers with z-score violations
                (winner IN ({top_carriers_str}) AND nat_z_score > {z_threshold})
                -- OR egregious outliers outside top N
                OR (winner NOT IN ({top_carriers_str}) AND impact > {egregious_threshold})
            )
        ORDER BY the_date, winner
    """
    return db.query(sql, db_path)


def build_enriched_cube(
    ds: str,
    mover_ind: bool,
    start_date: str,
    end_date: str,
    dma_z_threshold: float = 1.5,
    dma_pct_threshold: float = 30.0,
    rare_pair_impact_threshold: int = 15,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """Build enriched cube with all metrics needed for UI plan building.
    
    Returns pair-level data with national context for specified date range.
    Uses the pre-computed rolling views for efficiency.
    
    Args:
        ds: Dataset name
        mover_ind: True for movers, False for non-movers
        start_date: Start date for data
        end_date: End date for data
        dma_z_threshold: Z-score threshold for DMA-level outliers (default: 1.5)
        dma_pct_threshold: Percent change threshold for DMA-level outliers (default: 30.0)
        rare_pair_impact_threshold: Impact threshold for rare pairs (default: 15)
        db_path: Path to database
        
    Returns:
        DataFrame with all pair-level and national-level metrics
    """
    if db_path is None:
        db_path = db.get_default_db_path()
    
    # Assert correct database path
    assert db_path.endswith('data/databases/duck_suppression.db'), \
        f"ERROR: Wrong database path: {db_path}. Must use data/databases/duck_suppression.db"
    
    rolling_view = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_rolling"
    
    sql = f"""
        WITH pair_level AS (
            -- Get pair-level data with rolling metrics from the entire time series
            SELECT 
                the_date,
                day_of_week,
                winner,
                loser,
                dma_name,
                state,
                total_wins as pair_wins_current,
                avg_wins as pair_mu_wins,
                stddev_wins as pair_sigma_wins,
                zscore as pair_z,
                pct_change as pair_pct_change,
                is_first_appearance as new_pair,
                is_outlier as pair_outlier_pos,
                CASE WHEN pct_change > {dma_pct_threshold} THEN true ELSE false END as pct_outlier_pos,
                -- Rare pairs: Only if they have z-score violations AND impact > rare_pair_impact_threshold
                CASE 
                    WHEN appearance_rank <= 5 
                        AND zscore > {dma_z_threshold}
                        AND avg_wins IS NOT NULL 
                        AND NOT isnan(avg_wins)
                        AND (total_wins - avg_wins) > {rare_pair_impact_threshold}
                    THEN true 
                    ELSE false 
                END as rare_pair,
                n_periods as pair_mu_window,
                selected_window,
                -- Pair-level impact (for DMA-level suppression)
                CASE 
                    WHEN avg_wins IS NOT NULL AND NOT isnan(avg_wins) THEN 
                        CAST(ROUND(total_wins - avg_wins) AS INTEGER)
                    ELSE CAST(total_wins AS INTEGER)  -- First appearances: all wins are "excess"
                END as pair_impact
            FROM {rolling_view}
            WHERE (selected_window IS NOT NULL OR is_first_appearance = true)  -- Include valid rolling metrics OR first appearances
                -- Note: No volume filter here - we need all pairs for carriers with national outliers
                -- Volume filtering happens in plan building (auto stage) and distributed stage
        ),
        -- National aggregation for context (aggregate across all DMAs)
        national_daily AS (
            SELECT
                the_date,
                day_of_week,
                winner,
                SUM(pair_wins_current) as nat_total_wins,
                SUM(pair_impact) as nat_total_impact
            FROM pair_level
            GROUP BY the_date, day_of_week, winner
        ),
        market_daily AS (
            SELECT
                the_date,
                SUM(nat_total_wins) as nat_market_wins
            FROM national_daily
            GROUP BY the_date
        ),
        national_with_share AS (
            SELECT 
                n.the_date,
                n.day_of_week,
                n.winner,
                n.nat_total_wins,
                n.nat_total_impact,
                m.nat_market_wins,
                n.nat_total_wins * 1.0 / NULLIF(m.nat_market_wins, 0) as nat_share_current
            FROM national_daily n
            JOIN market_daily m ON n.the_date = m.the_date
        ),
        -- National rolling metrics using DOW-partitioned windows across entire series
        national_rolling AS (
            SELECT 
                the_date,
                winner,
                nat_total_wins,
                nat_total_impact,
                nat_market_wins,
                nat_share_current,
                -- DOW-partitioned rolling metrics (14-day lookback = ~2 periods)
                AVG(nat_total_wins) OVER w as nat_mu_wins,
                STDDEV(nat_total_wins) OVER w as nat_sigma_wins,
                AVG(nat_share_current) OVER w as nat_mu_share,
                STDDEV(nat_share_current) OVER w as nat_sigma_share,
                COUNT(*) OVER w as nat_n_periods
            FROM national_with_share
            WINDOW w AS (
                PARTITION BY winner, day_of_week
                ORDER BY the_date
                ROWS BETWEEN 27 PRECEDING AND 1 PRECEDING  -- 28-day window
            )
        ),
        -- Join pair-level with national-level context
        enriched AS (
            SELECT 
                p.*,
                n.nat_total_wins,
                n.nat_market_wins,
                n.nat_share_current,
                n.nat_mu_wins,
                n.nat_sigma_wins,
                n.nat_mu_share,
                n.nat_sigma_share,
                -- National z-score
                CASE 
                    WHEN n.nat_sigma_wins > 0 AND n.nat_sigma_wins IS NOT NULL 
                         AND n.nat_mu_wins IS NOT NULL AND NOT isnan(n.nat_mu_wins) 
                         AND NOT isnan(n.nat_sigma_wins) THEN 
                        (n.nat_total_wins - n.nat_mu_wins) / NULLIF(n.nat_sigma_wins, 0)
                    ELSE 0 
                END as nat_z_score,
                -- National impact (sum of all pair impacts)
                n.nat_total_impact as impact
            FROM pair_level p
            JOIN national_rolling n ON p.the_date = n.the_date AND p.winner = n.winner
        )
        -- Filter to graph window at the end
        SELECT *
        FROM enriched
        WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY the_date, winner, dma_name, loser
    """
    return db.query(sql, db_path)

