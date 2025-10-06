"""Plan building and outlier detection using database cubes."""
from typing import List, Optional
import pandas as pd
import sys
sys.path.insert(0, '/home/jloli/codebase-comparison/suppression_tools')
import tools.db as db


def get_top_n_carriers(
    ds: str, 
    mover_ind: bool, 
    n: int = 50,
    db_path: Optional[str] = None
) -> List[str]:
    """Get top N carriers by total wins over entire time series.
    
    Args:
        ds: Dataset name (e.g., 'gamoshi')
        mover_ind: True for movers, False for non-movers
        n: Number of top carriers to return
        db_path: Path to database (uses default if None)
        
    Returns:
        List of carrier names
    """
    if db_path is None:
        db_path = db.get_db_path()
    
    # Assert correct database path
    assert db_path.endswith('data/databases/duck_suppression.db'), \
        f"ERROR: Wrong database path: {db_path}. Must use data/databases/duck_suppression.db"
    
    cube_table = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"
    
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
        db_path = db.get_db_path()
    
    # Assert correct database path
    assert db_path.endswith('data/databases/duck_suppression.db'), \
        f"ERROR: Wrong database path: {db_path}. Must use data/databases/duck_suppression.db"
    
    cube_table = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"
    
    # Build winner filter
    winners_str = ','.join([f"'{w}'" for w in winners]) if winners else "''"
    
    sql = f"""
        SELECT 
            the_date,
            winner,
            SUM(total_wins) as total_wins,
            SUM(SUM(total_wins)) OVER (PARTITION BY the_date) as market_total,
            SUM(total_wins) / NULLIF(SUM(SUM(total_wins)) OVER (PARTITION BY the_date), 0) as win_share
        FROM {cube_table}
        WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
            AND winner IN ({winners_str})
        GROUP BY the_date, winner
        ORDER BY the_date, winner
    """
    return db.query(sql, db_path)


def scan_base_outliers(
    ds: str,
    mover_ind: bool,
    start_date: str,
    end_date: str,
    z_threshold: float = 2.5,
    top_n: int = 50,
    egregious_threshold: int = 40,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """Scan for national-level outliers using rolling views.
    
    Focuses on top N carriers, but flags egregious outliers outside top N.
    
    Args:
        ds: Dataset name
        mover_ind: True for movers, False for non-movers
        start_date: Start date for scan window
        end_date: End date for scan window
        z_threshold: Z-score threshold for outlier detection
        top_n: Number of top carriers to focus on
        egregious_threshold: Impact threshold for non-top-N carriers
        db_path: Path to database
        
    Returns:
        DataFrame with columns: the_date, winner, nat_z_score, impact
    """
    if db_path is None:
        db_path = db.get_db_path()
    
    # Assert correct database path
    assert db_path.endswith('data/databases/duck_suppression.db'), \
        f"ERROR: Wrong database path: {db_path}. Must use data/databases/duck_suppression.db"
    
    # Get top N carriers
    top_carriers = get_top_n_carriers(ds, mover_ind, top_n, db_path)
    top_carriers_str = ','.join([f"'{c}'" for c in top_carriers])
    
    rolling_view = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_rolling"
    
    # National aggregation with outlier detection
    sql = f"""
        WITH national_daily AS (
            SELECT 
                the_date,
                winner,
                SUM(total_wins) as nat_total_wins,
                SUM(SUM(total_wins)) OVER (PARTITION BY the_date) as nat_market_wins
            FROM {rolling_view}
            WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY the_date, winner, day_of_week
        ),
        with_rolling AS (
            SELECT 
                the_date,
                winner,
                nat_total_wins,
                nat_market_wins,
                nat_total_wins / NULLIF(nat_market_wins, 0) as nat_share_current,
                -- DOW-partitioned rolling metrics
                AVG(nat_total_wins) OVER (
                    PARTITION BY winner, EXTRACT(DOW FROM the_date)
                    ORDER BY the_date 
                    ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                ) as nat_mu_wins,
                STDDEV(nat_total_wins) OVER (
                    PARTITION BY winner, EXTRACT(DOW FROM the_date)
                    ORDER BY the_date 
                    ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                ) as nat_sigma_wins
            FROM national_daily
        ),
        with_zscore AS (
            SELECT 
                the_date,
                winner,
                nat_total_wins,
                nat_mu_wins,
                nat_sigma_wins,
                CASE 
                    WHEN nat_sigma_wins > 0 THEN 
                        (nat_total_wins - nat_mu_wins) / nat_sigma_wins
                    ELSE 0 
                END as nat_z_score,
                CAST(nat_total_wins - nat_mu_wins AS INTEGER) as impact
            FROM with_rolling
        )
        SELECT 
            the_date,
            winner,
            nat_z_score,
            impact,
            nat_total_wins,
            nat_mu_wins
        FROM with_zscore
        WHERE (
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
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """Build enriched cube with all metrics needed for UI plan building.
    
    Returns pair-level data with national context for specified date range.
    
    Args:
        ds: Dataset name
        mover_ind: True for movers, False for non-movers
        start_date: Start date for data
        end_date: End date for data
        db_path: Path to database
        
    Returns:
        DataFrame with all pair-level and national-level metrics
    """
    if db_path is None:
        db_path = db.get_db_path()
    
    # Assert correct database path
    assert db_path.endswith('data/databases/duck_suppression.db'), \
        f"ERROR: Wrong database path: {db_path}. Must use data/databases/duck_suppression.db"
    
    rolling_view = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_rolling"
    
    sql = f"""
        WITH pair_level AS (
            SELECT 
                the_date,
                winner,
                loser,
                dma_name,
                state,
                total_wins as pair_wins_current,
                avg_wins_28d as pair_mu_wins,
                stddev_wins_28d as pair_sigma_wins,
                zscore as pair_z,
                pct_change as pair_pct_change,
                is_first_appearance as new_pair,
                is_outlier as pair_outlier_pos,
                CASE WHEN pct_change > 30 THEN true ELSE false END as pct_outlier_pos,
                CASE WHEN appearance_rank <= 5 THEN true ELSE false END as rare_pair,
                n_periods_28d as pair_mu_window
            FROM {rolling_view}
            WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
                AND total_wins >= 5  -- Minimum volume filter
        ),
        national_agg AS (
            SELECT
                the_date,
                winner,
                SUM(pair_wins_current) as nat_total_wins,
                SUM(SUM(pair_wins_current)) OVER (PARTITION BY the_date) as nat_market_wins,
                SUM(pair_wins_current) / NULLIF(
                    SUM(SUM(pair_wins_current)) OVER (PARTITION BY the_date), 0
                ) as nat_share_current
            FROM pair_level
            GROUP BY the_date, winner
        ),
        national_rolling AS (
            SELECT 
                the_date,
                winner,
                nat_total_wins,
                nat_market_wins,
                nat_share_current,
                -- DOW-partitioned rolling metrics
                AVG(nat_total_wins) OVER (
                    PARTITION BY winner, EXTRACT(DOW FROM the_date)
                    ORDER BY the_date
                    ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                ) as nat_mu_wins,
                STDDEV(nat_total_wins) OVER (
                    PARTITION BY winner, EXTRACT(DOW FROM the_date)
                    ORDER BY the_date
                    ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                ) as nat_sigma_wins,
                AVG(nat_share_current) OVER (
                    PARTITION BY winner, EXTRACT(DOW FROM the_date)
                    ORDER BY the_date
                    ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                ) as nat_mu_share,
                STDDEV(nat_share_current) OVER (
                    PARTITION BY winner, EXTRACT(DOW FROM the_date)
                    ORDER BY the_date
                    ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                ) as nat_sigma_share
            FROM national_agg
        )
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
                WHEN n.nat_sigma_wins > 0 THEN 
                    (n.nat_total_wins - n.nat_mu_wins) / n.nat_sigma_wins
                ELSE 0 
            END as nat_z_score,
            -- Impact (excess over baseline)
            CAST(n.nat_total_wins - n.nat_mu_wins AS INTEGER) as impact
        FROM pair_level p
        JOIN national_rolling n USING (the_date, winner)
        ORDER BY the_date, winner, dma_name, loser
    """
    return db.query(sql, db_path)

