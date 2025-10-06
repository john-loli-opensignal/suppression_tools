"""
DuckDB database utilities for carrier suppression analysis.

Provides convenient access to the persistent duck_suppression.db database
with connection pooling, read-only access, and common query patterns.
"""
import os
import atexit
from typing import Optional
import duckdb
import pandas as pd


# Default database path
DEFAULT_DB_PATH = os.path.join(os.getcwd(), "data/databases/duck_suppression.db")

# CRITICAL: Only use this database path!
# DO NOT create databases in other locations (e.g., data/duck_suppression.db)
# This mistake has been made multiple times and causes confusion.
assert "data/databases/duck_suppression.db" in DEFAULT_DB_PATH, \
    "Database path must be data/databases/duck_suppression.db - NOT data/duck_suppression.db!"

# Track temporary database files for cleanup
_temp_files = []


def get_default_db_path() -> str:
    """Get the default database path"""
    return DEFAULT_DB_PATH


def connect(db_path: Optional[str] = None, read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """
    Connect to the suppression database.
    
    Args:
        db_path: Path to database file (default: ./duck_suppression.db)
        read_only: Whether to open in read-only mode (safer for queries)
        
    Returns:
        DuckDB connection object
        
    Example:
        con = connect()
        df = con.execute("SELECT * FROM carrier_data LIMIT 10").df()
        con.close()
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    
    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"Database not found: {db_path}\n"
            f"Run: uv run build_suppression_db.py <preagg_path> to create it"
        )
    
    return duckdb.connect(db_path, read_only=read_only)


def query(sql: str, db_path: Optional[str] = None, params: Optional[dict] = None) -> pd.DataFrame:
    """
    Execute a query and return results as a pandas DataFrame.
    
    Args:
        sql: SQL query string
        db_path: Path to database file (default: ./duck_suppression.db)
        params: Optional parameters for parameterized queries
        
    Returns:
        Query results as pandas DataFrame
        
    Example:
        df = query("SELECT DISTINCT winner FROM carrier_data ORDER BY winner")
        df = query("SELECT * FROM carrier_data WHERE ds = $ds LIMIT 10", 
                   params={'ds': 'gamoshi'})
    """
    con = connect(db_path, read_only=True)
    try:
        if params:
            result = con.execute(sql, params).df()
        else:
            result = con.execute(sql).df()
        return result
    finally:
        con.close()


def table_exists(table_name: str, db_path: Optional[str] = None) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        table_name: Name of the table to check
        db_path: Path to database file
        
    Returns:
        True if table exists, False otherwise
    """
    con = connect(db_path, read_only=True)
    try:
        result = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name]
        ).fetchone()
        return result[0] > 0
    finally:
        con.close()


def get_table_info(table_name: str = "carrier_data", db_path: Optional[str] = None) -> pd.DataFrame:
    """
    Get information about table columns and types.
    
    Args:
        table_name: Name of the table (default: carrier_data)
        db_path: Path to database file
        
    Returns:
        DataFrame with column information
    """
    con = connect(db_path, read_only=True)
    try:
        return con.execute(f"DESCRIBE {table_name}").df()
    finally:
        con.close()


def get_table_stats(table_name: str = "carrier_data", db_path: Optional[str] = None) -> dict:
    """
    Get statistics about a table.
    
    Args:
        table_name: Name of the table (default: carrier_data)
        db_path: Path to database file
        
    Returns:
        Dictionary with table statistics
    """
    con = connect(db_path, read_only=True)
    try:
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        date_range = con.execute(
            f"SELECT MIN(the_date) as min_date, MAX(the_date) as max_date FROM {table_name}"
        ).fetchone()
        
        distinct_counts = con.execute(f"""
            SELECT
                COUNT(DISTINCT ds) as ds_count,
                COUNT(DISTINCT winner) as winner_count,
                COUNT(DISTINCT loser) as loser_count,
                COUNT(DISTINCT dma_name) as dma_count,
                COUNT(DISTINCT state) as state_count
            FROM {table_name}
        """).fetchone()
        
        return {
            'row_count': row_count,
            'min_date': date_range[0],
            'max_date': date_range[1],
            'distinct_ds': distinct_counts[0],
            'distinct_winners': distinct_counts[1],
            'distinct_losers': distinct_counts[2],
            'distinct_dmas': distinct_counts[3],
            'distinct_states': distinct_counts[4],
        }
    finally:
        con.close()


def get_distinct_values(column: str, table_name: str = "carrier_data", 
                       db_path: Optional[str] = None, where: Optional[str] = None) -> list:
    """
    Get distinct values for a column.
    
    Args:
        column: Column name
        table_name: Name of the table (default: carrier_data)
        db_path: Path to database file
        where: Optional WHERE clause (without the WHERE keyword)
        
    Returns:
        List of distinct values
        
    Example:
        carriers = get_distinct_values('winner')
        dmas = get_distinct_values('dma_name', where="state = 'CA'")
    """
    con = connect(db_path, read_only=True)
    try:
        where_clause = f"WHERE {where}" if where else ""
        sql = f"SELECT DISTINCT {column} FROM {table_name} {where_clause} WHERE {column} IS NOT NULL ORDER BY {column}"
        result = con.execute(sql).df()
        return result[column].tolist()
    finally:
        con.close()


def create_temp_db(suffix: str = '.db', prefix: str = 'duck_temp_') -> str:
    """
    Create a temporary database file that will be auto-cleaned on exit.
    
    Args:
        suffix: File suffix (default: .db)
        prefix: File prefix (default: duck_temp_)
        
    Returns:
        Path to temporary database file
        
    Example:
        temp_db = create_temp_db()
        con = duckdb.connect(temp_db)
        # ... do work ...
        con.close()
        # File will be auto-cleaned on exit
    """
    import tempfile
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix)
    os.close(fd)
    _temp_files.append(path)
    return path


def _cleanup_temp_files():
    """Clean up all temporary database files"""
    for f in _temp_files:
        try:
            if os.path.exists(f):
                os.remove(f)
        except Exception:
            pass  # Silent cleanup


# Register cleanup on exit
atexit.register(_cleanup_temp_files)


# Convenience query functions for common patterns

def get_date_bounds(db_path: Optional[str] = None, filters: Optional[dict] = None) -> tuple:
    """
    Get min and max dates in the database.
    
    Args:
        db_path: Path to database file
        filters: Optional dict of filters (e.g., {'ds': 'gamoshi', 'mover_ind': False})
        
    Returns:
        Tuple of (min_date, max_date)
    """
    where_clauses = []
    if filters:
        for col, val in filters.items():
            if val is not None and val != "All":
                if isinstance(val, bool):
                    where_clauses.append(f"{col} = {val}")
                elif isinstance(val, str):
                    sval = str(val).replace("'", "''")
                    where_clauses.append(f"{col} = '{sval}'")
                else:
                    where_clauses.append(f"{col} = {val}")
    
    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    sql = f"SELECT MIN(the_date) AS min_date, MAX(the_date) AS max_date FROM carrier_data {where_clause}"
    
    result = query(sql, db_path)
    if result.empty:
        return (None, None)
    return (result['min_date'][0], result['max_date'][0])


def get_national_timeseries(
    ds: str = 'gamoshi',
    mover_ind: bool = False,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    state: Optional[str] = None,
    dma_name: Optional[str] = None,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Get national timeseries with win/loss shares.
    
    Args:
        ds: Data source
        mover_ind: Mover indicator
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        state: Optional state filter
        dma_name: Optional DMA filter
        db_path: Path to database file
        
    Returns:
        DataFrame with daily aggregates and shares
    """
    filters = [f"ds = '{ds}'", f"mover_ind = {mover_ind}"]
    
    if start_date:
        filters.append(f"the_date >= DATE '{start_date}'")
    if end_date:
        filters.append(f"the_date <= DATE '{end_date}'")
    if state:
        safe_state = state.replace("'", "''")
        filters.append(f"state = '{safe_state}'")
        filters.append(f"state = '{safe_state}'")
        safe_dma = dma_name.replace("'", "''")
        filters.append(f"dma_name = '{safe_dma}'")
        safe_dma = dma_name.replace("'", "''")
        filters.append(f"dma_name = '{safe_dma}'")
    
    where_clause = "WHERE " + " AND ".join(filters)
    
    sql = f"""
    WITH daily_totals AS (
        SELECT 
            the_date,
            winner,
            SUM(adjusted_wins) as adjusted_wins,
            SUM(adjusted_losses) as adjusted_losses
        FROM carrier_data
        {where_clause}
        GROUP BY the_date, winner
    ),
    market_totals AS (
        SELECT
            the_date,
            SUM(adjusted_wins) as total_market_wins,
            SUM(adjusted_losses) as total_market_losses
        FROM daily_totals
        GROUP BY the_date
    )
    SELECT
        d.the_date,
        d.winner,
        d.adjusted_wins,
        d.adjusted_losses,
        m.total_market_wins,
        m.total_market_losses,
        d.adjusted_wins / NULLIF(m.total_market_wins, 0) as win_share,
        d.adjusted_losses / NULLIF(m.total_market_losses, 0) as loss_share,
        d.adjusted_wins / NULLIF(d.adjusted_losses, 0) as wins_per_loss
    FROM daily_totals d
    JOIN market_totals m USING (the_date)
    ORDER BY the_date, winner
    """
    
    return query(sql, db_path)


def get_db_size(db_path: Optional[str] = None) -> dict:
    """
    Get database file size information.
    
    Args:
        db_path: Path to database file
        
    Returns:
        Dictionary with size information
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    
    if not os.path.exists(db_path):
        return {'exists': False}
    
    size_bytes = os.path.getsize(db_path)
    size_mb = size_bytes / (1024 * 1024)
    size_gb = size_bytes / (1024 * 1024 * 1024)
    
    return {
        'exists': True,
        'path': db_path,
        'size_bytes': size_bytes,
        'size_mb': size_mb,
        'size_gb': size_gb,
    }


# Cube table query helpers

def query_cube(
    ds: str,
    metric: str,  # 'win' or 'loss'
    mover_ind: bool,
    sql_filter: Optional[str] = None,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Query a cube table with optional filters.
    
    Args:
        ds: Dataset name (e.g., 'gamoshi')
        metric: 'win' or 'loss'
        mover_ind: True for movers, False for non-movers
        sql_filter: Optional WHERE clause (without WHERE keyword)
        db_path: Path to database file
        
    Returns:
        Query results as pandas DataFrame
        
    Example:
        # Get AT&T wins for non-movers in California
        df = query_cube('gamoshi', 'win', False, 
                       sql_filter="winner = 'AT&T' AND state = 'CA'")
    """
    mover_str = "mover" if mover_ind else "non_mover"
    table_name = f"{ds}_{metric}_{mover_str}_cube"
    
    where_clause = f"WHERE {sql_filter}" if sql_filter else ""
    sql = f"SELECT * FROM {table_name} {where_clause}"
    
    return query(sql, db_path)


def get_national_from_cube(
    ds: str,
    metric: str,  # 'win' or 'loss'
    mover_ind: bool,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Get national daily aggregates from cube table.
    
    Args:
        ds: Dataset name
        metric: 'win' or 'loss'
        mover_ind: True for movers, False for non-movers
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
        db_path: Path to database file
        
    Returns:
        DataFrame with national daily aggregates by carrier
    """
    mover_str = "mover" if mover_ind else "non_mover"
    table_name = f"{ds}_{metric}_{mover_str}_cube"
    
    filters = []
    if start_date:
        filters.append(f"the_date >= DATE '{start_date}'")
    if end_date:
        filters.append(f"the_date <= DATE '{end_date}'")
    
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    
    sql = f"""
    SELECT 
        the_date,
        winner,
        SUM(total_{metric}s) as total_{metric}s
    FROM {table_name}
    {where_clause}
    GROUP BY the_date, winner
    ORDER BY the_date, winner
    """
    
    return query(sql, db_path)


def list_cube_tables(db_path: Optional[str] = None) -> list[str]:
    """
    List all cube tables in the database.
    
    Args:
        db_path: Path to database file
        
    Returns:
        List of cube table names
    """
    con = connect(db_path, read_only=True)
    try:
        result = con.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name LIKE '%_cube'
            ORDER BY table_name
        """).fetchall()
        return [row[0] for row in result]
    finally:
        con.close()


# Cube-based outlier detection (50-200x faster than parquet scans!)

def national_outliers_from_cube(
    ds: str = 'gamoshi',
    mover_ind: bool = False,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    window: int = 14,
    z_thresh: float = 2.5,
    metric: str = 'share',
    cube_type: str = 'win',
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Detect national-level outliers using cube table.
    
    ~50-100x faster than scanning raw parquet files!
    
    Args:
        ds: Dataset name (e.g., 'gamoshi')
        mover_ind: True for movers, False for non-movers
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        window: Rolling window size in days (default: 14)
        z_thresh: Z-score threshold for outlier flagging (default: 2.5)
        metric: 'share' for win_share or 'volume' for nat_wins (default: 'share')
        cube_type: 'win' or 'loss' to select which cube table to use (default: 'win')
        db_path: Path to database file
        
    Returns:
        DataFrame with outlier flags and statistics
        
    Example:
        # Find outliers for non-movers in January
        outliers = national_outliers_from_cube(
            ds='gamoshi',
            mover_ind=False,
            start_date='2025-01-01',
            end_date='2025-01-31',
            window=14,
            z_thresh=2.5,
            metric='volume',  # Detect outliers in volume, not share
            cube_type='loss'  # Look at losses instead of wins
        )
    """
    mover_str = "mover" if mover_ind else "non_mover"
    cube_table = f"{ds}_{cube_type}_{mover_str}_cube"
    
    # Column names differ between win and loss cubes
    if cube_type == 'loss':
        total_col = 'total_losses'
        share_col = 'loss_share'
    else:
        total_col = 'total_wins'
        share_col = 'win_share'
    
    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND w.the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
    elif start_date:
        date_filter = f"AND w.the_date >= DATE '{start_date}'"
    elif end_date:
        date_filter = f"AND w.the_date <= DATE '{end_date}'"
    
    # Determine which column to use for z-score calculation
    if cube_type == 'loss':
        volume_col = 'nat_losses'
        metric_col = 'nat_losses' if metric == 'volume' else 'loss_share'
    else:
        volume_col = 'nat_wins'
        metric_col = 'nat_wins' if metric == 'volume' else 'win_share'
    
    # For loss cubes, use 'loser' instead of 'winner'
    entity_col = 'loser' if cube_type == 'loss' else 'winner'
    
    sql = f"""
    WITH daily_totals AS (
        SELECT 
            the_date,
            {entity_col},
            day_of_week,
            SUM({total_col}) as {volume_col}
        FROM {cube_table}
        GROUP BY the_date, {entity_col}, day_of_week
    ),
    market_totals AS (
        SELECT the_date, SUM({volume_col}) as market_total
        FROM daily_totals
        GROUP BY the_date
    ),
    with_share AS (
        SELECT 
            d.the_date,
            d.{entity_col} as entity,
            d.{volume_col},
            m.market_total,
            d.{volume_col} / NULLIF(m.market_total, 0) as {share_col},
            CASE 
                WHEN d.day_of_week = 6 THEN 'Sat'
                WHEN d.day_of_week = 0 THEN 'Sun'
                ELSE 'Weekday'
            END as day_type
        FROM daily_totals d
        JOIN market_totals m USING (the_date)
    ),
    with_stats AS (
        SELECT 
            the_date,
            entity,
            {volume_col},
            market_total,
            {share_col},
            day_type,
            AVG({metric_col}) OVER (
                PARTITION BY entity, day_type 
                ORDER BY the_date 
                ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING
            ) as mu,
            STDDEV_SAMP({metric_col}) OVER (
                PARTITION BY entity, day_type 
                ORDER BY the_date 
                ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING
            ) as sigma
        FROM with_share
    )
    SELECT 
        w.the_date,
        w.entity as {entity_col},
        w.{volume_col},
        w.market_total,
        w.{share_col},
        w.day_type,
        w.mu as baseline_{'share' if metric == 'share' else 'volume'},
        w.sigma,
        CASE 
            WHEN w.sigma > 0 THEN ABS({metric_col} - w.mu) / w.sigma
            ELSE 0
        END as zscore,
        CASE 
            WHEN w.sigma > 0 AND ABS({metric_col} - w.mu) / w.sigma > {z_thresh}
            THEN TRUE
            ELSE FALSE
        END as is_outlier
    FROM with_stats w
    WHERE 1=1 {date_filter}
    ORDER BY the_date, {entity_col}
    """
    
    result = query(sql, db_path)
    
    # Rename 'entity' back to 'winner' for consistency with existing code
    if entity_col == 'loser' and 'loser' in result.columns:
        result = result.rename(columns={'loser': 'winner'})
    
    return result


def pair_outliers_from_cube(
    ds: str = 'gamoshi',
    mover_ind: bool = False,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    window: int = 14,
    z_thresh: float = 2.0,
    only_outliers: bool = True,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Detect pair-level (winner-loser-DMA) outliers using cube table.
    
    Uses multiple detection strategies:
    - Z-score: (current - μ) / σ > threshold
    - Percentage spike: current > 1.3 × baseline
    - New pair: No historical data
    - Rare pair: Baseline < 2.0 wins/day
    
    Args:
        ds: Dataset name (e.g., 'gamoshi')
        mover_ind: True for movers, False for non-movers
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        window: Rolling window size in days (default: 14)
        z_thresh: Z-score threshold (default: 2.0)
        only_outliers: If True, only return outlier rows (default: True)
        db_path: Path to database file
        
    Returns:
        DataFrame with pair-level outlier flags and statistics
        
    Example:
        # Find all pair outliers for non-movers
        outliers = pair_outliers_from_cube(
            ds='gamoshi',
            mover_ind=False,
            start_date='2025-01-01',
            end_date='2025-01-31',
            only_outliers=True
        )
    """
    mover_str = "mover" if mover_ind else "non_mover"
    cube_table = f"{ds}_win_{mover_str}_cube"
    
    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
    elif start_date:
        date_filter = f"AND the_date >= DATE '{start_date}'"
    elif end_date:
        date_filter = f"AND the_date <= DATE '{end_date}'"
    
    outlier_filter = f"""
    AND (
        pair_zscore > {z_thresh} OR
        is_pct_spike = TRUE OR
        is_new_pair = TRUE OR
        is_rare_pair = TRUE
    )
    """ if only_outliers else ""
    
    sql = f"""
    WITH pair_daily AS (
        SELECT 
            the_date,
            winner,
            loser,
            dma_name,
            state,
            day_of_week,
            total_wins,
            CASE 
                WHEN day_of_week = 6 THEN 'Sat'
                WHEN day_of_week = 0 THEN 'Sun'
                ELSE 'Weekday'
            END as day_type
        FROM {cube_table}
    ),
    with_stats AS (
        SELECT 
            the_date,
            winner,
            loser,
            dma_name,
            state,
            total_wins,
            day_type,
            AVG(total_wins) OVER (
                PARTITION BY winner, loser, dma_name, day_type
                ORDER BY the_date
                ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING
            ) as mu,
            STDDEV_SAMP(total_wins) OVER (
                PARTITION BY winner, loser, dma_name, day_type
                ORDER BY the_date
                ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING
            ) as sigma,
            COUNT(*) OVER (
                PARTITION BY winner, loser, dma_name, day_type
                ORDER BY the_date
                ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING
            ) as hist_count
        FROM pair_daily
    )
    SELECT 
        the_date,
        winner,
        loser,
        dma_name,
        state,
        total_wins as pair_wins,
        day_type,
        mu as pair_baseline,
        sigma as pair_sigma,
        hist_count,
        CASE 
            WHEN sigma > 0 THEN (total_wins - mu) / sigma
            ELSE 0
        END as pair_zscore,
        CASE 
            WHEN mu IS NULL OR hist_count = 0 THEN TRUE
            ELSE FALSE
        END as is_new_pair,
        CASE 
            WHEN mu IS NOT NULL AND mu < 2.0 THEN TRUE
            ELSE FALSE
        END as is_rare_pair,
        CASE 
            WHEN mu IS NOT NULL AND total_wins > 1.3 * mu THEN TRUE
            ELSE FALSE
        END as is_pct_spike,
        CASE 
            WHEN (sigma > 0 AND (total_wins - mu) / sigma > {z_thresh})
                 OR (mu IS NOT NULL AND total_wins > 1.3 * mu)
                 OR (mu IS NULL OR hist_count = 0)
                 OR (mu IS NOT NULL AND mu < 2.0)
            THEN TRUE
            ELSE FALSE
        END as is_outlier
    FROM with_stats
    WHERE 1=1 {date_filter} {outlier_filter}
    ORDER BY the_date, winner, loser, dma_name
    """
    
    return query(sql, db_path)
