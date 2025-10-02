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
DEFAULT_DB_PATH = os.path.join(os.getcwd(), "duck_suppression.db")

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
                    where_clauses.append(f"{col} = '{val.replace(\"'\", \"''\"")}'")
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
        filters.append(f"state = '{state.replace(\"'\", \"''\"")}'")
    if dma_name:
        filters.append(f"dma_name = '{dma_name.replace(\"'\", \"''\"")}'")
    
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
