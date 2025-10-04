from typing import List

import duckdb
import pandas as pd


def _where_clause(ds: str, mover_ind: str, start_date: str, end_date: str) -> str:
    parts = []
    if ds:
        parts.append(f"ds = '{str(ds).replace("'","''")}'")
    if mover_ind in ("True", "False", True, False):
        b = 'TRUE' if str(mover_ind) == 'True' else 'FALSE'
        parts.append(f"mover_ind = {b}")
    if start_date and end_date:
        parts.append(f"CAST(the_date AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'")
    return (" WHERE " + " AND ".join(parts)) if parts else ""


def base_national_series(store_glob: str, ds: str, mover_ind: str, winners: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """Return time series of win_share per winner over time.

    Expects parquet files with columns: the_date, ds, mover_ind, winner, adjusted_wins, adjusted_losses.
    """
    con = duckdb.connect()
    try:
        where = _where_clause(ds, mover_ind, start_date, end_date)
        winners_list = ",".join([f"'{str(w).replace("'","''")}'" for w in (winners or [])]) if winners else None
        restrict = f" AND winner IN ({winners_list})" if (winners_list and where) else (f" WHERE winner IN ({winners_list})" if winners_list else "")
        q = f"""
        WITH ds AS (
            SELECT * FROM parquet_scan('{store_glob}')
        ), filt AS (
            SELECT * FROM ds {where} {restrict}
        ), market AS (
            SELECT the_date,
                   SUM(adjusted_wins) AS market_total_wins,
                   SUM(adjusted_losses) AS market_total_losses
            FROM filt
            GROUP BY 1
        ), selected AS (
            SELECT the_date, winner,
                   SUM(adjusted_wins) AS total_wins,
                   SUM(adjusted_losses) AS total_losses
            FROM filt
            GROUP BY 1,2
        )
        SELECT s.the_date, s.winner,
               s.total_wins / NULLIF(m.market_total_wins, 0) AS win_share
        FROM selected s
        JOIN market m USING (the_date)
        ORDER BY 1,2
        """
        df = con.execute(q).df()
        return df
    finally:
        con.close()


def scan_base_outliers(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str, window: int, z_thresh: float) -> pd.DataFrame:
    """Scan positive outliers by winner based on win_share z-score (DOW-partitioned).

    Returns columns: the_date, winner.
    """
    w = int(max(3, window))
    prev = w - 1
    con = duckdb.connect()
    try:
        where = _where_clause(ds, mover_ind, start_date, end_date)
        q = f"""
        WITH ds AS (
            SELECT * FROM parquet_scan('{store_glob}')
        ), filt AS (
            SELECT * FROM ds {where}
        ), market AS (
            SELECT the_date,
                   SUM(adjusted_wins) AS market_total_wins
            FROM filt
            GROUP BY 1
        ), selected AS (
            SELECT the_date, winner,
                   SUM(adjusted_wins) AS total_wins
            FROM filt
            GROUP BY 1,2
        ), metrics AS (
            SELECT s.the_date, s.winner,
                   s.total_wins / NULLIF(m.market_total_wins, 0) AS win_share
            FROM selected s
            JOIN market m USING (the_date)
        ), typed AS (
            SELECT *, CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                           WHEN strftime('%w', the_date)='0' THEN 'Sun'
                           ELSE 'Weekday' END AS day_type
            FROM metrics
        ), scored AS (
            SELECT the_date, winner,
                   CASE WHEN stddev_samp(win_share) OVER (
                            PARTITION BY winner, day_type
                            ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND 1 PRECEDING) > 0
                        THEN (win_share - avg(win_share) OVER (
                                PARTITION BY winner, day_type
                                ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND 1 PRECEDING))
                             / NULLIF(stddev_samp(win_share) OVER (
                                        PARTITION BY winner, day_type
                                        ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND 1 PRECEDING), 0)
                        ELSE 0 END AS z
            FROM typed
        )
        SELECT the_date, winner
        FROM scored
        WHERE z > {float(z_thresh)}
        ORDER BY 1,2
        """
        df = con.execute(q).df()
        return df
    finally:
        con.close()


def build_plan_for_winner_dates(*args, **kwargs) -> pd.DataFrame:
    """Placeholder for plan builder not used by main.py.

    Returns an empty DataFrame; main.py implements its own plan logic.
    """
    return pd.DataFrame(columns=['date', 'winner'])

