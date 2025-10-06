"""Shared metrics helpers - Database/Cube based.

Uses DuckDB database and cube tables for fast queries.
No more parquet scanning!
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import duckdb
import pandas as pd

# Import from db module for connection management
from tools import db


def _build_extra_filters(state: str | None, dma_name: str | None) -> str:
    """Render optional state/dma filters, ignoring sentinel values like 'All'."""
    def _norm(v: str | None) -> str | None:
        if v is None:
            return None
        s = str(v).strip()
        if s == '' or s.lower() in ('all', 'none'):
            return None
        return s

    filters = []
    st = _norm(state)
    dm = _norm(dma_name)
    if st:
        filters.append(f"state = '{st.replace("'", "''")}'")
    if dm:
        filters.append(f"dma_name = '{dm.replace("'", "''")}'")
    return (" AND " + " AND ".join(filters)) if filters else ""


def national_timeseries(
    ds: str,
    mover_ind: bool | str,
    start_date: str,
    end_date: str,
    state: str | None = None,
    dma_name: str | None = None,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Get national daily timeseries from cube table.
    
    Returns win_share, loss_share, wins_per_loss for each carrier per day.
    Uses cube tables - much faster than parquet scanning!
    """
    # Normalize mover_ind
    if isinstance(mover_ind, str):
        mover_ind = (mover_ind == 'True')
    
    mover_str = "mover" if mover_ind else "non_mover"
    cube_table = f"{ds}_win_{mover_str}_cube"
    
    # Build filter clauses
    extra_filters = _build_extra_filters(state, dma_name)
    where_extra = f"AND {extra_filters}" if extra_filters else ""
    
    sql = f"""
    WITH daily_totals AS (
        SELECT 
            the_date,
            winner,
            SUM(total_wins) as carrier_wins
        FROM {cube_table}
        WHERE the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        {where_extra}
        GROUP BY the_date, winner
    ),
    market_totals AS (
        SELECT 
            the_date,
            SUM(carrier_wins) as market_wins
        FROM daily_totals
        GROUP BY the_date
    ),
    loss_cube AS (
        SELECT 
            the_date,
            winner,
            SUM(total_losses) as carrier_losses
        FROM {ds}_loss_{mover_str}_cube
        WHERE the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        {where_extra}
        GROUP BY the_date, winner
    ),
    market_losses AS (
        SELECT 
            the_date,
            SUM(carrier_losses) as market_losses
        FROM loss_cube
        GROUP BY the_date
    )
    SELECT 
        d.the_date,
        d.winner,
        d.carrier_wins as total_wins,
        l.carrier_losses as total_losses,
        d.carrier_wins / NULLIF(m.market_wins, 0) as win_share,
        l.carrier_losses / NULLIF(ml.market_losses, 0) as loss_share,
        d.carrier_wins / NULLIF(l.carrier_losses, 0) as wins_per_loss
    FROM daily_totals d
    JOIN market_totals m USING (the_date)
    LEFT JOIN loss_cube l USING (the_date, winner)
    LEFT JOIN market_losses ml USING (the_date)
    ORDER BY the_date, winner
    """
    
    return db.query(sql, db_path)


def pair_metrics(
    ds: str,
    mover_ind: bool | str,
    start_date: str,
    end_date: str,
    state: str | None = None,
    dma_name: str | None = None,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Get pair-level (winner-loser-DMA) daily metrics from cube table.
    
    Uses cube tables - much faster than parquet scanning!
    """
    # Normalize mover_ind
    if isinstance(mover_ind, str):
        mover_ind = (mover_ind == 'True')
    
    mover_str = "mover" if mover_ind else "non_mover"
    cube_table = f"{ds}_win_{mover_str}_cube"
    
    # Build filter clauses
    extra_filters = _build_extra_filters(state, dma_name)
    where_extra = f"AND {extra_filters}" if extra_filters else ""
    
    sql = f"""
    SELECT 
        the_date,
        winner,
        loser,
        dma_name,
        state,
        day_of_week,
        total_wins as pair_wins_current,
        CASE 
            WHEN day_of_week = 6 THEN 'Sat'
            WHEN day_of_week = 0 THEN 'Sun'
            ELSE 'Weekday'
        END as day_type
    FROM {cube_table}
    WHERE the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
      AND total_wins > 0
      {where_extra}
    ORDER BY the_date, winner, loser, dma_name
    """
    
    return db.query(sql, db_path)


def competitor_view(
    ds: str,
    mover_ind: bool | str,
    start_date: str,
    end_date: str,
    primary: str,
    competitors: Iterable[str],
    state: str | None = None,
    dma_name: str | None = None,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Get head-to-head competitor view from cube table.
    
    Shows primary carrier wins/losses vs each competitor.
    Uses cube tables - much faster than parquet scanning!
    """
    # Normalize mover_ind
    if isinstance(mover_ind, str):
        mover_ind = (mover_ind == 'True')
    
    mover_str = "mover" if mover_ind else "non_mover"
    win_cube = f"{ds}_win_{mover_str}_cube"
    loss_cube = f"{ds}_loss_{mover_str}_cube"
    
    # Build competitor list
    # Build competitor list - escape single quotes properly
    escaped_comps = [str(c).replace("'", "''") for c in competitors]
    comp_list = ','.join([f"'{c}'" for c in escaped_comps])
    if not comp_list:
        return pd.DataFrame()
    
    # Build filter clauses
    extra_filters = _build_extra_filters(state, dma_name)
    where_extra = f"AND {extra_filters}" if extra_filters else ""
    
    sql = f"""
    WITH h2h_wins AS (
        SELECT 
            the_date,
            loser as competitor,
            SUM(total_wins) as h2h_wins
        FROM {win_cube}
        WHERE winner = '{primary.replace("'", "''")}'
          AND loser IN ({comp_list})
          AND the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
          {where_extra}
        GROUP BY the_date, loser
    ),
    h2h_losses AS (
        SELECT 
            the_date,
            winner as competitor,
            SUM(total_losses) as h2h_losses
        FROM {loss_cube}
        WHERE loser = '{primary.replace("'", "''")}'
          AND winner IN ({comp_list})
          AND the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
          {where_extra}
        GROUP BY the_date, winner
    ),
    primary_totals_win AS (
        SELECT 
            the_date,
            SUM(total_wins) as primary_total_wins
        FROM {win_cube}
        WHERE winner = '{primary.replace("'", "''")}'
          AND the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
          {where_extra}
        GROUP BY the_date
    ),
    primary_totals_loss AS (
        SELECT 
            the_date,
            SUM(total_losses) as primary_total_losses
        FROM {loss_cube}
        WHERE loser = '{primary.replace("'", "''")}'
          AND the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
          {where_extra}
        GROUP BY the_date
    )
    SELECT 
        COALESCE(w.the_date, l.the_date) as the_date,
        COALESCE(w.competitor, l.competitor) as competitor,
        COALESCE(w.h2h_wins, 0) as h2h_wins,
        COALESCE(l.h2h_losses, 0) as h2h_losses,
        COALESCE(pw.primary_total_wins, 0) as primary_total_wins,
        COALESCE(pl.primary_total_losses, 0) as primary_total_losses
    FROM h2h_wins w
    FULL OUTER JOIN h2h_losses l ON w.the_date = l.the_date AND w.competitor = l.competitor
    LEFT JOIN primary_totals_win pw ON pw.the_date = COALESCE(w.the_date, l.the_date)
    LEFT JOIN primary_totals_loss pl ON pl.the_date = COALESCE(w.the_date, l.the_date)
    WHERE COALESCE(w.competitor, l.competitor) IS NOT NULL
      AND COALESCE(w.the_date, l.the_date) IS NOT NULL
    ORDER BY the_date, competitor
    """
    
    return db.query(sql, db_path)
