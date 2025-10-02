"""Shared metrics helpers (scaffold).

Renders SQL templates in suppression_tools/sql and executes with DuckDB.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable

import duckdb
import pandas as pd

SQL_DIR = Path(__file__).resolve().parent.parent / 'sql'


def _load_sql(name: str) -> str:
    path = SQL_DIR / name
    return path.read_text()


def _render(template: str, params: Dict[str, Any]) -> str:
    # Minimal formatter; assumes params cover all placeholders
    return template.format(**params)


def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


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


def national_timeseries(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str, state: str | None = None, dma_name: str | None = None) -> pd.DataFrame:
    tmpl = _load_sql('national_timeseries.sql')
    extra_filters = _build_extra_filters(state, dma_name)
    sql = _render(tmpl, {
        'store_glob': store_glob,
        'ds': ds.replace("'", "''"),
        'mover_ind': 'TRUE' if str(mover_ind) == 'True' else 'FALSE',
        'start_date': start_date,
        'end_date': end_date,
        'extra_filters': extra_filters,
    })
    con = _con()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def pair_metrics(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str, state: str | None = None, dma_name: str | None = None) -> pd.DataFrame:
    tmpl = _load_sql('pair_metrics.sql')
    extra_filters = _build_extra_filters(state, dma_name)
    sql = _render(tmpl, {
        'store_glob': store_glob,
        'ds': ds.replace("'", "''"),
        'mover_ind': 'TRUE' if str(mover_ind) == 'True' else 'FALSE',
        'start_date': start_date,
        'end_date': end_date,
        'extra_filters': extra_filters,
    })
    con = _con()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def competitor_view(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str, primary: str, competitors: Iterable[str], state: str | None = None, dma_name: str | None = None) -> pd.DataFrame:
    comps = ",".join([f"'{str(c).replace("'","''")}'" for c in competitors]) or "''"
    tmpl = _load_sql('competitor_view.sql')
    extra_filters = _build_extra_filters(state, dma_name)
    sql = _render(tmpl, {
        'store_glob': store_glob,
        'ds': ds.replace("'", "''"),
        'mover_ind': 'TRUE' if str(mover_ind) == 'True' else 'FALSE',
        'start_date': start_date,
        'end_date': end_date,
        'primary': primary.replace("'", "''"),
        'competitors': comps,
        'extra_filters': extra_filters,
    })
    con = _con()
    try:
        return con.execute(sql).df()
    finally:
        con.close()
