"""Shared outlier helpers (scaffold).

National outlier days and cube outlier rows via unified SQL.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import duckdb
import pandas as pd

SQL_DIR = Path(__file__).resolve().parent.parent / 'sql'


def _load_sql(name: str) -> str:
    return (SQL_DIR / name).read_text()


def _render(tmpl: str, params: Dict[str, Any]) -> str:
    # Minimal formatter; assumes params cover all placeholders
    try:
        return tmpl.format(**params)
    except KeyError as e:
        raise ValueError(f"Missing placeholder in SQL template: {e}") from e


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


def national_outliers(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str, window: int = 14, z_thresh: float = 2.5, state: str | None = None, dma_name: str | None = None) -> pd.DataFrame:
    tmpl = _load_sql('nat_outliers.sql')
    extra_filters = _build_extra_filters(state, dma_name)
    sql = _render(tmpl, {
        'store_glob': store_glob,
        'ds': ds.replace("'", "''"),
        'mover_ind': 'TRUE' if str(mover_ind) == 'True' else 'FALSE',
        'start_date': start_date,
        'end_date': end_date,
        'window': int(window),
        'z_thresh': float(z_thresh),
        'extra_filters': extra_filters,
    })
    con = _con()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def cube_outliers(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str, window: int = 14, z_nat: float = 2.5, z_pair: float = 2.0, only_outliers: bool = True, state: str | None = None, dma_name: str | None = None) -> pd.DataFrame:
    tmpl = _load_sql('cube_outliers.sql')
    where_outlier = "WHERE ns.nat_outlier_pos" if only_outliers else ""
    extra_filters = _build_extra_filters(state, dma_name)
    sql = _render(tmpl, {
        'store_glob': store_glob,
        'ds': ds.replace("'", "''"),
        'mover_ind': 'TRUE' if str(mover_ind) == 'True' else 'FALSE',
        'start_date': start_date,
        'end_date': end_date,
        'window': int(window),
        'z_nat': float(z_nat),
        'z_pair': float(z_pair),
        'where_outlier': where_outlier,
        'extra_filters': extra_filters,
    })
    con = _con()
    try:
        return con.execute(sql).df()
    finally:
        con.close()
