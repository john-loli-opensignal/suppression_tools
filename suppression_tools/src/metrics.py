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


def national_timeseries(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str) -> pd.DataFrame:
    tmpl = _load_sql('national_timeseries.sql')
    sql = _render(tmpl, {
        'store_glob': store_glob,
        'ds': ds.replace("'", "''"),
        'mover_ind': 'TRUE' if str(mover_ind) == 'True' else 'FALSE',
        'start_date': start_date,
        'end_date': end_date,
    })
    con = _con()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def pair_metrics(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str) -> pd.DataFrame:
    tmpl = _load_sql('pair_metrics.sql')
    sql = _render(tmpl, {
        'store_glob': store_glob,
        'ds': ds.replace("'", "''"),
        'mover_ind': 'TRUE' if str(mover_ind) == 'True' else 'FALSE',
        'start_date': start_date,
        'end_date': end_date,
    })
    con = _con()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def competitor_view(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str, primary: str, competitors: Iterable[str]) -> pd.DataFrame:
    comps = ",".join([f"'{str(c).replace("'","''")}'" for c in competitors]) or "''"
    tmpl = _load_sql('competitor_view.sql')
    sql = _render(tmpl, {
        'store_glob': store_glob,
        'ds': ds.replace("'", "''"),
        'mover_ind': 'TRUE' if str(mover_ind) == 'True' else 'FALSE',
        'start_date': start_date,
        'end_date': end_date,
        'primary': primary.replace("'", "''"),
        'competitors': comps,
    })
    con = _con()
    try:
        return con.execute(sql).df()
    finally:
        con.close()

