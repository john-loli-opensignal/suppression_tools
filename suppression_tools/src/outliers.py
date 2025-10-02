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
    return tmpl.format(**params)


def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def national_outliers(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str, window: int = 14, z_thresh: float = 2.5) -> pd.DataFrame:
    tmpl = _load_sql('nat_outliers.sql')
    sql = _render(tmpl, {
        'store_glob': store_glob,
        'ds': ds.replace("'", "''"),
        'mover_ind': 'TRUE' if str(mover_ind) == 'True' else 'FALSE',
        'start_date': start_date,
        'end_date': end_date,
        'window': int(window),
        'z_thresh': float(z_thresh),
    })
    con = _con()
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def cube_outliers(store_glob: str, ds: str, mover_ind: str, start_date: str, end_date: str, window: int = 14, z_nat: float = 2.5, z_pair: float = 2.0, only_outliers: bool = True) -> pd.DataFrame:
    tmpl = _load_sql('cube_outliers.sql')
    where_outlier = "WHERE ns.nat_outlier_pos" if only_outliers else ""
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
    })
    con = _con()
    try:
        return con.execute(sql).df()
    finally:
        con.close()

