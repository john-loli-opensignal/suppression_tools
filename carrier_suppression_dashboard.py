import os
import glob
from datetime import datetime

import duckdb
import math
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objs as go
import json
import importlib.util
from datetime import date


def get_default_store_dir() -> str:
    return os.path.join(os.getcwd(), "duckdb_partitioned_store")


def get_store_glob(store_dir: str) -> str:
    # Expand '~' and similar user-home shortcuts to be user-friendly
    store_dir = os.path.expanduser(store_dir) if isinstance(store_dir, str) else store_dir
    if os.path.isdir(store_dir):
        return os.path.join(store_dir, "**", "*.parquet")
    if store_dir.endswith(".parquet"):
        return store_dir
    return os.path.join(store_dir, "*.parquet")


def init_session_state():
    if 'analysis_mode' not in st.session_state:
        st.session_state.analysis_mode = "National"
    if 'metric' not in st.session_state:
        st.session_state.metric = "win_share"
    if 'selection_mode' not in st.session_state:
        st.session_state.selection_mode = "Top N Carriers"
    if 'top_n' not in st.session_state:
        st.session_state.top_n = 10
    if 'selected_carriers' not in st.session_state:
        st.session_state.selected_carriers = []
    if 'primary_carrier' not in st.session_state:
        st.session_state.primary_carrier = None
    if 'competitor_carrier' not in st.session_state:
        st.session_state.competitor_carrier = []
    if 'filters' not in st.session_state:
        st.session_state.filters = {}
    if 'palette' not in st.session_state:
        st.session_state.palette = 'Dark24'
    if 'smoothing' not in st.session_state:
        st.session_state.smoothing = True
    if 'show_markers' not in st.session_state:
        st.session_state.show_markers = False
    if 'stacked' not in st.session_state:
        st.session_state.stacked = False
    if 'show_other' not in st.session_state:
        st.session_state.show_other = True
    if 'last_pdf' not in st.session_state:
        st.session_state.last_pdf = None
    if 'applied_signature' not in st.session_state:
        st.session_state.applied_signature = None
    if 'supp_rows' not in st.session_state:
        st.session_state.supp_rows = []  # unsaved suppressions in-session
    if 'auto_sel_winner' not in st.session_state:
        st.session_state.auto_sel_winner = None
    if 'auto_sel_date' not in st.session_state:
        st.session_state.auto_sel_date = None
    if 'scan_out_tbl' not in st.session_state:
        st.session_state.scan_out_tbl = None
    if 'last_comp_tbl' not in st.session_state:
        st.session_state.last_comp_tbl = None
    if 'last_dma_tbl' not in st.session_state:
        st.session_state.last_dma_tbl = None


def where_clause(filters: dict) -> str:
    clauses = []
    for col, val in (filters or {}).items():
        if val in (None, "All"):
            continue
        if col == 'mover_ind':
            to_bool = 'TRUE' if str(val) == 'True' else 'FALSE'
            clauses.append(f"{col} = {to_bool}")
        else:
            sval = str(val).replace("'", "''")
            clauses.append(f"{col} = '{sval}'")
    if clauses:
        return "WHERE " + " AND ".join(clauses)
    return ""


def load_suppressions(supp_dir: str) -> pd.DataFrame:
    if not supp_dir or not os.path.isdir(supp_dir):
        return pd.DataFrame(columns=['date','winner','loser','dma_name','mover_ind','remove_units'])
    files = sorted(glob.glob(os.path.join(supp_dir, "*.csv")))
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            frames.append(df)
        except Exception:
            pass
    if not frames:
        return pd.DataFrame(columns=['date','winner','loser','dma_name','mover_ind','remove_units'])
    out = pd.concat(frames, ignore_index=True)
    # Normalize columns
    for col in ['date','winner','loser','dma_name','mover_ind','remove_units']:
        if col not in out.columns:
            out[col] = None
    # Parse date
    # Accept either 'date' or legacy 'the_date'
    if 'date' not in out.columns and 'the_date' in out.columns:
        out['date'] = out['the_date']
    out['date'] = pd.to_datetime(out['date'], errors='coerce').dt.date
    # Keep only expected columns
    # Normalize mover_ind to boolean where possible
    if 'mover_ind' in out.columns:
        out['mover_ind'] = out['mover_ind'].apply(lambda v: True if str(v).strip().lower() in ('true','1') else (False if str(v).strip().lower() in ('false','0') else None))
    # Keep expected columns (including mover_ind if present)
    out = out[['date','winner','loser','dma_name','mover_ind','remove_units']]
    # Drop rows without required keys
    out = out.dropna(subset=['date','winner'])
    # Cast remove_units
    out['remove_units'] = pd.to_numeric(out['remove_units'], errors='coerce').fillna(0).astype(int)
    return out


def compute_national_pdf(ds_glob: str, filters: dict, selected_winners: list, show_other: bool, metric: str,
                         suppressions: pd.DataFrame | None, apply_supp: bool) -> pd.DataFrame:
    if not selected_winners:
        return pd.DataFrame(columns=["the_date", "winner", metric])
    con = duckdb.connect()
    try:
        where = where_clause(filters)
        winners_list = ",".join([f"'{str(w).replace("'","''")}'" for w in selected_winners])
        if apply_supp and suppressions is not None and not suppressions.empty:
            sup = suppressions.copy()
            sup['date'] = pd.to_datetime(sup['date'])
            con.register('sup_df', sup)
            join_adjust = """
            , sup AS (
                SELECT CAST(date AS DATE) AS d, winner, loser, dma_name, mover_ind, SUM(remove_units) AS remove_units
                FROM sup_df
                GROUP BY 1,2,3,4,5
            ), grp AS (
                SELECT CAST(the_date AS DATE) AS d, winner, loser, dma_name, mover_ind,
                       SUM(adjusted_wins) AS group_wins
                FROM filt
                GROUP BY 1,2,3,4,5
            ), joined AS (
                SELECT f.*, COALESCE(s.remove_units, 0) AS remove_units,
                       COALESCE(g.group_wins, 0) AS group_wins
                FROM filt f
                LEFT JOIN sup s
                  ON CAST(f.the_date AS DATE)=s.d AND f.winner=s.winner AND f.loser=s.loser AND f.dma_name=s.dma_name AND f.mover_ind = s.mover_ind
                LEFT JOIN grp g
                  ON CAST(f.the_date AS DATE)=g.d AND f.winner=g.winner AND f.loser=g.loser AND f.dma_name=g.dma_name AND f.mover_ind = g.mover_ind
            ), adj AS (
                SELECT the_date, winner, loser, dma_name, mover_ind,
                       GREATEST(0, adjusted_wins - (remove_units * (adjusted_wins / NULLIF(group_wins,0)))) AS adjusted_wins,
                       adjusted_losses
                FROM joined
            )
            """
            source_tbl = "adj"
        else:
            join_adjust = ""
            source_tbl = "filt"

        q = f"""
        WITH ds AS (
            SELECT * FROM parquet_scan('{ds_glob}')
        ), filt AS (
            SELECT the_date, winner, loser, dma_name, mover_ind, adjusted_wins, adjusted_losses FROM ds {where}
        )
        {join_adjust}
        , market AS (
            SELECT the_date,
                   SUM(adjusted_wins) AS market_total_wins,
                   SUM(adjusted_losses) AS market_total_losses
            FROM {source_tbl}
            GROUP BY 1
        ), selected AS (
            SELECT the_date, winner,
                   SUM(adjusted_wins) AS total_wins,
                   SUM(adjusted_losses) AS total_losses
            FROM {source_tbl}
            WHERE winner IN ({winners_list})
            GROUP BY 1,2
        ), selected_metrics AS (
            SELECT s.the_date, s.winner,
                   s.total_wins / NULLIF(m.market_total_wins, 0) AS win_share,
                   s.total_losses / NULLIF(m.market_total_losses, 0) AS loss_share,
                   s.total_wins / NULLIF(s.total_losses, 0) AS wins_per_loss
            FROM selected s
            JOIN market m USING (the_date)
        )
        SELECT the_date, winner, {metric} AS {metric}
        FROM selected_metrics
        ORDER BY 1,2
        """
        pdf = con.execute(q).df()
        return pdf
    finally:
        con.close()


def compute_suppression_summary(ds_glob: str, filters: dict, suppressions: pd.DataFrame) -> pd.DataFrame:
    """Build a running table: per date + (winner, loser), the base wins, removed, and after.
    Columns: the_date, carrier, competitor, w0, rm, w1.
    """
    if suppressions is None or suppressions.empty:
        return pd.DataFrame(columns=['the_date','carrier','competitor','w0','rm','w1'])
    con = duckdb.connect()
    try:
        where = where_clause(filters)
        # Aggregate suppressions by key
        sup = suppressions.copy()
        sup['date'] = pd.to_datetime(sup['date'])
        con.register('sup_df', sup)
        q = f"""
        WITH ds AS (SELECT * FROM parquet_scan('{ds_glob}') ),
        filt AS (
          SELECT CAST(the_date AS DATE) AS d, winner, loser, dma_name, mover_ind, adjusted_wins
          FROM ds {where}
        ),
        sup_agg AS (
          SELECT CAST(date AS DATE) AS d, winner, loser, mover_ind, SUM(remove_units) AS rm
          FROM sup_df
          GROUP BY 1,2,3,4
        ),
        grp AS (
          SELECT d, winner, loser, dma_name, mover_ind, SUM(adjusted_wins) AS group_wins
          FROM filt GROUP BY 1,2,3,4,5
        ),
        joined AS (
          SELECT f.*, COALESCE(s.rm,0) AS rm, COALESCE(g.group_wins,0) AS group_wins
          FROM filt f
          LEFT JOIN sup_agg s USING (d, winner, loser, mover_ind)
          LEFT JOIN grp g USING (d, winner, loser, dma_name, mover_ind)
        ),
        per_row AS (
          SELECT d, winner, loser, mover_ind,
                 adjusted_wins AS w0_row,
                 LEAST(adjusted_wins, COALESCE(rm,0) * (adjusted_wins / NULLIF(group_wins,0))) AS rm_row
          FROM joined
        ),
        summed AS (
          SELECT d, winner, loser, mover_ind,
                 SUM(w0_row) AS w0,
                 SUM(rm_row) AS rm
          FROM per_row
          GROUP BY 1,2,3,4
        ),
        -- National baseline (DOW-partitioned rolling mu for each winner)
        base_nat AS (
          SELECT CAST(the_date AS DATE) AS d, winner, adjusted_wins FROM ds {where}
        ),
        market AS (
          SELECT d, SUM(adjusted_wins) AS T FROM base_nat GROUP BY 1
        ),
        per_w AS (
          SELECT d, winner, SUM(adjusted_wins) AS W FROM base_nat GROUP BY 1,2
        ),
        nat AS (
          SELECT p.d, p.winner, p.W, m.T,
                 p.W / NULLIF(m.T,0) AS share,
                 CASE WHEN strftime('%w', p.d)='6' THEN 'Sat'
                      WHEN strftime('%w', p.d)='0' THEN 'Sun'
                      ELSE 'Weekday' END AS day_type
          FROM per_w p JOIN market m USING (d)
        ),
        nat_roll AS (
          SELECT d, winner, share, day_type,
                 COUNT(*) OVER (PARTITION BY winner, day_type ORDER BY d ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS c28,
                 AVG(share) OVER (PARTITION BY winner, day_type ORDER BY d ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS mu28,
                 COUNT(*) OVER (PARTITION BY winner, day_type ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS c14,
                 AVG(share) OVER (PARTITION BY winner, day_type ORDER BY d ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS mu14,
                 AVG(share) OVER (PARTITION BY winner, day_type ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS mu_all
          FROM nat
        ),
        nat_mu AS (
          SELECT d, winner,
                 CASE WHEN c28 >= 28 THEN 28 WHEN c14 >= 14 THEN 14 ELSE 0 END AS nat_mu_window,
                 CASE WHEN c28 >= 28 THEN mu28 WHEN c14 >= 14 THEN mu14 ELSE mu_all END AS nat_mu_share
          FROM nat_roll
        )
        SELECT s.d AS the_date,
               winner AS carrier,
               loser AS competitor,
               mover_ind,
               w0,
               rm,
               GREATEST(0, w0 - rm) AS w1,
               nm.nat_mu_window,
               nm.nat_mu_share
        FROM summed s
        LEFT JOIN nat_mu nm ON (nm.d = s.d AND nm.winner = s.winner)
        WHERE rm > 0
        ORDER BY 1 DESC, 2, 3
        """
        df = con.execute(q).df()
        return df
    finally:
        con.close()


def compute_pair_dma_details(ds_glob: str, filters: dict, suppressions: pd.DataFrame,
                             date_str: str, winner: str, loser: str, mover_ind: bool) -> pd.DataFrame:
    """Return DMA-level details for a selected (date, winner, loser, mover_ind):
    columns: dma_name, w0, rm, w1
    """
    if suppressions is None or suppressions.empty:
        return pd.DataFrame(columns=['dma_name','w0','rm','w1'])
    con = duckdb.connect()
    try:
        where = where_clause(filters)
        sup = suppressions.copy()
        sup['date'] = pd.to_datetime(sup['date'])
        con.register('sup_df', sup)
        d_q = str(pd.to_datetime(date_str).date())
        win_q = str(winner).replace("'","''")
        los_q = str(loser).replace("'","''")
        mi_q = 'TRUE' if mover_ind else 'FALSE'
        q = f"""
        WITH ds AS (SELECT * FROM parquet_scan('{ds_glob}') ),
        filt AS (
          SELECT CAST(the_date AS DATE) AS d, winner, loser, dma_name, mover_ind, adjusted_wins
          FROM ds {where}
        ),
        sup_agg AS (
          SELECT CAST(date AS DATE) AS d, winner, loser, mover_ind, dma_name, SUM(remove_units) AS rm
          FROM sup_df
          GROUP BY 1,2,3,4,5
        ),
        grp AS (
          SELECT d, winner, loser, mover_ind, dma_name, SUM(adjusted_wins) AS group_wins
          FROM filt GROUP BY 1,2,3,4,5
        ),
        joined AS (
          SELECT f.*, COALESCE(s.rm,0) AS rm, COALESCE(g.group_wins,0) AS group_wins
          FROM filt f
          LEFT JOIN sup_agg s USING (d, winner, loser, mover_ind, dma_name)
          LEFT JOIN grp g USING (d, winner, loser, mover_ind, dma_name)
        ),
        per_row AS (
          SELECT d, winner, loser, mover_ind, dma_name,
                 adjusted_wins AS w0_row,
                 LEAST(adjusted_wins, COALESCE(rm,0) * (adjusted_wins / NULLIF(group_wins,0))) AS rm_row
          FROM joined
        ),
        summed AS (
          SELECT d, winner, loser, mover_ind, dma_name,
                 SUM(w0_row) AS w0,
                 SUM(rm_row) AS rm
          FROM per_row
          GROUP BY 1,2,3,4,5
        ),
        -- Pair baseline per DMA at target date with DOW fallback
        daily AS (
          SELECT d, winner, mover_ind, loser, dma_name, SUM(adjusted_wins)::DOUBLE AS wins_day
          FROM filt GROUP BY 1,2,3,4,5
        ),
        base AS (
          SELECT d.d AS target_d, d.winner, d.mover_ind, d.loser, d.dma_name,
                 d.wins_day AS wins_current,
                 COUNT(*) FILTER (WHERE h.d < DATE '{d_q}' AND strftime('%w', h.d) = strftime('%w', DATE '{d_q}')) AS c_same,
                 AVG(h.wins_day) FILTER (WHERE h.d < DATE '{d_q}' AND strftime('%w', h.d) = strftime('%w', DATE '{d_q}')) AS mu_same,
                 STDDEV_SAMP(h.wins_day) FILTER (WHERE h.d < DATE '{d_q}' AND strftime('%w', h.d) = strftime('%w', DATE '{d_q}')) AS sd_same,
                 COUNT(*) FILTER (WHERE h.d < DATE '{d_q}') AS c_all,
                 AVG(h.wins_day) FILTER (WHERE h.d < DATE '{d_q}') AS mu_all,
                 STDDEV_SAMP(h.wins_day) FILTER (WHERE h.d < DATE '{d_q}') AS sd_all
          FROM daily d
          LEFT JOIN daily h USING (winner, mover_ind, loser, dma_name)
          WHERE d.d = DATE '{d_q}' AND d.winner = '{win_q}' AND d.loser = '{los_q}' AND d.mover_ind = {mi_q}
          GROUP BY 1,2,3,4,5,6
        ),
        picked AS (
          SELECT target_d AS d, winner, mover_ind, loser, dma_name,
                 wins_current AS pair_wins_current,
                 CASE WHEN c_same >= 28 THEN 28 WHEN c_same >= 14 THEN 14 ELSE 0 END AS pair_mu_window,
                 CASE WHEN c_same >= 28 THEN mu_same WHEN c_same >= 14 THEN mu_same ELSE mu_all END AS pair_mu_wins,
                 CASE WHEN c_same >= 28 THEN sd_same WHEN c_same >= 14 THEN sd_same ELSE sd_all END AS pair_sigma_wins
          FROM base
        )
        SELECT s.dma_name,
               CAST(s.w0 AS BIGINT) AS w0,
               CAST(s.rm AS BIGINT) AS rm,
               CAST(GREATEST(0, s.w0 - s.rm) AS BIGINT) AS w1,
               p.pair_wins_current,
               p.pair_mu_wins,
               p.pair_sigma_wins,
               p.pair_mu_window,
               CASE WHEN p.pair_sigma_wins > 0 THEN (p.pair_wins_current - p.pair_mu_wins) / p.pair_sigma_wins ELSE NULL END AS pair_z
        FROM summed s
        LEFT JOIN picked p USING (d, winner, mover_ind, loser, dma_name)
        WHERE s.d = DATE '{d_q}' AND s.winner = '{win_q}' AND s.loser = '{los_q}' AND s.mover_ind = {mi_q}
        ORDER BY rm DESC, w0 DESC, dma_name
        """
        df = con.execute(q).df()
        return df
    finally:
        con.close()


def compute_pair_qa_for_plan(ds_glob: str, filters: dict, plan_df: pd.DataFrame) -> pd.DataFrame:
    """Augment a suppression plan with pair-level QA columns using DuckDB windows.
    Expects plan_df columns: date, winner, mover_ind, loser, dma_name
    Adds: pair_wins_current, pair_mu_wins, pair_sigma_wins, pair_mu_window, pair_z
    """
    if plan_df is None or plan_df.empty:
        return plan_df
    con = duckdb.connect()
    try:
        where = where_clause(filters)
        tmp = plan_df[['date','winner','mover_ind','loser','dma_name']].drop_duplicates().copy()
        tmp['date'] = pd.to_datetime(tmp['date'])
        con.register('keys_df', tmp)
        q = f"""
        WITH ds AS (SELECT * FROM parquet_scan('{ds_glob}')),
        filt AS (
          SELECT CAST(the_date AS DATE) AS d, winner, mover_ind, loser, dma_name, adjusted_wins::DOUBLE AS wins
          FROM ds {where}
        ),
        daily AS (
          SELECT d, winner, mover_ind, loser, dma_name, SUM(wins) AS wins_day
          FROM filt GROUP BY 1,2,3,4,5
        ),
        joined AS (
          SELECT k.date AS d, k.winner, k.mover_ind, k.loser, k.dma_name, COALESCE(d.wins_day,0) AS wins_day
          FROM keys_df k LEFT JOIN daily d USING (d, winner, mover_ind, loser, dma_name)
        ),
        with_dow AS (
          SELECT j.*, CASE WHEN strftime('%w', j.d)='6' THEN 'Sat' WHEN strftime('%w', j.d)='0' THEN 'Sun' ELSE 'Weekday' END AS day_type
          FROM joined j
        ),
        hist AS (
          SELECT x.d AS target_d, x.winner, x.mover_ind, x.loser, x.dma_name, x.day_type, x.wins_day AS wins_current,
                 h.d AS h_d, h.wins_day AS h_wins,
                 CASE WHEN strftime('%w', h.d)='6' THEN 'Sat' WHEN strftime('%w', h.d)='0' THEN 'Sun' ELSE 'Weekday' END AS h_day_type
          FROM with_dow x
          JOIN daily h ON (h.winner=x.winner AND h.mover_ind=x.mover_ind AND h.loser=x.loser AND h.dma_name=x.dma_name AND h.d < x.d)
        ),
        same_dow AS (
          SELECT target_d, winner, mover_ind, loser, dma_name, wins_current,
                 COUNT(*) FILTER (WHERE h_day_type = day_type) AS c_same,
                 AVG(h_wins) FILTER (WHERE h_day_type = day_type) AS mu_same,
                 STDDEV_SAMP(h_wins) FILTER (WHERE h_day_type = day_type) AS sd_same,
                 COUNT(*) AS c_all,
                 AVG(h_wins) AS mu_all,
                 STDDEV_SAMP(h_wins) AS sd_all
          FROM hist
          GROUP BY 1,2,3,4,5,6
        ),
        pick AS (
          SELECT target_d AS date, winner, mover_ind, loser, dma_name, wins_current AS pair_wins_current,
                 CASE WHEN c_same >= 28 THEN 28 WHEN c_same >= 14 THEN 14 ELSE 0 END AS pair_mu_window,
                 CASE WHEN c_same >= 28 THEN mu_same WHEN c_same >= 14 THEN mu_same ELSE mu_all END AS pair_mu_wins,
                 CASE WHEN c_same >= 28 THEN sd_same WHEN c_same >= 14 THEN sd_same ELSE sd_all END AS pair_sigma_wins
          FROM same_dow
        )
        SELECT p.*,
               CASE WHEN pair_sigma_wins > 0 THEN (pair_wins_current - pair_mu_wins) / pair_sigma_wins ELSE NULL END AS pair_z
        FROM pick p
        """
        qa = con.execute(q).df()
        out = plan_df.merge(qa, on=['date','winner','mover_ind','loser','dma_name'], how='left')
        return out
    finally:
        con.close()


def compute_competitor_pdf(ds_glob: str, filters: dict, primary: str, competitors: list, metric: str,
                           suppressions: pd.DataFrame | None, apply_supp: bool) -> pd.DataFrame:
    if not primary or not competitors:
        return pd.DataFrame(columns=["the_date", "winner", metric])
    con = duckdb.connect()
    try:
        where = where_clause(filters)
        comps_list = ",".join([f"'{str(c).replace("'","''")}'" for c in competitors])
        primary_q = str(primary).replace("'", "''")
        if apply_supp and suppressions is not None and not suppressions.empty:
            sup = suppressions.copy(); sup['date'] = pd.to_datetime(sup['date']); con.register('sup_df', sup)
            join_adjust = """
            , sup AS (
                SELECT CAST(date AS DATE) AS d, winner, loser, dma_name, mover_ind, SUM(remove_units) AS remove_units
                FROM sup_df
                GROUP BY 1,2,3,4,5
            ), grp AS (
                SELECT CAST(the_date AS DATE) AS d, winner, loser, dma_name, mover_ind,
                       SUM(adjusted_wins) AS group_wins
                FROM filt
                GROUP BY 1,2,3,4,5
            ), joined AS (
                SELECT f.*, COALESCE(s.remove_units, 0) AS remove_units,
                       COALESCE(g.group_wins, 0) AS group_wins
                FROM filt f
                LEFT JOIN sup s
                  ON CAST(f.the_date AS DATE)=s.d AND f.winner=s.winner AND f.loser=s.loser AND f.dma_name=s.dma_name AND f.mover_ind = s.mover_ind
                LEFT JOIN grp g
                  ON CAST(f.the_date AS DATE)=g.d AND f.winner=g.winner AND f.loser=g.loser AND f.dma_name=g.dma_name AND f.mover_ind = g.mover_ind
            ), adj AS (
                SELECT the_date, winner, loser, dma_name, mover_ind,
                       GREATEST(0, adjusted_wins - (remove_units * (adjusted_wins / NULLIF(group_wins,0)))) AS adjusted_wins,
                       adjusted_losses
                FROM joined
            )
            """
            source_tbl = "adj"
        else:
            join_adjust = ""
            source_tbl = "filt"

        q = f"""
        WITH ds AS (
            SELECT * FROM parquet_scan('{ds_glob}')
        ), filt AS (
            SELECT the_date, winner, loser, dma_name, mover_ind, adjusted_wins, adjusted_losses FROM ds {where}
        )
        {join_adjust}
        , h2h AS (
            SELECT the_date, loser AS competitor,
                   SUM(adjusted_wins) AS h2h_wins,
                   SUM(adjusted_losses) AS h2h_losses
            FROM {source_tbl}
            WHERE winner = '{primary_q}' AND loser IN ({comps_list})
            GROUP BY 1,2
        ), primary_tot AS (
            SELECT the_date,
                   SUM(adjusted_wins) AS primary_total_wins,
                   SUM(adjusted_losses) AS primary_total_losses
            FROM {source_tbl}
            WHERE winner = '{primary_q}'
            GROUP BY 1
        )
        SELECT h2h.the_date AS the_date,
               h2h.competitor AS winner,
               h2h.h2h_wins / NULLIF(primary_tot.primary_total_wins, 0) AS win_share,
               h2h.h2h_losses / NULLIF(primary_tot.primary_total_losses, 0) AS loss_share,
               h2h.h2h_wins / NULLIF(h2h.h2h_losses, 0) AS wins_per_loss
        FROM h2h JOIN primary_tot USING (the_date)
        ORDER BY 1,2
        """
        pdf = con.execute(q).df()
        return pdf[['the_date','winner',metric]].sort_values(['the_date','winner'])
    finally:
        con.close()


def create_plot(pdf: pd.DataFrame, metric: str, analysis_mode="National", primary: str | None = None, label="Base") -> go.Figure:
    fig = go.Figure()
    if pdf is None or pdf.empty:
        return fig
    if not pd.api.types.is_datetime64_any_dtype(pdf['the_date']):
        pdf['the_date'] = pd.to_datetime(pdf['the_date'])
    carriers = sorted(pdf['winner'].unique())
    palette = px.colors.qualitative.Dark24
    color_map = {c: palette[i % len(palette)] for i, c in enumerate([c for c in carriers if c != 'Other'])}
    for i, carrier in enumerate(carriers):
        cdf = pdf[pdf['winner'] == carrier].sort_values('the_date')
        fig.add_trace(go.Scatter(
            x=cdf['the_date'], y=pd.to_numeric(cdf[metric], errors='coerce').fillna(0),
            mode='lines', name=f"{carrier} ({label})",
            line=dict(color=color_map.get(carrier, palette[i % len(palette)]), width=2, dash=None if label=='Base' else 'dash')
        ))
    title_base = metric.replace('_',' ').title()
    who = f" - H2H: {primary} vs Competitors" if analysis_mode=='Competitor' and primary else ''
    fig.update_layout(title=f"{title_base}{who}", xaxis_title='Date', yaxis_title=title_base)
    return fig


def compute_outliers_duckdb(ds_glob: str, filters: dict, winners: list, window: int = 14, z_thresh: float = 2.5) -> pd.DataFrame:
    if not winners:
        return pd.DataFrame(columns=['the_date','winner','day_type','zscore'])
    con = duckdb.connect()
    try:
        where = where_clause(filters)
        winners_list = ",".join([f"'{str(w).replace("'","''")}'" for w in winners])
        prev = max(1, int(window) - 1)
        q = f"""
        WITH ds AS (
          SELECT * FROM parquet_scan('{ds_glob}')
        ), filt AS (
          SELECT * FROM ds {where}
        ), market AS (
          SELECT the_date, SUM(adjusted_wins) AS market_total_wins FROM filt GROUP BY 1
        ), per_winner AS (
          SELECT f.the_date, f.winner, SUM(f.adjusted_wins) AS total_wins FROM filt f GROUP BY 1,2
        ), metrics AS (
          SELECT p.the_date,
                 p.winner,
                 CASE WHEN strftime('%w', p.the_date)='6' THEN 'Sat'
                      WHEN strftime('%w', p.the_date)='0' THEN 'Sun'
                      ELSE 'Weekday' END AS day_type,
                 p.total_wins / NULLIF(m.market_total_wins, 0) AS win_share
          FROM per_winner p JOIN market m USING (the_date)
          WHERE p.winner IN ({winners_list})
        ), zcalc AS (
          SELECT the_date, winner, day_type, win_share,
                 COUNT(*) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_count,
                 avg(win_share) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_mean,
                 stddev_samp(win_share) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_std
          FROM metrics
        )
        SELECT the_date, winner, day_type,
               CASE WHEN w_std > 0 THEN (win_share - w_mean) / w_std ELSE 0 END AS zscore
        FROM zcalc
        WHERE ((CASE WHEN day_type IN ('Sat','Sun') THEN w_count >= 10 ELSE w_count >= {int(window)} END))
          AND w_std > 0 AND ABS((win_share - w_mean) / w_std) > {float(z_thresh)}
        ORDER BY 1,2
        """
        return con.execute(q).df()
    finally:
        con.close()


def compute_ts_outliers(pdf: pd.DataFrame, window: int = 14, z_thresh: float = 2.5, positive_only: bool = False) -> pd.DataFrame:
    """Compute outliers on an in-memory time series DataFrame with columns: the_date, winner, win_share.
    Applies DOW partitioning and uses min_periods=10 for weekends else `window`.
    Returns: the_date, winner, day_type, zscore.
    """
    if pdf is None or pdf.empty:
        return pd.DataFrame(columns=['the_date','winner','day_type','zscore'])
    df = pdf.copy()
    if not pd.api.types.is_datetime64_any_dtype(df['the_date']):
        df['the_date'] = pd.to_datetime(df['the_date'])
    df['day_type'] = df['the_date'].dt.dayofweek.map(lambda x: 'Sat' if x==6 else ('Sun' if x==0 else 'Weekday'))
    out_rows = []
    for (winner, day_type), g in df.groupby(['winner','day_type']):
        g = g.sort_values('the_date')
        s = pd.to_numeric(g['win_share'], errors='coerce').fillna(0.0)
        minp = 10 if day_type in ('Sat','Sun') else int(window)
        mu = s.rolling(window=window, min_periods=minp).mean()
        sig = s.rolling(window=window, min_periods=minp).std(ddof=1)
        z = (s - mu) / sig
        if positive_only:
            mask = (sig > 0) & (z > float(z_thresh))
        else:
            mask = (sig > 0) & (z.abs() > float(z_thresh))
        if mask.any():
            m = g.loc[mask, ['the_date']].copy()
            m['winner'] = winner
            m['day_type'] = day_type
            m['zscore'] = z.loc[mask].values
            out_rows.append(m)
    return pd.concat(out_rows, ignore_index=True) if out_rows else pd.DataFrame(columns=['the_date','winner','day_type','zscore'])


def compute_national_outliers(ds_glob: str, filters: dict, winners: list, lookback_days: int, same_dow: bool, z_thresh: float) -> pd.DataFrame:
    if not winners:
        return pd.DataFrame(columns=['the_date','winner','share','mu','sigma','z'])
    con = duckdb.connect()
    try:
        where = where_clause(filters)
        winners_list = ",".join([f"'{str(w).replace("'","''")}'" for w in winners])
        q = f"""
        WITH ds AS (SELECT * FROM parquet_scan('{ds_glob}')),
        filt AS (
            SELECT CAST(the_date AS DATE) AS d, winner, adjusted_wins FROM ds {where}
        ), tot AS (
            SELECT d, SUM(adjusted_wins) AS T FROM filt GROUP BY 1
        ), win AS (
            SELECT d, winner, SUM(adjusted_wins) AS W FROM filt WHERE winner IN ({winners_list}) GROUP BY 1,2
        ), daily AS (
            SELECT w.d, w.winner, w.W, t.T, (w.W/NULLIF(t.T,0)) AS share FROM win w JOIN tot t USING(d)
        )
        SELECT * FROM daily ORDER BY 1,2
        """
        df = con.execute(q).df()
    finally:
        con.close()
    if df.empty:
        return df
    # Compute rolling/bounded baseline by lookback and DOW constraint
    out_rows = []
    for winner in sorted(set(df['winner'])):
        wdf = df[df['winner']==winner].copy()
        for d in sorted(set(wdf['d'])):
            end = pd.Timestamp(d)
            start = end - pd.Timedelta(days=int(lookback_days))
            base = wdf[(wdf['d']>=start) & (wdf['d']<end)].copy()
            if same_dow:
                base = base[pd.to_datetime(base['d']).dt.dayofweek == end.dayofweek]
            mu = base['share'].mean() if not base.empty else 0.0
            sigma = base['share'].std(ddof=1) if len(base)>=3 else 0.0
            s = float(wdf[wdf['d']==d]['share'].iloc[0])
            z = (s - mu) / sigma if sigma and sigma>0 else float('nan')
            if not pd.isna(z) and z >= z_thresh:
                out_rows.append({'the_date': pd.to_datetime(d), 'winner': winner, 'share': s, 'mu': mu, 'sigma': sigma, 'z': z})
    return pd.DataFrame(out_rows).sort_values(['the_date','winner'])


def main():
    st.set_page_config(page_title="Suppression Simulator (DuckDB)", page_icon="🧪", layout="wide")
    st.title("🧪 Suppression Simulator (DuckDB)")
    init_session_state()

    # Data source
    default_dir = get_default_store_dir()
    st.sidebar.header("📦 Data Source")
    store_dir = st.sidebar.text_input("Partitioned dataset directory", value=default_dir)
    ds_glob = get_store_glob(store_dir)
    if not glob.glob(ds_glob, recursive=True):
        st.warning("No parquet files found in the dataset directory.")

    # Filters
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔧 Filters")
    filter_columns = ['mover_ind', 'ds']
    for col in filter_columns:
        current = st.session_state.filters.get(col, 'All')
        if col == 'mover_ind':
            st.session_state.filters[col] = st.sidebar.selectbox("mover_ind", options=['All','True','False'], index=['All','True','False'].index(current if current in ['All','True','False'] else 'All'))
        else:
            st.session_state.filters[col] = st.sidebar.text_input(col, value=current if current!='All' else 'gamoshi')

    # Suppression controls
    st.sidebar.markdown("---")
    st.sidebar.subheader("🗜️ Suppressions")
    supp_dir = st.sidebar.text_input("Suppressions folder", value=os.path.expanduser('~/codebase-comparison/suppression_tools/suppressions'))
    apply_supp = st.sidebar.checkbox("Apply suppressions", value=True)
    os.makedirs(supp_dir, exist_ok=True)
    loaded = load_suppressions(supp_dir)
    pending_cnt = len(st.session_state.supp_rows) if isinstance(st.session_state.supp_rows, list) else 0
    st.sidebar.write(f"Loaded files: {len(glob.glob(os.path.join(supp_dir, '*.csv')))} | Rows: {len(loaded)} | Pending: {pending_cnt}")

    # Manual add + quick refresh
    if st.sidebar.button("🔄 Reload & Apply", help="Reload files from the folder and re-apply suppressions"):
        st.rerun()

    with st.sidebar.expander("Add suppression row"):
        d = st.date_input("date")
        winner = st.text_input("winner", value="AT&T")
        loser = st.text_input("loser (required)", value="Comcast")
        dma = st.text_input("dma_name (required)", value="")
        mi = st.selectbox("mover_ind", options=["False","True"], index=0)
        units = st.number_input("remove_units", min_value=1, step=1, value=10)
        if st.button("➕ Add to session"):
            st.session_state.supp_rows.append({'date': d, 'winner': winner, 'loser': loser, 'dma_name': dma, 'mover_ind': (True if mi=="True" else False), 'remove_units': int(units)})
        if st.session_state.supp_rows:
            st.caption("Pending (unsaved) rows:")
            st.dataframe(pd.DataFrame(st.session_state.supp_rows))
            if st.button("💾 Save pending rows to file"):
                ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                path = os.path.join(supp_dir, f"supp_{ts}.csv")
                pd.DataFrame(st.session_state.supp_rows).to_csv(path, index=False)
                st.session_state.supp_rows = []
                st.success(f"Saved {path}")
                st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.subheader("📈 Chart Options")
    analysis_mode = st.sidebar.selectbox("Analysis Mode", ["National", "Competitor"], index=0)
    metric = st.sidebar.selectbox("Metric", ["win_share","loss_share","wins_per_loss"], index=0)

    # Winners and competitors
    if analysis_mode == 'National':
        winners_text = st.sidebar.text_input(
            "Winners (comma-separated)",
            value="Spectrum, Comcast, T-Mobile FWA, AT&T, Verizon FWA, Frontier, Verizon, Cox Communications, Altice, CenturyLink"
        )
        winners = [w.strip() for w in winners_text.split(',') if w.strip()]
        primary=None; competitors=[]
    else:
        primary = st.sidebar.text_input("Primary winner", value="AT&T")
        competitors_text = st.sidebar.text_input("Competitors (comma-separated)", value="Comcast,Spectrum,Verizon,T-Mobile FWA")
        competitors = [c.strip() for c in competitors_text.split(',') if c.strip()]
        winners=[]

    # Compose combined suppressions (loaded + pending)
    sup_df = loaded.copy()
    if st.session_state.supp_rows:
        add = pd.DataFrame(st.session_state.supp_rows)
        add['date'] = pd.to_datetime(add['date']).dt.date
        sup_df = pd.concat([sup_df, add], ignore_index=True)

    # Compute base and suppressed series (plotting controlled below via toggle)
    filters = st.session_state.filters
    if analysis_mode == 'National':
        base_pdf = compute_national_pdf(ds_glob, filters, winners, show_other=False, metric=metric, suppressions=None, apply_supp=False)
        supp_pdf = compute_national_pdf(ds_glob, filters, winners, show_other=False, metric=metric, suppressions=sup_df, apply_supp=apply_supp)
    else:
        base_pdf = compute_competitor_pdf(ds_glob, filters, primary, competitors, metric, suppressions=None, apply_supp=False)
        supp_pdf = compute_competitor_pdf(ds_glob, filters, primary, competitors, metric, suppressions=sup_df, apply_supp=apply_supp)

    # Graph-level date filter (view-only)
    try:
        if base_pdf is not None and not base_pdf.empty:
            bd = pd.to_datetime(base_pdf['the_date'])
            min_d = bd.min().date()
            max_d = bd.max().date()
        else:
            # Fallback to a sane default window
            min_d = date(2025, 1, 1)
            max_d = date(2025, 12, 31)
    except Exception:
        min_d = date(2025, 1, 1)
        max_d = date(2025, 12, 31)

    st.sidebar.subheader("🗓️ Graph Window")
    view_start = st.sidebar.date_input("Start (view)", value=min_d, key='view_start')
    view_end = st.sidebar.date_input("End (view)", value=max_d, key='view_end')
    # Filter the series for plotting only
    def _clip(df):
        if df is None or df.empty:
            return df
        d2 = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(d2['the_date']):
            d2['the_date'] = pd.to_datetime(d2['the_date'])
        mask = (d2['the_date'] >= pd.Timestamp(view_start)) & (d2['the_date'] <= pd.Timestamp(view_end))
        return d2[mask]

    base_view = _clip(base_pdf)
    supp_view = _clip(supp_pdf)

    # Toggle how suppressed series is shown on the chart
    st.sidebar.markdown("---")
    st.sidebar.subheader("🧭 View Mode")
    replace_with_supp = st.sidebar.checkbox("Replace base with suppressed (no overlay)", value=False)
    scan_suppressed = st.sidebar.checkbox("Scan suppressed view for new outliers", value=False)

    # Rebuild plot according to toggle
    if analysis_mode == 'National':
        if apply_supp and replace_with_supp:
            fig = create_plot(supp_view, metric, analysis_mode, label="Suppressed")
            # In replace mode, show suppressed as solid lines
            for tr in fig.data:
                tr.line['dash'] = None
            # Overlay suppressed outliers as stars if enabled
            if scan_suppressed:
                try:
                    out_sup = compute_ts_outliers(supp_pdf, window=14, z_thresh=2.5, positive_only=True)
                    if not out_sup.empty:
                        out_sup_view = out_sup[(out_sup['the_date'] >= pd.Timestamp(view_start)) & (out_sup['the_date'] <= pd.Timestamp(view_end))]
                        for w in sorted(out_sup_view['winner'].unique()):
                            ow = out_sup_view[out_sup_view['winner']==w]
                            # y from suppressed view
                            ys = supp_view[supp_view['winner']==w][['the_date','win_share']].rename(columns={'win_share':'y'})
                            m = ow.merge(ys, on='the_date', how='left').dropna(subset=['y'])
                            if not m.empty:
                                fig.add_trace(go.Scatter(
                                    x=m['the_date'], y=m['y'], mode='markers', name=f"{w} outlier (+/-)",
                                    marker=dict(symbol='star', color='yellow', size=10, line=dict(color='black', width=0.6)),
                                    showlegend=False,
                                ))
                except Exception:
                    pass
        else:
            fig = create_plot(base_view, metric, analysis_mode, label="Base")
            if apply_supp:
                f2 = create_plot(supp_view, metric, analysis_mode, label="Suppressed")
                # make suppressed dashed
                for tr in f2.data:
                    tr.line['dash'] = 'dash'
                    fig.add_trace(tr)
        fig.update_layout(width=1400, height=820, margin=dict(l=40, r=40, t=60, b=40))
        st.plotly_chart(fig, use_container_width=False)
    else:
        if apply_supp and replace_with_supp:
            fig = create_plot(supp_view, metric, 'Competitor', primary, label="Suppressed")
            # In replace mode, show suppressed as solid lines
            for tr in fig.data:
                tr.line['dash'] = None
            # Overlay suppressed outliers as stars if enabled
            if scan_suppressed:
                try:
                    targets = [primary] + competitors if primary else []
                    if targets:
                        # Use in-memory detection on supp_pdf filtered to targets
                        supp_targets = supp_pdf[supp_pdf['winner'].isin(targets)] if supp_pdf is not None else None
                        out_sup = compute_ts_outliers(supp_targets, window=14, z_thresh=2.5, positive_only=True) if supp_targets is not None else pd.DataFrame()
                        if not out_sup.empty:
                            out_sup_view = out_sup[(out_sup['the_date'] >= pd.Timestamp(view_start)) & (out_sup['the_date'] <= pd.Timestamp(view_end))]
                            for w in sorted(out_sup_view['winner'].unique()):
                                ow = out_sup_view[out_sup_view['winner']==w]
                                ys = supp_view[supp_view['winner']==w][['the_date','win_share']].rename(columns={'win_share':'y'})
                                m = ow.merge(ys, on='the_date', how='left').dropna(subset=['y'])
                                if not m.empty:
                                    fig.add_trace(go.Scatter(
                                        x=m['the_date'], y=m['y'], mode='markers', name=f"{w} outlier (+/-)",
                                        marker=dict(symbol='star', color='yellow', size=10, line=dict(color='black', width=0.6)),
                                        showlegend=False,
                                    ))
                except Exception:
                    pass
        else:
            fig = create_plot(base_view, metric, 'Competitor', primary, label="Base")
            if apply_supp:
                f2 = create_plot(supp_view, metric, 'Competitor', primary, label="Suppressed")
                for tr in f2.data:
                    tr.line['dash'] = 'dash'
                    fig.add_trace(tr)
        fig.update_layout(width=1400, height=820, margin=dict(l=40, r=40, t=60, b=40))
        st.plotly_chart(fig, use_container_width=False)

    # Before graph (Carrier-style) with star outliers, synced to view
    try:
        st.subheader("Before (Carrier-style with outliers)")
        fig_before = create_plot(base_view, metric, analysis_mode, primary if analysis_mode=='Competitor' else None, label="Base")
        out_tbl = compute_outliers_duckdb(ds_glob, filters, winners if analysis_mode=='National' else [primary] + competitors if primary else winners, window=14, z_thresh=2.5)
        if out_tbl is not None and not out_tbl.empty:
            out_view = out_tbl[(out_tbl['the_date'] >= pd.Timestamp(view_start)) & (out_tbl['the_date'] <= pd.Timestamp(view_end))].copy()
            for w in sorted(out_view['winner'].unique()):
                ow = out_view[out_view['winner']==w]
                # find y from base_view
                ys = base_view[base_view['winner']==w][['the_date','win_share']].rename(columns={'win_share':'y'})
                m = ow.merge(ys, on='the_date', how='left').dropna(subset=['y'])
                if not m.empty:
                    fig_before.add_trace(go.Scatter(
                        x=m['the_date'], y=m['y'], mode='markers', name=f"{w} outlier (+/-)",
                        marker=dict(symbol='star', color='yellow', size=10, line=dict(color='black', width=0.6)),
                        showlegend=False,
                    ))
        # Match main chart size and style
        fig_before.update_layout(width=1400, height=820, margin=dict(l=40, r=40, t=60, b=40))
        st.plotly_chart(fig_before, use_container_width=False)
    except Exception:
        pass

    # Optional: scan suppressed view for new outliers in the view window
    if scan_suppressed and apply_supp and supp_view is not None and not supp_view.empty:
        try:
            st.subheader("Outliers after suppression (view window)")
            # Compute outliers over full suppressed series to preserve correct rolling history,
            # then filter to the current view window and selection.
            out_after_full = compute_ts_outliers(supp_pdf, window=14, z_thresh=2.5, positive_only=True)
            out_after = out_after_full[(out_after_full['the_date'] >= pd.Timestamp(view_start)) & (out_after_full['the_date'] <= pd.Timestamp(view_end))].copy()
            # Limit to currently selected entities
            if analysis_mode == 'National':
                sel = set(winners)
                out_after = out_after[out_after['winner'].isin(sel)]
            else:
                sel = set(([primary] if primary else []) + (competitors or []))
                out_after = out_after[out_after['winner'].isin(sel)]
            if not out_after.empty:
                out_after = out_after.sort_values(['the_date','winner'])
                st.dataframe(out_after)
                # stash for round-2 export
                st.session_state['out_after'] = out_after.copy()
                # Inline export control for Round 2
                st.markdown("---")
                st.subheader("➡ Export Round from above outliers")
                r2_name_inline = st.text_input("Round name", value="round2_from_view_inline")
                r2_copy_inline = st.checkbox("Copy Round plan into suppressions folder", value=True, key='r2_copy_inline')
                if st.button(f"{r2_name_inline} plan"):
                    try:
                        spec = importlib.util.spec_from_file_location('duckdb_suppression_planner', os.path.join(os.getcwd(), 'tools', 'duckdb_suppression_planner.py'))
                        mod = importlib.util.module_from_spec(spec)
                        assert spec and spec.loader
                        spec.loader.exec_module(mod)
                        # Winners→dates mapping
                        plan_parts = []
                        dates_by_winner = {}
                        winners_unique = sorted(out_after['winner'].unique())
                        prog = st.progress(0, text="Preparing Round 2...")
                        total = len(winners_unique)
                        done = 0
                        for w in winners_unique:
                            dts = sorted(pd.to_datetime(out_after[out_after['winner']==w]['the_date']).dt.date.unique())
                            dts = [str(d) for d in dts]
                            if dts:
                                with st.spinner(f"Planning {w} for {len(dts)} date(s)..."):
                                    part = mod.run(ds_glob, dts, ds=st.session_state.filters.get('ds','gamoshi'), mover_ind=st.session_state.filters.get('mover_ind','False'), winner=w, out_csv=None)
                                plan_parts.append(part)
                                dates_by_winner[w] = dts
                            done += 1
                            prog.progress(min(100, int(done/total*100)))
                        import pandas as _pd
                        if plan_parts:
                            r2_dir = os.path.expanduser(os.path.join('~/codebase-comparison/suppression_tools/suppressions/rounds', r2_name_inline))
                            os.makedirs(r2_dir, exist_ok=True)
                            out_csv = os.path.join(r2_dir, 'plan.csv')
                            r2_df = _pd.concat(plan_parts, ignore_index=True)
                            r2_df.to_csv(out_csv, index=False)
                            # Save config for this round using explicit mapping
                            cfg = {
                                'store_glob': ds_glob,
                                'ds': st.session_state.filters.get('ds','gamoshi'),
                                'mover_ind': st.session_state.filters.get('mover_ind','False'),
                                'mode': 'from_outliers',
                                'winners': sorted(list(dates_by_winner.keys())),
                                'dates_by_winner': dates_by_winner,
                                'view_window': {'start': str(view_start), 'end': str(view_end)}
                            }
                            with open(os.path.join(r2_dir, 'config.json'), 'w') as f:
                                json.dump(cfg, f, indent=2)
                            # Preselect this round in loader
                            st.session_state['round_select'] = r2_name_inline
                            st.success(f"Wrote Round 2 plan: {out_csv}")
                            if r2_copy_inline:
                                main_copy = os.path.expanduser('~/codebase-comparison/suppression_tools/suppressions')
                                os.makedirs(main_copy, exist_ok=True)
                                copy_path = os.path.join(main_copy, f'{r2_name_inline}.csv')
                                r2_df.to_csv(copy_path, index=False)
                                st.info(f"Copied Round 2 plan to suppressions folder: {copy_path}")
                            st.rerun()
                        else:
                            st.warning("No winners/dates to export for Round 2.")
                    except Exception as e:
                        st.error(f"Round 2 export failed: {e}")
            else:
                st.caption("No outliers detected in suppressed view at current settings.")
        except Exception:
            pass

    # Running suppression summary (per date + pair) with drilldown
    try:
        st.markdown("---")
        st.subheader("🧪 Plan QA Preview (from file)")
        try:
            plan_files = sorted(glob.glob(os.path.join(supp_dir, '*.csv')))
            pf = st.selectbox("Select a plan file to preview (QA fields are informational only)", options=["(select)"] + plan_files)
            if pf != "(select)":
                dfp = pd.read_csv(pf)
                # Show key + QA columns if present
                qa_cols = [
                    'date','winner','mover_ind','loser','dma_name','remove_units','stage',
                    'nat_share_current','nat_mu_share','nat_sigma_share','nat_mu_window','nat_zscore',
                    'pair_wins_current','pair_mu_wins','pair_sigma_wins','pair_mu_window','pair_z'
                ]
                show_cols = [c for c in qa_cols if c in dfp.columns]
                if show_cols:
                    st.dataframe(dfp[show_cols])
                else:
                    st.caption("No QA columns found in this file; showing first rows")
                    st.dataframe(dfp.head(200))
                st.caption("Note: Applying suppressions ignores QA fields; only date,winner,loser,dma_name,mover_ind,remove_units are used.")
        except Exception:
            pass

        st.markdown("---")
        st.subheader("🧮 Auto Suppression Preview (no apply)")
        st.caption("Build a plan from the current filters and view without applying it. Preview includes QA columns.")
        colp1, colp2 = st.columns(2)
        with colp1:
            preview_mode = st.selectbox("Dates source", options=["From suppressed outliers (view)", "From Top N winners in range"], index=0)
        with colp2:
            topn_val = st.number_input("Top N (for Top N mode)", min_value=1, max_value=200, value=25, step=1)
        if st.button("Preview plan"):
            try:
                # Build winners→dates
                winners_dates = {}
                if preview_mode == "From suppressed outliers (view)":
                    out_after = st.session_state.get('out_after')
                    if out_after is None or out_after.empty:
                        st.error("No suppressed outliers in view. Enable 'Scan suppressed view for new outliers'.")
                    else:
                        for w in sorted(out_after['winner'].unique()):
                            dts = sorted(pd.to_datetime(out_after[out_after['winner']==w]['the_date']).dt.date.unique())
                            winners_dates[w] = [str(d) for d in dts]
                else:
                    # Compute Top N by wins in the graph window
                    ds_q = st.session_state.filters.get('ds','gamoshi')
                    mi_val = st.session_state.filters.get('mover_ind','False')
                    mi_q = 'TRUE' if str(mi_val)=='True' else 'FALSE'
                    start_q = str(view_start); end_q = str(view_end)
                    con = duckdb.connect()
                    try:
                        q_top = f"""
                        WITH ds AS (SELECT * FROM parquet_scan('{ds_glob}')),
                        filt AS (
                          SELECT * FROM ds WHERE ds = '{ds_q}' AND mover_ind = {mi_q} AND CAST(the_date AS DATE) BETWEEN DATE '{start_q}' AND DATE '{end_q}'
                        )
                        SELECT winner, SUM(adjusted_wins) AS wins_in_period FROM filt GROUP BY 1 ORDER BY 2 DESC LIMIT {int(topn_val)};
                        """
                        winners_list = con.execute(q_top).df()['winner'].tolist()
                        # Detect outlier dates per winner within view window using DuckDB outlier logic
                        prev = 13
                        q_out = f"""
                        WITH ds AS (SELECT * FROM parquet_scan('{ds_glob}')),
                        filt AS (SELECT * FROM ds WHERE ds = '{ds_q}' AND mover_ind = {mi_q}),
                        market AS (SELECT the_date, SUM(adjusted_wins) AS market_total_wins FROM filt GROUP BY 1),
                        per_w AS (SELECT f.the_date, f.winner, SUM(f.adjusted_wins) AS total_wins FROM filt f GROUP BY 1,2),
                        metrics AS (
                          SELECT p.the_date, p.winner,
                                 CASE WHEN strftime('%w', p.the_date)='6' THEN 'Sat'
                                      WHEN strftime('%w', p.the_date)='0' THEN 'Sun'
                                      ELSE 'Weekday' END AS day_type,
                                 p.total_wins / NULLIF(m.market_total_wins, 0) AS win_share
                          FROM per_w p JOIN market m USING (the_date)
                        ), zcalc AS (
                          SELECT the_date, winner, day_type, win_share,
                                 COUNT(*) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_count,
                                 avg(win_share) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_mean,
                                 stddev_samp(win_share) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_std
                          FROM metrics
                        )
                        SELECT winner, the_date FROM zcalc
                        WHERE winner IN ({','.join(["'"+w.replace("'","''")+"'" for w in winners_list])})
                          AND EXTRACT(EPOCH FROM the_date) BETWEEN EXTRACT(EPOCH FROM DATE '{start_q}') AND EXTRACT(EPOCH FROM DATE '{end_q}')
                          AND ((CASE WHEN day_type IN ('Sat','Sun') THEN w_count >= 10 ELSE w_count >= 14 END))
                          AND w_std > 0 AND (win_share - w_mean) / w_std > 2.5
                        ORDER BY 1,2;
                        """
                        out_df = con.execute(q_out).df()
                        for w in winners_list:
                            dts = out_df[out_df['winner']==w]['the_date'].astype(str).tolist()
                            if dts:
                                winners_dates[w] = dts
                    finally:
                        con.close()
                # Run planner per winner and merge
                if winners_dates:
                    spec = importlib.util.spec_from_file_location('duckdb_suppression_planner', os.path.join(os.getcwd(), 'tools', 'duckdb_suppression_planner.py'))
                    mod = importlib.util.module_from_spec(spec)
                    assert spec and spec.loader
                    spec.loader.exec_module(mod)
                    parts = []
                    prog = st.progress(0, text="Building preview plan...")
                    done = 0; total = len(winners_dates)
                    for w, dts in winners_dates.items():
                        with st.spinner(f"Planning {w} for {len(dts)} date(s)..."):
                            parts.append(mod.run(ds_glob, dts, ds=st.session_state.filters.get('ds','gamoshi'), mover_ind=st.session_state.filters.get('mover_ind','False'), winner=w, out_csv=None))
                        done += 1; prog.progress(min(100, int(done/total*100)))
                    import pandas as _pd
                    plan_prev = _pd.concat(parts, ignore_index=True)
                    # Attach pair QA columns
                    plan_prev = compute_pair_qa_for_plan(ds_glob, filters, plan_prev)
                    st.dataframe(plan_prev)
                    st.caption("This is a preview only. Use 'Copy to suppressions folder' to apply.")
                    # Offer save
                    colsv1, colsv2 = st.columns(2)
                    with colsv1:
                        name = st.text_input("Save as round name", value="preview_round")
                    with colsv2:
                        if st.button("Save preview to rounds folder"):
                            rd = os.path.expanduser(os.path.join('~/codebase-comparison/suppression_tools/suppressions/rounds', name))
                            os.makedirs(rd, exist_ok=True)
                            plan_prev.to_csv(os.path.join(rd, 'plan.csv'), index=False)
                            st.success(f"Saved preview to {os.path.join(rd, 'plan.csv')}")
            except Exception as e:
                st.error(f"Preview failed: {e}")

        st.markdown("---")
        st.subheader("📒 Running Suppression Summary (date, carrier, competitor)")
        st.caption("w0: base wins before suppression; rm: units removed; w1: wins after suppression. nat_mu_window: rows used for national DOW rolling mean; nat_mu_share: corresponding mean share")
        if isinstance(sup_df, pd.DataFrame) and not sup_df.empty:
            summary = compute_suppression_summary(ds_glob, filters, sup_df)
            if not summary.empty:
                summary_disp = summary.rename(columns={'the_date':'date'})
                st.dataframe(summary_disp)

                # Build selection for drilldown
                opts = [f"{r['date']} | {r['carrier']} | {r['competitor']} | mover={r['mover_ind']}" for _, r in summary_disp.iterrows()]
                pick = st.selectbox("Drilldown: choose a row", options=["(select)"] + opts)
                if pick != "(select)":
                    parts = [p.strip() for p in pick.split('|')]
                    sel_date = parts[0]
                    sel_carrier = parts[1]
                    sel_competitor = parts[2]
                    sel_mover = parts[3].split('=')[1].strip()
                    sel_mover_bool = True if sel_mover.lower() == 'true' else False
                    # Show DMA-level details
                    details = compute_pair_dma_details(ds_glob, filters, sup_df, sel_date, sel_carrier, sel_competitor, sel_mover_bool)
                    st.subheader("DMA details for selection")
                    st.dataframe(details)

                # Export Round 2 from suppressed outliers
                st.markdown("---")
                st.subheader("➡ Export Round 2 from suppressed outliers")
                out_after = st.session_state.get('out_after')
                r2_name = st.text_input("Round 2 name", value="round2_from_view")
                r2_copy = st.checkbox("Copy Round 2 plan into suppressions folder", value=True, key='r2_copy')
                if st.button("Export Round 2"):
                    if out_after is None or out_after.empty:
                        st.error("No suppressed outliers found in view. Toggle 'Scan suppressed view for new outliers'.")
                    elif not r2_name:
                        st.error("Round 2 name is required.")
                    else:
                        try:
                            spec = importlib.util.spec_from_file_location('duckdb_suppression_planner', os.path.join(os.getcwd(), 'tools', 'duckdb_suppression_planner.py'))
                            mod = importlib.util.module_from_spec(spec)
                            assert spec and spec.loader
                            spec.loader.exec_module(mod)
                            # Build winners→dates mapping from suppressed outliers
                            plan_parts = []
                            for w in sorted(out_after['winner'].unique()):
                                dts = sorted(pd.to_datetime(out_after[out_after['winner']==w]['the_date']).dt.date.unique())
                                dts = [str(d) for d in dts]
                                if dts:
                                    part = mod.run(ds_glob, dts, ds=st.session_state.filters.get('ds','gamoshi'), mover_ind=st.session_state.filters.get('mover_ind','False'), winner=w, out_csv=None)
                                    plan_parts.append(part)
                            import pandas as _pd
                            if plan_parts:
                                r2_dir = os.path.expanduser(os.path.join('~/codebase-comparison/suppression_tools/suppressions/rounds', r2_name))
                                os.makedirs(r2_dir, exist_ok=True)
                                out_csv = os.path.join(r2_dir, 'plan.csv')
                                r2_df = _pd.concat(plan_parts, ignore_index=True)
                                r2_df.to_csv(out_csv, index=False)
                                st.success(f"Wrote Round 2 plan: {out_csv}")
                                if r2_copy:
                                    main_copy = os.path.expanduser('~/codebase-comparison/suppression_tools/suppressions')
                                    os.makedirs(main_copy, exist_ok=True)
                                    copy_path = os.path.join(main_copy, f'{r2_name}.csv')
                                    r2_df.to_csv(copy_path, index=False)
                                    st.info(f"Copied Round 2 plan to suppressions folder: {copy_path}")
                            else:
                                st.warning("No winners/dates to export for Round 2.")
                        except Exception as e:
                            st.error(f"Round 2 export failed: {e}")
            else:
                st.caption("No applied suppressions match the current filters.")
        else:
            st.caption("No suppressions loaded or pending.")
    except Exception:
        pass

    # Suppression Round (Config-driven): save and run via DuckDB planner
    st.markdown("---")
    st.header("🧪 Suppression Round (Config)")
    rounds_base = os.path.expanduser('~/codebase-comparison/suppression_tools/suppressions/rounds')
    os.makedirs(rounds_base, exist_ok=True)

    colA, colB = st.columns(2)
    with colA:
        round_name = st.text_input("Round name (required)", value="att_aug14_17")
    with colB:
        also_copy = st.checkbox("Copy plan into suppressions folder after run", value=True)

    # Winner selection mode
    sel_mode = st.selectbox("Winner selection", options=["Single winner", "Top N winners (by wins in range)"])
    # Winner and dates
    default_winner = 'AT&T'
    if sel_mode == "Single winner":
        winner_input = st.text_input("Winner", value=default_winner)
    else:
        winner_input = None
        top_n = st.number_input("Top N", min_value=1, max_value=200, value=25, step=1)
        nat_window_cfg = st.number_input("Outlier window (rows)", min_value=5, max_value=90, value=14, step=1)
        nat_z_cfg = st.number_input("Outlier z threshold", min_value=0.5, max_value=5.0, value=2.5, step=0.1)
    use_range = st.checkbox("Use date range", value=True)
    if use_range:
        r_start = st.date_input("Run start", value=date(2025,8,14), key='run_start')
        r_end = st.date_input("Run end", value=date(2025,8,17), key='run_end')
        dates_list = [str(d) for d in pd.date_range(r_start, r_end, freq='D').date]
    else:
        dates_csv = st.text_input("Dates (comma-separated YYYY-MM-DD)", value="2025-08-14,2025-08-15,2025-08-16,2025-08-17")
        dates_list = [d.strip() for d in dates_csv.split(',') if d.strip()]

    # Save config and run
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("💾 Save Config"):
            if not round_name:
                st.error("Round name is required.")
            else:
                rd = os.path.join(rounds_base, round_name)
                os.makedirs(rd, exist_ok=True)
                cfg = {
                    'store_glob': ds_glob,
                    'ds': st.session_state.filters.get('ds', 'gamoshi'),
                    'mover_ind': st.session_state.filters.get('mover_ind', 'False'),
                    'mode': ('single' if sel_mode == 'Single winner' else 'topn'),
                    'winner': winner_input,
                    'top_n': int(top_n) if sel_mode != 'Single winner' else None,
                    'window': int(nat_window_cfg) if sel_mode != 'Single winner' else None,
                    'z': float(nat_z_cfg) if sel_mode != 'Single winner' else None,
                    'dates': dates_list,
                    'range': {'start': str(r_start), 'end': str(r_end)} if use_range else None,
                }
                with open(os.path.join(rd, 'config.json'), 'w') as f:
                    json.dump(cfg, f, indent=2)
                st.success(f"Saved config to {os.path.join(rd, 'config.json')}")
    with col2:
        if st.button("▶ Run Round"):
            if not round_name:
                st.error("Round name is required.")
            else:
                rd = os.path.join(rounds_base, round_name)
                os.makedirs(rd, exist_ok=True)
                # dynamic import of duckdb planner
                try:
                    spec = importlib.util.spec_from_file_location('duckdb_suppression_planner', os.path.join(os.getcwd(), 'tools', 'duckdb_suppression_planner.py'))
                    mod = importlib.util.module_from_spec(spec)
                    assert spec and spec.loader
                    spec.loader.exec_module(mod)
                    out_csv = os.path.join(rd, 'plan.csv')
                    ds_val = st.session_state.filters.get('ds','gamoshi')
                    mi_val = st.session_state.filters.get('mover_ind','False')
                    if sel_mode == 'Single winner':
                        plan_df = mod.run(ds_glob, dates_list, ds=ds_val, mover_ind=mi_val, winner=winner_input, out_csv=out_csv)
                    else:
                        # Top N mode: compute winners in range and their outlier dates
                        con = duckdb.connect()
                        try:
                            start_q = str(r_start); end_q = str(r_end)
                            ds_q = str(ds_val).replace("'","''")
                            mi_q = 'TRUE' if str(mi_val)=='True' else 'FALSE'
                            # Top N winners by wins in range
                            q_top = f"""
                            WITH ds AS (
                              SELECT * FROM parquet_scan('{ds_glob}')
                            ), filt AS (
                              SELECT * FROM ds WHERE ds = '{ds_q}' AND mover_ind = {mi_q} AND CAST(the_date AS DATE) BETWEEN DATE '{start_q}' AND DATE '{end_q}'
                            )
                            SELECT winner, SUM(adjusted_wins) AS wins_in_period
                            FROM filt
                            GROUP BY 1
                            ORDER BY 2 DESC
                            LIMIT {int(top_n)};
                            """
                            winners_df = con.execute(q_top).df()
                            winners_list = winners_df['winner'].tolist()
                            # Outliers per winner in range using DOW-partitioned rolling z
                            prev = int(nat_window_cfg) - 1
                            q_out = f"""
                            WITH ds AS (
                              SELECT * FROM parquet_scan('{ds_glob}')
                            ), filt AS (
                              SELECT * FROM ds WHERE ds = '{ds_q}' AND mover_ind = {mi_q}
                            ), market AS (
                              SELECT the_date, SUM(adjusted_wins) AS market_total_wins FROM filt GROUP BY 1
                            ), per_winner AS (
                              SELECT f.the_date, f.winner, SUM(f.adjusted_wins) AS total_wins FROM filt f GROUP BY 1,2
                            ), metrics AS (
                              SELECT p.the_date, p.winner,
                                     CASE WHEN strftime('%w', p.the_date)='6' THEN 'Sat'
                                          WHEN strftime('%w', p.the_date)='0' THEN 'Sun'
                                          ELSE 'Weekday' END AS day_type,
                                     p.total_wins / NULLIF(m.market_total_wins, 0) AS win_share
                              FROM per_winner p JOIN market m USING (the_date)
                            ), zcalc AS (
                              SELECT the_date, winner, day_type, win_share,
                                     COUNT(*) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_count,
                                     avg(win_share) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_mean,
                                     stddev_samp(win_share) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_std
                              FROM metrics
                            )
                            SELECT winner, the_date
                            FROM zcalc
                            WHERE winner IN ({','.join(["'"+w.replace("'","''")+"'" for w in winners_list])})
                              AND EXTRACT(EPOCH FROM the_date) BETWEEN EXTRACT(EPOCH FROM DATE '{start_q}') AND EXTRACT(EPOCH FROM DATE '{end_q}')
                              AND ((CASE WHEN day_type IN ('Sat','Sun') THEN w_count >= 10 ELSE w_count >= {int(nat_window_cfg)} END))
                              AND w_std > 0 AND ABS((win_share - w_mean) / w_std) > {float(nat_z_cfg)}
                            ORDER BY 1,2;
                            """
                            out_df = con.execute(q_out).df()
                        finally:
                            con.close()
                        plan_parts = []
                        for w in winners_list:
                            dts = out_df[out_df['winner']==w]['the_date'].astype(str).tolist()
                            if not dts:
                                continue
                            plan_parts.append(mod.run(ds_glob, dts, ds=ds_val, mover_ind=mi_val, winner=w, out_csv=None))
                        import pandas as _pd
                        if plan_parts:
                            plan_df = _pd.concat(plan_parts, ignore_index=True)
                            plan_df.to_csv(out_csv, index=False)
                        else:
                            plan_df = _pd.DataFrame(columns=['date','winner','mover_ind','loser','dma_name','remove_units','stage'])
                    st.success(f"Wrote plan: {out_csv}")
                    if also_copy:
                        main_copy = os.path.expanduser('~/codebase-comparison/suppression_tools/suppressions')
                        os.makedirs(main_copy, exist_ok=True)
                        copy_path = os.path.join(main_copy, f'{round_name}.csv')
                        # read-write copy to normalize columns
                        dfp = pd.read_csv(out_csv)
                        dfp.to_csv(copy_path, index=False)
                        st.info(f"Copied plan to suppressions folder: {copy_path}")
                except Exception as e:
                    st.error(f"Run failed: {e}")
    with col3:
        # Load existing round
        dirs = [d for d in sorted(os.listdir(rounds_base)) if os.path.isdir(os.path.join(rounds_base, d))]
        choice = st.selectbox("Load existing round", options=["(select)"] + dirs, key='round_select')
        if st.button("📂 Load Plan") and choice != "(select)":
            plan_path = os.path.join(rounds_base, choice, 'plan.csv')
            if os.path.exists(plan_path):
                dfp = pd.read_csv(plan_path)
                # Merge into active suppressions by writing a combined temp file in suppressions folder
                main_copy = os.path.expanduser('~/codebase-comparison/suppression_tools/suppressions')
                os.makedirs(main_copy, exist_ok=True)
                copy_path = os.path.join(main_copy, f'{choice}.csv')
                dfp.to_csv(copy_path, index=False)
                st.success(f"Loaded plan to suppressions folder: {copy_path}")
            else:
                st.error("No plan.csv found for that round.")
        if st.button("▶ Run Round from Config") and choice != "(select)":
            # Execute based on config.json saved in the round folder
            cfg_path = os.path.join(rounds_base, choice, 'config.json')
            if not os.path.exists(cfg_path):
                st.error("No config.json found for that round.")
            else:
                try:
                    with open(cfg_path, 'r') as f:
                        cfg = json.load(f)
                    spec = importlib.util.spec_from_file_location('duckdb_suppression_planner', os.path.join(os.getcwd(), 'tools', 'duckdb_suppression_planner.py'))
                    mod = importlib.util.module_from_spec(spec)
                    assert spec and spec.loader
                    spec.loader.exec_module(mod)
                    store_glob = cfg.get('store_glob', ds_glob)
                    ds_val = cfg.get('ds', st.session_state.filters.get('ds','gamoshi'))
                    mi_val = cfg.get('mover_ind', st.session_state.filters.get('mover_ind','False'))
                    mode = cfg.get('mode', 'single')
                    out_csv = os.path.join(rounds_base, choice, 'plan.csv')
                    if mode == 'single':
                        winner_val = cfg.get('winner')
                        dates = cfg.get('dates') or []
                        mod.run(store_glob, dates, ds=ds_val, mover_ind=mi_val, winner=winner_val, out_csv=out_csv)
                    elif mode == 'topn':
                        # Reuse UI logic: compute winners and outliers in cfg range
                        start_q = (cfg.get('range', {}) or {}).get('start')
                        end_q = (cfg.get('range', {}) or {}).get('end')
                        top_n = int(cfg.get('top_n', 25))
                        nat_window_cfg = int(cfg.get('window', 14))
                        nat_z_cfg = float(cfg.get('z', 2.5))
                        import duckdb as _dd
                        con = _dd.connect()
                        try:
                            ds_q = str(ds_val).replace("'","''")
                            mi_q = 'TRUE' if str(mi_val)=='True' else 'FALSE'
                            q_top = f"""
                            WITH ds AS (SELECT * FROM parquet_scan('{store_glob}')),
                            filt AS (
                              SELECT * FROM ds WHERE ds = '{ds_q}' AND mover_ind = {mi_q} AND CAST(the_date AS DATE) BETWEEN DATE '{start_q}' AND DATE '{end_q}'
                            )
                            SELECT winner, SUM(adjusted_wins) AS wins_in_period
                            FROM filt GROUP BY 1 ORDER BY 2 DESC LIMIT {top_n};
                            """
                            winners_df = con.execute(q_top).df()
                            winners_list = winners_df['winner'].tolist()
                            prev = nat_window_cfg - 1
                            q_out = f"""
                            WITH ds AS (SELECT * FROM parquet_scan('{store_glob}')),
                            filt AS (SELECT * FROM ds WHERE ds = '{ds_q}' AND mover_ind = {mi_q}),
                            market AS (SELECT the_date, SUM(adjusted_wins) AS market_total_wins FROM filt GROUP BY 1),
                            per_w AS (SELECT f.the_date, f.winner, SUM(f.adjusted_wins) AS total_wins FROM filt f GROUP BY 1,2),
                            metrics AS (
                              SELECT p.the_date, p.winner,
                                     CASE WHEN strftime('%w', p.the_date)='6' THEN 'Sat'
                                          WHEN strftime('%w', p.the_date)='0' THEN 'Sun'
                                          ELSE 'Weekday' END AS day_type,
                                     p.total_wins / NULLIF(m.market_total_wins, 0) AS win_share
                              FROM per_w p JOIN market m USING (the_date)
                            ), zcalc AS (
                              SELECT the_date, winner, day_type, win_share,
                                     COUNT(*) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_count,
                                     avg(win_share) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_mean,
                                     stddev_samp(win_share) OVER (PARTITION BY winner, day_type ORDER BY the_date ROWS BETWEEN {prev} PRECEDING AND CURRENT ROW) AS w_std
                              FROM metrics
                            )
                            SELECT winner, the_date FROM zcalc
                            WHERE winner IN ({','.join(["'"+w.replace("'","''")+"'" for w in winners_list])})
                              AND EXTRACT(EPOCH FROM the_date) BETWEEN EXTRACT(EPOCH FROM DATE '{start_q}') AND EXTRACT(EPOCH FROM DATE '{end_q}')
                              AND ((CASE WHEN day_type IN ('Sat','Sun') THEN w_count >= 10 ELSE w_count >= {nat_window_cfg} END))
                              AND w_std > 0 AND ABS((win_share - w_mean) / w_std) > {nat_z_cfg}
                            ORDER BY 1,2;
                            """
                            out_df = con.execute(q_out).df()
                        finally:
                            con.close()
                        import pandas as _pd
                        plan_parts = []
                        for w in winners_list:
                            dts = out_df[out_df['winner']==w]['the_date'].astype(str).tolist()
                            if not dts:
                                continue
                            plan_parts.append(mod.run(store_glob, dts, ds=ds_val, mover_ind=mi_val, winner=w, out_csv=None))
                        if plan_parts:
                            _pd.concat(plan_parts, ignore_index=True).to_csv(out_csv, index=False)
                    elif mode == 'from_outliers':
                        m = cfg.get('dates_by_winner', {}) or {}
                        for w, dts in m.items():
                            mod.run(store_glob, dts, ds=ds_val, mover_ind=mi_val, winner=w, out_csv=None)
                        # gather combined CSVs by re-running aggregation
                        # For simplicity, build once more
                        import pandas as _pd
                        parts = []
                        for w, dts in m.items():
                            parts.append(mod.run(store_glob, dts, ds=ds_val, mover_ind=mi_val, winner=w, out_csv=None))
                        if parts:
                            _pd.concat(parts, ignore_index=True).to_csv(out_csv, index=False)
                    st.success(f"Ran round from config. Wrote {out_csv}")
                    if also_copy:
                        main_copy = os.path.expanduser('~/codebase-comparison/suppression_tools/suppressions')
                        os.makedirs(main_copy, exist_ok=True)
                        copy_path = os.path.join(main_copy, f'{choice}.csv')
                        pd.read_csv(out_csv).to_csv(copy_path, index=False)
                        st.info(f"Copied plan to suppressions folder: {copy_path}")
                except Exception as e:
                    st.error(f"Run from config failed: {e}")

    st.header("🤖 Auto-pick Suppressions (national outlier → DMA targets)")
    with st.expander("Configure and suggest"):
        sel_date = st.date_input("Outlier date", value=(st.session_state.auto_sel_date or pd.to_datetime('today').date()))
        sel_winner = st.text_input("Winner (carrier)", value=(st.session_state.auto_sel_winner or "AT&T"))
        lookback_days = st.number_input("Lookback days", min_value=7, max_value=120, value=28, step=7)
        same_dow = st.checkbox("Same day-of-week baseline", value=True)
        comp_z = st.number_input("Competitor z threshold", min_value=0.0, max_value=5.0, value=1.5, step=0.1)
        k_sigma = st.number_input("k-sigma (target = mu + kσ)", min_value=0.0, max_value=3.0, value=1.0, step=0.1)
        dma_z = st.number_input("DMA z threshold (monthly)", min_value=0.0, max_value=5.0, value=1.5, step=0.1)
        f_mid = st.slider("Midpoint fraction of required removal", min_value=0.0, max_value=1.0, value=0.5, step=0.05)
        max1 = st.slider("Max per-DMA in spike stage (ignored if respecting baseline)", 0.0, 1.0, 0.4, 0.05)
        max2 = st.slider("Max per-DMA partial pass 1", 0.0, 1.0, 0.25, 0.05)
        max3 = st.slider("Max per-DMA partial pass 2 (total)", 0.0, 1.0, 0.40, 0.05)
        respect_floor = st.checkbox("Respect baseline floor in partials (don't go below monthly mean)", value=True)
        partial_sort_mode = st.selectbox("Partial fill ranking", ["wins (big DMAs first)", "severity (wins/base_mean)"] , index=0)

        def suggest(ds_glob, filters, date_val, winner, lookback_days, same_dow, comp_z, k_sigma, dma_z, f_mid, max1, max2, max3):
            con = duckdb.connect()
            try:
                where = where_clause(filters)
                dq = f"""
                WITH ds AS (SELECT * FROM parquet_scan('{ds_glob}') ), filt AS (
                  SELECT CAST(the_date AS DATE) d, winner, loser, dma_name, adjusted_wins FROM ds {where}
                )
                SELECT * FROM filt
                """
                df = con.execute(dq).df()
            finally:
                con.close()
            if df.empty:
                return pd.DataFrame(), pd.DataFrame()
            dts = pd.Timestamp(date_val)
            ddf = df[df['d']==dts]
            if ddf.empty:
                return pd.DataFrame(), pd.DataFrame()
            W = ddf.loc[ddf['winner']==winner,'adjusted_wins'].sum()
            T = ddf['adjusted_wins'].sum()
            if T<=0 or W<=0:
                return pd.DataFrame(), pd.DataFrame()
            # National baseline for winner
            start = dts - pd.Timedelta(days=int(lookback_days))
            bdf = df[(df['d']>=start) & (df['d']<dts) & (df['winner']==winner)].copy()
            if same_dow:
                target_dow = dts.dayofweek
                bdf = bdf[pd.to_datetime(bdf['d']).dt.dayofweek==target_dow]
            gW = bdf.groupby('d')['adjusted_wins'].sum()
            gT = df[(df['d']>=start) & (df['d']<dts)].groupby('d')['adjusted_wins'].sum()
            bs = (gW / gT.reindex(gW.index)).dropna()
            mu = bs.mean() if not bs.empty else 0.0
            sigma = bs.std(ddof=1) if len(bs)>=3 else 0.0
            s = W / T
            target = min(s, mu + k_sigma * sigma) if sigma and sigma>0 else mu
            if target <= 0 or target >= 1:
                return pd.DataFrame(), pd.DataFrame()
            x_req = max(0.0, (W - target*T) / (1 - target))
            x_mid = int(max(0, round(x_req * f_mid)))
            # Competitor decomposition on day
            comp = ddf[ddf['winner']==winner].groupby('loser', as_index=False)['adjusted_wins'].sum().rename(columns={'adjusted_wins':'h'})
            # Competitor baseline shares
            comp_series = df[df['winner']==winner].groupby(['d','loser'], as_index=False)['adjusted_wins'].sum().rename(columns={'adjusted_wins':'h'})
            wt = df.groupby('d', as_index=False)['adjusted_wins'].sum().rename(columns={'adjusted_wins':'tot'})
            comp_series = comp_series.merge(wt, on='d', how='left')
            comp_series['share'] = comp_series['h'] / comp_series['tot']
            base = comp_series[(comp_series['d']>=start) & (comp_series['d']<dts)].copy()
            if same_dow:
                base = base[pd.to_datetime(base['d']).dt.dayofweek==dts.dayofweek]
            stats = base.groupby('loser').agg(mu=('share','mean'), sigma=('share','std')).reset_index()
            comp = comp.merge(stats, on='loser', how='left').fillna({'mu':0.0,'sigma':0.0})
            # Select competing losers by z
            comp['share_d'] = comp['h'] / T
            comp['z'] = (comp['share_d'] - comp['mu']) / comp['sigma']
            comp.loc[~comp['sigma'].gt(0), 'z'] = float('nan')
            spikes = comp[(comp['z']>=comp_z) & (comp['share_d']>comp['mu'])].copy()
            # Need per loser to reach loser target mu+kσ (using winner_total baseline T for decomposition)
            spikes['target_l'] = spikes.apply(lambda r: min(r['share_d'], r['mu'] + k_sigma * r['sigma']) if r['sigma'] and r['sigma']>0 else r['mu'], axis=1)
            spikes['need_l'] = spikes.apply(lambda r: max(0.0, min(r['h'], (r['h'] - r['target_l']*T) / (1 - r['target_l'])) ) if r['target_l']<1 else 0.0, axis=1)
            # Allocate x_mid across losers proportionally to need_l
            if x_mid>0:
                if spikes['need_l'].sum() <= 0:
                    spikes = comp.sort_values('share_d', ascending=False).head(5).copy()
                    spikes['need_l'] = spikes['h'] * 0.2
                scale = min(1.0, x_mid / max(1.0, spikes['need_l'].sum()))
                spikes['alloc_l'] = (spikes['need_l'] * scale).clip(upper=spikes['h'])
            else:
                spikes['alloc_l'] = 0
            comp_alloc = spikes[['loser','h','alloc_l']].rename(columns={'loser':'competitor','h':'w0','alloc_l':'rm'})
            comp_alloc['w1'] = (comp_alloc['w0'] - comp_alloc['rm']).clip(lower=0)

            # DMA allocation per loser
            rows = []
            for _, pr in comp_alloc.iterrows():
                loser = pr['competitor']; need = int(round(pr['rm']))
                if need <= 0: continue
                # get day wins by DMA for this pair
                day_pair = ddf[(ddf['winner']==winner) & (ddf['loser']==loser)].groupby('dma_name', as_index=False)['adjusted_wins'].sum().rename(columns={'adjusted_wins':'wins'})
                if day_pair.empty: continue
                # monthly baseline
                mon = pd.Period(dts, freq='M')
                mon_df = df[(pd.PeriodIndex(pd.to_datetime(df['d']), freq='M')==mon) & (df['winner']==winner) & (df['loser']==loser)].copy()
                daily_dma = mon_df.groupby(['dma_name','d'], as_index=False)['adjusted_wins'].sum().rename(columns={'adjusted_wins':'wins_day'})
                base_stats = (daily_dma[daily_dma['d'] != dts]
                              .groupby('dma_name', as_index=False)
                              .agg(base_mean=('wins_day','mean'), base_std=('wins_day','std')))
                frame = day_pair.merge(base_stats, on='dma_name', how='left').fillna({'base_mean':0.0,'base_std':0.0})
                def dz(r):
                    return (r['wins']-r['base_mean'])/r['base_std'] if r['base_std'] and r['base_std']>0 else (float('inf') if (r['base_mean']>0 and r['wins']>r['base_mean']) or (r['base_mean']==0 and r['wins']>0) else 0.0)
                frame['dma_z'] = frame.apply(dz, axis=1)
                frame = frame.sort_values(['dma_z','wins'], ascending=[False, False])
                remaining = need
                # Stage A: DMA z-spikes — remove the EXCESS above baseline (no % cap)
                for idx, rr in frame.iterrows():
                    if remaining<=0: break
                    if rr['dma_z'] < dma_z: continue
                    # Excess above baseline (allow full removal down to baseline; if baseline==0 and wins>0, remove all)
                    excess = int(max(0, rr['wins'] - (rr['base_mean'] if pd.notna(rr['base_mean']) else 0.0)))
                    cap = int(min(remaining, excess))
                    if cap>0:
                        rows.append({'date': dts.date().isoformat(), 'winner': winner, 'competitor': loser, 'dma_name': rr['dma_name'], 'w0': int(rr['wins']), 'rm': cap, 'w1': int(rr['wins']-cap), 'stage': 'dma_spike'})
                        remaining -= cap
                # Stage B: low-baseline full if base_mean<=5 and wins>base_mean
                for idx, rr in frame.iterrows():
                    if remaining<=0: break
                    if rr['base_mean'] <= 5 and rr['wins'] > rr['base_mean']:
                        cap = int(min(remaining, rr['wins']))
                        if cap>0:
                            rows.append({'date': dts.date().isoformat(), 'winner': winner, 'competitor': loser, 'dma_name': rr['dma_name'], 'w0': int(rr['wins']), 'rm': cap, 'w1': int(rr['wins']-cap), 'stage': 'low_baseline_full'})
                            remaining -= cap
                # Stage C/D: partial up to max2 then max3 total
                for stage, mx in [('partial_25pct', max2), ('partial_40pct', max3)]:
                    if remaining<=0: break
                    # sort by user preference
                    part = frame.sort_values('wins', ascending=False) if partial_sort_mode.startswith('wins') else frame.sort_values(['severity_ratio','wins'], ascending=[False, False])
                    for idx, rr in part.iterrows():
                        if remaining<=0: break
                        already = sum(rm['rm'] for rm in rows if rm['dma_name']==rr['dma_name'] and rm['competitor']==loser and rm['date']==dts.date().isoformat())
                        # Available without going below baseline if respecting floor
                        if respect_floor:
                            avail_floor = int(max(0, (rr['wins'] - already) - (rr['base_mean'] if pd.notna(rr['base_mean']) else 0.0)))
                        else:
                            avail_floor = int(max(0, rr['wins'] - already))
                        cap_pct = int(max(0, math.ceil(mx * rr['wins']) - already))
                        cap = int(min(remaining, avail_floor, cap_pct))
                        if cap>0:
                            rows.append({'date': dts.date().isoformat(), 'winner': winner, 'competitor': loser, 'dma_name': rr['dma_name'], 'w0': int(rr['wins']), 'rm': cap, 'w1': int(rr['wins']- (already+cap)), 'stage': stage})
                            remaining -= cap
            comp_table = comp_alloc.copy()
            dma_table = pd.DataFrame(rows)
            return comp_table, dma_table

        if st.button("Suggest auto suppressions"):
            comp_tbl, dma_tbl = suggest(ds_glob, filters, sel_date, sel_winner, lookback_days, same_dow, comp_z, k_sigma, dma_z, f_mid, max1, max2, max3)
            st.session_state.last_comp_tbl = comp_tbl.copy() if comp_tbl is not None else None
            st.session_state.last_dma_tbl = dma_tbl.copy() if dma_tbl is not None else None
        # Always render the latest suggestions if available
        if isinstance(st.session_state.last_comp_tbl, pd.DataFrame) and not st.session_state.last_comp_tbl.empty:
            st.subheader("Competitor suggestions (the_date, carrier, competitor, w0, rm, w1)")
            disp_comp = st.session_state.last_comp_tbl.copy()
            disp_comp.insert(0, 'the_date', pd.to_datetime(sel_date))
            disp_comp.insert(1, 'carrier', sel_winner)
            st.dataframe(disp_comp[['the_date','carrier','competitor','w0','rm','w1']])
        if isinstance(st.session_state.last_dma_tbl, pd.DataFrame) and not st.session_state.last_dma_tbl.empty:
            st.subheader("DMA suggestions (the_date, carrier, competitor, dma_name, w0, rm, w1)")
            disp_dma = st.session_state.last_dma_tbl.copy()
            st.dataframe(disp_dma[['date','winner','competitor','dma_name','w0','rm','w1']].rename(columns={'date':'the_date','winner':'carrier'}))
            if st.button("Add DMA suggestions to session"):
                for _, r in disp_dma.iterrows():
                    st.session_state.supp_rows.append({'date': r['date'], 'winner': r['winner'], 'loser': r['competitor'], 'dma_name': r['dma_name'], 'remove_units': int(r['rm'])})
                st.success("Added suggestions to pending rows.")
                st.rerun()
            if st.button("💾 Save suggestion tables to suppressions folder"):
                try:
                    ts = pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')
                    comp_path = os.path.join(supp_dir, f"auto_suggest_comp_{sel_winner.replace(' ','_')}_{sel_date}_{ts}.csv")
                    dma_path = os.path.join(supp_dir, f"auto_suggest_dma_{sel_winner.replace(' ','_')}_{sel_date}_{ts}.csv")
                    # Normalize date columns
                    out_comp = disp_comp.copy()
                    out_comp['the_date'] = pd.to_datetime(out_comp['the_date']).dt.date
                    out_comp.to_csv(comp_path, index=False)
                    out_dma = disp_dma.copy()
                    out_dma.to_csv(dma_path, index=False)
                    st.success(f"Saved: {comp_path} and {dma_path}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Save failed: {e}")

    st.markdown("---")
    st.header("📤 Load a suppression round (CSV/JSON)")
    up = st.file_uploader("Upload CSV or JSON with columns: date,winner,loser,dma_name,remove_units", type=["csv","json"])
    if up is not None:
        try:
            if up.type.endswith('json'):
                imp = pd.read_json(up)
            else:
                imp = pd.read_csv(up)
            imp = imp[['date','winner','loser','dma_name','remove_units']]
            imp['date'] = pd.to_datetime(imp['date']).dt.date
            st.session_state.supp_rows.extend(imp.to_dict('records'))
            st.success(f"Loaded {len(imp)} rows into pending suppressions.")
        except Exception as e:
            st.error(f"Failed to load: {e}")


if __name__ == "__main__":
    main()
