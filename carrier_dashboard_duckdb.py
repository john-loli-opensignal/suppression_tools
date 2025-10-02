import os
import glob
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objs as go


def get_default_store_dir() -> str:
    return os.path.join(os.getcwd(), "duckdb_partitioned_store")


def get_store_glob(store_dir: str) -> str:
    # Expand '~' so users can paste home-relative paths
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
    if 'outlier_window' not in st.session_state:
        st.session_state.outlier_window = 14
    if 'outlier_z' not in st.session_state:
        st.session_state.outlier_z = 3.0
    if 'outlier_show' not in st.session_state:
        st.session_state.outlier_show = 'All'


def create_plot(pdf: pd.DataFrame, metric: str, active_filters=None, analysis_mode="National", primary: str | None = None) -> go.Figure:
    if pdf is None or pdf.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data to display. Click RUN ANALYSIS or adjust filters.", xref='paper', yref='paper', showarrow=False, font=dict(size=14))
        fig.update_layout(xaxis={'visible': False}, yaxis={'visible': False})
        return fig

    if not pd.api.types.is_datetime64_any_dtype(pdf['the_date']):
        pdf['the_date'] = pd.to_datetime(pdf['the_date'])

    carriers = sorted(pdf['winner'].unique())

    palette_name = st.session_state.get('palette', 'Dark24')
    # High-contrast default in Competitor
    if analysis_mode == 'Competitor' and (palette_name is None or palette_name == 'Plotly'):
        palette_name = 'Dark24'

    if palette_name == 'Plotly':
        palette = px.colors.qualitative.Plotly
    elif palette_name == 'D3':
        palette = px.colors.qualitative.D3
    elif palette_name == 'G10':
        palette = px.colors.qualitative.G10
    elif palette_name == 'Dark24':
        palette = px.colors.qualitative.Dark24
    else:
        palette = ['#000000', '#E69F00', '#56B4E9', '#009E73', '#F0E442', '#0072B2', '#D55E00', '#CC79A7']

    color_map = {c: palette[i % len(palette)] for i, c in enumerate([c for c in carriers if c != 'Other'])}

    smoothing_on = st.session_state.get('smoothing', True)
    show_markers = st.session_state.get('show_markers', False)
    stacked = st.session_state.get('stacked', False)

    fig = go.Figure()
    for i, carrier in enumerate(carriers):
        cdf = pdf[pdf['winner'] == carrier].sort_values('the_date')
        # Be tolerant if the metric column is missing (e.g., stale cached df)
        if metric not in cdf.columns:
            # Create a zero series to avoid KeyError and prompt rerun
            series = pd.Series([0] * len(cdf), index=cdf.index)
        else:
            series = pd.to_numeric(cdf[metric], errors='coerce').fillna(0)
        dates = cdf['the_date']

        if smoothing_on and len(series) >= 3:
            smooth = series.rolling(window=3, center=True, min_periods=1).mean()
        else:
            smooth = series

        raw_vals = series.round(6).astype(str)
        smooth_vals = smooth.round(6).astype(str)
        hover_text = [f"{carrier}<br>{d.date()}<br>raw: {r}<br>smoothed: {s}" for d, r, s in zip(dates, raw_vals, smooth_vals)]

        line_kwargs = dict(color='black', width=2) if carrier == 'Other' else dict(color=color_map.get(carrier, palette[i % len(palette)]), width=2)
        if carrier == 'Other':
            line_kwargs['dash'] = 'dash'

        mode = 'lines+markers' if show_markers else 'lines'
        fig.add_trace(go.Scatter(
            x=dates, y=smooth, mode=mode, name=carrier, line=line_kwargs,
            hoverinfo='text', hovertext=hover_text,
            stackgroup='one' if stacked and metric in ('win_share', 'loss_share') else None,
        ))

        # Overlay outlier markers if available (align marker y with plotted line)
        if 'is_outlier' in cdf.columns:
            cdf_out = cdf[cdf['is_outlier'] == True]
            if not cdf_out.empty:
                zvals = cdf_out['zscore'].round(2).astype(str) if 'zscore' in cdf_out.columns else None
                dayt = cdf_out['day_type'] if 'day_type' in cdf_out.columns else None
                # Split by sign and optionally filter by positive only
                show_mode = st.session_state.get('outlier_show', 'All')
                pos_out = cdf_out[cdf_out['zscore'] >= 0]
                neg_out = cdf_out[cdf_out['zscore'] < 0]
                if show_mode == 'Positive only':
                    neg_out = neg_out.iloc[0:0]

                # Positive (stars)
                if not pos_out.empty:
                    if zvals is not None and dayt is not None:
                        hover_o_pos = [f"{carrier}<br>{d.date()}<br>{metric}: {v:.6f}<br>z: {z}<br>{dt}"
                                       for d, v, z, dt in zip(pos_out['the_date'], pos_out[metric], pos_out['zscore'].round(2).astype(str), pos_out['day_type'])]
                    else:
                        hover_o_pos = [f"{carrier}<br>{d.date()}<br>{metric}: {v:.6f}" for d, v in zip(pos_out['the_date'], pos_out[metric])]
                    y_marker_pos = (smooth.loc[pos_out.index] if isinstance(smooth, pd.Series) and smoothing_on else pos_out[metric])
                    fig.add_trace(go.Scatter(
                        x=pos_out['the_date'], y=y_marker_pos,
                        mode='markers', name=f"{carrier} outlier (+)",
                        marker=dict(symbol='star', color='yellow', size=11, line=dict(color='black', width=0.6), opacity=0.95),
                        hoverinfo='text', hovertext=hover_o_pos,
                        showlegend=False,
                    ))

                # Negative (minus signs)
                if not neg_out.empty:
                    hover_o_neg = [f"{carrier}<br>{d.date()}<br>{metric}: {v:.6f}<br>z: {z}<br>{dt}"
                                   for d, v, z, dt in zip(neg_out['the_date'], neg_out[metric], neg_out['zscore'].round(2).astype(str), neg_out['day_type'])]
                    y_marker_neg = (smooth.loc[neg_out.index] if isinstance(smooth, pd.Series) and smoothing_on else neg_out[metric])
                    fig.add_trace(go.Scatter(
                        x=neg_out['the_date'], y=y_marker_neg,
                        mode='markers', name=f"{carrier} outlier (-)",
                        marker=dict(symbol='line-ew', color='red', size=12, line=dict(color='black', width=0.6), opacity=0.95),
                        hoverinfo='text', hovertext=hover_o_neg,
                        showlegend=False,
                    ))

    # Dynamic title, include primary in competitor mode
    title_base = metric.replace('_', ' ').title()
    if analysis_mode == 'Competitor':
        who = primary or 'Primary'
        title = f"{title_base} - Head to Head: {who} vs Competitors"
    else:
        title = f"{title_base} Over Time"
    if active_filters:
        title += f" ({', '.join(active_filters)})"
    fig.update_layout(
        title=dict(text=title, x=0.01, xanchor='left'),
        xaxis_title='Date',
        yaxis_title=metric.replace('_', ' ').title(),
        legend=dict(orientation='v', x=1.02, y=0.5),
        autosize=True,
        width=1100,
        height=650,
        margin=dict(l=40, r=200, t=80, b=40)
    )
    return fig


def where_clause(filters: dict) -> str:
    clauses = []
    for col, val in (filters or {}).items():
        if val in (None, "All"):
            continue
        if col == 'mover_ind':
            # incoming is string 'True'/'False'
            to_bool = 'TRUE' if str(val) == 'True' else 'FALSE'
            clauses.append(f"{col} = {to_bool}")
        else:
            # quote single quotes inside
            sval = str(val).replace("'", "''")
            clauses.append(f"{col} = '{sval}'")
    if clauses:
        return "WHERE " + " AND ".join(clauses)
    return ""


@st.cache_data
def get_date_bounds(ds_glob: str, filters: dict):
    con = duckdb.connect()
    try:
        where = where_clause(filters)
        q = f"SELECT MIN(CAST(the_date AS DATE)) AS mn, MAX(CAST(the_date AS DATE)) AS mx FROM parquet_scan('{ds_glob}') {where}"
        df = con.execute(q).df()
        if df.empty or pd.isna(df['mn'][0]) or pd.isna(df['mx'][0]):
            return (pd.to_datetime('1970-01-01').date(), pd.to_datetime('1970-01-01').date())
        return (pd.to_datetime(df['mn'][0]).date(), pd.to_datetime(df['mx'][0]).date())
    finally:
        con.close()


@st.cache_data
def get_distinct_options(ds_glob: str, column: str):
    con = duckdb.connect()
    try:
        q = f"SELECT DISTINCT {column} FROM parquet_scan('{ds_glob}') WHERE {column} IS NOT NULL ORDER BY 1"
        df = con.execute(q).df()
        values = df.iloc[:, 0].tolist()
        values = [str(v) for v in values]
        return ["All"] + values
    finally:
        con.close()


@st.cache_data
def get_ranked_winners(ds_glob: str, filters: dict):
    con = duckdb.connect()
    try:
        where = where_clause(filters)
        q = f"""
        WITH ds AS (
            SELECT * FROM parquet_scan('{ds_glob}')
        )
        SELECT winner, SUM(adjusted_wins) AS total_wins
        FROM ds
        {where}
        GROUP BY 1
        ORDER BY 2 DESC
        """
        df = con.execute(q).df()
        return df['winner'].dropna().tolist()
    finally:
        con.close()


@st.cache_data
def compute_national_pdf(ds_glob: str, filters: dict, selected_winners: list, show_other: bool, metric: str, window: int, z_thresh: float, start_date: str, end_date: str) -> pd.DataFrame:
    # Defensive local imports to avoid Streamlit cache scope issues
    from suppression_tools.src import metrics as _metrics
    from suppression_tools.src import outliers as _outliers
    if not selected_winners:
        return pd.DataFrame(columns=["the_date", "winner", metric])
    _ds = filters.get('ds', 'gamoshi')
    _mover_ind = filters.get('mover_ind', 'False')
    _state = filters.get('state') if filters else None
    _dma = filters.get('dma_name') if filters else None

    base = _metrics.national_timeseries(ds_glob, _ds, _mover_ind, start_date, end_date, state=_state, dma_name=_dma)
    if base.empty:
        return pd.DataFrame(columns=["the_date", "winner", metric])
    keep = base[['the_date', 'winner', metric]].copy()
    keep = keep[keep['winner'].isin(selected_winners)]

    outs = _outliers.national_outliers(ds_glob, _ds, _mover_ind, start_date, end_date, window, z_thresh, state=_state, dma_name=_dma, metric=metric)
    if not outs.empty:
        keep = keep.merge(
            outs[['the_date', 'winner', 'z', 'nat_outlier_pos']].rename(columns={'z': 'zscore', 'nat_outlier_pos': 'is_outlier'}),
            on=['the_date', 'winner'], how='left')
        keep['is_outlier'] = keep['is_outlier'].fillna(False)
        keep['zscore'] = keep['zscore'].fillna(0.0)
    else:
        keep['is_outlier'] = False
        keep['zscore'] = 0.0
    keep['day_type'] = pd.to_datetime(keep['the_date']).dt.dayofweek.map(lambda x: 'Sat' if x == 6 else ('Sun' if x == 0 else 'Weekday'))

    if show_other:
        all_df = base[['the_date', 'winner', metric]].copy()
        other = all_df[~all_df['winner'].isin(selected_winners)].groupby('the_date', as_index=False)[metric].sum()
        if not other.empty:
            other['winner'] = 'Other'
            other['zscore'] = 0.0
            other['is_outlier'] = False
            other['day_type'] = pd.to_datetime(other['the_date']).dt.dayofweek.map(lambda x: 'Sat' if x == 6 else ('Sun' if x == 0 else 'Weekday'))
            keep = pd.concat([keep, other[['the_date', 'winner', metric, 'day_type', 'zscore', 'is_outlier']]], ignore_index=True)

    return keep[['the_date', 'winner', metric, 'day_type', 'zscore', 'is_outlier']].sort_values(['the_date', 'winner'])


@st.cache_data
def compute_competitor_pdf(ds_glob: str, filters: dict, primary: str, competitors: list, metric: str, window: int, z_thresh: float, start_date: str, end_date: str) -> pd.DataFrame:
    # Defensive local import to avoid Streamlit cache scope issues
    from suppression_tools.src import metrics as _metrics
    if not primary or not competitors:
        return pd.DataFrame(columns=["the_date", "winner", metric])

    _ds = filters.get('ds', 'gamoshi')
    _mover_ind = filters.get('mover_ind', 'False')
    _state = filters.get('state') if filters else None
    _dma = filters.get('dma_name') if filters else None

    base = _metrics.competitor_view(ds_glob, _ds, _mover_ind, start_date, end_date, primary, competitors, state=_state, dma_name=_dma)
    if base.empty:
        return pd.DataFrame(columns=["the_date", "winner", metric])

    df = base.copy()
    if metric == 'win_share':
        df[metric] = df['h2h_wins'] / df['primary_total_wins'].replace(0, pd.NA)
    elif metric == 'loss_share':
        df[metric] = df['h2h_losses'] / df['primary_total_losses'].replace(0, pd.NA)
    else:
        df[metric] = df['h2h_wins'] / df['h2h_losses'].replace(0, pd.NA)

    df = df.rename(columns={'competitor': 'winner'})[['the_date', 'winner', metric]].copy()
    df['the_date'] = pd.to_datetime(df['the_date'])
    df['day_type'] = df['the_date'].dt.dayofweek.map(lambda x: 'Sat' if x == 6 else ('Sun' if x == 0 else 'Weekday'))

    def _z_for_group(g):
        s = g[metric]
        win = max(2, int(window) - 1)
        mu = s.shift(1).rolling(window=win, min_periods=2).mean()
        sigma = s.shift(1).rolling(window=win, min_periods=2).std(ddof=1)
        z = (s - mu) / sigma.replace({0: pd.NA})
        return z.fillna(0.0)

    df = df.sort_values(['winner', 'day_type', 'the_date'])
    df['zscore'] = df.groupby(['winner', 'day_type'], as_index=False, group_keys=False).apply(_z_for_group)
    df['is_outlier'] = df['zscore'] > float(z_thresh)
    return df[['the_date', 'winner', metric, 'day_type', 'zscore', 'is_outlier']].sort_values(['the_date', 'winner'])


def main():
    st.set_page_config(page_title="Carrier Win/Loss Share (DuckDB)", page_icon="🦆", layout="wide")
    st.title("🦆 Carrier Win/Loss Share Dashboard (DuckDB)")
    st.markdown("---")

    init_session_state()

    # Data source
    default_dir = get_default_store_dir()
    st.sidebar.header("📦 Data Source")
    store_dir = st.sidebar.text_input("Partitioned dataset directory", value=default_dir)
    ds_glob = get_store_glob(store_dir)
    if not glob.glob(ds_glob, recursive=True):
        st.warning("No parquet files found in the dataset directory. Use the platform builder to create it.")

    # Controls
    st.sidebar.header("📋 Dashboard Controls")
    st.session_state.analysis_mode = st.sidebar.selectbox("Analysis Mode", ["National", "Competitor"], index=0 if st.session_state.analysis_mode == "National" else 1)

    metric_options = ["win_share", "loss_share", "wins_per_loss"]
    metric_index = metric_options.index(st.session_state.metric) if st.session_state.metric in metric_options else 0
    st.session_state.metric = st.sidebar.selectbox("Select Metric", metric_options, index=metric_index, format_func=lambda x: x.replace('_', ' ').title())

    if st.session_state.analysis_mode == "National":
        selection_modes = ["Top N Carriers", "Custom Selection"]
        selection_index = selection_modes.index(st.session_state.selection_mode) if st.session_state.selection_mode in selection_modes else 0
        st.session_state.selection_mode = st.sidebar.radio("Selection Mode", selection_modes, index=selection_index)

        # Custom multi-select with search for carriers (preloaded from ranked list)
        if st.session_state.selection_mode == "Custom Selection":
            try:
                carrier_options = get_ranked_winners(ds_glob, st.session_state.filters)
            except Exception:
                carrier_options = []
            prev = st.session_state.selected_carriers if isinstance(st.session_state.selected_carriers, list) else []
            default_sel = [c for c in prev if c in carrier_options]
            st.session_state.selected_carriers = st.sidebar.multiselect(
                "Select carriers",
                options=carrier_options,
                default=default_sel,
                help="Type to search. Applies on RUN ANALYSIS."
            )

        st.session_state.show_other = st.sidebar.checkbox("Show 'Other' carriers", value=st.session_state.show_other)
        st.session_state.stacked = st.sidebar.checkbox("Stacked (fill) view", value=st.session_state.stacked)
        st.session_state.smoothing = st.sidebar.checkbox("Smoothing (rolling mean)", value=st.session_state.smoothing)
        st.session_state.show_markers = st.sidebar.checkbox("Show markers", value=st.session_state.show_markers)
        palette_options = ["Plotly", "D3", "G10", "Dark24", "Safe"]
        st.session_state.palette = st.sidebar.selectbox("Color palette", options=palette_options, index=palette_options.index(st.session_state.palette) if st.session_state.palette in palette_options else 3)
    else:
        # competitor mode
        # Primary / Competitors selection using ranked winners (filtered) for consistency with v2
        ranked_options = []
        try:
            ranked_options = get_ranked_winners(ds_glob, st.session_state.filters)
        except Exception:
            ranked_options = []
        primary_index = ranked_options.index(st.session_state.primary_carrier) if st.session_state.primary_carrier in ranked_options else 0
        st.session_state.primary_carrier = st.sidebar.selectbox("Select Primary Carrier", options=ranked_options, index=primary_index if primary_index < len(ranked_options) else 0)
        prev_comp = st.session_state.competitor_carrier if isinstance(st.session_state.competitor_carrier, list) else []
        default_comp = [c for c in prev_comp if c in ranked_options]
        st.session_state.competitor_carrier = st.sidebar.multiselect("Select Competitors (can select multiple)", options=ranked_options, default=default_comp)

        st.session_state.smoothing = st.sidebar.checkbox("Smoothing (rolling mean)", value=st.session_state.smoothing)
        st.session_state.show_markers = st.sidebar.checkbox("Show markers", value=st.session_state.show_markers)
        palette_options = ["Plotly", "D3", "G10", "Dark24", "Safe"]
        st.session_state.palette = st.sidebar.selectbox("Color palette", options=palette_options, index=palette_options.index(st.session_state.palette) if st.session_state.palette in palette_options else 3)

    # Filters
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔧 Filters")
    filter_columns = ['mover_ind', 'ds', 'state', 'dma_name']
    for col in filter_columns:
        try:
            options = get_distinct_options(ds_glob, col)
        except Exception:
            options = ["All"]
        current_value = st.session_state.filters.get(col, "All")
        if current_value not in options:
            current_value = "All"
        st.session_state.filters[col] = st.sidebar.selectbox(f"Filter by {col}", options=options, index=options.index(current_value))

    # Graph Window (date range)
    st.sidebar.subheader("🗓️ Graph Window")
    try:
        dmin, dmax = get_date_bounds(ds_glob, st.session_state.filters)
    except Exception:
        dmin, dmax = (pd.to_datetime('1970-01-01').date(), pd.to_datetime('1970-01-01').date())
    if 'graph_start' not in st.session_state or not st.session_state.get('graph_start'):
        st.session_state.graph_start = dmin
    if 'graph_end' not in st.session_state or not st.session_state.get('graph_end'):
        st.session_state.graph_end = dmax
    st.session_state.graph_start = st.sidebar.date_input("Start date", value=st.session_state.graph_start, min_value=dmin, max_value=dmax)
    st.session_state.graph_end = st.sidebar.date_input("End date", value=st.session_state.graph_end, min_value=dmin, max_value=dmax)
    if st.session_state.graph_start > st.session_state.graph_end:
        st.sidebar.error("Start date must be on or before End date.")

    # Run + Reset
    st.sidebar.markdown("---")
    st.sidebar.subheader("✨ Outliers")
    st.session_state.outlier_window = st.sidebar.slider("Outlier window (days)", min_value=7, max_value=60, value=st.session_state.outlier_window, step=1)
    st.session_state.outlier_z = st.sidebar.slider("Outlier z-score threshold", min_value=1.0, max_value=4.0, value=float(st.session_state.outlier_z), step=0.1)
    st.session_state.outlier_show = st.sidebar.radio("Show outliers", options=["All", "Positive only"], index=0 if st.session_state.outlier_show == 'All' else 1)
    st.sidebar.markdown("---")
    run_analysis = st.sidebar.button("🚀 RUN ANALYSIS", type="primary")
    if st.sidebar.button("Reset to defaults"):
        st.session_state.selected_carriers = []
        st.session_state.primary_carrier = None
        st.session_state.competitor_carrier = []
        st.session_state.top_n = 10
        st.session_state.filters = {}
        st.session_state.last_pdf = None
        st.session_state.applied_signature = None
        st.rerun()

    # Prepare data on RUN
    if run_analysis:
        # compute ranked list under filters
        ranked_filtered = get_ranked_winners(ds_glob, st.session_state.filters)
        # Defaults by filtered scope
        if st.session_state.analysis_mode == "National" and st.session_state.selection_mode == "Top N Carriers":
            st.session_state.top_n = min(10, len(ranked_filtered)) if ranked_filtered else 10
        if st.session_state.analysis_mode == "Competitor":
            if not st.session_state.primary_carrier or st.session_state.primary_carrier not in ranked_filtered:
                st.session_state.primary_carrier = ranked_filtered[0] if ranked_filtered else None
            comp = st.session_state.competitor_carrier
            if not isinstance(comp, list) or not comp:
                st.session_state.competitor_carrier = [c for c in ranked_filtered[1:10] if c != st.session_state.primary_carrier]
        # compute data now and store snapshot
        if st.session_state.analysis_mode == "Competitor":
            pdf = compute_competitor_pdf(
                ds_glob,
                st.session_state.filters,
                st.session_state.primary_carrier,
                st.session_state.competitor_carrier if isinstance(st.session_state.competitor_carrier, list) else [],
                st.session_state.metric,
                st.session_state.outlier_window,
                float(st.session_state.outlier_z),
                str(st.session_state.graph_start),
                str(st.session_state.graph_end),
            )
        else:
            if st.session_state.selection_mode == "Top N Carriers":
                ranked_filtered = get_ranked_winners(ds_glob, st.session_state.filters)
                selected_winners = ranked_filtered[: st.session_state.top_n]
            else:
                selected_winners = st.session_state.selected_carriers
            pdf = compute_national_pdf(
                ds_glob,
                st.session_state.filters,
                selected_winners,
                show_other=st.session_state.show_other,
                metric=st.session_state.metric,
                window=st.session_state.outlier_window,
                z_thresh=float(st.session_state.outlier_z),
                start_date=str(st.session_state.graph_start),
                end_date=str(st.session_state.graph_end),
            )
        st.session_state.last_pdf = pdf
        # capture applied signature
        try:
            sig = (
                st.session_state.analysis_mode,
                st.session_state.metric,
                st.session_state.selection_mode,
                st.session_state.top_n,
                tuple(st.session_state.selected_carriers) if isinstance(st.session_state.selected_carriers, list) else tuple(),
                st.session_state.primary_carrier,
                tuple(st.session_state.competitor_carrier) if isinstance(st.session_state.competitor_carrier, list) else tuple(),
                tuple(sorted((k, v) for k, v in st.session_state.filters.items())),
                st.session_state.show_other,
                st.session_state.smoothing,
                st.session_state.show_markers,
                st.session_state.palette,
                st.session_state.stacked,
                int(st.session_state.outlier_window),
                float(st.session_state.outlier_z),
                str(st.session_state.graph_start),
                str(st.session_state.graph_end),
            )
        except Exception:
            sig = None
        st.session_state.applied_signature = sig

    # Selection summary and plotting
    st.sidebar.markdown("---")
    # Sidebar selection summary mirroring v2
    if st.session_state.analysis_mode == "National":
        st.sidebar.write(f"**Total Selected:** {st.session_state.top_n if st.session_state.selection_mode == 'Top N Carriers' else len(st.session_state.selected_carriers)} carriers")
        try:
            total_in_filtered = len(get_ranked_winners(ds_glob, st.session_state.filters))
        except Exception:
            total_in_filtered = 0
        others = max(0, total_in_filtered - (st.session_state.top_n if st.session_state.selection_mode == 'Top N Carriers' else len(st.session_state.selected_carriers)))
        st.sidebar.write(f"**Others Aggregated:** {others} carriers")
    else:
        if st.session_state.primary_carrier and st.session_state.competitor_carrier:
            st.sidebar.write(f"**Matchup:** {st.session_state.primary_carrier} vs {', '.join(st.session_state.competitor_carrier)}")
    if any(v not in (None, 'All') for v in st.session_state.filters.values()):
        st.sidebar.write("**Active Filters:**")
        for k, v in st.session_state.filters.items():
            if v not in (None, 'All'):
                st.sidebar.write(f"• {k} = {v}")
    # Slightly narrower chart area compared to v2
    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader(f"{st.session_state.metric.replace('_', ' ').title()} Over Time")
        # Use last computed dataset unless RUN ANALYSIS was clicked
        pdf = st.session_state.last_pdf if isinstance(st.session_state.last_pdf, pd.DataFrame) else pd.DataFrame()

        fig = create_plot(
            pdf,
            st.session_state.metric,
            [f"{k}={v}" for k, v in st.session_state.filters.items() if v not in (None, 'All')],
            st.session_state.analysis_mode,
            primary=st.session_state.primary_carrier,
        )
        st.plotly_chart(fig, config={"responsive": True})

    # Indicate pending changes since last run
    try:
        current_sig = (
            st.session_state.analysis_mode,
            st.session_state.metric,
            st.session_state.selection_mode,
            st.session_state.top_n,
            tuple(st.session_state.selected_carriers) if isinstance(st.session_state.selected_carriers, list) else tuple(),
            st.session_state.primary_carrier,
            tuple(st.session_state.competitor_carrier) if isinstance(st.session_state.competitor_carrier, list) else tuple(),
            tuple(sorted((k, v) for k, v in st.session_state.filters.items())),
            st.session_state.show_other,
            st.session_state.smoothing,
            st.session_state.show_markers,
            st.session_state.palette,
            st.session_state.stacked,
            int(st.session_state.outlier_window),
            float(st.session_state.outlier_z),
            str(st.session_state.graph_start),
            str(st.session_state.graph_end),
        )
        if st.session_state.applied_signature and current_sig != st.session_state.applied_signature:
            st.info("Selections changed. Click RUN ANALYSIS to update the chart.")
    except Exception:
        pass

    with col2:
        st.subheader("📈 Summary")
        if st.session_state.analysis_mode == "National":
            if st.session_state.selection_mode == "Top N Carriers":
                ranked_filtered = get_ranked_winners(ds_glob, st.session_state.filters)
                st.metric("Top N", min(st.session_state.top_n, len(ranked_filtered)))
            else:
                st.metric("Selected Carriers", len(st.session_state.selected_carriers))
        else:
            st.write(f"Primary: {st.session_state.primary_carrier or 'None'}")
            st.write(f"Competitors: {', '.join(st.session_state.competitor_carrier) if st.session_state.competitor_carrier else 'None'}")

        # Small outliers table beneath the summary
        try:
            if isinstance(pdf, pd.DataFrame) and not pdf.empty and 'is_outlier' in pdf.columns:
                out = pdf[pdf['is_outlier'] == True].copy()
                if not out.empty:
                    if st.session_state.get('outlier_show', 'All') == 'Positive only':
                        out = out[out['zscore'] >= 0]
                    cols_pref = ['the_date', 'winner', st.session_state.metric, 'zscore', 'day_type']
                    cols = [c for c in cols_pref if c in out.columns]
                    out = out[cols].sort_values(['the_date', 'winner'], ascending=[True, False])
                    st.markdown("**Outliers (sorted by date, carrier desc)**")
                    st.dataframe(out.head(50), width='stretch')
        except Exception:
            pass

    # Optional: Top N preview in sidebar like v2
    if st.session_state.analysis_mode == "National" and st.session_state.selection_mode == "Top N Carriers":
        try:
            ranked_opts = get_ranked_winners(ds_glob, st.session_state.filters)
        except Exception:
            ranked_opts = []
        if ranked_opts:
            st.sidebar.write(f"**Top {min(st.session_state.top_n, len(ranked_opts))} Carriers:**")
            for i, c in enumerate(ranked_opts[: st.session_state.top_n], 1):
                st.sidebar.write(f"{i}. {c}")
        else:
            st.sidebar.write("Top carriers will be shown after you click RUN ANALYSIS.")

    # View Raw Data like v2
    with st.expander("📊 View Raw Data"):
        if 'pdf' in locals() and isinstance(pdf, pd.DataFrame) and not pdf.empty:
            pivot = pd.pivot_table(pdf, index='the_date', columns='winner', values=st.session_state.metric, aggfunc='first').fillna(0)
            st.dataframe(pivot, width='stretch')

    with st.expander("🔧 Debug"):
        st.write(f"Data dir: {store_dir}")
        st.write(f"Glob: {ds_glob}")


if __name__ == "__main__":
    main()
