import os
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objs as go

from tools import db
from tools.src import metrics, outliers


def get_default_db_path() -> str:
    """Get default database path"""
    return os.path.join(os.getcwd(), "data/databases/duck_suppression.db")


def init_session_state():
    if 'analysis_mode' not in st.session_state:
        st.session_state.analysis_mode = "National"
    if 'metric' not in st.session_state:
        st.session_state.metric = "win_share"
    if 'display_mode' not in st.session_state:
        st.session_state.display_mode = "share"  # 'share' or 'volume'
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
    display_mode = st.session_state.get('display_mode', 'share')

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

        # Build enhanced hover text with volume breakdown
        hover_text = []
        for idx, (d, r, s) in enumerate(zip(dates, series, smooth)):
            row_idx = cdf.index[idx]
            hover_parts = [f"<b>{carrier}</b>", f"Date: {d.date()}"]
            
            # For wins_per_loss, show actual wins and losses in tooltip
            if metric == 'wins_per_loss' and 'raw_wins' in cdf.columns and 'raw_losses' in cdf.columns:
                raw_w = int(cdf.loc[row_idx, 'raw_wins']) if pd.notna(cdf.loc[row_idx, 'raw_wins']) else 0
                raw_l = int(cdf.loc[row_idx, 'raw_losses']) if pd.notna(cdf.loc[row_idx, 'raw_losses']) else 0
                hover_parts.append(f"Wins: {raw_w:,}")
                hover_parts.append(f"Losses: {raw_l:,}")
                hover_parts.append(f"Ratio: {r:.3f}")
                if smoothing_on:
                    hover_parts.append(f"Smoothed: {s:.3f}")
            elif display_mode == 'volume' and metric in ['wins', 'losses']:
                # For volume mode, show count with comma formatting
                hover_parts.append(f"{metric.title()}: {int(r):,}")
                if smoothing_on:
                    hover_parts.append(f"Smoothed: {int(s):,}")
            else:
                # Standard display (shares)
                hover_parts.append(f"Value: {r:.6f}")
                if smoothing_on:
                    hover_parts.append(f"Smoothed: {s:.6f}")
            
            hover_text.append("<br>".join(hover_parts))

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
                    # Guard: metric column may be missing under rare cache paths; fall back to smoothed line
                    has_metric = metric in pos_out.columns
                    vals_pos = pd.to_numeric(pos_out[metric], errors='coerce') if has_metric else None
                    if zvals is not None and dayt is not None and has_metric:
                        hover_o_pos = [f"{carrier}<br>{d.date()}<br>{metric}: {v:.6f}<br>z: {z}<br>{dt}"
                                       for d, v, z, dt in zip(pos_out['the_date'], vals_pos.fillna(0), pos_out['zscore'].round(2).astype(str), pos_out['day_type'])]
                    elif has_metric:
                        hover_o_pos = [f"{carrier}<br>{d.date()}<br>{metric}: {v:.6f}" for d, v in zip(pos_out['the_date'], vals_pos.fillna(0))]
                    else:
                        hover_o_pos = [f"{carrier}<br>{d.date()}" for d in pos_out['the_date']]
                    y_marker_pos = (smooth.loc[pos_out.index] if isinstance(smooth, pd.Series) and smoothing_on else (vals_pos if has_metric else None))
                    fig.add_trace(go.Scatter(
                        x=pos_out['the_date'], y=y_marker_pos,
                        mode='markers', name=f"{carrier} outlier (+)",
                        marker=dict(symbol='star', color='yellow', size=11, line=dict(color='black', width=0.6), opacity=0.95),
                        hoverinfo='text', hovertext=hover_o_pos,
                        showlegend=False,
                    ))

                # Negative (minus signs)
                if not neg_out.empty:
                    has_metric_n = metric in neg_out.columns
                    vals_neg = pd.to_numeric(neg_out[metric], errors='coerce') if has_metric_n else None
                    if has_metric_n:
                        hover_o_neg = [f"{carrier}<br>{d.date()}<br>{metric}: {v:.6f}<br>z: {z}<br>{dt}"
                                       for d, v, z, dt in zip(neg_out['the_date'], vals_neg.fillna(0), neg_out['zscore'].round(2).astype(str), neg_out['day_type'])]
                    else:
                        hover_o_neg = [f"{carrier}<br>{d.date()}<br>z: {z}<br>{dt}"
                                       for d, z, dt in zip(neg_out['the_date'], neg_out['zscore'].round(2).astype(str), neg_out['day_type'])]
                    y_marker_neg = (smooth.loc[neg_out.index] if isinstance(smooth, pd.Series) and smoothing_on else (vals_neg if has_metric_n else None))
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
    
    # Update y-axis title based on display mode
    if display_mode == 'volume':
        if 'win' in metric:
            yaxis_title = "Wins (Volume)"
        elif 'loss' in metric:
            yaxis_title = "Losses (Volume)"
        else:
            yaxis_title = metric.replace('_', ' ').title()
    else:
        yaxis_title = metric.replace('_', ' ').title()
    
    fig.update_layout(
        title=dict(text=title, x=0.01, xanchor='left'),
        xaxis_title='Date',
        yaxis_title=yaxis_title,
        legend=dict(orientation='v', x=1.02, y=0.5),
        autosize=True,
        width=1200,
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
def get_date_bounds(db_path: str, filters: dict):
    """Get min/max dates from database for the filtered data"""
    try:
        # Build WHERE clause from filters
        where_parts = []
        
        if 'ds' in filters and filters['ds'] != 'All':
            safe_ds = filters['ds'].replace("'", "''")
            where_parts.append(f"ds = '{safe_ds}'")
            
        if 'mover_ind' in filters and filters['mover_ind'] != 'All':
            mover_val = 'TRUE' if filters['mover_ind'] == 'True' else 'FALSE'
            where_parts.append(f"mover_ind = {mover_val}")
            
        if 'state' in filters and filters['state'] != 'All':
            safe_state = filters['state'].replace("'", "''")
            where_parts.append(f"state = '{safe_state}'")
            
        if 'dma_name' in filters and filters['dma_name'] != 'All':
            safe_dma = filters['dma_name'].replace("'", "''")
            where_parts.append(f"dma_name = '{safe_dma}'")
        
        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        
        sql = f"""
        SELECT 
            MIN(the_date) as min_date,
            MAX(the_date) as max_date
        FROM carrier_data
        {where_clause}
        """
        
        result = db.query(sql, db_path)
        if result.empty or pd.isna(result['min_date'].iloc[0]):
            return (pd.to_datetime('1970-01-01').date(), pd.to_datetime('1970-01-01').date())
        
        return (
            pd.to_datetime(result['min_date'].iloc[0]).date(),
            pd.to_datetime(result['max_date'].iloc[0]).date()
        )
    except Exception as e:
        st.error(f"Error getting date bounds: {e}")
        return (pd.to_datetime('1970-01-01').date(), pd.to_datetime('1970-01-01').date())


@st.cache_data
def get_distinct_options(db_path: str, column: str, table: str = 'carrier_data'):
    """Get distinct values for a column"""
    try:
        # Special handling for boolean mover_ind column
        if column == 'mover_ind':
            return ["All", "True", "False"]
        
        values = db.get_distinct_values(column, table, db_path=db_path)
        return ["All"] + [str(v) for v in values if v]
    except Exception as e:
        st.error(f"Error getting options for {column}: {e}")
        return ["All"]


@st.cache_data
def get_ranked_winners(db_path: str, filters: dict):
    """Get carriers ranked by total wins"""
    try:
        # Build filter dict for query
        filter_params = {}
        if 'ds' in filters and filters['ds'] != 'All':
            filter_params['ds'] = filters['ds']
        if 'mover_ind' in filters and filters['mover_ind'] != 'All':
            filter_params['mover_ind'] = (filters['mover_ind'] == 'True')
        if 'state' in filters and filters['state'] != 'All':
            filter_params['state'] = filters['state']
        if 'dma_name' in filters and filters['dma_name'] != 'All':
            filter_params['dma_name'] = filters['dma_name']
        
        # Query database
        where_parts = []
        for key, val in filter_params.items():
            if isinstance(val, bool):
                where_parts.append(f"{key} = {val}")
            elif val:
                safe_val = str(val).replace("'", "''")
                where_parts.append(f"{key} = '{safe_val}'")
        
        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        
        sql = f"""
        SELECT winner, SUM(adjusted_wins) AS total_wins
        FROM carrier_data
        {where_clause}
        GROUP BY winner
        ORDER BY total_wins DESC
        """
        
        df = db.query(sql, db_path)
        return df['winner'].dropna().tolist()
    except Exception as e:
        st.error(f"Error getting ranked winners: {e}")
        return []


@st.cache_data
def compute_national_pdf(db_path: str, filters: dict, selected_winners: list, show_other: bool, metric: str, window: int, z_thresh: float, start_date: str, end_date: str, display_mode: str = "share") -> pd.DataFrame:
    """Compute national PDF with outliers using database"""
    if not selected_winners:
        return pd.DataFrame(columns=["the_date", "winner", metric])
    
    # Map volume metrics to database column names
    db_metric = metric
    if display_mode == "volume":
        if metric == "wins":
            db_metric = "win_share"  # We'll use total_wins column instead
        elif metric == "losses":
            db_metric = "loss_share"  # We'll use total_losses column instead
    
    _ds = filters.get('ds', 'gamoshi')
    _mover_ind = (filters.get('mover_ind', 'False') == 'True')
    _state = filters.get('state') if filters else None
    _dma = filters.get('dma_name') if filters else None
    
    # Get base metrics from database/cubes
    base = metrics.national_timeseries(
        ds=_ds,
        mover_ind=_mover_ind,
        start_date=start_date,
        end_date=end_date,
        state=_state,
        dma_name=_dma,
        db_path=db_path
    )
    
    if base.empty:
        return pd.DataFrame(columns=["the_date", "winner", metric])
    
    # Get outliers (use share-based metrics for outlier detection even in volume mode)
    outs = outliers.national_outliers(
        ds=_ds,
        mover_ind=_mover_ind,
        start_date=start_date,
        end_date=end_date,
        window=window,
        z_thresh=z_thresh,
        state=_state,
        dma_name=_dma,
        metric=db_metric,  # Use share-based metric for outlier detection
        db_path=db_path
    )
    
    # Merge base and outliers
    if not outs.empty:
        pdf = base.merge(outs[['the_date', 'winner', 'z', 'nat_outlier_pos']],
                        on=['the_date', 'winner'], how='left')
        pdf['is_outlier'] = pdf['nat_outlier_pos'].fillna(False)
        pdf['zscore'] = pdf['z'].fillna(0)
    else:
        pdf = base.copy()
        pdf['is_outlier'] = False
        pdf['zscore'] = 0
    
    # Add raw_wins and raw_losses columns for tooltips (for wins_per_loss metric)
    if 'total_wins' in pdf.columns:
        pdf['raw_wins'] = pdf['total_wins']
    if 'total_losses' in pdf.columns:
        pdf['raw_losses'] = pdf['total_losses']
    
    # Map volume metrics to the correct column names
    if display_mode == "volume":
        if metric == "wins" and 'total_wins' in pdf.columns:
            pdf[metric] = pdf['total_wins']
        elif metric == "losses" and 'total_losses' in pdf.columns:
            pdf[metric] = pdf['total_losses']
    
    # Filter to selected winners
    pdf = pdf[pdf['winner'].isin(selected_winners)].copy()
    
    # Add "Other" aggregation if needed
    if show_other and not pdf.empty:
        all_winners = base['winner'].unique()
        other_winners = [w for w in all_winners if w not in selected_winners]
        if other_winners:
            other_data = base[base['winner'].isin(other_winners)].copy()
            if not other_data.empty:
                # For volume mode, aggregate the raw counts
                if display_mode == "volume":
                    if metric == "wins" and 'total_wins' in other_data.columns:
                        other_agg = other_data.groupby('the_date')['total_wins'].sum().reset_index()
                        other_agg.rename(columns={'total_wins': metric}, inplace=True)
                    elif metric == "losses" and 'total_losses' in other_data.columns:
                        other_agg = other_data.groupby('the_date')['total_losses'].sum().reset_index()
                        other_agg.rename(columns={'total_losses': metric}, inplace=True)
                    else:
                        other_agg = other_data.groupby('the_date')[db_metric].sum().reset_index()
                        if metric not in other_agg.columns:
                            other_agg.rename(columns={db_metric: metric}, inplace=True)
                else:
                    other_agg = other_data.groupby('the_date')[metric].sum().reset_index()
                
                other_agg['winner'] = 'Other'
                other_agg['is_outlier'] = False
                other_agg['zscore'] = 0
                # Add raw wins/losses for Other as well
                if 'total_wins' in other_data.columns:
                    other_agg['raw_wins'] = other_data.groupby('the_date')['total_wins'].sum().values
                if 'total_losses' in other_data.columns:
                    other_agg['raw_losses'] = other_data.groupby('the_date')['total_losses'].sum().values
                pdf = pd.concat([pdf, other_agg], ignore_index=True)
    
    return pdf


@st.cache_data
def compute_competitor_pdf(db_path: str, filters: dict, primary: str, competitors: list, metric: str, window: int, z_thresh: float, start_date: str, end_date: str, display_mode: str = "share") -> pd.DataFrame:
    """Compute competitor PDF with outliers using database"""
    if not competitors:
        return pd.DataFrame(columns=["the_date", "competitor", metric])
    
    # Map volume metrics to database column names for outlier detection
    db_metric = metric
    if display_mode == "volume":
        if metric == "wins":
            db_metric = "win_share"
        elif metric == "losses":
            db_metric = "loss_share"
    
    _ds = filters.get('ds', 'gamoshi')
    _mover_ind = (filters.get('mover_ind', 'False') == 'True')
    _state = filters.get('state') if filters else None
    _dma = filters.get('dma_name') if filters else None
    
    # Get competitor view from database/cubes
    base = metrics.competitor_view(
        ds=_ds,
        mover_ind=_mover_ind,
        start_date=start_date,
        end_date=end_date,
        primary=primary,
        competitors=competitors,
        state=_state,
        dma_name=_dma,
        db_path=db_path
    )
    
    if base.empty:
        return pd.DataFrame(columns=["the_date", "competitor", metric])
    
    # Store raw wins and losses for tooltips
    base['raw_wins'] = base['h2h_wins']
    base['raw_losses'] = base['h2h_losses']
    
    # Calculate metrics (both share and volume)
    base['win_share'] = base['h2h_wins'] / base['primary_total_wins'].replace(0, pd.NA)
    base['loss_share'] = base['h2h_losses'] / base['primary_total_losses'].replace(0, pd.NA)
    base['wins_per_loss'] = base['h2h_wins'] / base['h2h_losses'].replace(0, pd.NA)
    
    # For volume mode, add volume columns
    if display_mode == "volume":
        base['wins'] = base['h2h_wins']
        base['losses'] = base['h2h_losses']
    
    # Compute outliers inline (day-type grouped z-score) - always use share-based for detection
    def _z_for_group(g):
        g = g.sort_values('the_date')
        g['day_type'] = g['the_date'].apply(lambda d: 'Sat' if pd.Timestamp(d).weekday() == 5
                                            else 'Sun' if pd.Timestamp(d).weekday() == 6
                                            else 'Weekday')
        g['zscore'] = 0.0
        g['is_outlier'] = False
        
        for dt in g['day_type'].unique():
            mask = g['day_type'] == dt
            vals = g.loc[mask, db_metric]  # Use share-based metric for outlier detection
            if len(vals) > 1:
                mu = vals.shift(1).rolling(window, min_periods=1).mean()
                sigma = vals.shift(1).rolling(window, min_periods=1).std()
                z = (vals - mu) / sigma.replace(0, pd.NA)
                g.loc[mask, 'zscore'] = z.fillna(0)
                g.loc[mask, 'is_outlier'] = (z.abs() > z_thresh)
        
        return g
    
    if 'competitor' in base.columns:
        pdf = base.groupby('competitor', group_keys=False).apply(_z_for_group).reset_index(drop=True)
    else:
        pdf = base.copy()
        pdf['zscore'] = 0
        pdf['is_outlier'] = False
    
    # Rename competitor to winner for consistency
    if 'competitor' in pdf.columns:
        pdf['winner'] = pdf['competitor']
    
    return pdf


def main():
    st.set_page_config(page_title="Carrier Win/Loss Share (DuckDB)", page_icon="🦆", layout="wide")
    st.title("🦆 Carrier Win/Loss Share Dashboard (DuckDB)")
    st.markdown("---")

    init_session_state()

    # Data source
    st.title("📊 Carrier Performance Dashboard")

    # Sidebar: Database path
    st.sidebar.header("📦 Database")
    db_path = st.sidebar.text_input("Database path", value=get_default_db_path())
    
    # Verify database exists
    if not os.path.exists(db_path):
        st.error(f"Database not found: {db_path}. Run: uv run build_suppression_db.py <preagg.parquet>")
        st.stop()

    # Controls
    st.sidebar.header("📋 Dashboard Controls")
    st.session_state.analysis_mode = st.sidebar.selectbox("Analysis Mode", ["National", "Competitor"], index=0 if st.session_state.analysis_mode == "National" else 1)

    # Volume vs Share toggle - MOVED BEFORE METRIC SELECTION
    st.session_state.display_mode = st.sidebar.radio(
        "Display Mode", 
        ["share", "volume"], 
        index=0 if st.session_state.display_mode == "share" else 1,
        format_func=lambda x: "Share (%)" if x == "share" else "Volume (Count)",
        help="Share shows percentage, Volume shows actual win/loss counts"
    )
    
    # Metric selection - changes based on display mode
    if st.session_state.display_mode == "volume":
        metric_options = ["wins", "losses", "wins_per_loss"]
        metric_display_names = {"wins": "Wins", "losses": "Losses", "wins_per_loss": "Wins Per Loss"}
    else:
        metric_options = ["win_share", "loss_share", "wins_per_loss"]
        metric_display_names = {"win_share": "Win Share", "loss_share": "Loss Share", "wins_per_loss": "Wins Per Loss"}
    
    # Map current metric to new format if display mode changed
    current_metric = st.session_state.metric
    if st.session_state.display_mode == "volume" and current_metric in ["win_share", "loss_share"]:
        # Convert share metrics to volume metrics
        if current_metric == "win_share":
            current_metric = "wins"
        elif current_metric == "loss_share":
            current_metric = "losses"
    elif st.session_state.display_mode == "share" and current_metric in ["wins", "losses"]:
        # Convert volume metrics to share metrics
        if current_metric == "wins":
            current_metric = "win_share"
        elif current_metric == "losses":
            current_metric = "loss_share"
    
    metric_index = metric_options.index(current_metric) if current_metric in metric_options else 0
    st.session_state.metric = st.sidebar.selectbox(
        "Select Metric", 
        metric_options, 
        index=metric_index, 
        format_func=lambda x: metric_display_names.get(x, x.replace('_', ' ').title())
    )

    # Collapsible sections for better organization
    with st.sidebar.expander("🎯 Carrier Selection", expanded=True):
        if st.session_state.analysis_mode == "National":
            selection_modes = ["Top N Carriers", "Custom Selection"]
            selection_index = selection_modes.index(st.session_state.selection_mode) if st.session_state.selection_mode in selection_modes else 0
            st.session_state.selection_mode = st.radio("Selection Mode", selection_modes, index=selection_index)

            # Top N slider when in Top N mode
            if st.session_state.selection_mode == "Top N Carriers":
                st.session_state.top_n = st.slider(
                    "Top N", 
                    min_value=3, 
                    max_value=50, 
                    value=st.session_state.top_n, 
                    step=1,
                    help="Number of top carriers to display"
                )
            
            # Custom multi-select with search for carriers (preloaded from ranked list)
            if st.session_state.selection_mode == "Custom Selection":
                try:
                    carrier_options = get_ranked_winners(db_path, st.session_state.filters)
                except Exception:
                    carrier_options = []
                prev = st.session_state.selected_carriers if isinstance(st.session_state.selected_carriers, list) else []
                default_sel = [c for c in prev if c in carrier_options]
                st.session_state.selected_carriers = st.multiselect(
                    "Select carriers",
                    options=carrier_options,
                    default=default_sel,
                    help="Type to search. Applies on RUN ANALYSIS."
                )
        else:
            # competitor mode
            # Primary / Competitors selection using ranked winners (filtered) for consistency with v2
            ranked_options = []
            try:
                ranked_options = get_ranked_winners(db_path, st.session_state.filters)
            except Exception:
                ranked_options = []
            primary_index = ranked_options.index(st.session_state.primary_carrier) if st.session_state.primary_carrier in ranked_options else 0
            st.session_state.primary_carrier = st.selectbox("Select Primary Carrier", options=ranked_options, index=primary_index if primary_index < len(ranked_options) else 0)
            prev_comp = st.session_state.competitor_carrier if isinstance(st.session_state.competitor_carrier, list) else []
            default_comp = [c for c in prev_comp if c in ranked_options]
            st.session_state.competitor_carrier = st.multiselect("Select Competitors (can select multiple)", options=ranked_options, default=default_comp)

    with st.sidebar.expander("🎨 Display Settings"):
        if st.session_state.analysis_mode == "National":
            st.session_state.show_other = st.checkbox("Show 'Other' carriers", value=st.session_state.show_other)
            st.session_state.stacked = st.checkbox("Stacked (fill) view", value=st.session_state.stacked)
        st.session_state.smoothing = st.checkbox("Smoothing (rolling mean)", value=st.session_state.smoothing)
        st.session_state.show_markers = st.checkbox("Show markers", value=st.session_state.show_markers)
        palette_options = ["Plotly", "D3", "G10", "Dark24", "Safe"]
        st.session_state.palette = st.selectbox("Color palette", options=palette_options, index=palette_options.index(st.session_state.palette) if st.session_state.palette in palette_options else 3)

    # Filters
    with st.sidebar.expander("🔧 Filters", expanded=True):
        filter_columns = ['mover_ind', 'ds', 'state', 'dma_name']
        for col in filter_columns:
            try:
                options = get_distinct_options(db_path, col)
            except Exception:
                options = ["All"]
            current_value = st.session_state.filters.get(col, "All")
            if current_value not in options:
                current_value = "All"
            st.session_state.filters[col] = st.selectbox(f"{col}", options=options, index=options.index(current_value), help=f"Filter data by {col}")

    # Graph Window (date range)
    with st.sidebar.expander("🗓️ Graph Window"):
        try:
            dmin, dmax = get_date_bounds(db_path, st.session_state.filters)
        except Exception:
            dmin, dmax = (pd.to_datetime('1970-01-01').date(), pd.to_datetime('1970-01-01').date())
        
        # Initialize or clamp session state values to valid range
        if 'graph_start' not in st.session_state or not st.session_state.get('graph_start'):
            st.session_state.graph_start = dmin
        else:
            # Clamp to valid range if outside bounds
            if st.session_state.graph_start < dmin:
                st.session_state.graph_start = dmin
            elif st.session_state.graph_start > dmax:
                st.session_state.graph_start = dmax
        
        if 'graph_end' not in st.session_state or not st.session_state.get('graph_end'):
            st.session_state.graph_end = dmax
        else:
            # Clamp to valid range if outside bounds
            if st.session_state.graph_end < dmin:
                st.session_state.graph_end = dmin
            elif st.session_state.graph_end > dmax:
                st.session_state.graph_end = dmax
        
        st.session_state.graph_start = st.date_input("Start date", value=st.session_state.graph_start, min_value=dmin, max_value=dmax)
        st.session_state.graph_end = st.date_input("End date", value=st.session_state.graph_end, min_value=dmin, max_value=dmax)
        if st.session_state.graph_start > st.session_state.graph_end:
            st.error("Start date must be on or before End date.")

    # Outliers
    with st.sidebar.expander("✨ Outliers"):
        st.session_state.outlier_window = st.slider("Outlier window (days)", min_value=7, max_value=60, value=st.session_state.outlier_window, step=1, help="Rolling window size for outlier detection")
        st.session_state.outlier_z = st.slider("Outlier z-score threshold", min_value=1.0, max_value=4.0, value=float(st.session_state.outlier_z), step=0.1, help="Z-score threshold for flagging outliers")
        st.session_state.outlier_show = st.radio("Show outliers", options=["All", "Positive only"], index=0 if st.session_state.outlier_show == 'All' else 1, help="Filter outlier markers on chart")
    
    # Run + Reset
    st.sidebar.markdown("---")
    run_analysis = st.sidebar.button("🚀 RUN ANALYSIS", type="primary")
    
    # Validate required filters before running analysis
    if run_analysis:
        if st.session_state.filters.get('ds') == 'All' or not st.session_state.filters.get('ds'):
            st.error("⚠️ Please select a specific dataset (ds). 'All' is not currently supported for cube queries.")
            st.info("💡 To enable 'All' support, run: `uv run build_cubes_in_db.py --all --aggregate`")
            run_analysis = False
        elif st.session_state.filters.get('mover_ind') == 'All' or not st.session_state.filters.get('mover_ind'):
            st.error("⚠️ Please select either 'True' or 'False' for mover_ind. 'All' is not currently supported.")
            run_analysis = False
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
        ranked_filtered = get_ranked_winners(db_path, st.session_state.filters)
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
                db_path,
                st.session_state.filters,
                st.session_state.primary_carrier,
                st.session_state.competitor_carrier if isinstance(st.session_state.competitor_carrier, list) else [],
                st.session_state.metric,
                st.session_state.outlier_window,
                float(st.session_state.outlier_z),
                str(st.session_state.graph_start),
                str(st.session_state.graph_end),
                display_mode=st.session_state.display_mode,
            )
        else:
            if st.session_state.selection_mode == "Top N Carriers":
                ranked_filtered = get_ranked_winners(db_path, st.session_state.filters)
                selected_winners = ranked_filtered[: st.session_state.top_n]
            else:
                selected_winners = st.session_state.selected_carriers
            pdf = compute_national_pdf(
                db_path,
                st.session_state.filters,
                selected_winners,
                show_other=st.session_state.show_other,
                metric=st.session_state.metric,
                window=st.session_state.outlier_window,
                z_thresh=float(st.session_state.outlier_z),
                start_date=str(st.session_state.graph_start),
                end_date=str(st.session_state.graph_end),
                display_mode=st.session_state.display_mode,
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
                st.session_state.display_mode,
            )
        except Exception:
            sig = None
        st.session_state.applied_signature = sig

    # Selection summary and plotting
    st.sidebar.markdown("---")
    # Updated layout: 12:1.5 ratio for graph vs summary (much wider graph)
    col1, col2 = st.columns([12, 1.5])
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
        st.plotly_chart(fig, config={'displayModeBar': True, 'displaylogo': False})

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
            st.session_state.display_mode,
        )
        if st.session_state.applied_signature and current_sig != st.session_state.applied_signature:
            st.info("Selections changed. Click RUN ANALYSIS to update the chart.")
    except Exception:
        pass

    with col2:
        # Compact summary panel with smaller font
        st.markdown("<style>.small-font { font-size:11px; }</style>", unsafe_allow_html=True)
        st.markdown("### 📈")
        if st.session_state.analysis_mode == "National":
            if st.session_state.selection_mode == "Top N Carriers":
                ranked_filtered = get_ranked_winners(db_path, st.session_state.filters)
                st.markdown(f"<p class='small-font'><b>Top N:</b> {min(st.session_state.top_n, len(ranked_filtered))}</p>", unsafe_allow_html=True)
            else:
                st.markdown(f"<p class='small-font'><b>Selected:</b> {len(st.session_state.selected_carriers)}</p>", unsafe_allow_html=True)
        else:
            st.markdown(f"<p class='small-font'><b>Primary:</b><br/>{st.session_state.primary_carrier or 'None'}</p>", unsafe_allow_html=True)
            st.markdown("<p class='small-font'><b>Competitors:</b></p>", unsafe_allow_html=True)
            if st.session_state.competitor_carrier:
                for comp in st.session_state.competitor_carrier:
                    st.markdown(f"<p class='small-font'>• {comp}</p>", unsafe_allow_html=True)
            else:
                st.markdown("<p class='small-font'>• None</p>", unsafe_allow_html=True)

    # Outliers table - FULL WIDTH UNDER THE GRAPH
    try:
        if isinstance(pdf, pd.DataFrame) and not pdf.empty and 'is_outlier' in pdf.columns:
            out = pdf[pdf['is_outlier'] == True].copy()
            if not out.empty:
                if st.session_state.get('outlier_show', 'All') == 'Positive only':
                    out = out[out['zscore'] >= 0]
                cols_pref = ['the_date', 'winner', st.session_state.metric, 'zscore', 'day_type']
                cols = [c for c in cols_pref if c in out.columns]
                out = out[cols].sort_values(['the_date', 'winner'], ascending=[True, False])
                st.markdown("---")
                st.markdown("**📊 Outliers (sorted by date, carrier desc)**")
                st.dataframe(out.head(50), width='stretch')
    except Exception:
        pass

    # View Raw Data like v2
    with st.expander("📊 View Raw Data"):
        if 'pdf' in locals() and isinstance(pdf, pd.DataFrame) and not pdf.empty:
            pivot = pd.pivot_table(pdf, index='the_date', columns='winner', values=st.session_state.metric, aggfunc='first').fillna(0)
            st.dataframe(pivot, width='stretch')

    with st.expander("🔧 Debug"):
        st.write(f"Database: {db_path}")


if __name__ == "__main__":
    main()
