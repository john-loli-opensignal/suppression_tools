#!/usr/bin/env python3
"""
Census Block Outlier Detection Dashboard (POC)

Hierarchical outlier detection:
  National (ds, mover_ind) → H2H matchups → State → DMA → Census Block

Use Cases:
  1. Outlier Detection Hierarchy
  2. Quality Assurance at source level
  3. Fraud Detection with geo-spatial anomaly detection
"""
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Database configuration
DB_PATH = "data/databases/duck_suppression.db"

st.set_page_config(
    page_title="Census Block Outlier Detection",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# Database Query Functions
# ============================================================================

@st.cache_resource
def get_db_connection():
    """Get cached database connection."""
    return duckdb.connect(DB_PATH, read_only=True)

@st.cache_data(ttl=300)
def get_available_datasets(_con):
    """Get list of datasets with census block cubes."""
    try:
        result = _con.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name LIKE '%_census_cube'
        """).fetchall()
        
        # Extract unique dataset names
        datasets = set()
        for (table_name,) in result:
            # Format: {ds}_{win/loss}_{mover/non_mover}_census_cube
            parts = table_name.split('_')
            if len(parts) >= 4:
                ds = '_'.join(parts[:-3])  # Everything before _win/loss_mover/non_mover_census_cube
                datasets.add(ds)
        
        return sorted(list(datasets))
    except Exception as e:
        st.error(f"Error getting datasets: {e}")
        return []

@st.cache_data(ttl=300)
def get_national_stats(_con, ds, mover_ind, metric_type):
    """Get national-level statistics for initial outlier screening."""
    mover_str = 'mover' if mover_ind else 'non_mover'
    table = f"{ds}_{metric_type}_{mover_str}_census_cube"
    
    sql = f"""
    SELECT 
        winner,
        loser,
        COUNT(DISTINCT census_blockid) as unique_blocks,
        COUNT(DISTINCT state) as unique_states,
        COUNT(DISTINCT dma_name) as unique_dmas,
        SUM(total_{metric_type}s) as total_metric,
        AVG(total_{metric_type}s) as avg_metric,
        STDDEV(total_{metric_type}s) as stddev_metric,
        MAX(total_{metric_type}s) as max_metric,
        MIN(total_{metric_type}s) as min_metric,
        COUNT(*) as total_records
    FROM {table}
    GROUP BY winner, loser
    ORDER BY total_metric DESC
    """
    
    return _con.execute(sql).fetchdf()

@st.cache_data(ttl=300)
def get_h2h_timeseries(_con, ds, mover_ind, metric_type, winner, loser):
    """Get time series for a specific H2H matchup."""
    mover_str = 'mover' if mover_ind else 'non_mover'
    table = f"{ds}_{metric_type}_{mover_str}_census_cube"
    
    sql = f"""
    SELECT 
        the_date,
        SUM(total_{metric_type}s) as total_metric,
        COUNT(DISTINCT census_blockid) as unique_blocks,
        COUNT(*) as record_count
    FROM {table}
    WHERE winner = ? AND loser = ?
    GROUP BY the_date
    ORDER BY the_date
    """
    
    return _con.execute(sql, [winner, loser]).fetchdf()

@st.cache_data(ttl=300)
def get_state_breakdown(_con, ds, mover_ind, metric_type, winner, loser):
    """Get state-level breakdown for H2H matchup."""
    mover_str = 'mover' if mover_ind else 'non_mover'
    table = f"{ds}_{metric_type}_{mover_str}_census_cube"
    
    sql = f"""
    SELECT 
        state,
        COUNT(DISTINCT census_blockid) as unique_blocks,
        COUNT(DISTINCT dma_name) as unique_dmas,
        SUM(total_{metric_type}s) as total_metric,
        AVG(total_{metric_type}s) as avg_metric,
        STDDEV(total_{metric_type}s) as stddev_metric,
        MAX(total_{metric_type}s) as max_metric,
        COUNT(*) as record_count
    FROM {table}
    WHERE winner = ? AND loser = ?
    GROUP BY state
    ORDER BY total_metric DESC
    """
    
    return _con.execute(sql, [winner, loser]).fetchdf()

@st.cache_data(ttl=300)
def get_dma_breakdown(_con, ds, mover_ind, metric_type, winner, loser, state=None):
    """Get DMA-level breakdown."""
    mover_str = 'mover' if mover_ind else 'non_mover'
    table = f"{ds}_{metric_type}_{mover_str}_census_cube"
    
    where_clause = "WHERE winner = ? AND loser = ?"
    params = [winner, loser]
    
    if state:
        where_clause += " AND state = ?"
        params.append(state)
    
    sql = f"""
    SELECT 
        dma_name,
        state,
        COUNT(DISTINCT census_blockid) as unique_blocks,
        SUM(total_{metric_type}s) as total_metric,
        AVG(total_{metric_type}s) as avg_metric,
        STDDEV(total_{metric_type}s) as stddev_metric,
        MAX(total_{metric_type}s) as max_metric,
        COUNT(*) as record_count
    FROM {table}
    {where_clause}
    GROUP BY dma_name, state
    ORDER BY total_metric DESC
    """
    
    return _con.execute(sql, params).fetchdf()

@st.cache_data(ttl=300)
def get_census_block_breakdown(_con, ds, mover_ind, metric_type, winner, loser, state=None, dma=None):
    """Get census block-level breakdown - the finest granularity."""
    mover_str = 'mover' if mover_ind else 'non_mover'
    table = f"{ds}_{metric_type}_{mover_str}_census_cube"
    
    where_clause = "WHERE winner = ? AND loser = ?"
    params = [winner, loser]
    
    if state:
        where_clause += " AND state = ?"
        params.append(state)
    
    if dma:
        where_clause += " AND dma_name = ?"
        params.append(dma)
    
    sql = f"""
    SELECT 
        census_blockid,
        state,
        dma_name,
        the_date,
        total_{metric_type}s as metric_value,
        record_count
    FROM {table}
    {where_clause}
    ORDER BY metric_value DESC, the_date DESC
    LIMIT 1000
    """
    
    return _con.execute(sql, params).fetchdf()

@st.cache_data(ttl=300)
def detect_census_block_outliers(_con, ds, mover_ind, metric_type, winner, loser, threshold_std=3.0):
    """
    Detect outlier census blocks using statistical methods.
    Returns blocks where metrics exceed threshold_std standard deviations.
    """
    mover_str = 'mover' if mover_ind else 'non_mover'
    table = f"{ds}_{metric_type}_{mover_str}_census_cube"
    
    sql = f"""
    WITH block_stats AS (
        SELECT 
            census_blockid,
            state,
            dma_name,
            SUM(total_{metric_type}s) as total_metric,
            COUNT(*) as record_count,
            COUNT(DISTINCT the_date) as days_active
        FROM {table}
        WHERE winner = ? AND loser = ?
        GROUP BY census_blockid, state, dma_name
    ),
    global_stats AS (
        SELECT 
            AVG(total_metric) as mean_metric,
            STDDEV(total_metric) as stddev_metric
        FROM block_stats
    )
    SELECT 
        b.*,
        g.mean_metric,
        g.stddev_metric,
        (b.total_metric - g.mean_metric) / NULLIF(g.stddev_metric, 0) as z_score,
        b.total_metric / NULLIF(b.days_active, 0) as avg_daily_metric
    FROM block_stats b
    CROSS JOIN global_stats g
    WHERE ABS((b.total_metric - g.mean_metric) / NULLIF(g.stddev_metric, 0)) > ?
    ORDER BY ABS((b.total_metric - g.mean_metric) / NULLIF(g.stddev_metric, 0)) DESC
    """
    
    return _con.execute(sql, [winner, loser, threshold_std]).fetchdf()

# ============================================================================
# UI Helper Functions
# ============================================================================

def calculate_outlier_score(value, mean, stddev):
    """Calculate z-score for outlier detection."""
    if stddev == 0:
        return 0
    return abs((value - mean) / stddev)

def flag_outliers(df, column, threshold=3.0):
    """Add outlier flag column based on z-score."""
    if df.empty or column not in df.columns:
        return df
    
    mean = df[column].mean()
    std = df[column].std()
    
    df['z_score'] = df[column].apply(lambda x: calculate_outlier_score(x, mean, std))
    df['is_outlier'] = df['z_score'] > threshold
    
    return df

# ============================================================================
# Main Dashboard
# ============================================================================

def main():
    st.title("🔬 Census Block Outlier Detection Dashboard (POC)")
    st.markdown("""
    **Hierarchical outlier detection from national level down to individual census blocks**
    
    Drill-down path: **National → H2H → State → DMA → Census Block**
    """)
    
    # Get database connection
    try:
        con = get_db_connection()
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        st.info(f"Ensure {DB_PATH} exists. Run `build_census_block_cubes.py` first.")
        return
    
    # ========================================================================
    # Sidebar: Configuration
    # ========================================================================
    
    st.sidebar.header("🔍 Detection Parameters")
    
    # Dataset selection
    datasets = get_available_datasets(con)
    if not datasets:
        st.error("No census block cubes found. Run `build_census_block_cubes.py` first.")
        return
    
    ds = st.sidebar.selectbox("Dataset", datasets, key="ds_select")
    
    if not ds:
        st.warning("⚠️ Please select a dataset")
        return
    
    # Mover indicator
    mover_ind = st.sidebar.radio(
        "Mover Type",
        [True, False],
        format_func=lambda x: "Movers" if x else "Non-Movers",
        key="mover_select"
    )
    
    # Metric type
    metric_type = st.sidebar.radio(
        "Metric Type",
        ["win", "loss"],
        format_func=lambda x: x.title() + "s",
        key="metric_select"
    )
    
    # Outlier threshold
    outlier_threshold = st.sidebar.slider(
        "Outlier Z-Score Threshold",
        min_value=1.0,
        max_value=5.0,
        value=3.0,
        step=0.5,
        help="Number of standard deviations for outlier detection"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Hierarchy")
    st.sidebar.markdown("""
    1. **National**: Overview by carrier
    2. **H2H**: Head-to-head matchups
    3. **State**: Geographic patterns
    4. **DMA**: Market-level analysis
    5. **Census Block**: Pinpoint locations
    """)
    
    # ========================================================================
    # Level 1: National Overview
    # ========================================================================
    
    st.header("📊 Level 1: National Overview")
    st.markdown(f"**Dataset:** {ds} | **Type:** {'Movers' if mover_ind else 'Non-Movers'} | **Metric:** {metric_type.title()}s")
    
    with st.spinner("Loading national statistics..."):
        national_df = get_national_stats(con, ds, mover_ind, metric_type)
    
    if national_df.empty:
        st.warning("No data available")
        return
    
    # Flag outliers
    national_df = flag_outliers(national_df, 'total_metric', outlier_threshold)
    
    # Display top carriers
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Top H2H Matchups")
        display_cols = ['winner', 'loser', 'total_metric', 'unique_blocks', 'unique_states', 'unique_dmas', 'is_outlier']
        if 'z_score' in national_df.columns:
            display_cols.append('z_score')
        
        # Color code outliers
        styled_df = national_df[display_cols].head(20).style.applymap(
            lambda x: 'background-color: #ffcccc' if x else '',
            subset=['is_outlier']
        )
        st.dataframe(styled_df, width="stretch")
    
    with col2:
        st.metric("Total H2H Pairs", len(national_df))
        outlier_count = national_df['is_outlier'].sum() if 'is_outlier' in national_df.columns else 0
        st.metric("Outlier Pairs", outlier_count)
        st.metric("Outlier %", f"{100 * outlier_count / len(national_df):.1f}%")
    
    # ========================================================================
    # Level 2: H2H Selection & Time Series
    # ========================================================================
    
    st.header("🥊 Level 2: Head-to-Head Analysis")
    
    col1, col2 = st.columns(2)
    with col1:
        winner = st.selectbox(
            "Winner",
            options=sorted(national_df['winner'].unique()),
            key="winner_select"
        )
    
    with col2:
        # Filter losers for selected winner
        available_losers = national_df[national_df['winner'] == winner]['loser'].unique()
        loser = st.selectbox(
            "Loser",
            options=sorted(available_losers),
            key="loser_select"
        )
    
    if winner and loser:
        # Time series
        ts_df = get_h2h_timeseries(con, ds, mover_ind, metric_type, winner, loser)
        
        if not ts_df.empty:
            fig = px.line(
                ts_df,
                x='the_date',
                y='total_metric',
                title=f"{winner} vs {loser} - {metric_type.title()}s Over Time",
                markers=True
            )
            fig.update_layout(xaxis_title="Date", yaxis_title=f"Total {metric_type.title()}s")
            st.plotly_chart(fig, width="stretch")
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total", f"{ts_df['total_metric'].sum():,.0f}")
            col2.metric("Avg Daily", f"{ts_df['total_metric'].mean():,.1f}")
            col3.metric("Max Daily", f"{ts_df['total_metric'].max():,.0f}")
            col4.metric("Unique Blocks", f"{ts_df['unique_blocks'].sum():,}")
        
        # ====================================================================
        # Level 3: State Breakdown
        # ====================================================================
        
        st.header("🗺️ Level 3: State Analysis")
        
        state_df = get_state_breakdown(con, ds, mover_ind, metric_type, winner, loser)
        
        if not state_df.empty:
            state_df = flag_outliers(state_df, 'total_metric', outlier_threshold)
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.subheader("State Breakdown")
                display_cols = ['state', 'total_metric', 'unique_blocks', 'unique_dmas', 'avg_metric', 'max_metric', 'is_outlier']
                if 'z_score' in state_df.columns:
                    display_cols.append('z_score')
                
                styled_state = state_df[display_cols].style.applymap(
                    lambda x: 'background-color: #ffcccc' if x else '',
                    subset=['is_outlier']
                )
                st.dataframe(styled_state, width="stretch", height=400)
            
            with col2:
                # Choropleth map (if we want to add later)
                outlier_states = state_df[state_df['is_outlier'] == True]['state'].tolist()
                st.metric("Total States", len(state_df))
                st.metric("Outlier States", len(outlier_states))
                
                if outlier_states:
                    st.warning("Outlier States:")
                    for state in outlier_states[:10]:
                        st.text(f"  • {state}")
        
        # ====================================================================
        # Level 4: DMA Breakdown
        # ====================================================================
        
        st.header("📍 Level 4: DMA (Market) Analysis")
        
        # Optional state filter
        selected_state = st.selectbox(
            "Filter by State (optional)",
            options=['All States'] + sorted(state_df['state'].unique().tolist()) if not state_df.empty else ['All States'],
            key="state_filter"
        )
        
        state_filter = None if selected_state == 'All States' else selected_state
        dma_df = get_dma_breakdown(con, ds, mover_ind, metric_type, winner, loser, state_filter)
        
        if not dma_df.empty:
            dma_df = flag_outliers(dma_df, 'total_metric', outlier_threshold)
            
            st.subheader("DMA Breakdown")
            display_cols = ['dma_name', 'state', 'total_metric', 'unique_blocks', 'avg_metric', 'max_metric', 'is_outlier']
            if 'z_score' in dma_df.columns:
                display_cols.append('z_score')
            
            styled_dma = dma_df[display_cols].head(50).style.applymap(
                lambda x: 'background-color: #ffcccc' if x else '',
                subset=['is_outlier']
            )
            st.dataframe(styled_dma, width="stretch", height=400)
            
            outlier_dmas = dma_df[dma_df['is_outlier'] == True]
            if not outlier_dmas.empty:
                st.warning(f"⚠️ {len(outlier_dmas)} outlier DMA(s) detected")
        
        # ====================================================================
        # Level 5: Census Block Analysis (Finest Granularity)
        # ====================================================================
        
        st.header("🎯 Level 5: Census Block Analysis")
        st.markdown("**Pinpoint exact locations with suspicious activity**")
        
        # Optional DMA filter
        dma_filter = None
        if not dma_df.empty:
            selected_dma = st.selectbox(
                "Filter by DMA (optional)",
                options=['All DMAs'] + sorted(dma_df['dma_name'].unique().tolist()),
                key="dma_filter"
            )
            dma_filter = None if selected_dma == 'All DMAs' else selected_dma
        
        # Outlier detection button
        if st.button("🔍 Detect Census Block Outliers", key="detect_outliers_btn"):
            with st.spinner("Analyzing census blocks for outliers..."):
                outlier_blocks = detect_census_block_outliers(
                    con, ds, mover_ind, metric_type, winner, loser, outlier_threshold
                )
            
            if not outlier_blocks.empty:
                st.success(f"Found {len(outlier_blocks)} outlier census blocks")
                
                # Display outliers
                st.subheader("🚨 Outlier Census Blocks")
                display_cols = ['census_blockid', 'state', 'dma_name', 'total_metric', 'days_active', 'z_score', 'avg_daily_metric']
                
                # Color code by severity
                def color_z_score(val):
                    if val > 5:
                        return 'background-color: #ff0000; color: white'
                    elif val > 4:
                        return 'background-color: #ff6666'
                    elif val > 3:
                        return 'background-color: #ffcccc'
                    return ''
                
                styled_outliers = outlier_blocks[display_cols].style.applymap(
                    color_z_score,
                    subset=['z_score']
                )
                st.dataframe(styled_outliers, width="stretch", height=500)
                
                # Statistics
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Outlier Blocks", len(outlier_blocks))
                col2.metric("Avg Z-Score", f"{outlier_blocks['z_score'].mean():.2f}")
                col3.metric("Max Z-Score", f"{outlier_blocks['z_score'].max():.2f}")
                col4.metric("States Affected", outlier_blocks['state'].nunique())
                
                # Distribution chart
                fig = px.histogram(
                    outlier_blocks,
                    x='z_score',
                    nbins=30,
                    title="Distribution of Outlier Z-Scores"
                )
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("No outlier census blocks detected at this threshold")
        
        # Show all census blocks for selected filters
        st.subheader("Census Block Details")
        block_df = get_census_block_breakdown(con, ds, mover_ind, metric_type, winner, loser, state_filter, dma_filter)
        
        if not block_df.empty:
            st.dataframe(block_df, width="stretch", height=400)
            
            # Download option
            csv = block_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Census Block Data",
                data=csv,
                file_name=f"census_blocks_{winner}_vs_{loser}_{ds}.csv",
                mime="text/csv"
            )
        else:
            st.info("No census block data available for selected filters")


if __name__ == "__main__":
    main()
