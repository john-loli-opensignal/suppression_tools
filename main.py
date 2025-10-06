#!/usr/bin/env python3
import os
from datetime import date
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objs as go
import plotly.express as px
import tools.db as db
from tools.src.plan import (
    get_top_n_carriers,
    base_national_series,
    scan_base_outliers,
    build_enriched_cube
)


def ui():
    st.set_page_config(page_title='Suppression Tools', page_icon='🧰', layout='wide')
    st.title('🧰 Suppression Tools (Base → Outliers → Plan)')

    st.sidebar.header('Configuration')
    ds = st.sidebar.text_input('Dataset (ds)', value='gamoshi')
    mover_ind = st.sidebar.selectbox('Mover Type', options=[False, True], index=0, 
                                     format_func=lambda x: 'Mover' if x else 'Non-Mover')
    
    # Database info (read-only display)
    db_path = db.get_default_db_path()
    st.sidebar.info(f"📊 Database: `{os.path.basename(db_path)}`")

    st.sidebar.header('Graph Window')
    view_start = st.sidebar.date_input('Start Date', value=date(2025,6,1))
    view_end = st.sidebar.date_input('End Date', value=date(2025,8,31))

    st.sidebar.header('Outlier Detection Thresholds')
    top_n = st.sidebar.slider('Top N Carriers', min_value=10, max_value=100, value=25, step=5,
                               help='Focus on top N carriers by total wins')
    z_threshold = st.sidebar.slider('Z-Score Threshold', min_value=0.5, max_value=5.0, value=2.5, step=0.1,
                                     help='Statistical outlier threshold (default: 2.5)')
    egregious_threshold = st.sidebar.slider('Egregious Impact', min_value=10, max_value=100, value=40, step=5,
                                            help='Flag outliers outside top N with impact > this')
    
    st.sidebar.header('Suppression Thresholds')
    auto_min_wins = st.sidebar.slider('Min Wins for Auto Suppression', min_value=1, max_value=20, value=2, step=1,
                                      help='Minimum current wins for pair to be auto-suppressed (outliers, first appearance, etc.)')
    distributed_min_wins = st.sidebar.slider('Min Wins for Distribution', min_value=1, max_value=50, value=1, step=1,
                                             help='Minimum current wins for pair to be eligible for distributed suppression (default: 1)')
    
    # Advanced filters (optional)
    with st.sidebar.expander('🔍 Advanced Filters', expanded=False):
        st.caption('Optional: Further refine carrier selection')
        min_share_pct = st.slider('Min Overall Share %', min_value=0.0, max_value=2.0, value=0.5, step=0.1,
                                   help='Only show carriers with >= this % of total wins (entire series)')
        st.info(f'💡 With {min_share_pct}% threshold, you\'ll see ~20 carriers in top 25')

    # Step 0: Preview base graph (unsuppressed)
    # Remove the separate "Preview Base Graph" section - just show it in scan results
    # Step 1: Scan base outliers
    st.subheader('1️⃣ Scan National-Level Outliers')
    st.caption('Detect carriers with abnormal win patterns using DOW-aware rolling statistics.')
    
    if st.button('Scan for Outliers', key='scan_outliers'):
        try:
            with st.spinner('Scanning for national outliers using rolling views...'):
                outliers_df = scan_base_outliers(
                    ds=ds,
                    mover_ind=mover_ind,
                    start_date=str(view_start),
                    end_date=str(view_end),
                    z_threshold=z_threshold,
                    top_n=top_n,
                    min_share_pct=min_share_pct,
                    egregious_threshold=egregious_threshold,
                    db_path=db_path
                )
            
            st.session_state['base_outliers'] = outliers_df if not outliers_df.empty else None
            
            # Always show graph with all top carriers (filtered by share if specified)
            with st.spinner('Loading national time series...'):
                all_top_carriers = get_top_n_carriers(ds, mover_ind, n=top_n, min_share_pct=min_share_pct, db_path=db_path)
                ts = base_national_series(
                    ds=ds,
                    mover_ind=mover_ind,
                    winners=all_top_carriers,
                    start_date=str(view_start),
                    end_date=str(view_end),
                    db_path=db_path
                )
            
            if outliers_df.empty:
                st.success('✅ No national-level outliers detected in this date range!')
            else:
                st.success(f'✅ Found {len(outliers_df)} outlier instances')
                
                # Display summary
                unique_dates = outliers_df['the_date'].nunique()
                unique_winners = outliers_df['winner'].nunique()
                total_impact = outliers_df['impact'].sum()
                
                col1, col2, col3 = st.columns(3)
                col1.metric('Dates with Outliers', unique_dates)
                col2.metric('Carriers Flagged', unique_winners)
                col3.metric('Total Impact (wins)', f'{total_impact:,}')
            
            # Display graph with all top carriers + outlier markers
            if not ts.empty:
                st.subheader(f'National Win Share - Top {len(all_top_carriers)} Carriers (min share: {min_share_pct}%)')
                st.caption(f'Showing top {top_n} carriers with >= {min_share_pct}% overall share + egregious outliers')
                try:
                    # Rank carriers by total wins
                    carrier_totals = ts.groupby('winner')['total_wins'].sum().sort_values(ascending=False)
                    carriers_ranked = carrier_totals.index.tolist()
                    
                    # Merge outlier flags into time series
                    ts_copy = ts.copy()
                    ts_copy['the_date'] = pd.to_datetime(ts_copy['the_date'])
                    
                    if not outliers_df.empty:
                        outliers_copy = outliers_df.copy()
                        outliers_copy['the_date'] = pd.to_datetime(outliers_copy['the_date'])
                        outliers_copy['is_outlier'] = True
                        
                        # Mark egregious outliers (those not in top carriers list)
                        outliers_copy['is_egregious'] = ~outliers_copy['winner'].isin(all_top_carriers)
                        
                        ts_with_outliers = ts_copy.merge(
                            outliers_copy[['the_date', 'winner', 'is_outlier', 'is_egregious', 'nat_z_score', 'impact']],
                            on=['the_date', 'winner'],
                            how='left'
                        )
                        ts_with_outliers['is_outlier'] = ts_with_outliers['is_outlier'].fillna(False)
                        ts_with_outliers['is_egregious'] = ts_with_outliers['is_egregious'].fillna(False)
                    else:
                        ts_with_outliers = ts_copy.copy()
                        ts_with_outliers['is_outlier'] = False
                        ts_with_outliers['is_egregious'] = False
                    
                    # Use same color palette with ranking
                    palette = px.colors.qualitative.Dark24
                    color_map = {c: palette[i % len(palette)] for i, c in enumerate(carriers_ranked)}
                    
                    # Create figure
                    fig = go.Figure()
                    
                    # Store smoothed series for each carrier (for marker alignment)
                    carrier_smoothed = {}
                    
                    # Add base lines for each carrier with smoothing
                    for w in carriers_ranked:
                        sub = ts_with_outliers[ts_with_outliers['winner'] == w].sort_values('the_date')
                        series = sub['win_share'] * 100  # Convert to percentage
                        
                        # Apply 3-period rolling smoothing
                        if len(series) >= 3:
                            smooth = series.rolling(window=3, center=True, min_periods=1).mean()
                        else:
                            smooth = series
                        
                        # Store for marker positioning
                        carrier_smoothed[w] = smooth.copy()
                        carrier_smoothed[w].index = sub['the_date']
                        
                        # Create hover text
                        raw_vals = series.round(4).astype(str)
                        smooth_vals = smooth.round(4).astype(str)
                        hover_text = [
                            f"{w}<br>{d.date()}<br>raw: {r}%<br>smoothed: {s}%"
                            for d, r, s in zip(sub['the_date'], raw_vals, smooth_vals)
                        ]
                        
                        fig.add_trace(go.Scatter(
                            x=sub['the_date'],
                            y=smooth,
                            mode='lines',
                            name=w,
                            line=dict(color=color_map[w], width=2),
                            hoverinfo='text',
                            hovertext=hover_text
                        ))
                    
                    # Add outlier markers if any detected
                    if not outliers_df.empty:
                        for w in carriers_ranked:
                            outlier_sub = ts_with_outliers[
                                (ts_with_outliers['winner'] == w) & 
                                (ts_with_outliers['is_outlier'] == True)
                            ]
                            if not outlier_sub.empty:
                                # Get smoothed values from pre-computed series
                                smooth_series = carrier_smoothed[w]
                                
                                # Split into positive and negative z-scores, and egregious vs regular
                                pos_out = outlier_sub[outlier_sub['nat_z_score'] >= 0]
                                neg_out = outlier_sub[outlier_sub['nat_z_score'] < 0]
                                
                                # Positive outliers - split by egregious flag
                                if not pos_out.empty:
                                    pos_egregious = pos_out[pos_out['is_egregious'] == True]
                                    pos_regular = pos_out[pos_out['is_egregious'] == False]
                                    
                                    # Regular positive outliers (yellow stars)
                                    if not pos_regular.empty:
                                        pos_y = [smooth_series.get(d, (pos_regular.loc[pos_regular['the_date']==d, 'win_share'].iloc[0] * 100)) 
                                                 for d in pos_regular['the_date']]
                                        
                                        hover_text = [
                                            f"{w}<br>{d.date()}<br>Share: {y:.4f}%<br>Z-score: {z:.2f}<br>Impact: {int(imp)}"
                                            for d, y, z, imp in zip(
                                                pos_regular['the_date'], 
                                                pos_y,
                                                pos_regular['nat_z_score'].fillna(0),
                                                pos_regular['impact'].fillna(0)
                                            )
                                        ]
                                        fig.add_trace(go.Scatter(
                                            x=pos_regular['the_date'],
                                            y=pos_y,
                                            mode='markers',
                                            name=f'{w} outlier (+)',
                                            marker=dict(
                                                symbol='star', 
                                                color='yellow', 
                                                size=11, 
                                                line=dict(color='black', width=0.6),
                                                opacity=0.95
                                            ),
                                            showlegend=False,
                                            hoverinfo='text',
                                            hovertext=hover_text
                                        ))
                                    
                                    # Egregious positive outliers (orange diamonds - more prominent)
                                    if not pos_egregious.empty:
                                        pos_y = [smooth_series.get(d, (pos_egregious.loc[pos_egregious['the_date']==d, 'win_share'].iloc[0] * 100)) 
                                                 for d in pos_egregious['the_date']]
                                        
                                        hover_text = [
                                            f"⚠️ EGREGIOUS: {w}<br>{d.date()}<br>Share: {y:.4f}%<br>Impact: {int(imp)} (>{egregious_threshold})"
                                            for d, y, imp in zip(
                                                pos_egregious['the_date'], 
                                                pos_y,
                                                pos_egregious['impact'].fillna(0)
                                            )
                                        ]
                                        fig.add_trace(go.Scatter(
                                            x=pos_egregious['the_date'],
                                            y=pos_y,
                                            mode='markers',
                                            name=f'⚠️ {w} EGREGIOUS',
                                            marker=dict(
                                                symbol='diamond', 
                                                color='orange', 
                                                size=15, 
                                                line=dict(color='darkred', width=2),
                                                opacity=1.0
                                            ),
                                            showlegend=True,  # Show in legend so they stand out
                                            hoverinfo='text',
                                            hovertext=hover_text
                                        ))
                                
                                # Negative outliers (red minus signs) - usually not egregious, but check anyway
                                if not neg_out.empty:
                                    neg_egregious = neg_out[neg_out['is_egregious'] == True]
                                    neg_regular = neg_out[neg_out['is_egregious'] == False]
                                    
                                    # Regular negative outliers
                                    if not neg_regular.empty:
                                        neg_y = [smooth_series.get(d, (neg_regular.loc[neg_regular['the_date']==d, 'win_share'].iloc[0] * 100)) 
                                                 for d in neg_regular['the_date']]
                                        
                                        hover_text = [
                                            f"{w}<br>{d.date()}<br>Share: {y:.4f}%<br>Z-score: {z:.2f}<br>Impact: {int(imp)}"
                                            for d, y, z, imp in zip(
                                                neg_regular['the_date'], 
                                                neg_y,
                                                neg_regular['nat_z_score'].fillna(0),
                                                neg_regular['impact'].fillna(0)
                                            )
                                        ]
                                        fig.add_trace(go.Scatter(
                                            x=neg_regular['the_date'],
                                            y=neg_y,
                                            mode='markers',
                                            name=f'{w} outlier (-)',
                                            marker=dict(
                                                symbol='line-ew', 
                                                color='red', 
                                                size=12, 
                                                line=dict(color='darkred', width=1),
                                                opacity=0.85
                                            ),
                                            showlegend=False,
                                            hoverinfo='text',
                                            hovertext=hover_text
                                        ))
                                    
                                    # Egregious negative outliers (if any - rare)
                                    if not neg_egregious.empty:
                                        neg_y = [smooth_series.get(d, (neg_egregious.loc[neg_egregious['the_date']==d, 'win_share'].iloc[0] * 100)) 
                                                 for d in neg_egregious['the_date']]
                                        
                                        hover_text = [
                                            f"⚠️ EGREGIOUS DROP: {w}<br>{d.date()}<br>Share: {y:.4f}%<br>Impact: {int(imp)}"
                                            for d, y, imp in zip(
                                                neg_egregious['the_date'], 
                                                neg_y,
                                                neg_egregious['impact'].fillna(0)
                                            )
                                        ]
                                        fig.add_trace(go.Scatter(
                                            x=neg_egregious['the_date'],
                                            y=neg_y,
                                            mode='markers',
                                            name=f'⚠️ {w} EGREGIOUS DROP',
                                            marker=dict(
                                                symbol='diamond', 
                                                color='purple', 
                                                size=15, 
                                                line=dict(color='black', width=2),
                                                opacity=1.0
                                            ),
                                            showlegend=True,
                                            hoverinfo='text',
                                            hovertext=hover_text
                                        ))
                    
                    fig.update_layout(
                        title=dict(text=f'National Win Share - {ds} {"Mover" if mover_ind else "Non-Mover"}', x=0.01, xanchor='left'),
                        width=1100,
                        height=650,
                        xaxis_title='Date',
                        yaxis_title='Win Share (%)',
                        legend=dict(orientation='v', x=1.02, y=0.5),
                        margin=dict(l=40, r=200, t=80, b=40)
                    )
                    st.plotly_chart(fig, width='content')
                except Exception as e:
                    st.error(f'Failed to create graph: {e}')
                    import traceback
                    with st.expander('Show traceback'):
                        st.code(traceback.format_exc())
        except Exception as e:
            st.error(f'❌ Scan failed: {e}')
            import traceback
            with st.expander('Show traceback'):
                st.code(traceback.format_exc())
    # Show cached outliers if available
    base_outliers = st.session_state.get('base_outliers')
    if base_outliers is not None and not base_outliers.empty:
        st.info(f'📌 {len(base_outliers)} outliers detected for {len(base_outliers["winner"].unique())} carriers')
        
        # National Outliers Summary Table - with dates
        with st.expander('📋 National Outliers Summary (by Date)', expanded=True):
            # Detailed view with dates
            detailed = base_outliers[['the_date', 'winner', 'impact', 'nat_total_wins', 'nat_z_score']].copy()
            detailed['the_date'] = pd.to_datetime(detailed['the_date']).dt.date
            detailed = detailed.sort_values(['the_date', 'impact'], ascending=[True, False])
            
            # Format for display
            detailed['impact'] = detailed['impact'].astype(int)
            detailed['nat_total_wins'] = detailed['nat_total_wins'].astype(int)
            detailed['nat_z_score'] = detailed['nat_z_score'].round(2)
            detailed.columns = ['Date', 'Carrier', 'Impact', 'Total Wins', 'Z-Score']
            
            st.dataframe(
                detailed,
                width='stretch',
                hide_index=True,
                column_config={
                    'Date': st.column_config.DateColumn('Date', width='small'),
                    'Carrier': st.column_config.TextColumn('Carrier', width='medium'),
                    'Impact': st.column_config.NumberColumn('Impact', format='%d', help='Excess wins over baseline'),
                    'Total Wins': st.column_config.NumberColumn('Total Wins', format='%d'),
                    'Z-Score': st.column_config.NumberColumn('Z-Score', format='%.2f')
                }
            )
            
            st.caption(f'Showing all {len(base_outliers)} outlier instances. Click "Build Plan" to generate suppression strategy.')
    else:
        st.caption('No outliers scanned yet. Click "Scan for Outliers" to begin.')

    # Step 2: Build suppression plan
    st.subheader('2️⃣ Build Suppression Plan')
    st.caption('Generate auto + distributed suppression plan from detected outliers.')
    
    if st.button('Build Plan', key='build_plan'):
        base_outliers = st.session_state.get('base_outliers')
        if base_outliers is None or base_outliers.empty:
            st.error('❌ No outliers available. Please run "Scan for Outliers" first.')
        else:
            try:
                with st.spinner('Building enriched cube and generating suppression plan...'):
                    # Get enriched cube for entire scan window
                    enriched = build_enriched_cube(
                        ds=ds,
                        mover_ind=mover_ind,
                        start_date=str(view_start),
                        end_date=str(view_end),
                        db_path=db_path
                    )
                    
                    if enriched.empty:
                        st.warning('No data found in enriched cube.')
                    else:
                        # Filter to only outlier dates/winners
                        outlier_keys = base_outliers[['the_date', 'winner']].drop_duplicates()
                        outlier_keys['the_date'] = pd.to_datetime(outlier_keys['the_date'])
                        enriched['the_date'] = pd.to_datetime(enriched['the_date'])
                        
                        # Merge to get enriched data for outliers only
                        enriched_outliers = enriched.merge(outlier_keys, on=['the_date', 'winner'], how='inner')
                        
                        if enriched_outliers.empty:
                            st.warning('No enriched data matched the outliers.')
                        else:
                            # Build suppression plan using your distribution algorithm
                            plan_rows = []
                            insufficient_threshold_cases = []  # Track carriers that couldn't meet distribution threshold
                            
                            for (the_date, winner), sub in enriched_outliers.groupby(['the_date', 'winner']):
                                # Calculate removal need (excess over baseline)
                                nat_info = sub.iloc[0]
                                W = float(nat_info['nat_total_wins'])
                                T = float(nat_info['nat_market_wins'])
                                mu_share = float(nat_info['nat_mu_share'])
                                
                                # Calculate need: remove excess to bring share back to baseline
                                need = int(np.ceil(max((W - mu_share * T) / max(1e-12, (1 - mu_share)), 0)))
                                
                                if need <= 0:
                                    continue  # No removal needed
                                
                                # DMA-level aggregates for share calculations
                                dma_totals = sub.groupby('dma_name')['pair_wins_current'].sum().to_dict()
                                dma_mu_totals = sub.groupby('dma_name')['pair_mu_wins'].sum().to_dict()
                                
                                # ===  STAGE 1: AUTO SUPPRESSION ===
                                # Trigger conditions per requirements:
                                # - pair_outlier_pos (z-score violation)
                                # - pct_outlier_pos (30% spike)
                                # - rare_pair (appearance_rank <= 5) WITH z-score violation AND impact > 15
                                # - new_pair (first appearance at DMA level)
                                
                                # Rare pairs need both z-score AND impact > 15
                                is_rare_eligible = (sub['rare_pair'] == True) & (sub['pair_z'] > 1.5) & (sub['impact'].abs() > 15)
                                
                                auto_candidates = sub[
                                    (sub['pair_outlier_pos'] == True) |
                                    (sub['pct_outlier_pos'] == True) |
                                    (is_rare_eligible) |
                                    (sub['new_pair'] == True)
                                ].copy()
                                
                                # Apply minimum volume filter (configurable)
                                auto_candidates = auto_candidates[auto_candidates['pair_wins_current'] >= auto_min_wins]
                                
                                if not auto_candidates.empty:
                                    # Calculate removal amount: NO CAP, remove FULL excess
                                    pw = pd.to_numeric(auto_candidates['pair_wins_current'], errors='coerce').fillna(0.0)
                                    mu_eff = pd.to_numeric(auto_candidates['pair_mu_wins'], errors='coerce').fillna(0.0)
                                    
                                    # Remove full excess (current - baseline)
                                    rm_excess = np.ceil(np.maximum(0.0, pw - mu_eff)).astype(int)
                                    auto_candidates['rm_pair'] = rm_excess
                                    
                                    # Sort by severity (z-score, then current wins)
                                    auto_candidates = auto_candidates.sort_values(
                                        ['pair_z', 'pair_wins_current'],
                                        ascending=[False, False]
                                    )
                                    
                                    # Allocate removals up to need
                                    auto_candidates['cum_remove'] = auto_candidates['rm_pair'].cumsum()
                                    auto_candidates['rm_final'] = np.where(
                                        auto_candidates['cum_remove'] <= need,
                                        auto_candidates['rm_pair'],
                                        np.maximum(0, need - auto_candidates['cum_remove'].shift(fill_value=0))
                                    ).astype(int)
                                    
                                    auto_final = auto_candidates[auto_candidates['rm_final'] > 0]
                                    auto_removed = int(auto_final['rm_final'].sum())
                                else:
                                    auto_final = pd.DataFrame()
                                    auto_removed = 0
                                
                                # === STAGE 2: DISTRIBUTED SUPPRESSION ===
                                need_remaining = max(0, need - auto_removed)
                                
                                if need_remaining > 0:
                                    # Get all pairs NOT already in auto suppression
                                    auto_pairs = set()
                                    if not auto_final.empty:
                                        for _, r in auto_final.iterrows():
                                            auto_pairs.add((r['loser'], r['dma_name']))
                                    
                                    # Filter to pairs meeting distributed minimum threshold
                                    eligible_pairs = sub[
                                        (~sub.apply(lambda r: (r['loser'], r['dma_name']) in auto_pairs, axis=1)) &
                                        (sub['pair_wins_current'] >= distributed_min_wins)
                                    ].copy()
                                    
                                    if len(eligible_pairs) == 0:
                                        # Track this case for reporting
                                        insufficient_threshold_cases.append({
                                            'date': pd.to_datetime(the_date).date(),
                                            'winner': winner,
                                            'need_remaining': need_remaining,
                                            'auto_removed': auto_removed,
                                            'min_wins_required': distributed_min_wins,
                                            'reason': f'All remaining pairs have < {distributed_min_wins} wins'
                                        })
                                        distributed_final = pd.DataFrame()
                                    else:
                                        # Distribute proportionally across eligible pairs
                                        eligible_pairs['capacity'] = pd.to_numeric(eligible_pairs['pair_wins_current'], errors='coerce').fillna(0.0)
                                        total_eligible = eligible_pairs['capacity'].sum()
                                        
                                        if total_eligible > 0:
                                            eligible_pairs['proportion'] = eligible_pairs['capacity'] / total_eligible
                                            eligible_pairs['rm_final'] = (eligible_pairs['proportion'] * need_remaining).round().astype(int)
                                            
                                            # Only keep pairs with actual removals
                                            distributed_final = eligible_pairs[eligible_pairs['rm_final'] > 0].copy()
                                        else:
                                            distributed_final = pd.DataFrame()
                                else:
                                    distributed_final = pd.DataFrame()
                                
                                # === COLLECT PLAN ROWS ===
                                # Auto stage rows
                                for _, r in auto_final.iterrows():
                                    dma_total = dma_totals.get(r['dma_name'], 0)
                                    dma_mu_total = dma_mu_totals.get(r['dma_name'], 0)
                                    
                                    pair_share = (r['pair_wins_current'] / dma_total) if dma_total > 0 else None
                                    pair_share_mu = (r['pair_mu_wins'] / dma_mu_total) if dma_mu_total > 0 else None
                                    
                                    plan_rows.append({
                                        'date': pd.to_datetime(the_date).date(),
                                        'winner': winner,
                                        'loser': r['loser'],
                                        'dma_name': r['dma_name'],
                                        'state': r['state'],
                                        'mover_ind': mover_ind,
                                        'remove_units': int(r['rm_final']),
                                        'stage': 'auto',
                                        'pair_wins_current': r['pair_wins_current'],
                                        'pair_mu_wins': r['pair_mu_wins'],
                                        'pair_sigma_wins': r['pair_sigma_wins'],
                                        'pair_z': r['pair_z'],
                                        'pair_pct_change': r['pair_pct_change'],
                                        'dma_wins': dma_total,
                                        'pair_share': pair_share,
                                        'pair_share_mu': pair_share_mu,
                                        'nat_total_wins': nat_info['nat_total_wins'],
                                        'nat_share_current': nat_info['nat_share_current'],
                                        'nat_mu_share': nat_info['nat_mu_share'],
                                        'nat_z_score': nat_info['nat_z_score'],
                                        'impact': int(r['rm_final'])
                                    })
                                
                                # Distributed stage rows
                                for _, r in distributed_final.iterrows():
                                    dma_total = dma_totals.get(r['dma_name'], 0)
                                    dma_mu_total = dma_mu_totals.get(r['dma_name'], 0)
                                    
                                    pair_share = (r['pair_wins_current'] / dma_total) if dma_total > 0 else None
                                    pair_share_mu = (r['pair_mu_wins'] / dma_mu_total) if dma_mu_total > 0 else None
                                    
                                    plan_rows.append({
                                        'date': pd.to_datetime(the_date).date(),
                                        'winner': winner,
                                        'loser': r['loser'],
                                        'dma_name': r['dma_name'],
                                        'state': r['state'],
                                        'mover_ind': mover_ind,
                                        'remove_units': int(r['rm_final']),
                                        'stage': 'distributed',
                                        'pair_wins_current': r['pair_wins_current'],
                                        'pair_mu_wins': r['pair_mu_wins'],
                                        'pair_sigma_wins': r['pair_sigma_wins'],
                                        'pair_z': r['pair_z'],
                                        'pair_pct_change': r['pair_pct_change'],
                                        'dma_wins': dma_total,
                                        'pair_share': pair_share,
                                        'pair_share_mu': pair_share_mu,
                                        'nat_total_wins': nat_info['nat_total_wins'],
                                        'nat_share_current': nat_info['nat_share_current'],
                                        'nat_mu_share': nat_info['nat_mu_share'],
                                        'nat_z_score': nat_info['nat_z_score'],
                                        'impact': int(r['rm_final'])
                                    })
                            
                            if plan_rows:
                                plan_df = pd.DataFrame(plan_rows)
                                st.session_state['suppression_plan'] = plan_df
                                
                                # Display summary
                                total_removals = plan_df['remove_units'].sum()
                                auto_removals = plan_df[plan_df['stage'] == 'auto']['remove_units'].sum()
                                dist_removals = plan_df[plan_df['stage'] == 'distributed']['remove_units'].sum()
                                
                                col1, col2, col3, col4 = st.columns(4)
                                col1.metric('Total Removals', f'{total_removals:,}')
                                col2.metric('Auto Stage', f'{auto_removals:,}')
                                col3.metric('Distributed', f'{dist_removals:,}')
                                col4.metric('Plan Rows', len(plan_df))
                                
                                # Display plan
                                display_plan = plan_df.copy()
                                
                                # Format percentages and round numbers
                                for c in ['nat_share_current', 'nat_mu_share', 'pair_share', 'pair_share_mu', 'pair_pct_change']:
                                    if c in display_plan.columns:
                                        display_plan[c] = (pd.to_numeric(display_plan[c], errors='coerce') * 100).round(2)
                                
                                for c in ['pair_z', 'nat_z_score']:
                                    if c in display_plan.columns:
                                        display_plan[c] = pd.to_numeric(display_plan[c], errors='coerce').round(2)
                                
                                # Select and reorder columns for display
                                display_cols = ['date', 'stage', 'winner', 'loser', 'dma_name', 'state',
                                              'remove_units', 'impact', 'pair_wins_current', 'pair_mu_wins',
                                              'pair_z', 'dma_wins', 'pair_share', 'pair_share_mu',
                                              'nat_share_current', 'nat_mu_share', 'nat_z_score']
                                display_cols = [c for c in display_cols if c in display_plan.columns]
                                display_plan = display_plan[display_cols]
                                
                                st.dataframe(display_plan, width='stretch')
                                st.success('✅ Suppression plan generated successfully!')
                                
                                # Display carriers that didn't meet distribution threshold
                                if insufficient_threshold_cases:
                                    st.warning(f'⚠️ {len(insufficient_threshold_cases)} carrier-date combinations could not fully distribute (all pairs < {distributed_min_wins} wins)')
                                    with st.expander('📊 Carriers Not Meeting Distribution Threshold', expanded=True):
                                        insufficient_df = pd.DataFrame(insufficient_threshold_cases)
                                        
                                        # Aggregate by carrier to show total unaddressed impact
                                        carrier_summary = insufficient_df.groupby('winner').agg({
                                            'date': 'count',
                                            'need_remaining': 'sum',
                                            'auto_removed': 'sum'
                                        }).reset_index()
                                        carrier_summary.columns = ['Carrier', 'Dates Affected', 'Total Unaddressed Impact', 'Total Auto-Removed']
                                        carrier_summary = carrier_summary.sort_values('Total Unaddressed Impact', ascending=False)
                                        
                                        st.markdown('**Summary by Carrier:**')
                                        st.dataframe(carrier_summary, width='stretch', hide_index=True)
                                        
                                        st.markdown('**Details by Date:**')
                                        st.dataframe(
                                            insufficient_df[['date', 'winner', 'need_remaining', 'auto_removed', 'min_wins_required', 'reason']],
                                            width='stretch',
                                            hide_index=True
                                        )
                                        
                                        st.info(f'💡 Tip: Lower "Min Wins for Distribution" threshold to {max(1, distributed_min_wins // 2)} to address these cases')
                            else:
                                st.warning('⚠️  No suppression plan rows were generated from the outliers.')
            except Exception as e:
                st.error(f'❌ Plan building failed: {e}')
                import traceback
                with st.expander('Show traceback'):
                    st.code(traceback.format_exc())
    
    # Show cached plan if available
    plan_df = st.session_state.get('suppression_plan')
    if plan_df is not None and not plan_df.empty:
        st.info(f'📌 Suppression plan loaded: {len(plan_df)} rows, {plan_df["remove_units"].sum():,} total removals')
    #  Step 3: Save suppression plan
    st.subheader('3️⃣ Save Suppression Plan')
    st.caption('Save plan to database and CSV file for use in dashboards.')
    
    col1, col2 = st.columns(2)
    with col1:
        round_name = st.text_input('Round Name', value='base_outliers_round', 
                                   help='Unique name for this suppression round')
    with col2:
        overwrite = st.checkbox('Overwrite if exists', value=False,
                               help='Allow overwriting existing round')
    
    if st.button('Save Plan', key='save_plan'):
        plan_df = st.session_state.get('suppression_plan')
        if plan_df is None or plan_df.empty:
            st.error('❌ No plan to save. Build a plan first.')
        else:
            try:
                # Check if round already exists
                csv_dir = os.path.join(os.getcwd(), 'suppressions', 'rounds')
                os.makedirs(csv_dir, exist_ok=True)
                csv_path = os.path.join(csv_dir, f'{round_name}.csv')
                
                if os.path.exists(csv_path) and not overwrite:
                    st.error(f'❌ Round "{round_name}" already exists! Check "Overwrite if exists" to replace it.')
                else:
                    # Save to CSV
                    plan_df.to_csv(csv_path, index=False)
                    
                    # TODO: Save to database (suppressions schema)
                    # For now, CSV only
                    
                    st.success(f'✅ Saved plan to: `{csv_path}`')
                    st.info(f'📊 {len(plan_df)} rows, {plan_df["remove_units"].sum():,} total removals')
                    st.caption('Use the carrier_suppression_dashboard.py to apply and visualize this plan.')
                    
            except Exception as e:
                st.error(f'❌ Save failed: {e}')
                import traceback
                with st.expander('Show traceback'):
                    st.code(traceback.format_exc())

    st.subheader('4️⃣ Build Suppressed Dataset (Optional)')
    st.caption('Generate suppressed parquet files (advanced users only).')
    if st.button('Run Dataset Builder'):
        try:
            import subprocess, sys
            cmd = [sys.executable, os.path.join(os.getcwd(), 'tools', 'build_suppressed_dataset.py')]
            res = subprocess.run(cmd, capture_output=True, text=True)
            if res.returncode == 0:
                st.success('✅ Dataset builder completed')
                st.code(res.stdout)
            else:
                st.error('❌ Dataset builder failed')
                st.code(res.stderr or res.stdout)
        except Exception as e:
            st.error(f'❌ Builder failed: {e}')

    # Step 5: Preview before/after with suppressions
    st.subheader('5️⃣ Preview Before/After Comparison')
    st.caption('Apply suppressions in-memory and visualize the effect on national win share.')
    
    # Toggle for display mode
    col1, col2 = st.columns([1, 3])
    with col1:
        show_mode = st.radio(
            'Display Mode',
            options=['Overlay (Both)', 'Original Only', 'Suppressed Only'],
            index=0,
            help='Toggle between original, suppressed, or both views'
        )
    
    if st.button('Generate Preview', key='preview_graph'):
        plan_df = st.session_state.get('suppression_plan')
        if plan_df is None or plan_df.empty:
            st.error('❌ No plan available. Build a plan first.')
        else:
            try:
                with st.spinner('Generating before/after comparison...'):
                    # Get ALL top N carriers (not just those in plan)
                    all_top_carriers = get_top_n_carriers(
                        ds=ds, 
                        mover_ind=mover_ind, 
                        n=top_n, 
                        min_share_pct=min_share_pct, 
                        db_path=db_path
                    )
                    
                    # Get base national series for ALL top carriers
                    base_series = base_national_series(
                        ds=ds,
                        mover_ind=mover_ind,
                        winners=all_top_carriers,
                        start_date=str(view_start),
                        end_date=str(view_end),
                        db_path=db_path
                    )
                    
                    if base_series.empty:
                        st.warning('No base data found for comparison.')
                    else:
                        # Apply suppressions to create suppressed series
                        # Aggregate plan by date/winner/loser/dma
                        suppressions = plan_df.groupby(['date', 'winner', 'loser', 'dma_name'])['remove_units'].sum().reset_index()
                        suppressions['date'] = pd.to_datetime(suppressions['date'])
                        
                        # Get detailed pair-level data from cube for ALL top carriers
                        cube_table = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"
                        
                        import duckdb
                        con = duckdb.connect(db_path)
                        
                        # Query cube data for ALL top carriers
                        winners_str = ','.join([f"'{w}'" for w in all_top_carriers])
                        pair_data = con.execute(f"""
                            SELECT 
                                the_date,
                                winner,
                                loser,
                                dma_name,
                                total_wins
                            FROM {cube_table}
                            WHERE the_date BETWEEN '{view_start}' AND '{view_end}'
                                AND winner IN ({winners_str})
                        """).df()
                        con.close()
                        
                        # Merge suppressions
                        pair_data['the_date'] = pd.to_datetime(pair_data['the_date'])
                        suppressions = suppressions.rename(columns={'date': 'the_date'})
                        
                        pair_suppressed = pair_data.merge(
                            suppressions,
                            on=['the_date', 'winner', 'loser', 'dma_name'],
                            how='left'
                        )
                        pair_suppressed['remove_units'] = pair_suppressed['remove_units'].fillna(0)
                        
                        # Apply suppressions
                        pair_suppressed['suppressed_wins'] = np.maximum(
                            0,
                            pair_suppressed['total_wins'] - pair_suppressed['remove_units']
                        )
                        
                        # Calculate suppressed national series
                        suppressed_agg = pair_suppressed.groupby(['the_date', 'winner']).agg({
                            'suppressed_wins': 'sum'
                        }).reset_index()
                        
                        suppressed_market = suppressed_agg.groupby('the_date')['suppressed_wins'].sum().reset_index()
                        suppressed_market = suppressed_market.rename(columns={'suppressed_wins': 'market_total'})
                        
                        suppressed_series = suppressed_agg.merge(suppressed_market, on='the_date')
                        suppressed_series['win_share'] = suppressed_series['suppressed_wins'] / suppressed_series['market_total']
                        suppressed_series = suppressed_series.rename(columns={'suppressed_wins': 'total_wins'})
                        
                        # Create overlay chart with beautiful formatting (matching carrier_dashboard_duckdb.py)
                        # Sort winners by total base wins (ascending for proper ranking)
                        winner_totals = base_series.groupby('winner')['total_wins'].sum().sort_values(ascending=False)
                        winners_sorted = winner_totals.index.tolist()
                        
                        # Use same color palette with ranking
                        palette = px.colors.qualitative.Dark24
                        color_map = {c: palette[i % len(palette)] for i, c in enumerate(winners_sorted)}
                        
                        fig = go.Figure()
                        
                        # Determine what to show based on toggle
                        show_suppressed = show_mode in ['Overlay (Both)', 'Suppressed Only']
                        show_original = show_mode in ['Overlay (Both)', 'Original Only']
                        
                        # FIRST LAYER: Add suppressed series (dashed for overlay, solid for suppressed-only)
                        if show_suppressed:
                            for w in winners_sorted:
                                supp_sub = suppressed_series[suppressed_series['winner'] == w].sort_values('the_date')
                                if not supp_sub.empty:
                                    series = supp_sub['win_share'] * 100
                                    
                                    # Apply 3-period rolling smoothing
                                    if len(series) >= 3:
                                        smooth = series.rolling(window=3, center=True, min_periods=1).mean()
                                    else:
                                        smooth = series
                                    
                                    hover_text = [
                                        f"{w} (SUPPRESSED)<br>{d.date()}<br>Share: {s:.4f}%"
                                        for d, s in zip(supp_sub['the_date'], smooth)
                                    ]
                                    
                                    # Use dashed if overlay, solid if suppressed-only
                                    line_style = 'dash' if show_mode == 'Overlay (Both)' else 'solid'
                                    line_width = 2 if show_mode == 'Overlay (Both)' else 2.5
                                    
                                    fig.add_trace(go.Scatter(
                                        x=supp_sub['the_date'],
                                        y=smooth,
                                        mode='lines',
                                        name=f'{w}',
                                        line=dict(color=color_map[w], width=line_width, dash=line_style),
                                        legendgroup=w,
                                        hoverinfo='text',
                                        hovertext=hover_text,
                                        opacity=0.7 if show_mode == 'Overlay (Both)' else 1.0
                                    ))
                        
                        # SECOND LAYER: Add base series (solid lines, foreground)
                        if show_original:
                            for w in winners_sorted:
                                base_sub = base_series[base_series['winner'] == w].sort_values('the_date')
                                if not base_sub.empty:
                                    series = base_sub['win_share'] * 100
                                    
                                    # Apply 3-period rolling smoothing
                                    if len(series) >= 3:
                                        smooth = series.rolling(window=3, center=True, min_periods=1).mean()
                                    else:
                                        smooth = series
                                    
                                    # Create hover text with raw and smoothed values
                                    raw_vals = series.round(4).astype(str)
                                    smooth_vals = smooth.round(4).astype(str)
                                    hover_text = [
                                        f"{w} (ORIGINAL)<br>{d.date()}<br>raw: {r}%<br>smoothed: {s}%"
                                        for d, r, s in zip(base_sub['the_date'], raw_vals, smooth_vals)
                                    ]
                                    
                                    fig.add_trace(go.Scatter(
                                        x=base_sub['the_date'],
                                        y=smooth,
                                        mode='lines',
                                        name=f'{w}',
                                        line=dict(color=color_map[w], width=2.5),
                                        legendgroup=w,
                                        hoverinfo='text',
                                        hovertext=hover_text
                                    ))
                        
                        fig.update_layout(
                            title=dict(
                                text=f'Before/After Suppression - {ds} {"Mover" if mover_ind else "Non-Mover"}<br>'
                                     '<sub>Solid lines = Original | Dashed lines = After Suppression</sub>',
                                x=0.01,
                                xanchor='left'
                            ),
                            width=1200,
                            height=700,
                            xaxis_title='Date',
                            yaxis_title='Win Share (%)',
                            hovermode='closest',
                            legend=dict(
                                orientation='v',
                                yanchor='middle',
                                y=0.5,
                                xanchor='left',
                                x=1.02,
                                font=dict(size=10)
                            ),
                            margin=dict(l=40, r=250, t=100, b=40)
                        )
                        
                        st.plotly_chart(fig, width='content')
                        
                        # Show summary stats
                        total_base = base_series.groupby('winner')['total_wins'].sum()
                        total_suppressed = suppressed_series.groupby('winner')['total_wins'].sum()
                        total_removed = total_base - total_suppressed
                        
                        # Calculate win shares
                        market_base = total_base.sum()
                        market_suppressed = total_suppressed.sum()
                        
                        summary = pd.DataFrame({
                            'Winner': total_base.index,
                            'Base Wins': total_base.values,
                            'Removed': total_removed.values,
                            'Wins after Suppression': total_suppressed.values,
                            'Old %': (total_base / market_base * 100).round(2).values,
                            'Removed %': (total_removed / total_base * 100).round(2).values,
                            'New %': (total_suppressed / market_suppressed * 100).round(2).values
                        }).sort_values('Removed', ascending=False).reset_index(drop=True)
                        
                        st.subheader('Suppression Summary')
                        st.dataframe(summary, width='stretch')
                        st.success('✅ Preview generated! Solid lines = base, dashed lines = suppressed (click legend to toggle)')
                        
            except Exception as e:
                st.error(f'❌ Preview failed: {e}')
                import traceback
                with st.expander('Show traceback'):
                    st.code(traceback.format_exc())


if __name__ == '__main__':
    ui()

