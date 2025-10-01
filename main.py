#!/usr/bin/env python3
import os
from datetime import date
import pandas as pd
import streamlit as st
import plotly.graph_objs as go
from suppression_tools.src.plan import scan_base_outliers, build_plan_for_winner_dates, base_national_series
from suppression_tools.src.util import expand


def default_store() -> str:
    return expand('~/codebase-comparison/duckdb_partitioned_store/**/*.parquet')


def ui():
    st.set_page_config(page_title='Suppression Tools', page_icon='🧰', layout='wide')
    st.title('🧰 Suppression Tools (Base → Outliers → Plan)')

    st.sidebar.header('Data Source')
    store_glob = st.sidebar.text_input('Parquet glob', value=default_store())
    ds = st.sidebar.text_input('ds', value='gamoshi')
    mover_ind = st.sidebar.selectbox('mover_ind', ['False','True'], index=0)

    st.sidebar.header('Cubes')
    cube_mover = st.sidebar.text_input('Mover cube CSV', value=os.path.expanduser('~/codebase-comparison/current_run_duckdb/win_cube_mover.csv'))
    cube_non_mover = st.sidebar.text_input('Non-mover cube CSV', value=os.path.expanduser('~/codebase-comparison/current_run_duckdb/win_cube_non_mover.csv'))
    colc1, colc2 = st.sidebar.columns(2)
    with colc1:
        if st.button('Build mover cube (True)'):
            import subprocess, sys
            cmd=[sys.executable, os.path.expanduser('~/codebase-comparison/suppression_tools/build_win_cube.py'), '--mover-ind','True','-o', cube_mover]
            res=subprocess.run(cmd, capture_output=True, text=True)
            st.code(res.stdout or res.stderr)
    with colc2:
        if st.button('Build non-mover cube (False)'):
            import subprocess, sys
            cmd=[sys.executable, os.path.expanduser('~/codebase-comparison/suppression_tools/build_win_cube.py'), '--mover-ind','False','-o', cube_non_mover]
            res=subprocess.run(cmd, capture_output=True, text=True)
            st.code(res.stdout or res.stderr)

    st.sidebar.header('Graph Window (view)')
    view_start = st.sidebar.date_input('Start', value=date(2025,6,1))
    view_end = st.sidebar.date_input('End', value=date(2025,8,31))

    st.sidebar.header('Outlier Detection')
    window = st.sidebar.number_input('DOW rows (window)', min_value=5, max_value=90, value=14)
    z = st.sidebar.number_input('z threshold', min_value=0.5, max_value=5.0, value=2.5, step=0.1)

    # Step 0: Preview base graph
    st.subheader('0) Preview base graph (unsuppressed)')
    default_winners = "Spectrum, Comcast, T-Mobile FWA, AT&T, Verizon FWA, Frontier, Verizon, Cox Communications, Altice, CenturyLink"
    winners_text = st.text_input('Winners (comma-separated)', value=default_winners)
    winners = [w.strip() for w in winners_text.split(',') if w.strip()]
    if st.button('Show base graph'):
        try:
            ts = base_national_series(store_glob, ds, mover_ind, winners, str(view_start), str(view_end))
            if ts.empty:
                st.caption('No data for selected winners / window.')
            else:
                fig = go.Figure()
                for w in sorted(ts['winner'].unique()):
                    sub = ts[ts['winner']==w]
                    fig.add_trace(go.Scatter(x=sub['the_date'], y=sub['win_share'], mode='lines', name=w, line=dict(width=2)))
                fig.update_layout(width=1000, height=650, margin=dict(l=40,r=40,t=60,b=40), xaxis_title='Date', yaxis_title='Win Share')
                st.plotly_chart(fig, use_container_width=False)
        except Exception as e:
            st.error(f'Base graph failed: {e}')

    # Step 1: Base outliers
    st.subheader('1) Scan base outliers (positive only)')
    use_cube = st.checkbox('Use cube for outliers (faster)', value=True)
    if st.button('Scan base outliers (view)'):
        try:
            if use_cube:
                cube_path = cube_mover if mover_ind=='True' else cube_non_mover
                df = pd.read_csv(cube_path, parse_dates=['the_date'])
                out = df[(df['the_date']>=pd.Timestamp(view_start)) & (df['the_date']<=pd.Timestamp(view_end)) & (df['nat_outlier_pos']==True)][['the_date','winner']].drop_duplicates().sort_values(['the_date','winner'])
            else:
                out = scan_base_outliers(store_glob, ds, mover_ind, str(view_start), str(view_end), int(window), float(z))
            st.session_state['base_out'] = out
        except Exception as e:
            st.error(f'Scan failed: {e}')
    base_out = st.session_state.get('base_out')
    if isinstance(base_out, pd.DataFrame) and not base_out.empty:
        st.dataframe(base_out)
    else:
        st.caption('No outliers scanned yet. Click the button above.')

    # Step 2: Plan from these outliers
    st.subheader('2) Build plan from current base outliers (no apply)')
    if st.button('Build plan preview'):
        out = st.session_state.get('base_out')
        if out is None or out.empty:
            st.error('No base outliers available. Run the scan first.')
        else:
            try:
                import numpy as np
                cube_path = cube_mover if mover_ind=='True' else cube_non_mover
                cube = pd.read_csv(cube_path, parse_dates=['the_date'])
                # filter to targets
                targets = out.copy()
                targets['the_date']=pd.to_datetime(targets['the_date'])
                key = ['the_date','winner']
                cube_f = cube.merge(targets, on=key, how='inner')
                # Build plan per (date, winner)
                rows = []
                for (d,w), sub in cube_f.groupby(key):
                    # target remove from national shares
                    nat = sub.drop_duplicates(subset=['the_date','winner'])[['nat_total_wins','nat_market_wins','nat_mu_share']].iloc[0]
                    W = float(nat['nat_total_wins']); T = float(nat['nat_market_wins']); mu = float(nat['nat_mu_share'])
                    need = int(np.ceil(max((W - mu*T)/max(1e-12, (1-mu)), 0)))
                    # DMA aggregates for shares
                    dma_tot = sub.groupby('dma_name', as_index=False)['pair_wins_current'].sum().set_index('dma_name')['pair_wins_current'].to_dict()
                    dma_mu_tot = sub.groupby('dma_name', as_index=False)['pair_mu_wins'].sum().set_index('dma_name')['pair_mu_wins'].to_dict()
                    # Stage 1 auto from pair outliers
                    # Triggers: z-based OR 30% jump OR rare/new pair
                    auto = sub[(sub.get('pair_outlier_pos')==True) |
                               (sub.get('pct_outlier_pos')==True) |
                               (sub.get('rare_pair')==True) |
                               (sub.get('new_pair')==True)].copy()
                    # Enforce day-of minimum volume for Stage 1
                    auto = auto[auto['pair_wins_current'] > 5]
                    if not auto.empty:
                        # Robust rm calculation: remove all if baseline is tiny/NaN; else remove excess
                        pw = pd.to_numeric(auto['pair_wins_current'], errors='coerce').fillna(0.0)
                        mu_eff = pd.to_numeric(auto['pair_mu_wins'], errors='coerce').fillna(0.0)
                        remove_all = mu_eff < 5.0
                        rm_excess = np.ceil(np.maximum(0.0, pw - mu_eff))
                        auto['rm_pair'] = np.where(remove_all, np.ceil(pw), rm_excess).astype(int)
                        auto = auto.sort_values(['pair_z','pair_wins_current'], ascending=[False, False])
                        auto['cum'] = auto['rm_pair'].cumsum()
                        auto['rm1'] = np.where(auto['cum']<=need, auto['rm_pair'], np.maximum(0, need - auto['cum'].shift(fill_value=0))).astype(int)
                        auto = auto[auto['rm1']>0]
                        need_after = int(max(0, need - int(auto['rm1'].sum())))
                    else:
                        need_after = need
                    # Stage 2 equalized
                    caps = sub[['loser','dma_name','pair_wins_current']].copy()
                    if not caps.empty and need_after>0:
                        caps['pair_wins_current'] = pd.to_numeric(caps['pair_wins_current'], errors='coerce').fillna(0.0)
                        m = len(caps)
                        base = need_after//m
                        caps['rm_base'] = np.minimum(caps['pair_wins_current'], base).astype(int)
                        remaining = int(max(0, need_after - int(caps['rm_base'].sum())))
                        caps['residual'] = (caps['pair_wins_current'] - caps['rm_base']).astype(int)
                        caps = caps.sort_values(['residual','pair_wins_current'], ascending=[False, False]).reset_index(drop=True)
                        caps['extra'] = 0
                        if remaining>0:
                            idx = caps.index[caps['residual']>0][:remaining]
                            caps.loc[idx, 'extra'] = 1
                        caps['rm2'] = (caps['rm_base'] + caps['extra']).astype(int)
                        caps = caps[caps['rm2']>0]
                    else:
                        caps = caps.iloc[0:0].copy()
                    # Collect rows
                    if not auto.empty:
                        for _, r in auto.iterrows():
                            _dmaw = float(dma_tot.get(r['dma_name'])) if r['dma_name'] in dma_tot else None
                            _dmamu = float(dma_mu_tot.get(r['dma_name'])) if r['dma_name'] in dma_mu_tot else None
                            _pair_share = (float(r['pair_wins_current'])/_dmaw) if (_dmaw and _dmaw>0) else None
                            _pair_share_mu = (float(r['pair_mu_wins'])/_dmamu) if (_dmamu and _dmamu>0) else None
                            rows.append({'date': d.date(), 'winner': w, 'mover_ind': (mover_ind=='True'), 'loser': r['loser'], 'dma_name': r['dma_name'], 'remove_units': int(r['rm1']), 'impact': int(r['rm1']), 'stage':'auto',
                                         'nat_share_current': sub['nat_share_current'].iloc[0], 'nat_mu_share': sub['nat_mu_share'].iloc[0], 'nat_sigma_share': sub['nat_sigma_share'].iloc[0], 'nat_mu_window': sub['nat_mu_window'].iloc[0],
                                         'pair_wins_current': r['pair_wins_current'], 'pair_mu_wins': r['pair_mu_wins'], 'pair_sigma_wins': r['pair_sigma_wins'], 'pair_mu_window': r['pair_mu_window'], 'pair_z': r['pair_z'],
                                         'dma_wins': _dmaw, 'pair_share': _pair_share, 'pair_share_mu': _pair_share_mu})
                    if not caps.empty:
                        for _, r in caps.iterrows():
                            # find QA from sub for this loser/dma
                            qa = sub[(sub['loser']==r['loser']) & (sub['dma_name']==r['dma_name'])].head(1)
                            _dmaw = float(dma_tot.get(r['dma_name'])) if r['dma_name'] in dma_tot else None
                            _dmamu = float(dma_mu_tot.get(r['dma_name'])) if r['dma_name'] in dma_mu_tot else None
                            _pwc = float(qa['pair_wins_current'].iloc[0]) if not qa.empty else None
                            _pmu = float(qa['pair_mu_wins'].iloc[0]) if not qa.empty else None
                            _pair_share = (_pwc/_dmaw) if (_dmaw and _dmaw>0 and _pwc is not None) else None
                            _pair_share_mu = (_pmu/_dmamu) if (_dmamu and _dmamu>0 and _pmu is not None) else None
                            rows.append({'date': d.date(), 'winner': w, 'mover_ind': (mover_ind=='True'), 'loser': r['loser'], 'dma_name': r['dma_name'], 'remove_units': int(r['rm2']), 'impact': int(r['rm2']), 'stage':'distributed',
                                         'nat_share_current': sub['nat_share_current'].iloc[0], 'nat_mu_share': sub['nat_mu_share'].iloc[0], 'nat_sigma_share': sub['nat_sigma_share'].iloc[0], 'nat_mu_window': sub['nat_mu_window'].iloc[0],
                                         'pair_wins_current': _pwc, 'pair_mu_wins': _pmu, 'pair_sigma_wins': qa['pair_sigma_wins'].iloc[0] if not qa.empty else None, 'pair_mu_window': qa['pair_mu_window'].iloc[0] if not qa.empty else None, 'pair_z': qa['pair_z'].iloc[0] if not qa.empty else None,
                                         'dma_wins': _dmaw, 'pair_share': _pair_share, 'pair_share_mu': _pair_share_mu})
                if rows:
                    plan_prev = pd.DataFrame(rows)
                    st.session_state['plan_prev'] = plan_prev
                    # Build a display frame with requested columns, names, and formatting
                    disp = plan_prev.copy()
                    # Rename columns to abbreviated display names
                    rename_map = {
                        'pair_wins_current': 'pair_wins',
                        'pair_mu_wins': 'pair_mu',
                        'nat_share_current': 'nat_share',
                        'nat_mu_share': 'nat_share_mu',
                        'nat_zscore': 'nat_z',
                    }
                    disp = disp.rename(columns=rename_map)
                    # Percent shares and rounding to 2 decimals
                    for c in ['nat_share', 'nat_share_mu', 'pair_share', 'pair_share_mu']:
                        if c in disp.columns:
                            disp[c] = (pd.to_numeric(disp[c], errors='coerce') * 100).round(2)
                    for c in ['pair_wins', 'pair_mu', 'dma_wins', 'impact', 'pair_z', 'nat_z']:
                        if c in disp.columns:
                            disp[c] = pd.to_numeric(disp[c], errors='coerce').round(2)
                    # Reorder columns per request
                    want = ['date', 'stage', 'mover_ind', 'dma_name', 'winner', 'loser',
                            'pair_wins', 'pair_mu', 'dma_wins', 'pair_share', 'pair_share_mu',
                            'impact', 'pair_z', 'nat_share', 'nat_share_mu', 'nat_z']
                    cols = [c for c in want if c in disp.columns]
                    disp = disp[cols]
                    st.dataframe(disp)
                    st.caption('Preview from cube. Save a copy to apply. Note: remove_units kept for apply, impact shown for QA.')
                else:
                    st.warning('No plan rows were generated from the current outliers.')
            except Exception as e:
                st.error(f'Plan preview failed: {e}')

    plan_prev = st.session_state.get('plan_prev')
    st.subheader('3) Save plan to suppressions folder (apply on reload)')
    col1, col2 = st.columns(2)
    with col1:
        round_name = st.text_input('Round name', value='base_outliers_round')
    with col2:
        if st.button('Save plan CSV'):
            if plan_prev is None or plan_prev.empty:
                st.error('No plan preview to save.')
            else:
                out_dir = expand('~/codebase-comparison/suppression_tools/suppressions')
                os.makedirs(out_dir, exist_ok=True)
                path = os.path.join(out_dir, f'{round_name}.csv')
                plan_prev.to_csv(path, index=False)
                st.success(f'Saved plan to: {path}')
                st.info('Open your suppression dashboard and click Reload & Apply to use it.')

    st.subheader('4) Build suppressed dataset (optional)')
    if st.button('Run dataset builder'):
        try:
            # Call the local builder script (kept under suppression_tools/tools)
            import subprocess, sys
            cmd = [sys.executable, os.path.expanduser('~/codebase-comparison/suppression_tools/tools/build_suppressed_dataset.py')]
            res = subprocess.run(cmd, capture_output=True, text=True)
            st.code(res.stdout or res.stderr)
        except Exception as e:
            st.error(f'Builder failed: {e}')

    # Step 5: Preview graph applying current plan in-memory (no files written)
    st.subheader('5) Preview graph with current plan (no apply to files)')
    st.caption('Applies the plan in-memory and shows base vs previewed-suppressed national share for winners in the plan within the view window.')
    if st.button('Preview graph with plan'):
        try:
            import duckdb
            plan_prev = st.session_state.get('plan_prev')
            if plan_prev is None or plan_prev.empty:
                st.error('No plan available. Build a plan preview first.')
            else:
                winners = sorted(plan_prev['winner'].unique().tolist())
                if not winners:
                    st.error('No winners found in the plan.')
                else:
                    # Build base and adjusted national series via DuckDB
                    con = duckdb.connect()
                    try:
                        con.register('sup_df', plan_prev)
                        winners_list = ",".join([f"'{str(w).replace("'","''")}'" for w in winners])
                        ds_q = str(ds).replace("'","''")
                        mi_q = 'TRUE' if mover_ind=='True' else 'FALSE'
                        start_q = str(view_start); end_q = str(view_end)
                        q = f"""
                        WITH ds AS (SELECT * FROM parquet_scan('{store_glob}')),
                        filt AS (
                          SELECT the_date, winner, loser, dma_name, adjusted_wins, adjusted_losses
                          FROM ds
                          WHERE ds = '{ds_q}' AND mover_ind = {mi_q}
                        ),
                        sup AS (
                          SELECT CAST(date AS DATE) AS d, winner, loser, dma_name, SUM(remove_units) AS remove_units
                          FROM sup_df
                          GROUP BY 1,2,3,4
                        ),
                        grp AS (
                          SELECT CAST(the_date AS DATE) AS d, winner, loser, dma_name, SUM(adjusted_wins) AS group_wins
                          FROM filt GROUP BY 1,2,3,4
                        ),
                        joined AS (
                          SELECT f.*, COALESCE(s.remove_units, 0) AS remove_units, COALESCE(g.group_wins, 0) AS group_wins,
                                 CAST(the_date AS DATE) AS d
                          FROM filt f
                          LEFT JOIN sup s ON CAST(f.the_date AS DATE)=s.d AND f.winner=s.winner AND f.loser=s.loser AND f.dma_name=s.dma_name
                          LEFT JOIN grp g ON CAST(f.the_date AS DATE)=g.d AND f.winner=g.winner AND f.loser=g.loser AND f.dma_name=g.dma_name
                        ),
                        adj AS (
                          SELECT the_date, winner, loser, dma_name,
                                 GREATEST(0, adjusted_wins - (remove_units * (adjusted_wins / NULLIF(group_wins,0)))) AS adjusted_wins,
                                 adjusted_losses
                          FROM joined
                        ),
                        -- Base national series
                        base_market AS (
                          SELECT CAST(the_date AS DATE) AS d, SUM(adjusted_wins) AS T FROM filt GROUP BY 1
                        ), base_w AS (
                          SELECT CAST(the_date AS DATE) AS d, winner, SUM(adjusted_wins) AS W FROM filt GROUP BY 1,2
                        ), base_series AS (
                          SELECT b.d AS the_date, b.winner, b.W / NULLIF(m.T,0) AS win_share
                          FROM base_w b JOIN base_market m USING (d)
                          WHERE b.winner IN ({winners_list}) AND b.d BETWEEN DATE '{start_q}' AND DATE '{end_q}'
                        ),
                        -- Adjusted national series
                        adj_market AS (
                          SELECT CAST(the_date AS DATE) AS d, SUM(adjusted_wins) AS T FROM adj GROUP BY 1
                        ), adj_w AS (
                          SELECT CAST(the_date AS DATE) AS d, winner, SUM(adjusted_wins) AS W FROM adj GROUP BY 1,2
                        )
                        SELECT 'Base' AS label, the_date, winner, win_share FROM base_series
                        UNION ALL
                        SELECT 'Suppressed' AS label, a.d AS the_date, a.winner, a.W / NULLIF(m.T,0) AS win_share
                        FROM adj_w a JOIN adj_market m USING (d)
                        WHERE a.winner IN ({winners_list}) AND a.d BETWEEN DATE '{start_q}' AND DATE '{end_q}';
                        """
                        pdf = con.execute(q).df()
                    finally:
                        con.close()
                    if pdf.empty:
                        st.caption('No series to plot for the selected window/winners.')
                    else:
                        # Stack vertically: Base on top, Preview (Suppressed) below; both solid lines
                        base_df = pdf[pdf['label']=='Base'].copy()
                        supp_df = pdf[pdf['label']=='Suppressed'].copy()
                        # Rank winners by total base win_share in view (desc) and cap legend to top N
                        top_n_legend = 20
                        agg = base_df.groupby('winner', as_index=False)['win_share'].sum().sort_values('win_share', ascending=False)
                        winners_sorted = agg['winner'].tolist()
                        legend_set = set(winners_sorted[:top_n_legend])
                        y_max = float(pd.concat([base_df['win_share'], supp_df['win_share']], ignore_index=True).max()) if not pdf.empty else 1.0

                        st.subheader('Base')
                        fig_b = go.Figure()
                        for w in winners_sorted:
                            sub_b = base_df[base_df['winner']==w].sort_values('the_date')
                            if not sub_b.empty:
                                fig_b.add_trace(go.Scatter(x=sub_b['the_date'], y=sub_b['win_share'], mode='lines', name=w, line=dict(width=2), showlegend=(w in legend_set)))
                        fig_b.update_layout(width=1400, height=400, margin=dict(l=40,r=40,t=40,b=40), xaxis_title='Date', yaxis_title='Win Share', yaxis=dict(range=[0, y_max*1.05]))
                        st.plotly_chart(fig_b, use_container_width=False)

                        st.subheader('Preview (Suppressed)')
                        fig_s = go.Figure()
                        for w in winners_sorted:
                            sub_s = supp_df[supp_df['winner']==w].sort_values('the_date')
                            if not sub_s.empty:
                                fig_s.add_trace(go.Scatter(x=sub_s['the_date'], y=sub_s['win_share'], mode='lines', name=w, line=dict(width=2), showlegend=(w in legend_set)))
                        fig_s.update_layout(width=1400, height=400, margin=dict(l=40,r=40,t=40,b=40), xaxis_title='Date', yaxis_title='Win Share', yaxis=dict(range=[0, y_max*1.05]))
                        st.plotly_chart(fig_s, use_container_width=False)
        except Exception as e:
            st.error(f'Preview graph failed: {e}')


if __name__ == '__main__':
    ui()
