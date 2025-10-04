#!/usr/bin/env python3
"""Regenerate overlay graphs with proper visualization (solid on top, dashed underneath)"""
import json
import pandas as pd
import plotly.graph_objs as go
from pathlib import Path

def load_json(filepath):
    """Load JSON file"""
    with open(filepath, 'r') as f:
        return json.load(f)

def create_overlay_graphs(mover_ind):
    """Create overlay graphs for a given mover_ind"""
    results_dir = Path("analysis_results/suppression")
    graphs_dir = results_dir / "graphs"
    graphs_dir.mkdir(parents=True, exist_ok=True)
    
    # Load data
    before_file = results_dir / "data" / f"win_share_before_mover_{mover_ind}.json"
    after_file = results_dir / "data" / f"win_share_after_mover_{mover_ind}.json"
    
    if not before_file.exists() or not after_file.exists():
        print(f"Missing data files for mover_ind={mover_ind}")
        return
    
    before_data = load_json(before_file)
    after_data = load_json(after_file)
    
    # Convert to DataFrames
    df_before = pd.DataFrame(before_data)
    df_after = pd.DataFrame(after_data)
    
    # Target dates to mark
    target_dates = ['2025-06-19', '2025-08-15', '2025-08-16', '2025-08-17', '2025-08-18']
    
    # Get top carriers
    top_carriers = df_before.groupby('winner')['win_share'].mean().nlargest(10).index.tolist()
    
    # Create time series overlay
    fig_ts = go.Figure()
    
    # Add DASHED lines FIRST (underneath)
    for carrier in top_carriers:
        df_c_after = df_after[df_after['winner'] == carrier].sort_values('the_date')
        fig_ts.add_trace(go.Scatter(
            x=df_c_after['the_date'],
            y=df_c_after['win_share'] * 100,
            name=f'{carrier} (after)',
            mode='lines',
            line=dict(dash='dash', width=2),
            showlegend=True,
            legendgroup=carrier
        ))
    
    # Add SOLID lines SECOND (on top)
    for carrier in top_carriers:
        df_c_before = df_before[df_before['winner'] == carrier].sort_values('the_date')
        fig_ts.add_trace(go.Scatter(
            x=df_c_before['the_date'],
            y=df_c_before['win_share'] * 100,
            name=f'{carrier} (before)',
            mode='lines',
            line=dict(width=3),
            showlegend=True,
            legendgroup=carrier
        ))
    
    # Add vertical lines for target dates
    for date in target_dates:
        fig_ts.add_vline(x=date, line=dict(color='red', dash='dot', width=1), opacity=0.5)
    
    fig_ts.update_layout(
        title=f"Win Share Over Time - {'Movers' if mover_ind else 'Non-Movers'}<br><sub>Solid = Before Suppression | Dashed = After Suppression</sub>",
        xaxis_title="Date",
        yaxis_title="Win Share (%)",
        hovermode='x unified',
        height=600,
        template='plotly_white',
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        )
    )
    
    output_file = graphs_dir / f"overlay_timeseries_mover_{mover_ind}.png"
    fig_ts.write_image(str(output_file), width=1400, height=600)
    print(f"✓ Created: {output_file}")
    
    # Create target dates comparison
    df_before_target = df_before[df_before['the_date'].isin(target_dates)]
    df_after_target = df_after[df_after['the_date'].isin(target_dates)]
    
    fig_target = go.Figure()
    
    # Group by date
    for date in target_dates:
        df_b = df_before_target[df_before_target['the_date'] == date].nlargest(10, 'win_share')
        df_a = df_after_target[df_after_target['the_date'] == date]
        
        # Merge to get before/after for same carriers
        df_merged = pd.merge(
            df_b[['winner', 'win_share']],
            df_a[['winner', 'win_share']],
            on='winner',
            suffixes=('_before', '_after'),
            how='left'
        ).fillna(0)
        
        # Add bars for this date - AFTER first (underneath)
        fig_target.add_trace(go.Bar(
            name=f'{date} (after)',
            x=df_merged['winner'],
            y=df_merged['win_share_after'] * 100,
            marker_color='orange',
            opacity=0.7,
            legendgroup=date
        ))
        
        # BEFORE second (on top)
        fig_target.add_trace(go.Bar(
            name=f'{date} (before)',
            x=df_merged['winner'],
            y=df_merged['win_share_before'] * 100,
            marker_color='blue',
            opacity=0.5,
            legendgroup=date
        ))
    
    fig_target.update_layout(
        title=f"Win Share on Target Dates - {'Movers' if mover_ind else 'Non-Movers'}<br><sub>Blue = Before | Orange = After Suppression</sub>",
        xaxis_title="Carrier",
        yaxis_title="Win Share (%)",
        barmode='overlay',
        height=600,
        template='plotly_white',
        xaxis_tickangle=-45
    )
    
    output_file = graphs_dir / f"overlay_target_dates_mover_{mover_ind}.png"
    fig_target.write_image(str(output_file), width=1400, height=600)
    print(f"✓ Created: {output_file}")

if __name__ == "__main__":
    print("Regenerating overlay graphs with proper layering...")
    print("=" * 60)
    
    # Delete old graphs
    graphs_dir = Path("analysis_results/suppression/graphs")
    if graphs_dir.exists():
        for img in graphs_dir.glob("*.png"):
            img.unlink()
            print(f"✗ Deleted: {img}")
    
    print()
    create_overlay_graphs(True)  # Movers
    create_overlay_graphs(False)  # Non-movers
    
    print()
    print("=" * 60)
    print("✓ All graphs regenerated successfully!")
    print("  - Solid lines (before) are layered ON TOP")
    print("  - Dashed lines (after) are layered UNDERNEATH")
    print("  - This makes suppression changes clearly visible")
