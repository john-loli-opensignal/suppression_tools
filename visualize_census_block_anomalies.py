#!/usr/bin/env python3
"""
Generate visualizations for census block anomaly analysis.
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import glob
import duckdb

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

DB_PATH = "duck_suppression.db"
DATASET = "gamoshi"
ANALYSIS_DATES = ['2025-06-19', '2025-08-15', '2025-08-16', '2025-08-17', '2025-08-18']
OUTPUT_DIR = Path("census_block_analysis_results")
GRAPHS_DIR = OUTPUT_DIR / "graphs"
GRAPHS_DIR.mkdir(exist_ok=True, parents=True)

def load_outlier_data():
    """Load all outlier CSV files."""
    files = glob.glob(str(OUTPUT_DIR / "outliers_*.csv"))
    data = {}
    for f in files:
        key = Path(f).stem.replace('outliers_', '').rsplit('_', 2)[0]
        data[key] = pd.read_csv(f)
    return data

def plot_outlier_distribution(outliers):
    """Plot distribution of outliers by date and type."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Census Block Outlier Distribution by Date', fontsize=16, fontweight='bold')
    
    positions = [(0, 0), (0, 1), (1, 0), (1, 1)]
    categories = ['mover_win', 'mover_loss', 'non_mover_win', 'non_mover_loss']
    titles = ['Mover Wins', 'Mover Losses', 'Non-Mover Wins', 'Non-Mover Losses']
    
    for (row, col), cat, title in zip(positions, categories, titles):
        ax = axes[row, col]
        if cat in outliers and len(outliers[cat]) > 0:
            df = outliers[cat]
            date_counts = df['the_date'].value_counts().sort_index()
            
            bars = ax.bar(range(len(date_counts)), date_counts.values, color='coral', alpha=0.7, edgecolor='darkred')
            ax.set_xticks(range(len(date_counts)))
            ax.set_xticklabels([d[:10] for d in date_counts.index], rotation=45, ha='right')
            ax.set_ylabel('Number of Outlier Blocks', fontsize=11)
            ax.set_xlabel('Date', fontsize=11)
            ax.set_title(f'{title}\n({len(df)} total outliers)', fontsize=12, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}',
                       ha='center', va='bottom', fontsize=9)
        else:
            ax.text(0.5, 0.5, 'No Outliers Found', ha='center', va='center', 
                   transform=ax.transAxes, fontsize=14, color='gray')
            ax.set_title(title, fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / 'outlier_distribution.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPHS_DIR / 'outlier_distribution.png'}")
    plt.close()

def plot_top_anomalous_blocks(outliers):
    """Plot top anomalous census blocks."""
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('Top 15 Anomalous Census Blocks by Z-Score', fontsize=16, fontweight='bold')
    
    positions = [(0, 0), (0, 1), (1, 0), (1, 1)]
    categories = ['mover_win', 'mover_loss', 'non_mover_win', 'non_mover_loss']
    titles = ['Mover Wins', 'Mover Losses', 'Non-Mover Wins', 'Non-Mover Losses']
    
    for (row, col), cat, title in zip(positions, categories, titles):
        ax = axes[row, col]
        if cat in outliers and len(outliers[cat]) > 0:
            df = outliers[cat].copy()
            df['z_score_abs'] = df['z_score'].abs()
            top15 = df.nlargest(15, 'z_score_abs')
            
            # Create labels
            top15['label'] = top15.apply(
                lambda x: f"{str(x['census_blockid'])[-8:]}\n{x['state'][:2] if pd.notna(x['state']) else 'XX'}", 
                axis=1
            )
            
            colors = ['red' if z > 0 else 'blue' for z in top15['z_score']]
            bars = ax.barh(range(len(top15)), top15['z_score'], color=colors, alpha=0.6, edgecolor='black')
            ax.set_yticks(range(len(top15)))
            ax.set_yticklabels(top15['label'].values, fontsize=8)
            ax.set_xlabel('Z-Score', fontsize=11)
            ax.set_title(f'{title}', fontsize=12, fontweight='bold')
            ax.axvline(x=0, color='black', linestyle='--', linewidth=0.8)
            ax.grid(axis='x', alpha=0.3)
            
            # Add value labels
            for i, (bar, val) in enumerate(zip(bars, top15['z_score'])):
                width = bar.get_width()
                label = f'{val:.1f}'
                ax.text(width, bar.get_y() + bar.get_height()/2., label,
                       ha='left' if width > 0 else 'right', va='center', fontsize=8, fontweight='bold')
        else:
            ax.text(0.5, 0.5, 'No Outliers Found', ha='center', va='center',
                   transform=ax.transAxes, fontsize=14, color='gray')
            ax.set_title(title, fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / 'top_anomalous_blocks.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPHS_DIR / 'top_anomalous_blocks.png'}")
    plt.close()

def plot_state_distribution(outliers):
    """Plot outliers by state."""
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('Outlier Distribution by State (Top 15)', fontsize=16, fontweight='bold')
    
    positions = [(0, 0), (0, 1), (1, 0), (1, 1)]
    categories = ['mover_win', 'mover_loss', 'non_mover_win', 'non_mover_loss']
    titles = ['Mover Wins', 'Mover Losses', 'Non-Mover Wins', 'Non-Mover Losses']
    
    for (row, col), cat, title in zip(positions, categories, titles):
        ax = axes[row, col]
        if cat in outliers and len(outliers[cat]) > 0:
            df = outliers[cat]
            state_counts = df['state'].fillna('Unknown').value_counts().head(15)
            
            bars = ax.barh(range(len(state_counts)), state_counts.values, color='steelblue', alpha=0.7, edgecolor='darkblue')
            ax.set_yticks(range(len(state_counts)))
            ax.set_yticklabels(state_counts.index, fontsize=10)
            ax.set_xlabel('Number of Outlier Blocks', fontsize=11)
            ax.set_title(f'{title}', fontsize=12, fontweight='bold')
            ax.grid(axis='x', alpha=0.3)
            
            # Add value labels
            for bar in bars:
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2., f'{int(width)}',
                       ha='left', va='center', fontsize=9, fontweight='bold')
        else:
            ax.text(0.5, 0.5, 'No Outliers Found', ha='center', va='center',
                   transform=ax.transAxes, fontsize=14, color='gray')
            ax.set_title(title, fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / 'outliers_by_state.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPHS_DIR / 'outliers_by_state.png'}")
    plt.close()

def plot_carrier_pairs(outliers):
    """Plot top carrier pairs involved in outliers."""
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('Top 10 Carrier Pairs in Outliers', fontsize=16, fontweight='bold')
    
    positions = [(0, 0), (0, 1), (1, 0), (1, 1)]
    categories = ['mover_win', 'mover_loss', 'non_mover_win', 'non_mover_loss']
    titles = ['Mover Wins', 'Mover Losses', 'Non-Mover Wins', 'Non-Mover Losses']
    
    for (row, col), cat, title in zip(positions, categories, titles):
        ax = axes[row, col]
        if cat in outliers and len(outliers[cat]) > 0:
            df = outliers[cat].copy()
            df['h2h'] = df['winner'] + ' vs ' + df['loser']
            h2h_counts = df['h2h'].value_counts().head(10)
            
            bars = ax.barh(range(len(h2h_counts)), h2h_counts.values, color='orange', alpha=0.7, edgecolor='darkorange')
            ax.set_yticks(range(len(h2h_counts)))
            
            # Truncate long labels
            labels = [label if len(label) <= 35 else label[:32] + '...' for label in h2h_counts.index]
            ax.set_yticklabels(labels, fontsize=9)
            ax.set_xlabel('Number of Outlier Blocks', fontsize=11)
            ax.set_title(f'{title}', fontsize=12, fontweight='bold')
            ax.grid(axis='x', alpha=0.3)
            
            # Add value labels
            for bar in bars:
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2., f'{int(width)}',
                       ha='left', va='center', fontsize=9, fontweight='bold')
        else:
            ax.text(0.5, 0.5, 'No Outliers Found', ha='center', va='center',
                   transform=ax.transAxes, fontsize=14, color='gray')
            ax.set_title(title, fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / 'outliers_by_carrier_pair.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPHS_DIR / 'outliers_by_carrier_pair.png'}")
    plt.close()

def plot_metric_value_distribution(outliers):
    """Plot distribution of metric values for outliers."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Distribution of Metric Values in Outliers', fontsize=16, fontweight='bold')
    
    positions = [(0, 0), (0, 1), (1, 0), (1, 1)]
    categories = ['mover_win', 'mover_loss', 'non_mover_win', 'non_mover_loss']
    titles = ['Mover Wins', 'Mover Losses', 'Non-Mover Wins', 'Non-Mover Losses']
    
    for (row, col), cat, title in zip(positions, categories, titles):
        ax = axes[row, col]
        if cat in outliers and len(outliers[cat]) > 0:
            df = outliers[cat]
            values = df['metric_value'].values
            
            ax.hist(values, bins=30, color='purple', alpha=0.6, edgecolor='black')
            ax.set_xlabel('Metric Value', fontsize=11)
            ax.set_ylabel('Frequency', fontsize=11)
            ax.set_title(f'{title}\nMean: {values.mean():.2f}, Max: {values.max():.0f}', 
                        fontsize=12, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)
            
            # Add vertical line for mean
            ax.axvline(values.mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {values.mean():.2f}')
            ax.legend()
        else:
            ax.text(0.5, 0.5, 'No Outliers Found', ha='center', va='center',
                   transform=ax.transAxes, fontsize=14, color='gray')
            ax.set_title(title, fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / 'metric_value_distribution.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPHS_DIR / 'metric_value_distribution.png'}")
    plt.close()

def plot_kansas_block_analysis(outliers):
    """Deep dive into the Kansas block that appears frequently."""
    # Kansas block: 201550018002021
    kansas_block = '201550018002021'
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle(f'Deep Dive: Census Block {kansas_block} (Kansas)', fontsize=16, fontweight='bold')
    
    # Collect data for this block
    win_data = []
    loss_data = []
    
    for cat in ['mover_win', 'non_mover_win']:
        if cat in outliers:
            df = outliers[cat]
            block_df = df[df['census_blockid'] == kansas_block]
            for _, row in block_df.iterrows():
                win_data.append({
                    'date': row['the_date'],
                    'type': 'Mover' if 'mover' in cat else 'Non-Mover',
                    'h2h': f"{row['winner']} vs {row['loser']}",
                    'value': row['metric_value'],
                    'z_score': row['z_score']
                })
    
    for cat in ['mover_loss', 'non_mover_loss']:
        if cat in outliers:
            df = outliers[cat]
            block_df = df[df['census_blockid'] == kansas_block]
            for _, row in block_df.iterrows():
                loss_data.append({
                    'date': row['the_date'],
                    'type': 'Mover' if 'mover' in cat else 'Non-Mover',
                    'h2h': f"{row['winner']} vs {row['loser']}",
                    'value': row['metric_value'],
                    'z_score': row['z_score']
                })
    
    # Plot 1: Wins by date
    ax = axes[0, 0]
    if win_data:
        win_df = pd.DataFrame(win_data)
        dates = sorted(win_df['date'].unique())
        date_values = [win_df[win_df['date'] == d]['value'].sum() for d in dates]
        ax.bar(range(len(dates)), date_values, color='green', alpha=0.7, edgecolor='darkgreen')
        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels([d[:10] for d in dates], rotation=45, ha='right')
        ax.set_ylabel('Total Wins', fontsize=11)
        ax.set_title('Wins Over Time', fontsize=12, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)
    
    # Plot 2: Losses by date
    ax = axes[0, 1]
    if loss_data:
        loss_df = pd.DataFrame(loss_data)
        dates = sorted(loss_df['date'].unique())
        date_values = [loss_df[loss_df['date'] == d]['value'].sum() for d in dates]
        ax.bar(range(len(dates)), date_values, color='red', alpha=0.7, edgecolor='darkred')
        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels([d[:10] for d in dates], rotation=45, ha='right')
        ax.set_ylabel('Total Losses', fontsize=11)
        ax.set_title('Losses Over Time', fontsize=12, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)
    
    # Plot 3: Top H2H pairs by wins
    ax = axes[1, 0]
    if win_data:
        win_df = pd.DataFrame(win_data)
        h2h_counts = win_df.groupby('h2h')['value'].sum().sort_values(ascending=True).tail(10)
        ax.barh(range(len(h2h_counts)), h2h_counts.values, color='green', alpha=0.7)
        ax.set_yticks(range(len(h2h_counts)))
        labels = [label if len(label) <= 30 else label[:27] + '...' for label in h2h_counts.index]
        ax.set_yticklabels(labels, fontsize=9)
        ax.set_xlabel('Total Wins', fontsize=11)
        ax.set_title('Top Carrier Pairs (Wins)', fontsize=12, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
    
    # Plot 4: Top H2H pairs by losses
    ax = axes[1, 1]
    if loss_data:
        loss_df = pd.DataFrame(loss_data)
        h2h_counts = loss_df.groupby('h2h')['value'].sum().sort_values(ascending=True).tail(10)
        ax.barh(range(len(h2h_counts)), h2h_counts.values, color='red', alpha=0.7)
        ax.set_yticks(range(len(h2h_counts)))
        labels = [label if len(label) <= 30 else label[:27] + '...' for label in h2h_counts.index]
        ax.set_yticklabels(labels, fontsize=9)
        ax.set_xlabel('Total Losses', fontsize=11)
        ax.set_title('Top Carrier Pairs (Losses)', fontsize=12, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(GRAPHS_DIR / 'kansas_block_deep_dive.png', dpi=150, bbox_inches='tight')
    print(f"Saved: {GRAPHS_DIR / 'kansas_block_deep_dive.png'}")
    plt.close()

def main():
    print("="*80)
    print("GENERATING VISUALIZATIONS")
    print("="*80)
    
    outliers = load_outlier_data()
    print(f"\nLoaded {len(outliers)} outlier datasets")
    
    print("\nGenerating plots...")
    plot_outlier_distribution(outliers)
    plot_top_anomalous_blocks(outliers)
    plot_state_distribution(outliers)
    plot_carrier_pairs(outliers)
    plot_metric_value_distribution(outliers)
    plot_kansas_block_analysis(outliers)
    
    print("\n" + "="*80)
    print("VISUALIZATION COMPLETE")
    print(f"All graphs saved to: {GRAPHS_DIR}")
    print("="*80)

if __name__ == "__main__":
    main()
