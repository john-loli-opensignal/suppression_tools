#!/usr/bin/env python3
"""
Iterative Suppression Testing Framework

Tests the effectiveness of outlier detection and removal through multiple rounds.
Goal: Remove DMA-level outliers until national and H2H metrics stabilize.
"""

import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
import json
from typing import Dict, List, Tuple
import sys

# Database path assertion
DB_PATH = Path(__file__).parent / "data" / "databases" / "duck_suppression.db"
assert DB_PATH.exists(), f"❌ CRITICAL: Database not found at {DB_PATH}. DO NOT create multiple DBs!"

# Configuration
DATASET = "gamoshi"
MOVER_IND = True  # Start with movers
ANALYSIS_START = "2025-06-01"
ANALYSIS_END = "2025-09-04"
Z_THRESHOLD = 1.5
MIN_WINS_THRESHOLD = 10
MAX_ROUNDS = 3


class SuppressionRound:
    """Tracks a single round of suppression testing"""
    
    def __init__(self, round_num: int):
        self.round_num = round_num
        self.timestamp = datetime.now().isoformat()
        self.outliers_detected = []
        self.records_suppressed = 0
        self.metrics_before = {}
        self.metrics_after = {}
        
    def to_dict(self):
        return {
            "round": self.round_num,
            "timestamp": self.timestamp,
            "outliers_detected": len(self.outliers_detected),
            "records_suppressed": self.records_suppressed,
            "metrics_before": self.metrics_before,
            "metrics_after": self.metrics_after
        }


def get_national_metrics(con: duckdb.DuckDBPyConnection, mover_type: str, 
                        suppression_table: str = None) -> pd.DataFrame:
    """Calculate national win shares for each carrier"""
    
    base_table = f"{DATASET}_win_{mover_type}_cube"
    
    if suppression_table:
        # Apply suppressions
        query = f"""
        WITH base AS (
            SELECT c.* 
            FROM {base_table} c
            LEFT JOIN {suppression_table} s
                ON c.the_date = s.the_date
                AND c.dma_name = s.dma_name
                AND c.winner = s.winner
                AND c.loser = s.loser
            WHERE s.the_date IS NULL  -- Not suppressed
                AND c.the_date BETWEEN '{ANALYSIS_START}' AND '{ANALYSIS_END}'
        )
        SELECT 
            the_date,
            winner as carrier,
            SUM(total_wins) as wins,
            SUM(SUM(total_wins)) OVER (PARTITION BY the_date) as total_daily_wins
        FROM base
        GROUP BY the_date, winner
        ORDER BY the_date, carrier
        """
    else:
        query = f"""
        SELECT 
            the_date,
            winner as carrier,
            SUM(total_wins) as wins,
            SUM(SUM(total_wins)) OVER (PARTITION BY the_date) as total_daily_wins
        FROM {base_table}
        WHERE the_date BETWEEN '{ANALYSIS_START}' AND '{ANALYSIS_END}'
        GROUP BY the_date, winner
        ORDER BY the_date, carrier
        """
    
    df = con.execute(query).df()
    df['win_share'] = df['wins'] / df['total_daily_wins'] * 100
    return df


def get_h2h_metrics(con: duckdb.DuckDBPyConnection, mover_type: str,
                   suppression_table: str = None) -> pd.DataFrame:
    """Calculate H2H win rates for carrier pairs"""
    
    base_table = f"{DATASET}_win_{mover_type}_cube"
    
    if suppression_table:
        query = f"""
        WITH base AS (
            SELECT c.* 
            FROM {base_table} c
            LEFT JOIN {suppression_table} s
                ON c.the_date = s.the_date
                AND c.dma_name = s.dma_name
                AND c.winner = s.winner
                AND c.loser = s.loser
            WHERE s.the_date IS NULL
                AND c.the_date BETWEEN '{ANALYSIS_START}' AND '{ANALYSIS_END}'
        )
        SELECT 
            the_date,
            winner,
            loser,
            SUM(total_wins) as wins
        FROM base
        GROUP BY the_date, winner, loser
        ORDER BY the_date, winner, loser
        """
    else:
        query = f"""
        SELECT 
            the_date,
            winner,
            loser,
            SUM(total_wins) as wins
        FROM {base_table}
        WHERE the_date BETWEEN '{ANALYSIS_START}' AND '{ANALYSIS_END}'
        GROUP BY the_date, winner, loser
        ORDER BY the_date, winner, loser
        """
    
    df = con.execute(query).df()
    return df


def calculate_anomaly_score(df: pd.DataFrame, metric_col: str = 'win_share') -> Dict:
    """Calculate overall anomaly metrics for a dataset"""
    
    # Group by carrier and calculate statistics
    stats = df.groupby('carrier')[metric_col].agg([
        'mean', 'std', 'min', 'max', 
        ('q25', lambda x: x.quantile(0.25)),
        ('q75', lambda x: x.quantile(0.75))
    ]).reset_index()
    
    # Calculate IQR and coefficient of variation
    stats['iqr'] = stats['q75'] - stats['q25']
    stats['cv'] = stats['std'] / stats['mean']
    stats['range'] = stats['max'] - stats['min']
    
    # Overall volatility score
    total_volatility = stats['cv'].mean()
    max_range = stats['range'].max()
    
    return {
        'total_volatility': float(total_volatility),
        'max_range': float(max_range),
        'mean_std': float(stats['std'].mean()),
        'num_carriers': len(stats),
        'top_volatile': stats.nlargest(5, 'cv')[['carrier', 'cv', 'range']].to_dict('records')
    }


def detect_dma_outliers(con: duckdb.DuckDBPyConnection, mover_type: str,
                       suppression_table: str = None) -> pd.DataFrame:
    """Detect DMA-level outliers using the rolling view"""
    
    view_name = f"{DATASET}_win_{mover_type}_rolling"
    
    if suppression_table:
        # Apply existing suppressions
        query = f"""
        WITH base AS (
            SELECT v.*,
                   CASE WHEN v.avg_wins_28d > 0 
                        THEN v.current_wins - v.avg_wins_28d
                        ELSE v.current_wins 
                   END as impact
            FROM {view_name} v
            LEFT JOIN {suppression_table} s
                ON v.the_date = s.the_date
                AND v.dma_name = s.dma_name
                AND v.winner = s.winner
                AND v.loser = s.loser
            WHERE s.the_date IS NULL
                AND v.the_date BETWEEN '{ANALYSIS_START}' AND '{ANALYSIS_END}'
        )
        SELECT *
        FROM base
        WHERE current_wins >= {MIN_WINS_THRESHOLD}
            AND (
                z_score_28d > {Z_THRESHOLD}
                OR pct_change_28d > 0.30
                OR is_first_appearance = true
            )
        ORDER BY impact DESC
        """
    else:
        query = f"""
        WITH base AS (
            SELECT *,
                   CASE WHEN avg_wins_28d > 0 
                        THEN current_wins - avg_wins_28d
                        ELSE current_wins 
                   END as impact
            FROM {view_name}
            WHERE the_date BETWEEN '{ANALYSIS_START}' AND '{ANALYSIS_END}'
        )
        SELECT *
        FROM base
        WHERE current_wins >= {MIN_WINS_THRESHOLD}
            AND (
                z_score_28d > {Z_THRESHOLD}
                OR pct_change_28d > 0.30
                OR is_first_appearance = true
            )
        ORDER BY impact DESC
        """
    
    return con.execute(query).df()


def run_suppression_round(con: duckdb.DuckDBPyConnection, round_num: int,
                         mover_type: str, prev_suppression_table: str = None) -> SuppressionRound:
    """Execute one round of outlier detection and suppression"""
    
    print(f"\n{'='*80}")
    print(f"ROUND {round_num}: Detecting and Suppressing Outliers")
    print(f"{'='*80}\n")
    
    round_obj = SuppressionRound(round_num)
    
    # 1. Get baseline metrics BEFORE suppression
    print("📊 Calculating baseline metrics...")
    nat_before = get_national_metrics(con, mover_type, prev_suppression_table)
    h2h_before = get_h2h_metrics(con, mover_type, prev_suppression_table)
    
    round_obj.metrics_before = {
        'national': calculate_anomaly_score(nat_before),
        'h2h_records': len(h2h_before),
        'total_wins': int(nat_before['wins'].sum())
    }
    
    print(f"  National volatility: {round_obj.metrics_before['national']['total_volatility']:.4f}")
    print(f"  Max carrier range: {round_obj.metrics_before['national']['max_range']:.2f}%")
    print(f"  Total wins: {round_obj.metrics_before['total_wins']:,}")
    
    # 2. Detect outliers
    print("\n🔍 Detecting DMA-level outliers...")
    outliers = detect_dma_outliers(con, mover_type, prev_suppression_table)
    
    if len(outliers) == 0:
        print("✅ No outliers detected! Suppression complete.")
        # Set after metrics same as before since nothing changed
        round_obj.metrics_after = round_obj.metrics_before
        return round_obj
    
    round_obj.outliers_detected = outliers.to_dict('records')
    round_obj.records_suppressed = len(outliers)
    
    print(f"  Found {len(outliers)} outlier records")
    print(f"  Total impact to suppress: {outliers['impact'].sum():,.0f} wins")
    
    # Show top outliers
    print("\n  Top 10 outliers by impact:")
    top_outliers = outliers.head(10)
    for idx, row in top_outliers.iterrows():
        print(f"    {row['the_date']} | {row['dma_name']:20s} | {row['winner']:15s} vs {row['loser']:15s}")
        print(f"      Current: {row['current_wins']:6.0f} | Avg: {row['avg_wins_28d']:6.1f} | "
              f"Z-score: {row['z_score_28d']:5.2f} | Impact: {row['impact']:6.0f}")
    
    # 3. Create suppression table for this round
    suppression_table = f"suppression_round_{round_num}_{'mover' if mover_type == 'mover' else 'non_mover'}"
    
    # Select key columns for suppression tracking
    outliers_subset = outliers[['the_date', 'dma_name', 'winner', 'loser', 'state',
                                 'current_wins', 'avg_wins_28d', 'z_score_28d', 'pct_change_28d', 
                                 'impact', 'is_first_appearance']].copy()
    outliers_subset['suppression_round'] = round_num
    
    # Combine with previous suppressions if they exist
    if prev_suppression_table:
        con.execute(f"""
        CREATE OR REPLACE TABLE {suppression_table} AS
        SELECT * FROM {prev_suppression_table}
        UNION ALL
        SELECT * FROM outliers_subset
        """)
        outliers_subset.to_sql(name='outliers_subset', con=con, if_exists='replace', index=False)
        con.execute(f"""
        CREATE OR REPLACE TABLE {suppression_table} AS
        SELECT * FROM {prev_suppression_table}
        UNION ALL
        SELECT * FROM outliers_subset
        """)
    else:
        # Create new suppression table
        outliers_subset.to_sql(name=suppression_table, con=con, if_exists='replace', index=False)
    
    # 4. Calculate metrics AFTER suppression
    print("\n📊 Calculating metrics after suppression...")
    nat_after = get_national_metrics(con, mover_type, suppression_table)
    h2h_after = get_h2h_metrics(con, mover_type, suppression_table)
    
    round_obj.metrics_after = {
        'national': calculate_anomaly_score(nat_after),
        'h2h_records': len(h2h_after),
        'total_wins': int(nat_after['wins'].sum())
    }
    
    print(f"  National volatility: {round_obj.metrics_after['national']['total_volatility']:.4f}")
    print(f"  Max carrier range: {round_obj.metrics_after['national']['max_range']:.2f}%")
    print(f"  Total wins: {round_obj.metrics_after['total_wins']:,}")
    
    # 5. Calculate improvement
    vol_before = round_obj.metrics_before['national']['total_volatility']
    vol_after = round_obj.metrics_after['national']['total_volatility']
    improvement = (vol_before - vol_after) / vol_before * 100
    
    print(f"\n📈 Improvement: {improvement:.2f}% reduction in volatility")
    
    return round_obj


def run_iterative_suppression(mover_type: str = "mover") -> List[SuppressionRound]:
    """Run multiple rounds of suppression until convergence or max rounds"""
    
    print(f"\n{'#'*80}")
    print(f"# ITERATIVE SUPPRESSION TEST")
    print(f"# Dataset: {DATASET} | Mover: {mover_type}")
    print(f"# Period: {ANALYSIS_START} to {ANALYSIS_END}")
    print(f"# Max Rounds: {MAX_ROUNDS}")
    print(f"{'#'*80}\n")
    
    con = duckdb.connect(str(DB_PATH))
    rounds = []
    
    prev_suppression_table = None
    
    for round_num in range(1, MAX_ROUNDS + 1):
        round_obj = run_suppression_round(con, round_num, mover_type, prev_suppression_table)
        rounds.append(round_obj)
        
        # Check if we found any outliers
        if len(round_obj.outliers_detected) == 0:
            print(f"\n✅ Convergence achieved in {round_num} rounds!")
            break
        
        # Update for next round
        prev_suppression_table = f"suppression_round_{round_num}_{'mover' if mover_type == 'mover' else 'non_mover'}"
        
        # Check improvement
        if round_num > 1:
            prev_vol = rounds[-2].metrics_after['national']['total_volatility']
            curr_vol = rounds[-1].metrics_after['national']['total_volatility']
            
            if abs(prev_vol - curr_vol) / prev_vol < 0.01:  # Less than 1% improvement
                print(f"\n⚠️  Minimal improvement detected. Stopping after {round_num} rounds.")
                break
    
    con.close()
    
    # Generate summary
    print_summary(rounds, mover_type)
    
    # Save results
    save_results(rounds, mover_type)
    
    return rounds


def print_summary(rounds: List[SuppressionRound], mover_type: str):
    """Print comprehensive summary of all rounds"""
    
    print(f"\n{'='*80}")
    print(f"FINAL SUMMARY: {DATASET} - {mover_type}")
    print(f"{'='*80}\n")
    
    # Create comparison table
    summary_data = []
    for r in rounds:
        summary_data.append({
            'Round': r.round_num,
            'Outliers': len(r.outliers_detected),
            'Records Suppressed': r.records_suppressed,
            'Volatility Before': r.metrics_before['national']['total_volatility'],
            'Volatility After': r.metrics_after['national']['total_volatility'],
            'Max Range Before': r.metrics_before['national']['max_range'],
            'Max Range After': r.metrics_after['national']['max_range'],
            'Wins Before': r.metrics_before['total_wins'],
            'Wins After': r.metrics_after['total_wins']
        })
    
    df_summary = pd.DataFrame(summary_data)
    print(df_summary.to_string(index=False))
    
    # Calculate total improvement
    initial_vol = rounds[0].metrics_before['national']['total_volatility']
    final_vol = rounds[-1].metrics_after['national']['total_volatility']
    total_improvement = (initial_vol - final_vol) / initial_vol * 100
    
    print(f"\n📊 TOTAL IMPROVEMENT: {total_improvement:.2f}% reduction in volatility")
    print(f"📉 Initial Volatility: {initial_vol:.4f}")
    print(f"📉 Final Volatility: {final_vol:.4f}")
    
    # Total records suppressed
    total_suppressed = sum(r.records_suppressed for r in rounds)
    print(f"\n🚫 Total records suppressed: {total_suppressed:,}")


def save_results(rounds: List[SuppressionRound], mover_type: str):
    """Save results to JSON file"""
    
    output_file = Path("analysis_results") / f"iterative_suppression_{mover_type}_{DATASET}.json"
    output_file.parent.mkdir(exist_ok=True)
    
    results = {
        'dataset': DATASET,
        'mover_type': mover_type,
        'analysis_period': {
            'start': ANALYSIS_START,
            'end': ANALYSIS_END
        },
        'configuration': {
            'z_threshold': Z_THRESHOLD,
            'min_wins_threshold': MIN_WINS_THRESHOLD,
            'max_rounds': MAX_ROUNDS
        },
        'rounds': [r.to_dict() for r in rounds],
        'summary': {
            'total_rounds': len(rounds),
            'total_outliers': sum(len(r.outliers_detected) for r in rounds),
            'initial_volatility': rounds[0].metrics_before['national']['total_volatility'],
            'final_volatility': rounds[-1].metrics_after['national']['total_volatility'],
            'improvement_pct': (
                (rounds[0].metrics_before['national']['total_volatility'] - 
                 rounds[-1].metrics_after['national']['total_volatility']) /
                rounds[0].metrics_before['national']['total_volatility'] * 100
            )
        }
    }
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to: {output_file}")


if __name__ == "__main__":
    # Run for movers first
    mover_rounds = run_iterative_suppression("mover")
    
    print("\n" + "="*80)
    print("Would you like to run for non-movers as well? (This was configured for movers)")
    print("="*80)
