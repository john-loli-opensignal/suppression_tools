#!/usr/bin/env python3
"""
Census Block Anomaly Analysis for Gamoshi
Dates: 2025-06-19 and 2025-08-15 through 2025-08-18

This script performs detailed outlier detection at the census block level
using statistical methods (z-score, IQR) and temporal analysis.
"""
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime

DB_PATH = "duck_suppression.db"
DATASET = "gamoshi"
ANALYSIS_DATES = ['2025-06-19', '2025-08-15', '2025-08-16', '2025-08-17', '2025-08-18']

def get_connection():
    """Get database connection."""
    return duckdb.connect(DB_PATH, read_only=True)

def analyze_date_range_overview(con):
    """Get overview of activity across all dates."""
    print("\n" + "="*80)
    print("DATE RANGE OVERVIEW")
    print("="*80)
    
    results = {}
    for mover_type in ['mover', 'non_mover']:
        for metric in ['win', 'loss']:
            table = f"{DATASET}_{metric}_{mover_type}_census_cube"
            
            sql = f"""
            SELECT 
                the_date,
                COUNT(DISTINCT census_blockid) as unique_blocks,
                COUNT(DISTINCT CONCAT(winner, '|', loser)) as unique_h2h,
                COUNT(DISTINCT state) as unique_states,
                COUNT(DISTINCT dma_name) as unique_dmas,
                SUM(total_{metric}s) as total_metric,
                AVG(total_{metric}s) as avg_metric,
                STDDEV(total_{metric}s) as stddev_metric,
                MAX(total_{metric}s) as max_metric,
                COUNT(*) as record_count
            FROM {table}
            WHERE the_date IN ({','.join([f"'{d}'" for d in ANALYSIS_DATES])})
            GROUP BY the_date
            ORDER BY the_date
            """
            
            df = con.execute(sql).fetchdf()
            results[f"{mover_type}_{metric}"] = df
            
            print(f"\n{mover_type.upper()} {metric.upper()}S:")
            print(df.to_string(index=False))
    
    return results

def find_statistical_outliers(con, z_threshold=3.0, iqr_multiplier=3.0):
    """Find census blocks with statistical anomalies."""
    print("\n" + "="*80)
    print(f"STATISTICAL OUTLIER DETECTION (Z-Score > {z_threshold}, IQR > {iqr_multiplier}x)")
    print("="*80)
    
    outliers = {}
    
    for mover_type in ['mover', 'non_mover']:
        for metric in ['win', 'loss']:
            table = f"{DATASET}_{metric}_{mover_type}_census_cube"
            
            sql = f"""
            WITH date_blocks AS (
                SELECT 
                    the_date,
                    census_blockid,
                    state,
                    dma_name,
                    winner,
                    loser,
                    total_{metric}s as metric_value
                FROM {table}
                WHERE the_date IN ({','.join([f"'{d}'" for d in ANALYSIS_DATES])})
            ),
            stats AS (
                SELECT 
                    the_date,
                    AVG(metric_value) as mean_val,
                    STDDEV(metric_value) as stddev_val,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY metric_value) as q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) as q3
                FROM date_blocks
                GROUP BY the_date
            ),
            outliers AS (
                SELECT 
                    b.*,
                    s.mean_val,
                    s.stddev_val,
                    s.q1,
                    s.q3,
                    (s.q3 - s.q1) as iqr,
                    (b.metric_value - s.mean_val) / NULLIF(s.stddev_val, 0) as z_score,
                    CASE 
                        WHEN b.metric_value > s.q3 + {iqr_multiplier} * (s.q3 - s.q1) THEN 'HIGH_IQR'
                        WHEN b.metric_value < s.q1 - {iqr_multiplier} * (s.q3 - s.q1) THEN 'LOW_IQR'
                        ELSE 'NORMAL'
                    END as iqr_flag
                FROM date_blocks b
                JOIN stats s ON b.the_date = s.the_date
                WHERE ABS((b.metric_value - s.mean_val) / NULLIF(s.stddev_val, 0)) > {z_threshold}
                   OR b.metric_value > s.q3 + {iqr_multiplier} * (s.q3 - s.q1)
                   OR b.metric_value < s.q1 - {iqr_multiplier} * (s.q3 - s.q1)
            )
            SELECT * FROM outliers
            ORDER BY ABS(z_score) DESC, the_date, metric_value DESC
            """
            
            df = con.execute(sql).fetchdf()
            outliers[f"{mover_type}_{metric}"] = df
            
            print(f"\n{mover_type.upper()} {metric.upper()}S - Found {len(df)} outlier blocks:")
            if len(df) > 0:
                print(f"  Top 10 by Z-Score:")
                print(df[['the_date', 'census_blockid', 'state', 'winner', 'loser', 
                         'metric_value', 'z_score', 'iqr_flag']].head(10).to_string(index=False))
            else:
                print("  No outliers found")
    
    return outliers

def analyze_temporal_anomalies(con):
    """Find blocks with unusual temporal patterns (sudden spikes/drops)."""
    print("\n" + "="*80)
    print("TEMPORAL ANOMALY DETECTION")
    print("="*80)
    
    temporal_anomalies = {}
    
    for mover_type in ['mover', 'non_mover']:
        for metric in ['win', 'loss']:
            table = f"{DATASET}_{metric}_{mover_type}_census_cube"
            
            # Find blocks active on multiple analysis dates with big variance
            sql = f"""
            WITH block_activity AS (
                SELECT 
                    census_blockid,
                    state,
                    dma_name,
                    winner,
                    loser,
                    the_date,
                    total_{metric}s as metric_value
                FROM {table}
                WHERE the_date IN ({','.join([f"'{d}'" for d in ANALYSIS_DATES])})
            ),
            block_stats AS (
                SELECT 
                    census_blockid,
                    state,
                    dma_name,
                    winner,
                    loser,
                    COUNT(*) as date_count,
                    AVG(metric_value) as avg_value,
                    STDDEV(metric_value) as stddev_value,
                    MAX(metric_value) as max_value,
                    MIN(metric_value) as min_value,
                    (MAX(metric_value) - MIN(metric_value)) as range_value
                FROM block_activity
                GROUP BY census_blockid, state, dma_name, winner, loser
                HAVING COUNT(*) > 1  -- Active on multiple dates
            )
            SELECT *,
                   range_value / NULLIF(avg_value, 0) as coefficient_of_variation
            FROM block_stats
            WHERE stddev_value > avg_value * 2  -- High variability
            ORDER BY coefficient_of_variation DESC
            LIMIT 100
            """
            
            df = con.execute(sql).fetchdf()
            temporal_anomalies[f"{mover_type}_{metric}"] = df
            
            print(f"\n{mover_type.upper()} {metric.upper()}S - Found {len(df)} blocks with temporal anomalies:")
            if len(df) > 0:
                print(f"  Top 5 by coefficient of variation:")
                print(df[['census_blockid', 'state', 'winner', 'loser', 'date_count',
                         'avg_value', 'stddev_value', 'coefficient_of_variation']].head(5).to_string(index=False))
    
    return temporal_anomalies

def analyze_geographic_concentration(con):
    """Find geographic areas with unusual concentration of activity."""
    print("\n" + "="*80)
    print("GEOGRAPHIC CONCENTRATION ANALYSIS")
    print("="*80)
    
    concentrations = {}
    
    for mover_type in ['mover', 'non_mover']:
        for metric in ['win', 'loss']:
            table = f"{DATASET}_{metric}_{mover_type}_census_cube"
            
            # Find DMAs and states with abnormally high activity
            sql = f"""
            WITH dma_activity AS (
                SELECT 
                    the_date,
                    state,
                    dma_name,
                    winner,
                    loser,
                    COUNT(DISTINCT census_blockid) as unique_blocks,
                    SUM(total_{metric}s) as total_metric,
                    AVG(total_{metric}s) as avg_metric,
                    MAX(total_{metric}s) as max_metric
                FROM {table}
                WHERE the_date IN ({','.join([f"'{d}'" for d in ANALYSIS_DATES])})
                GROUP BY the_date, state, dma_name, winner, loser
            ),
            dma_stats AS (
                SELECT 
                    AVG(total_metric) as mean_metric,
                    STDDEV(total_metric) as stddev_metric
                FROM dma_activity
            )
            SELECT 
                a.*,
                (a.total_metric - s.mean_metric) / NULLIF(s.stddev_metric, 0) as z_score
            FROM dma_activity a, dma_stats s
            WHERE ABS((a.total_metric - s.mean_metric) / NULLIF(s.stddev_metric, 0)) > 2.5
            ORDER BY ABS(z_score) DESC
            LIMIT 50
            """
            
            df = con.execute(sql).fetchdf()
            concentrations[f"{mover_type}_{metric}"] = df
            
            print(f"\n{mover_type.upper()} {metric.upper()}S - Found {len(df)} DMAs with concentration anomalies:")
            if len(df) > 0:
                print(f"  Top 5 by Z-Score:")
                print(df[['the_date', 'state', 'dma_name', 'winner', 'loser',
                         'unique_blocks', 'total_metric', 'z_score']].head(5).to_string(index=False))
    
    return concentrations

def analyze_impossible_patterns(con):
    """Find blocks with impossible or highly suspicious patterns."""
    print("\n" + "="*80)
    print("IMPOSSIBLE PATTERN DETECTION")
    print("="*80)
    
    impossible = {}
    
    for mover_type in ['mover', 'non_mover']:
        # Find blocks where the same block has very high wins AND losses for same H2H
        sql = f"""
        WITH win_blocks AS (
            SELECT 
                census_blockid,
                state,
                dma_name,
                winner,
                loser,
                the_date,
                total_wins
            FROM {DATASET}_win_{mover_type}_census_cube
            WHERE the_date IN ({','.join([f"'{d}'" for d in ANALYSIS_DATES])})
        ),
        loss_blocks AS (
            SELECT 
                census_blockid,
                state,
                dma_name,
                winner,
                loser,
                the_date,
                total_losss
            FROM {DATASET}_loss_{mover_type}_census_cube
            WHERE the_date IN ({','.join([f"'{d}'" for d in ANALYSIS_DATES])})
        )
        SELECT 
            w.the_date,
            w.census_blockid,
            w.state,
            w.dma_name,
            w.winner,
            w.loser,
            w.total_wins,
            l.total_losss as total_losses,
            w.total_wins + l.total_losss as total_activity,
            CAST(w.total_wins AS FLOAT) / NULLIF(w.total_wins + l.total_losss, 0) as win_rate
        FROM win_blocks w
        JOIN loss_blocks l 
            ON w.census_blockid = l.census_blockid 
            AND w.the_date = l.the_date
            AND w.winner = l.winner
            AND w.loser = l.loser
        WHERE w.total_wins > 1000 OR l.total_losss > 1000  -- High activity threshold
        ORDER BY total_activity DESC
        LIMIT 100
        """
        
        df = con.execute(sql).fetchdf()
        impossible[mover_type] = df
        
        print(f"\n{mover_type.upper()} - Found {len(df)} high-activity blocks:")
        if len(df) > 0:
            print(f"  Top 10 by total activity:")
            print(df[['the_date', 'census_blockid', 'state', 'winner', 'loser',
                     'total_wins', 'total_losses', 'total_activity', 'win_rate']].head(10).to_string(index=False))
    
    return impossible

def analyze_carrier_pairs(con):
    """Analyze specific carrier pairs for anomalies."""
    print("\n" + "="*80)
    print("TOP CARRIER PAIRS ANALYSIS")
    print("="*80)
    
    for mover_type in ['mover', 'non_mover']:
        sql = f"""
        SELECT 
            winner,
            loser,
            COUNT(DISTINCT census_blockid) as unique_blocks,
            COUNT(DISTINCT the_date) as active_dates,
            COUNT(DISTINCT state) as unique_states,
            SUM(total_wins) as total_wins
        FROM {DATASET}_win_{mover_type}_census_cube
        WHERE the_date IN ({','.join([f"'{d}'" for d in ANALYSIS_DATES])})
        GROUP BY winner, loser
        ORDER BY total_wins DESC
        LIMIT 20
        """
        
        df = con.execute(sql).fetchdf()
        print(f"\n{mover_type.upper()} - Top 20 Carrier Pairs by Total Wins:")
        print(df.to_string(index=False))

def generate_summary_stats(outliers, temporal, concentrations, impossible):
    """Generate summary statistics for the report."""
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    
    total_outliers = sum(len(df) for df in outliers.values())
    total_temporal = sum(len(df) for df in temporal.values())
    total_concentration = sum(len(df) for df in concentrations.values())
    total_impossible = sum(len(df) for df in impossible.values())
    
    print(f"\nTotal Statistical Outliers: {total_outliers}")
    print(f"Total Temporal Anomalies: {total_temporal}")
    print(f"Total Geographic Concentrations: {total_concentration}")
    print(f"Total High-Activity Patterns: {total_impossible}")
    print(f"\nGrand Total Flagged Anomalies: {total_outliers + total_temporal + total_concentration + total_impossible}")
    
    # Break down by category
    print("\nBreakdown by Category:")
    for category, data in [
        ("Statistical Outliers", outliers),
        ("Temporal Anomalies", temporal),
        ("Geographic Concentrations", concentrations),
        ("High-Activity Patterns", impossible)
    ]:
        print(f"\n{category}:")
        for key, df in data.items():
            print(f"  {key}: {len(df)} anomalies")

def export_results(outliers, temporal, concentrations, impossible):
    """Export results to CSV files."""
    output_dir = Path("census_block_analysis_results")
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("\n" + "="*80)
    print("EXPORTING RESULTS")
    print("="*80)
    
    # Export each category
    for category, data, prefix in [
        ("Statistical Outliers", outliers, "outliers"),
        ("Temporal Anomalies", temporal, "temporal"),
        ("Geographic Concentrations", concentrations, "concentration"),
        ("High-Activity Patterns", impossible, "high_activity")
    ]:
        for key, df in data.items():
            if len(df) > 0:
                filename = output_dir / f"{prefix}_{key}_{timestamp}.csv"
                df.to_csv(filename, index=False)
                print(f"Exported: {filename} ({len(df)} rows)")
    
    print(f"\nAll results exported to: {output_dir}")

def main():
    """Run complete analysis."""
    print("="*80)
    print("CENSUS BLOCK ANOMALY ANALYSIS - GAMOSHI")
    print(f"Analysis Dates: {', '.join(ANALYSIS_DATES)}")
    print("="*80)
    
    con = get_connection()
    
    try:
        # Run all analyses
        overview = analyze_date_range_overview(con)
        outliers = find_statistical_outliers(con, z_threshold=3.0, iqr_multiplier=3.0)
        temporal = analyze_temporal_anomalies(con)
        concentrations = analyze_geographic_concentration(con)
        impossible = analyze_impossible_patterns(con)
        analyze_carrier_pairs(con)
        
        # Generate summary
        generate_summary_stats(outliers, temporal, concentrations, impossible)
        
        # Export results
        export_results(outliers, temporal, concentrations, impossible)
        
        print("\n" + "="*80)
        print("ANALYSIS COMPLETE")
        print("="*80)
        
    finally:
        con.close()

if __name__ == "__main__":
    main()
