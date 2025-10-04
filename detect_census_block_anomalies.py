#!/usr/bin/env python3
"""
Comprehensive Census Block Anomaly Detection with DOW patterns and First Appearance tracking.

Detects multiple types of anomalies:
1. Statistical Outliers (Z-score, IQR) - accounting for day-of-week patterns
2. First Appearances - new carrier activity in census blocks
3. Volume Spikes - unusual increases in activity
4. Geographic Concentration - suspicious clustering
5. Impossible Metrics - data quality issues
"""
import argparse
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json

DB_PATH = "duck_suppression.db"
OUTPUT_DIR = Path("census_block_analysis_results")


class CensusBlockAnomalyDetector:
    def __init__(self, db_path, ds, target_dates):
        self.db_path = db_path
        self.ds = ds
        self.target_dates = target_dates
        self.con = duckdb.connect(db_path, read_only=True)
        
        # Results storage
        self.results = {
            'statistical_outliers': [],
            'first_appearances': [],
            'volume_spikes': [],
            'geographic_concentrations': [],
            'impossible_metrics': [],
            'summary_stats': {}
        }
    
    def get_dow_baseline(self, mover_ind, metric_type, lookback_days=90):
        """
        Calculate day-of-week baseline statistics to account for weekend/weekday patterns.
        
        Returns baseline stats grouped by day of week.
        """
        mover_str = 'mover' if mover_ind else 'non_mover'
        table_name = f"{self.ds}_{metric_type}_{mover_str}_census_cube"
        
        # Get historical data by day of week
        sql = f"""
        WITH historical_data AS (
            SELECT 
                the_date,
                DAYOFWEEK(the_date) as dow,
                DAYNAME(the_date) as dow_name,
                census_blockid,
                winner,
                loser,
                state,
                dma_name,
                total_{metric_type}s as metric_value
            FROM {table_name}
            WHERE the_date < '{min(self.target_dates)}'
              AND the_date >= CAST('{min(self.target_dates)}' AS DATE) - INTERVAL {lookback_days} DAY
        )
        SELECT 
            dow,
            dow_name,
            census_blockid,
            winner,
            loser,
            state,
            dma_name,
            AVG(metric_value) as mean_value,
            STDDEV(metric_value) as std_value,
            MIN(metric_value) as min_value,
            MAX(metric_value) as max_value,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY metric_value) as q1,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) as q3,
            COUNT(*) as historical_count
        FROM historical_data
        GROUP BY dow, dow_name, census_blockid, winner, loser, state, dma_name
        HAVING COUNT(*) >= 2  -- Need at least 2 observations
        """
        
        return self.con.execute(sql).df()
    
    def detect_statistical_outliers(self, mover_ind, metric_type, z_threshold=3.0, iqr_multiplier=1.5):
        """
        Detect statistical outliers accounting for day-of-week patterns.
        """
        print(f"\n[INFO] Detecting statistical outliers for {metric_type} {'movers' if mover_ind else 'non-movers'}...")
        
        mover_str = 'mover' if mover_ind else 'non_mover'
        table_name = f"{self.ds}_{metric_type}_{mover_str}_census_cube"
        
        # Get baseline stats by day of week
        baseline = self.get_dow_baseline(mover_ind, metric_type)
        
        if baseline.empty:
            print(f"[WARNING] No baseline data available for {metric_type} {mover_str}")
            return []
        
        # Get target date data with day of week
        dates_str = "', '".join(self.target_dates)
        sql = f"""
        SELECT 
            the_date,
            DAYOFWEEK(the_date) as dow,
            DAYNAME(the_date) as dow_name,
            census_blockid,
            winner,
            loser,
            state,
            dma_name,
            total_{metric_type}s as metric_value
        FROM {table_name}
        WHERE the_date IN ('{dates_str}')
        """
        
        target_data = self.con.execute(sql).df()
        
        if target_data.empty:
            print(f"[WARNING] No target data found for dates: {self.target_dates}")
            return []
        
        # Merge with baseline on dow and identifiers
        merged = target_data.merge(
            baseline,
            on=['dow', 'census_blockid', 'winner', 'loser', 'state', 'dma_name'],
            how='left',
            suffixes=('_target', '_baseline')
        )
        
        # Calculate Z-scores and IQR outliers
        merged['z_score'] = (merged['metric_value'] - merged['mean_value']) / (merged['std_value'] + 1e-10)
        merged['iqr'] = merged['q3'] - merged['q1']
        merged['iqr_lower'] = merged['q1'] - iqr_multiplier * merged['iqr']
        merged['iqr_upper'] = merged['q3'] + iqr_multiplier * merged['iqr']
        
        # Flag outliers
        merged['is_z_outlier'] = merged['z_score'].abs() > z_threshold
        merged['is_iqr_outlier'] = (merged['metric_value'] < merged['iqr_lower']) | (merged['metric_value'] > merged['iqr_upper'])
        merged['is_outlier'] = merged['is_z_outlier'] | merged['is_iqr_outlier']
        
        outliers = merged[merged['is_outlier']].copy()
        outliers['outlier_type'] = 'statistical'
        outliers['mover_ind'] = mover_ind
        outliers['metric_type'] = metric_type
        
        print(f"[INFO] Found {len(outliers)} statistical outliers")
        
        return outliers.to_dict('records')
    
    def detect_first_appearances(self, mover_ind, metric_type):
        """
        Detect first appearances - carriers appearing in census blocks for the first time.
        """
        print(f"\n[INFO] Detecting first appearances for {metric_type} {'movers' if mover_ind else 'non-movers'}...")
        
        mover_str = 'mover' if mover_ind else 'non_mover'
        table_name = f"{self.ds}_{metric_type}_{mover_str}_census_cube"
        
        dates_str = "', '".join(self.target_dates)
        
        # Find records on target dates that have no historical precedent
        sql = f"""
        WITH target_records AS (
            SELECT 
                the_date,
                census_blockid,
                winner,
                loser,
                state,
                dma_name,
                total_{metric_type}s as metric_value
            FROM {table_name}
            WHERE the_date IN ('{dates_str}')
        ),
        historical_records AS (
            SELECT DISTINCT
                census_blockid,
                winner,
                loser
            FROM {table_name}
            WHERE the_date < '{min(self.target_dates)}'
        )
        SELECT 
            t.*,
            CASE WHEN h.census_blockid IS NULL THEN TRUE ELSE FALSE END as is_first_appearance
        FROM target_records t
        LEFT JOIN historical_records h
            ON t.census_blockid = h.census_blockid
            AND t.winner = h.winner
            AND t.loser = h.loser
        WHERE h.census_blockid IS NULL  -- Only new combinations
        """
        
        first_appearances = self.con.execute(sql).df()
        
        if not first_appearances.empty:
            first_appearances['outlier_type'] = 'first_appearance'
            first_appearances['mover_ind'] = mover_ind
            first_appearances['metric_type'] = metric_type
            print(f"[INFO] Found {len(first_appearances)} first appearances")
        else:
            print(f"[INFO] No first appearances found")
        
        return first_appearances.to_dict('records')
    
    def detect_volume_spikes(self, mover_ind, metric_type, spike_multiplier=5.0):
        """
        Detect volume spikes - dramatic increases compared to historical average.
        """
        print(f"\n[INFO] Detecting volume spikes for {metric_type} {'movers' if mover_ind else 'non-movers'}...")
        
        mover_str = 'mover' if mover_ind else 'non_mover'
        table_name = f"{self.ds}_{metric_type}_{mover_str}_census_cube"
        
        dates_str = "', '".join(self.target_dates)
        
        sql = f"""
        WITH historical_avg AS (
            SELECT 
                census_blockid,
                winner,
                loser,
                AVG(total_{metric_type}s) as avg_historical,
                STDDEV(total_{metric_type}s) as std_historical,
                COUNT(*) as historical_days
            FROM {table_name}
            WHERE the_date < '{min(self.target_dates)}'
              AND the_date >= CAST('{min(self.target_dates)}' AS DATE) - INTERVAL 90 DAY
            GROUP BY census_blockid, winner, loser
            HAVING COUNT(*) >= 3
        ),
        target_data AS (
            SELECT 
                the_date,
                census_blockid,
                winner,
                loser,
                state,
                dma_name,
                total_{metric_type}s as metric_value
            FROM {table_name}
            WHERE the_date IN ('{dates_str}')
        )
        SELECT 
            t.*,
            h.avg_historical,
            h.std_historical,
            h.historical_days,
            (t.metric_value / NULLIF(h.avg_historical, 0)) as spike_ratio
        FROM target_data t
        INNER JOIN historical_avg h
            ON t.census_blockid = h.census_blockid
            AND t.winner = h.winner
            AND t.loser = h.loser
        WHERE t.metric_value > {spike_multiplier} * h.avg_historical
        """
        
        spikes = self.con.execute(sql).df()
        
        if not spikes.empty:
            spikes['outlier_type'] = 'volume_spike'
            spikes['mover_ind'] = mover_ind
            spikes['metric_type'] = metric_type
            print(f"[INFO] Found {len(spikes)} volume spikes (>{spike_multiplier}x historical)")
        else:
            print(f"[INFO] No volume spikes found")
        
        return spikes.to_dict('records')
    
    def detect_geographic_concentrations(self, mover_ind, metric_type, concentration_threshold=0.8):
        """
        Detect suspicious geographic concentrations - when a small number of blocks account for large percentage of activity.
        """
        print(f"\n[INFO] Detecting geographic concentrations for {metric_type} {'movers' if mover_ind else 'non-movers'}...")
        
        mover_str = 'mover' if mover_ind else 'non_mover'
        table_name = f"{self.ds}_{metric_type}_{mover_str}_census_cube"
        
        dates_str = "', '".join(self.target_dates)
        
        sql = f"""
        WITH daily_totals AS (
            SELECT 
                the_date,
                winner,
                state,
                SUM(total_{metric_type}s) as total_daily
            FROM {table_name}
            WHERE the_date IN ('{dates_str}')
            GROUP BY the_date, winner, state
        ),
        block_contributions AS (
            SELECT 
                t.the_date,
                t.census_blockid,
                t.winner,
                t.loser,
                t.state,
                t.dma_name,
                t.total_{metric_type}s as block_metric,
                d.total_daily,
                (t.total_{metric_type}s / NULLIF(d.total_daily, 0)) as contribution_pct
            FROM {table_name} t
            INNER JOIN daily_totals d
                ON t.the_date = d.the_date
                AND t.winner = d.winner
                AND t.state = d.state
            WHERE t.the_date IN ('{dates_str}')
        )
        SELECT *
        FROM block_contributions
        WHERE contribution_pct >= {concentration_threshold}
        ORDER BY contribution_pct DESC
        """
        
        concentrations = self.con.execute(sql).df()
        
        if not concentrations.empty:
            concentrations['outlier_type'] = 'geographic_concentration'
            concentrations['mover_ind'] = mover_ind
            concentrations['metric_type'] = metric_type
            print(f"[INFO] Found {len(concentrations)} geographic concentrations (>={concentration_threshold*100}%)")
        else:
            print(f"[INFO] No geographic concentrations found")
        
        return concentrations.to_dict('records')
    
    def detect_impossible_metrics(self, mover_ind):
        """
        Detect impossible metrics - data quality issues like negative values, impossibly high values, etc.
        """
        print(f"\n[INFO] Detecting impossible metrics for {'movers' if mover_ind else 'non-movers'}...")
        
        mover_str = 'mover' if mover_ind else 'non_mover'
        impossible_records = []
        
        for metric_type in ['win', 'loss']:
            table_name = f"{self.ds}_{metric_type}_{mover_str}_census_cube"
            dates_str = "', '".join(self.target_dates)
            
            sql = f"""
            SELECT 
                the_date,
                census_blockid,
                winner,
                loser,
                state,
                dma_name,
                total_{metric_type}s as metric_value,
                '{metric_type}' as metric_type
            FROM {table_name}
            WHERE the_date IN ('{dates_str}')
              AND (
                  total_{metric_type}s < 0  -- Negative values
                  OR total_{metric_type}s > 1000000  -- Impossibly high
                  OR (winner = loser)  -- Same carrier as winner and loser
              )
            """
            
            impossible = self.con.execute(sql).df()
            
            if not impossible.empty:
                impossible['outlier_type'] = 'impossible_metric'
                impossible['mover_ind'] = mover_ind
                impossible_records.extend(impossible.to_dict('records'))
        
        print(f"[INFO] Found {len(impossible_records)} impossible metrics")
        return impossible_records
    
    def run_full_analysis(self):
        """Run all anomaly detection methods."""
        print("\n" + "=" * 80)
        print(f"CENSUS BLOCK ANOMALY DETECTION")
        print(f"Dataset: {self.ds}")
        print(f"Target Dates: {', '.join(self.target_dates)}")
        print("=" * 80)
        
        # Run for both movers and non-movers
        for mover_ind in [True, False]:
            mover_label = 'Movers' if mover_ind else 'Non-Movers'
            print(f"\n{'*' * 80}")
            print(f"Analyzing {mover_label}")
            print(f"{'*' * 80}")
            
            for metric_type in ['win', 'loss']:
                # Statistical outliers (with DOW adjustment)
                self.results['statistical_outliers'].extend(
                    self.detect_statistical_outliers(mover_ind, metric_type)
                )
                
                # First appearances
                self.results['first_appearances'].extend(
                    self.detect_first_appearances(mover_ind, metric_type)
                )
                
                # Volume spikes
                self.results['volume_spikes'].extend(
                    self.detect_volume_spikes(mover_ind, metric_type)
                )
                
                # Geographic concentrations
                self.results['geographic_concentrations'].extend(
                    self.detect_geographic_concentrations(mover_ind, metric_type)
                )
            
            # Impossible metrics (checks both win and loss)
            self.results['impossible_metrics'].extend(
                self.detect_impossible_metrics(mover_ind)
            )
        
        # Generate summary statistics
        self.generate_summary_stats()
        
        return self.results
    
    def generate_summary_stats(self):
        """Generate summary statistics across all anomaly types."""
        self.results['summary_stats'] = {
            'total_statistical_outliers': len(self.results['statistical_outliers']),
            'total_first_appearances': len(self.results['first_appearances']),
            'total_volume_spikes': len(self.results['volume_spikes']),
            'total_geographic_concentrations': len(self.results['geographic_concentrations']),
            'total_impossible_metrics': len(self.results['impossible_metrics']),
            'grand_total': sum([
                len(self.results['statistical_outliers']),
                len(self.results['first_appearances']),
                len(self.results['volume_spikes']),
                len(self.results['geographic_concentrations']),
                len(self.results['impossible_metrics'])
            ])
        }
        
        # Breakdown by date
        date_breakdown = {}
        for date in self.target_dates:
            date_breakdown[date] = {
                'statistical_outliers': sum(1 for x in self.results['statistical_outliers'] if x['the_date'] == date),
                'first_appearances': sum(1 for x in self.results['first_appearances'] if x['the_date'] == date),
                'volume_spikes': sum(1 for x in self.results['volume_spikes'] if x['the_date'] == date),
                'geographic_concentrations': sum(1 for x in self.results['geographic_concentrations'] if x['the_date'] == date),
                'impossible_metrics': sum(1 for x in self.results['impossible_metrics'] if x['the_date'] == date)
            }
        
        self.results['summary_stats']['by_date'] = date_breakdown
    
    def save_results(self, output_dir):
        """Save results to files."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True, parents=True)
        
        # Save each anomaly type to CSV
        for anomaly_type, records in self.results.items():
            if anomaly_type == 'summary_stats':
                continue
            
            if records:
                df = pd.DataFrame(records)
                filename = output_path / f"{self.ds}_{anomaly_type}.csv"
                df.to_csv(filename, index=False)
                print(f"[INFO] Saved {len(records)} records to {filename}")
        
        # Save summary stats to JSON
        summary_file = output_path / f"{self.ds}_summary_stats.json"
        with open(summary_file, 'w') as f:
            json.dump(self.results['summary_stats'], f, indent=2, default=str)
        print(f"[INFO] Saved summary stats to {summary_file}")
        
        return output_path
    
    def close(self):
        """Close database connection."""
        self.con.close()


def main():
    parser = argparse.ArgumentParser(
        description="Detect census block anomalies with DOW patterns and first appearances"
    )
    parser.add_argument('--db', default=DB_PATH, help=f'Path to DuckDB database (default: {DB_PATH})')
    parser.add_argument('--ds', required=True, help='Dataset name (e.g., gamoshi)')
    parser.add_argument('--dates', required=True, help='Comma-separated dates (YYYY-MM-DD)')
    parser.add_argument('--output', default=OUTPUT_DIR, help=f'Output directory (default: {OUTPUT_DIR})')
    parser.add_argument('--z-threshold', type=float, default=3.0, help='Z-score threshold (default: 3.0)')
    parser.add_argument('--iqr-multiplier', type=float, default=1.5, help='IQR multiplier (default: 1.5)')
    parser.add_argument('--spike-multiplier', type=float, default=5.0, help='Volume spike multiplier (default: 5.0)')
    
    args = parser.parse_args()
    
    # Parse dates
    target_dates = [d.strip() for d in args.dates.split(',')]
    
    # Run analysis
    detector = CensusBlockAnomalyDetector(args.db, args.ds, target_dates)
    results = detector.run_full_analysis()
    
    # Save results
    output_path = detector.save_results(args.output)
    
    # Print summary
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nSummary:")
    for key, value in results['summary_stats'].items():
        if key != 'by_date':
            print(f"  {key}: {value}")
    
    print(f"\nResults saved to: {output_path}")
    
    detector.close()


if __name__ == '__main__':
    main()
