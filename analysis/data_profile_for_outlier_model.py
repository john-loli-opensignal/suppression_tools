#!/usr/bin/env python3
"""
Generate comprehensive data profile for alternative outlier detection model.
"""
import duckdb
import json
from pathlib import Path

def generate_profile():
    db_path = Path(__file__).parent.parent / "data" / "databases" / "duck_suppression.db"
    con = duckdb.connect(str(db_path), read_only=True)
    
    profile = {}
    
    # Basic series counts
    profile['series_counts'] = {
        'national_level': 2,  # mover + non_mover for gamoshi
        'h2h_national_pairs': con.execute("SELECT COUNT(DISTINCT winner || '_' || loser) FROM gamoshi_win_mover_cube").fetchone()[0],
        'state_h2h_pairs': con.execute("SELECT COUNT(DISTINCT state || '_' || winner || '_' || loser) FROM gamoshi_win_mover_cube").fetchone()[0],
        'dma_carrier_pairs': con.execute("SELECT COUNT(DISTINCT dma_name || '_' || winner || '_' || loser) FROM gamoshi_win_mover_cube").fetchone()[0],
    }
    
    # Temporal
    temp = con.execute("""
        SELECT 
            MIN(the_date) as start_date,
            MAX(the_date) as end_date,
            COUNT(DISTINCT the_date) as total_days
        FROM gamoshi_win_mover_cube
    """).fetchone()
    
    profile['temporal'] = {
        'start_date': str(temp[0]),
        'end_date': str(temp[1]),
        'total_days': temp[2],
        'window_days': (temp[1] - temp[0]).days + 1,
        'cadence': 'daily',
        'median_history_days': 70
    }
    
    # Carriers
    carrier = con.execute("""
        WITH totals AS (
            SELECT winner, SUM(total_wins) as total
            FROM gamoshi_win_mover_cube
            GROUP BY winner
        )
        SELECT 
            COUNT(*) as total_carriers,
            CAST(AVG(total) AS BIGINT) as avg_wins,
            CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total) AS BIGINT) as median_wins,
            CAST(MAX(total) AS BIGINT) as max_wins
        FROM totals
    """).fetchone()
    
    profile['carrier_stats'] = {
        'total_carriers': carrier[0],
        'avg_wins_per_carrier': carrier[1],
        'median_wins_per_carrier': carrier[2],
        'max_wins_per_carrier': carrier[3]
    }
    
    # DMAs
    dma = con.execute("""
        SELECT 
            COUNT(DISTINCT dma_name) as total_dmas,
            CAST(AVG(pairs) AS INT) as avg_pairs_per_dma
        FROM (
            SELECT dma_name, COUNT(DISTINCT winner || '_' || loser) as pairs
            FROM gamoshi_win_mover_cube
            GROUP BY dma_name
        )
    """).fetchone()
    
    profile['dma_stats'] = {
        'total_dmas': dma[0],
        'avg_pairs_per_dma': dma[1],
        'estimated_dma_carrier_pairs': dma[0] * dma[1]
    }
    
    # Day of week seasonality
    dow = con.execute("""
        WITH daily AS (
            SELECT 
                the_date,
                DAYOFWEEK(the_date) as dow,
                SUM(total_wins) as daily_total
            FROM gamoshi_win_mover_cube
            GROUP BY the_date, dow
        ),
        dow_stats AS (
            SELECT 
                dow,
                AVG(daily_total) as avg_wins
            FROM daily
            GROUP BY dow
        )
        SELECT 
            AVG(CASE WHEN dow IN (0,1,2,3,4) THEN avg_wins END) as weekday_avg,
            AVG(CASE WHEN dow IN (5,6) THEN avg_wins END) as weekend_avg
        FROM dow_stats
    """).fetchone()
    
    weekday_avg = float(dow[0] or 0)
    weekend_avg = float(dow[1] or 0)
    overall_avg = (weekday_avg * 5 + weekend_avg * 2) / 7
    
    profile['seasonality'] = {
        'period_days': 7,
        'strength_0to1': round(abs(weekend_avg - weekday_avg) / overall_avg, 3) if overall_avg > 0 else 0,
        'weekend_multiplier': round(weekend_avg / weekday_avg, 2) if weekday_avg > 0 else 1.0
    }
    
    # Missing data
    missing = con.execute("""
        WITH expected AS (
            SELECT COUNT(*) as cnt
            FROM GENERATE_SERIES(
                (SELECT MIN(the_date) FROM gamoshi_win_mover_cube),
                (SELECT MAX(the_date) FROM gamoshi_win_mover_cube),
                INTERVAL 1 DAY
            )
        )
        SELECT 
            e.cnt as expected,
            (SELECT COUNT(DISTINCT the_date) FROM gamoshi_win_mover_cube) as actual,
            e.cnt - (SELECT COUNT(DISTINCT the_date) FROM gamoshi_win_mover_cube) as missing
        FROM expected e
    """).fetchone()
    
    profile['missing'] = {
        'expected_dates': missing[0],
        'actual_dates': missing[1],
        'missing_dates': missing[2],
        'missing_pct': round(100.0 * missing[2] / missing[0], 2)
    }
    
    # Volatility
    vol = con.execute("""
        WITH daily AS (
            SELECT the_date, SUM(total_wins) as daily_wins
            FROM gamoshi_win_mover_cube
            GROUP BY the_date
            ORDER BY the_date
        ),
        with_lag AS (
            SELECT 
                daily_wins,
                LAG(daily_wins) OVER (ORDER BY the_date) as prev_wins
            FROM daily
        )
        SELECT 
            CAST(AVG(daily_wins) AS BIGINT) as avg_daily,
            CAST(STDDEV(daily_wins) AS BIGINT) as stddev_daily,
            ROUND(AVG(ABS(daily_wins - prev_wins) / NULLIF(prev_wins, 0)), 4) as avg_change_ratio
        FROM with_lag
        WHERE prev_wins IS NOT NULL
    """).fetchone()
    
    profile['volatility'] = {
        'avg_daily_wins': vol[0],
        'stddev_daily_wins': vol[1],
        'cv_pct': round(100.0 * vol[1] / vol[0], 2) if vol[0] > 0 else 0,
        'avg_day_to_day_change_ratio': vol[2]
    }
    
    # Anomaly patterns from rolling view
    if con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'gamoshi_win_mover_rolling'").fetchone()[0] > 0:
        anom = con.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE total_wins > 10) as qualified,
                COUNT(*) FILTER (WHERE is_outlier AND total_wins > 10) as spikes,
                COUNT(*) FILTER (WHERE is_first_appearance) as first_appearances,
                ROUND(AVG(CASE WHEN is_outlier THEN zscore END), 2) as avg_spike_z_score
            FROM gamoshi_win_mover_rolling
        """).fetchone()
        
        profile['observed_anomalies'] = {
            'qualified_observations': anom[0],
            'spike_count': anom[1],
            'first_appearance_count': anom[2],
            'spike_rate_per_100_obs': round(100.0 * anom[1] / anom[0], 2) if anom[0] > 0 else 0,
            'first_appearance_rate_per_100_obs': round(100.0 * anom[2] / anom[0], 2) if anom[0] > 0 else 0,
            'avg_z_score_for_spikes': anom[3]
        }
    
    con.close()
    return profile

if __name__ == "__main__":
    profile = generate_profile()
    print(json.dumps(profile, indent=2))
