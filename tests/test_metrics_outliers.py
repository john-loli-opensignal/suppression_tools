import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from tools.src.metrics import national_timeseries, pair_metrics, competitor_view
from tools.src.outliers import national_outliers
from tools import db


# Use actual database for testing
@pytest.fixture
def db_path():
    """Get path to test database (must exist)"""
    path = os.path.join(os.getcwd(), "duck_suppression.db")
    if not os.path.exists(path):
        pytest.skip(f"Database not found: {path}. Run: uv run build_suppression_db.py <preagg.parquet>")
    return path


@pytest.fixture
def test_params(db_path):
    """Get test parameters from actual database"""
    # Get available dataset and date range
    datasets = db.get_distinct_values('ds', 'carrier_data', db_path=db_path)
    ds = datasets[0] if datasets else 'gamoshi'
    
    # Get actual date range
    stats = db.get_table_stats('carrier_data', db_path=db_path)
    min_date = str(stats['min_date'])
    max_date = str(stats['max_date'])
    
    # Use a small date window for faster tests
    from datetime import datetime, timedelta
    end = datetime.strptime(min_date, '%Y-%m-%d') + timedelta(days=3)
    
    return {
        'db_path': db_path,
        'ds': ds,
        'mover_ind': False,
        'start_date': min_date,
        'end_date': str(end.date()),
    }


def test_national_timeseries_columns(test_params):
    """Test that national_timeseries returns expected columns"""
    df = national_timeseries(
        ds=test_params['ds'],
        mover_ind=test_params['mover_ind'],
        start_date=test_params['start_date'],
        end_date=test_params['end_date'],
        db_path=test_params['db_path']
    )
    
    assert not df.empty, "Should return data"
    required_cols = {'the_date', 'winner', 'win_share', 'loss_share', 'wins_per_loss'}
    assert required_cols.issubset(df.columns), f"Missing columns: {required_cols - set(df.columns)}"
    
    # Data quality checks
    assert df['winner'].notna().all(), "All rows should have winner"
    assert (df['win_share'] >= 0).all(), "Win shares should be non-negative"
    assert (df['win_share'] <= 1).all(), "Win shares should be <= 1"


def test_pair_metrics_positive_and_keys(test_params):
    """Test that pair_metrics returns valid pair-level data"""
    df = pair_metrics(
        ds=test_params['ds'],
        mover_ind=test_params['mover_ind'],
        start_date=test_params['start_date'],
        end_date=test_params['end_date'],
        db_path=test_params['db_path']
    )
    
    assert not df.empty, "Should return data"
    
    # Check required columns
    required_cols = {'winner', 'loser', 'dma_name', 'pair_wins_current'}
    assert required_cols.issubset(df.columns), f"Missing columns: {required_cols - set(df.columns)}"
    
    # Data quality checks
    assert df['winner'].notna().all(), "All rows should have winner"
    assert df['loser'].notna().all(), "All rows should have loser"
    assert df['dma_name'].notna().all(), "All rows should have DMA"
    assert (df['pair_wins_current'] > 0).all(), "Pair wins should be positive"


def test_national_outliers_reasonable(test_params):
    """Test that national_outliers returns valid outlier flags"""
    df = national_outliers(
        ds=test_params['ds'],
        mover_ind=test_params['mover_ind'],
        start_date=test_params['start_date'],
        end_date=test_params['end_date'],
        window=3,
        z_thresh=1.0,
        db_path=test_params['db_path']
    )
    
    assert not df.empty, "Should return data"
    
    # Expected columns
    required_cols = {'the_date', 'winner', 'z', 'nat_outlier_pos'}
    assert required_cols.issubset(df.columns), f"Missing columns: {required_cols - set(df.columns)}"
    
    # Data quality checks
    assert df['z'].notna().all(), "Z-scores should be computed (or 0)"
    assert df['nat_outlier_pos'].isin([True, False]).all(), "Outlier flags should be boolean"
    
    # With low threshold (1.0), we should find some outliers
    # But not too many (sanity check)
    outlier_pct = df['nat_outlier_pos'].mean()
    assert 0 <= outlier_pct <= 0.5, f"Outlier percentage seems unreasonable: {outlier_pct:.1%}"


def test_competitor_view_h2h(test_params):
    """Test that competitor_view returns head-to-head metrics"""
    # Get some actual carriers from database
    carriers = db.get_distinct_values('winner', 'carrier_data', db_path=test_params['db_path'])
    
    if len(carriers) < 2:
        pytest.skip("Need at least 2 carriers for H2H test")
    
    primary = carriers[0]
    competitors = carriers[1:3]  # Take 2 competitors
    
    df = competitor_view(
        ds=test_params['ds'],
        mover_ind=test_params['mover_ind'],
        start_date=test_params['start_date'],
        end_date=test_params['end_date'],
        primary=primary,
        competitors=competitors,
        db_path=test_params['db_path']
    )
    
    # Might be empty if no H2H data in this window
    if not df.empty:
        required_cols = {'the_date', 'competitor', 'h2h_wins', 'primary_total_wins'}
        assert required_cols.issubset(df.columns), f"Missing columns: {required_cols - set(df.columns)}"
        
        assert df['competitor'].isin(competitors).all(), "Competitors should match requested"


def test_database_has_cube_tables(db_path):
    """Test that cube tables exist in database"""
    cubes = db.list_cube_tables(db_path)
    
    # Should have some cube tables if they were built
    # This test will pass even if no cubes, just informational
    print(f"\nCube tables found: {cubes}")
    
    if not cubes:
        pytest.skip("No cube tables found. Run: uv run build_cubes_in_db.py")


def test_imports():
    """Test that all modules import without errors"""
    from tools import db
    from tools.src import metrics, outliers
    
    # Check functions exist
    assert hasattr(metrics, 'national_timeseries')
    assert hasattr(metrics, 'pair_metrics')
    assert hasattr(metrics, 'competitor_view')
    assert hasattr(outliers, 'national_outliers')
    assert hasattr(outliers, 'cube_outliers')
    assert hasattr(db, 'query')
    assert hasattr(db, 'connect')
