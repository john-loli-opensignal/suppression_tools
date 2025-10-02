import tempfile
from pathlib import Path
import pandas as pd
from suppression_tools.src.metrics import national_timeseries, pair_metrics
from suppression_tools.src.outliers import national_outliers

def build_parquet_from_fixture(tmpdir: str) -> str:
    csv_path = Path(__file__).parent / 'fixtures' / 'mini_store.csv'
    df = pd.read_csv(csv_path)
    df['the_date'] = pd.to_datetime(df['the_date']).dt.date
    df['mover_ind'] = df['mover_ind'].astype(bool)
    out_dir = Path(tmpdir) / 'mini_store'
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / 'data.parquet'
    df.to_parquet(out_path, index=False)
    return str(out_path)

def test_national_timeseries_columns():
    with tempfile.TemporaryDirectory() as td:
        p = build_parquet_from_fixture(td)
        df = national_timeseries(p, ds='test', mover_ind='True', start_date='2025-01-01', end_date='2025-01-04')
        assert set(['the_date', 'winner', 'win_share', 'loss_share', 'wins_per_loss']).issubset(df.columns)

def test_pair_metrics_positive_and_keys():
    with tempfile.TemporaryDirectory() as td:
        p = build_parquet_from_fixture(td)
        df = pair_metrics(p, ds='test', mover_ind='True', start_date='2025-01-01', end_date='2025-01-04')
        assert df['winner'].notna().all() and df['loser'].notna().all() and df['dma_name'].notna().all()
        assert (df['pair_wins_current'] > 0).all()

def test_national_outliers_reasonable():
    with tempfile.TemporaryDirectory() as td:
        p = build_parquet_from_fixture(td)
        df = national_outliers(p, ds='test', mover_ind='True', start_date='2025-01-01', end_date='2025-01-04', window=3, z_thresh=1.0)
        assert set(['the_date', 'winner', 'z', 'nat_outlier_pos']).issubset(df.columns)
        assert len(df) == 8
        assert df['z'].isna().sum() == 0
        assert df['nat_outlier_pos'].isin([True, False]).all()