#!/usr/bin/env python3
"""
Build census block-level cube tables for granular outlier detection.

Usage:
    python build_census_block_cubes.py --ds gamoshi
    python build_census_block_cubes.py --all
"""
import argparse
import duckdb
import sys
from pathlib import Path
import time

DB_PATH = "duck_suppression.db"
PARQUET_STORE = "duckdb_partitioned_store"


def get_available_datasets(con):
    """Get list of datasets from partitioned parquet store."""
    store_path = Path(PARQUET_STORE)
    if not store_path.exists():
        return []
    
    datasets = []
    for ds_dir in store_path.iterdir():
        if ds_dir.is_dir() and ds_dir.name.startswith('ds='):
            ds_name = ds_dir.name.replace('ds=', '')
            datasets.append(ds_name)
    
    return sorted(datasets)


def build_census_block_cube(con, ds, mover_ind, metric_type):
    """
    Build census block-level cube for a specific dataset, mover type, and metric.
    
    Args:
        con: DuckDB connection
        ds: Dataset name
        mover_ind: True for movers, False for non-movers
        metric_type: 'win' or 'loss'
    """
    mover_str = 'mover' if mover_ind else 'non_mover'
    table_name = f"{ds}_{metric_type}_{mover_str}_census_cube"
    
    # Determine source column based on metric type
    if metric_type == 'win':
        metric_col = 'adjusted_wins'
        opposite_col = 'adjusted_losses'
    else:  # loss
        metric_col = 'adjusted_losses'
        opposite_col = 'adjusted_wins'
    
    print(f"\n[INFO] Building census block cube: {table_name}")
    print(f"       Aggregating {metric_col} from primary_geoid level...")
    
    # Path to partitioned parquet files
    parquet_path = f"{PARQUET_STORE}/ds={ds}/p_mover_ind={mover_ind}/**/*.parquet"
    
    # Drop existing table if it exists
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    start_time = time.time()
    
    # Build aggregated cube at census block (primary_geoid) level
    sql = f"""
    CREATE TABLE {table_name} AS
    SELECT 
        the_date,
        primary_geoid as census_blockid,
        state,
        dma_name,
        winner,
        loser,
        SUM({metric_col}) as total_{metric_type}s,
        SUM({opposite_col}) as opposite_metric,
        COUNT(*) as record_count
    FROM read_parquet('{parquet_path}')
    GROUP BY the_date, primary_geoid, state, dma_name, winner, loser
    ORDER BY the_date, state, dma_name, census_blockid, winner, loser
    """
    
    print("[INFO] Executing aggregation query...")
    con.execute(sql)
    elapsed = time.time() - start_time
    
    # Get statistics
    stats = con.execute(f"""
        SELECT 
            MIN(the_date) as min_date,
            MAX(the_date) as max_date,
            SUM(total_{metric_type}s) as total_metric_sum,
            COUNT(DISTINCT census_blockid) as unique_blocks,
            COUNT(DISTINCT winner) as unique_winners,
            COUNT(DISTINCT loser) as unique_losers,
            COUNT(DISTINCT dma_name) as unique_dmas,
            COUNT(DISTINCT state) as unique_states,
            COUNT(*) as total_rows
        FROM {table_name}
    """).fetchone()
    
    print(f"[INFO] Created table with {stats[8]:,} rows in {elapsed:.2f}s")
    
    # Create indexes for fast lookups
    print("[INFO] Creating indexes...")
    idx_start = time.time()
    con.execute(f"CREATE INDEX idx_{table_name}_date ON {table_name}(the_date)")
    con.execute(f"CREATE INDEX idx_{table_name}_block ON {table_name}(census_blockid)")
    con.execute(f"CREATE INDEX idx_{table_name}_state ON {table_name}(state)")
    con.execute(f"CREATE INDEX idx_{table_name}_dma ON {table_name}(dma_name)")
    con.execute(f"CREATE INDEX idx_{table_name}_winner ON {table_name}(winner)")
    con.execute(f"CREATE INDEX idx_{table_name}_loser ON {table_name}(loser)")
    con.execute(f"CREATE INDEX idx_{table_name}_h2h ON {table_name}(winner, loser)")
    idx_elapsed = time.time() - idx_start
    print(f"[INFO] Indexes created in {idx_elapsed:.2f}s")
    
    print(f"[SUCCESS] Table: {table_name}")
    print(f"  Stats:")
    print(f"    - Date range: {stats[0]} to {stats[1]}")
    print(f"    - Total {metric_type}s: {stats[2]:,.0f}")
    print(f"    - Unique census blocks: {stats[3]:,}")
    print(f"    - Unique winners: {stats[4]:,}")
    print(f"    - Unique losers: {stats[5]:,}")
    print(f"    - Unique DMAs: {stats[6]:,}")
    print(f"    - Unique states: {stats[7]:,}")
    print(f"    - Total rows: {stats[8]:,}")
    
    return True


def build_all_census_cubes(ds_list, db_path=DB_PATH):
    """Build all census block cube combinations for given datasets."""
    print(f"[INFO] Building census block cubes for {len(ds_list)} dataset(s): {', '.join(ds_list)}")
    
    success_count = 0
    fail_count = 0
    
    for ds in ds_list:
        print("\n" + "=" * 70)
        print(f"Building census block cubes for dataset: {ds}")
        print(f"Database: {db_path}")
        print("=" * 70)
        
        con = duckdb.connect(db_path)
        
        # Build 4 cubes: win/loss × mover/non_mover
        cubes = [
            (True, 'win'),   # mover wins
            (True, 'loss'),  # mover losses
            (False, 'win'),  # non_mover wins
            (False, 'loss')  # non_mover losses
        ]
        
        for mover_ind, metric_type in cubes:
            try:
                build_census_block_cube(con, ds, mover_ind, metric_type)
                success_count += 1
            except Exception as e:
                print(f"[ERROR] Failed to build cube: {e}")
                import traceback
                traceback.print_exc()
                fail_count += 1
        
        con.close()
    
    print("\n" + "=" * 70)
    print(f"Summary: {success_count}/{success_count + fail_count} census block cubes built successfully")
    print("=" * 70)
    
    if fail_count > 0:
        print(f"\n[WARNING] {fail_count} cube(s) failed")
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Build census block-level cube tables for granular outlier detection"
    )
    parser.add_argument('--db', default=DB_PATH, help=f'Path to DuckDB database (default: {DB_PATH})')
    parser.add_argument('--ds', help='Dataset name (e.g., gamoshi)')
    parser.add_argument('--all', action='store_true', help='Build cubes for all available datasets')
    parser.add_argument('--list', action='store_true', help='List available datasets')
    
    args = parser.parse_args()
    
    # Connect to get available datasets
    con = duckdb.connect(args.db)
    available_datasets = get_available_datasets(con)
    con.close()
    
    if args.list:
        print("Available datasets in partitioned store:")
        for ds in available_datasets:
            print(f"  - {ds}")
        return
    
    if not available_datasets:
        print("[ERROR] No datasets found in partitioned store. Run partition_pre_agg_to_duckdb.py first.")
        sys.exit(1)
    
    # Determine which datasets to process
    if args.all:
        datasets = available_datasets
    elif args.ds:
        if args.ds not in available_datasets:
            print(f"[ERROR] Dataset '{args.ds}' not found. Available: {', '.join(available_datasets)}")
            sys.exit(1)
        datasets = [args.ds]
    else:
        print("[ERROR] Must specify --ds or --all")
        parser.print_help()
        sys.exit(1)
    
    # Build cubes
    success = build_all_census_cubes(datasets, args.db)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
