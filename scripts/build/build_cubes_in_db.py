#!/usr/bin/env python3
"""
Build pre-aggregated cube tables inside the DuckDB database.

Creates 4 cube tables per dataset (ds):
  - {ds}_win_mover_cube
  - {ds}_win_non_mover_cube
  - {ds}_loss_mover_cube
  - {ds}_loss_non_mover_cube

Each cube table aggregates on all indexed dimensions and lives inside
the database - no separate parquet files needed!

Usage:
    uv run build_cubes_in_db.py [--db duck_suppression.db] [--ds gamoshi]
"""
import os
import sys
import argparse
from typing import Optional


def build_cube_table(
    db_path: str,
    ds: str,
    mover_ind: bool,
    metric: str,  # 'win' or 'loss'
) -> bool:
    """
    Build a single cube table inside the database.
    
    Args:
        db_path: Path to DuckDB database
        ds: Data source filter
        mover_ind: True for movers, False for non-movers
        metric: 'win' or 'loss'
        
    Returns:
        True if successful
    """
    try:
        import duckdb
    except ImportError as e:
        print(f"[ERROR] Missing dependency: {e}", file=sys.stderr)
        return False
    
    mover_str = "mover" if mover_ind else "non_mover"
    table_name = f"{ds}_{metric}_{mover_str}_cube"
    
    print(f"[INFO] Building {metric} cube table: {table_name}")
    
    # Select the appropriate metric column
    if metric == "win":
        metric_col = "adjusted_wins"
        total_col = "total_wins"
    else:
        metric_col = "adjusted_losses"
        total_col = "total_losses"  # Fixed typo!
    
    con = duckdb.connect(db_path, read_only=False)
    try:
        # Drop existing table if it exists
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create cube table with aggregation
        create_query = f"""
        CREATE TABLE {table_name} AS
        SELECT
            the_date,
            year,
            month,
            day,
            day_of_week,
            winner,
            loser,
            dma,
            dma_name,
            state,
            SUM({metric_col}) as {total_col},
            COUNT(*) as record_count
        FROM carrier_data
        WHERE ds = '{ds.replace("'", "''")}'
          AND mover_ind = {str(mover_ind).upper()}
        GROUP BY
            the_date,
            year,
            month,
            day,
            day_of_week,
            winner,
            loser,
            dma,
            dma_name,
            state
        ORDER BY
            the_date,
            winner,
            loser,
            dma_name
        """
        
        print(f"[INFO] Executing aggregation query...")
        con.execute(create_query)
        
        # Get row count
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        if row_count == 0:
            print(f"[WARNING] No data in cube table {table_name}", file=sys.stderr)
            return False
        
        print(f"[INFO] Created table with {row_count:,} rows")
        
        # Create indexes on key columns for fast filtering
        print(f"[INFO] Creating indexes...")
        indexes = [
            f"idx_{table_name}_date",
            f"idx_{table_name}_winner",
            f"idx_{table_name}_loser",
            f"idx_{table_name}_dma",
            f"idx_{table_name}_state",
        ]
        index_cols = [
            "the_date",
            "winner",
            "loser",
            "dma_name",
            "state",
        ]
        
        for idx_name, col in zip(indexes, index_cols):
            try:
                con.execute(f"CREATE INDEX {idx_name} ON {table_name}({col})")
            except Exception as e:
                print(f"  [WARNING] Failed to create index {idx_name}: {e}", file=sys.stderr)
        
        # Analyze for query optimization
        con.execute(f"ANALYZE {table_name}")
        
        # Show sample stats
        metric_col = f"total_{metric}s" if metric == "win" else "total_losses"
        stats = con.execute(f"""
            SELECT
                MIN(the_date) as min_date,
                MAX(the_date) as max_date,
                SUM({metric_col}) as total_metric_sum,
                COUNT(DISTINCT winner) as unique_winners,
                COUNT(DISTINCT loser) as unique_losers,
                COUNT(DISTINCT dma_name) as unique_dmas
            FROM {table_name}
        """).fetchone()
        
        print(f"[SUCCESS] Table: {table_name}")
        print(f"  Stats:")
        print(f"    - Date range: {stats[0]} to {stats[1]}")
        metric_label = "wins" if metric == "win" else "losses"
        print(f"    - Total {metric_label}: {stats[2]:,.0f}")
        print(f"    - Unique winners: {stats[3]}")
        print(f"    - Unique losers: {stats[4]}")
        print(f"    - Unique DMAs: {stats[5]}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to build cube table: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False
    finally:
        pass
    
    # Build aggregate cubes if requested
    if args.aggregate:
        total_count += 4  # We'll build 4 aggregate cubes
    
    # Build dataset-specific cubes


def build_all_cube_tables(
    db_path: str,
    ds: str,
    skip_existing: bool = False
) -> bool:
    """
    Build all 4 cube tables for a given dataset.
    
    Args:
        db_path: Path to DuckDB database
        ds: Data source to process
        skip_existing: Skip if table already exists
        
    Returns:
        True if all cubes built successfully
    """
    print(f"\n{'='*70}")
    print(f"Building cube tables for dataset: {ds}")
    print(f"Database: {db_path}")
    print(f"{'='*70}\n")
    
    # Define all cube combinations
    cubes = [
        (True, "win"),    # mover wins
        (False, "win"),   # non-mover wins
        (True, "loss"),   # mover losses
        (False, "loss"),  # non-mover losses
    ]
    
    results = []
    for mover_ind, metric in cubes:
        mover_str = "mover" if mover_ind else "non_mover"
        table_name = f"{ds}_{metric}_{mover_str}_cube"
        
        if skip_existing:
            # Check if table exists
            try:
                import duckdb
                con = duckdb.connect(db_path, read_only=True)
                exists = con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                    [table_name]
                ).fetchone()[0] > 0
                con.close()
                
                if exists:
                    print(f"[SKIP] Table {table_name} already exists")
                    results.append(True)
                    continue
            except Exception:
                pass
        
        success = build_cube_table(db_path, ds, mover_ind, metric)
        results.append(success)
        print()  # Blank line between cubes
    
    success_count = sum(results)
    total_count = len(results)
    
    print(f"\n{'='*70}")
    print(f"Summary: {success_count}/{total_count} cube tables built successfully")
    print(f"{'='*70}\n")
    
    return all(results)


def get_available_datasets(db_path: str) -> list[str]:
    """Get list of available datasets from the database"""
    try:
        import duckdb
        con = duckdb.connect(db_path, read_only=True)
        try:
            result = con.execute("SELECT DISTINCT ds FROM carrier_data ORDER BY ds").fetchall()
            return [row[0] for row in result]
        finally:
            con.close()
    except Exception as e:
        print(f"[ERROR] Failed to get datasets: {e}", file=sys.stderr)
        return []


def list_cube_tables(db_path: str) -> None:
    """List all cube tables in the database"""
    try:
        import duckdb
        con = duckdb.connect(db_path, read_only=True)
        try:
            result = con.execute("""
                SELECT table_name, 
                       (SELECT COUNT(*) FROM information_schema.columns 
                        WHERE table_name = t.table_name) as column_count
                FROM information_schema.tables t
                WHERE table_name LIKE '%_cube'
                ORDER BY table_name
            """).fetchall()
            
            if not result:
                print("No cube tables found in database")
                return
            
            print("\nCube tables in database:")
            for table_name, col_count in result:
                # Get row count
                row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"  - {table_name}: {row_count:,} rows")
        finally:
            con.close()
    except Exception as e:
        print(f"[ERROR] Failed to list cube tables: {e}", file=sys.stderr)


def parse_args(argv=None):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Build pre-aggregated cube tables inside DuckDB database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build cube tables for default dataset (gamoshi)
  uv run build_cubes_in_db.py
  
  # Build cube tables for specific dataset
  uv run build_cubes_in_db.py --ds gamoshi
  
  # Build cube tables for all datasets
  uv run build_cubes_in_db.py --all
  
  # Skip existing tables (incremental build)
  uv run build_cubes_in_db.py --skip-existing
  
  # Build all datasets + aggregate cubes
  uv run build_cubes_in_db.py --all --aggregate
  
  # List existing cube tables
  uv run build_cubes_in_db.py --list
        """
    )
    
    parser.add_argument(
        "--db",
        default=os.path.join(os.getcwd(), "data/databases/duck_suppression.db"),
        help="Path to DuckDB database (default: ./data/databases/duck_suppression.db)"
    )
    
    parser.add_argument(
        "--ds",
        default="gamoshi",
        help="Dataset to process (default: gamoshi)"
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Build cube tables for all datasets in the database"
    )
    
    parser.add_argument(
        "--aggregate",
        action="store_true",
        help="Also build aggregate cubes across all datasets (all_* tables)"
    )
    
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip building tables that already exist"
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="List existing cube tables and exit"
    )
    
    parser.add_argument(
        "--list-datasets",
        action="store_true",
        help="List available datasets and exit"
    )
    
    return parser.parse_args(argv)


def build_aggregate_cube(con, metric: str, mover_ind: bool) -> bool:
    """Build aggregate cube across ALL datasets"""
    mover_str = "mover" if mover_ind else "non_mover"
    table_name = f"all_{metric}_{mover_str}_cube"
    
    print(f"[INFO] Building aggregate: {table_name}")
    
    metric_col = "adjusted_wins" if metric == "win" else "adjusted_losses"
    total_col = "total_wins" if metric == "win" else "total_losses"
    
    try:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        con.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT
            ds, the_date,
            EXTRACT(YEAR FROM the_date) as year,
            EXTRACT(MONTH FROM the_date) as month,
            EXTRACT(DAY FROM the_date) as day,
            EXTRACT(DOW FROM the_date) as day_of_week,
            winner, loser, dma, dma_name, state,
            SUM({metric_col}) as {total_col},
            COUNT(*) as record_count
        FROM carrier_data
        WHERE mover_ind = {str(mover_ind).upper()}
        GROUP BY ds, the_date, year, month, day, day_of_week,
                 winner, loser, dma, dma_name, state
        """)
        
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[SUCCESS] Created {table_name} with {count:,} rows")
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed: {e}")
        return False


def main(argv=None):
    """Main entry point"""
    args = parse_args(argv)
    
    # Check database exists
    if not os.path.exists(args.db):
        print(f"[ERROR] Database not found: {args.db}", file=sys.stderr)
        print(f"[ERROR] Run: uv run build_suppression_db.py <preagg.parquet>", file=sys.stderr)
        return 1
    
    # List cube tables if requested
    if args.list:
        list_cube_tables(args.db)
        return 0
    
    # List datasets if requested
    if args.list_datasets:
        datasets = get_available_datasets(args.db)
        if datasets:
            print("Available datasets:")
            for ds in datasets:
                print(f"  - {ds}")
        else:
            print("No datasets found in database")
        return 0
    
    # Build cube tables for all datasets or just one
    if args.all:
        datasets = get_available_datasets(args.db)
        if not datasets:
            print("[ERROR] No datasets found in database", file=sys.stderr)
            return 1
        
        print(f"[INFO] Building cube tables for {len(datasets)} dataset(s): {', '.join(datasets)}\n")
        
        all_success = True
        for ds in datasets:
            success = build_all_cube_tables(args.db, ds, args.skip_existing)
            if not success:
                all_success = False
                print(f"[WARNING] Some cube tables failed for dataset: {ds}\n")
        
        # Build aggregate cubes if requested
        if args.aggregate:
            print("\n" + "=" * 70)
            print("Building aggregate cubes across all datasets")
            print("=" * 70 + "\n")
            
            import duckdb
            con = duckdb.connect(args.db)
            
            agg_success = 0
            agg_total = 0
            for metric in ['win', 'loss']:
                for mover_ind in [True, False]:
                    agg_total += 1
                    if build_aggregate_cube(con, metric, mover_ind):
                        agg_success += 1
                    print()
            
            con.close()
            
            print("=" * 70)
            print(f"Aggregate cubes: {agg_success}/{agg_total} built successfully")
            print("=" * 70)
            
            all_success = all_success and (agg_success == agg_total)
        
        return 0 if all_success else 1
    else:
        # Build cube tables for single dataset
        success = build_all_cube_tables(args.db, args.ds, args.skip_existing)
        
        # Build aggregate cubes if requested
        if args.aggregate:
            print("\n" + "=" * 70)
            print("Building aggregate cubes across all datasets")
            print("=" * 70 + "\n")
            
            import duckdb
            con = duckdb.connect(args.db)
            
            agg_success = 0
            agg_total = 0
            for metric in ['win', 'loss']:
                for mover_ind in [True, False]:
                    agg_total += 1
                    if build_aggregate_cube(con, metric, mover_ind):
                        agg_success += 1
                    print()
            
            con.close()
            
            print("=" * 70)
            print(f"Aggregate cubes: {agg_success}/{agg_total} built successfully")
            print("=" * 70)
            
            success = success and (agg_success == agg_total)
        
        return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
