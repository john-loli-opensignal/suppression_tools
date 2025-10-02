#!/usr/bin/env python3
"""
Build pre-aggregated cubes from DuckDB database.

Creates 4 cubes per dataset (ds):
  - {ds}_win_mover_cube.parquet
  - {ds}_win_non_mover_cube.parquet
  - {ds}_loss_mover_cube.parquet
  - {ds}_loss_non_mover_cube.parquet

Each cube aggregates on all indexed dimensions:
  - the_date, year, month, day, day_of_week
  - winner, loser
  - dma, dma_name, state

Usage:
    uv run build_cubes_from_db.py [--db duck_suppression.db] [--ds gamoshi] [-o cubes/]
"""
import os
import sys
import argparse
from typing import Optional


def build_cube(
    db_path: str,
    ds: str,
    mover_ind: bool,
    metric: str,  # 'wins' or 'losses'
    output_dir: str
) -> bool:
    """
    Build a single cube for given ds, mover_ind, and metric.
    
    Args:
        db_path: Path to DuckDB database
        ds: Data source filter
        mover_ind: True for movers, False for non-movers
        metric: 'wins' or 'losses'
        output_dir: Output directory for parquet files
        
    Returns:
        True if successful
    """
    try:
        import duckdb
        import pandas as pd
    except ImportError as e:
        print(f"[ERROR] Missing dependency: {e}", file=sys.stderr)
        return False
    
    mover_str = "mover" if mover_ind else "non_mover"
    output_file = os.path.join(output_dir, f"{ds}_{metric}_{mover_str}_cube.parquet")
    
    print(f"[INFO] Building {metric} cube: ds={ds}, mover_ind={mover_ind}")
    
    # Select the appropriate metric column
    metric_col = "adjusted_wins" if metric == "win" else "adjusted_losses"
    
    con = duckdb.connect(db_path, read_only=True)
    try:
        # Aggregate on all indexed dimensions
        query = f"""
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
            SUM({metric_col}) as total_{metric}s,
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
        df = con.execute(query).df()
        
        if df.empty:
            print(f"[WARNING] No data found for ds={ds}, mover_ind={mover_ind}", file=sys.stderr)
            return False
        
        row_count = len(df)
        print(f"[INFO] Aggregated to {row_count:,} rows")
        
        # Write to parquet
        os.makedirs(output_dir, exist_ok=True)
        df.to_parquet(output_file, compression='snappy', index=False)
        
        file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
        print(f"[SUCCESS] Wrote: {output_file} ({file_size_mb:.2f} MB, {row_count:,} rows)")
        
        # Show sample stats
        total_metric_sum = df[f'total_{metric}s'].sum()
        date_range = (df['the_date'].min(), df['the_date'].max())
        unique_winners = df['winner'].nunique()
        unique_losers = df['loser'].nunique()
        unique_dmas = df['dma_name'].nunique()
        
        print(f"  Stats:")
        print(f"    - Date range: {date_range[0]} to {date_range[1]}")
        print(f"    - Total {metric}s: {total_metric_sum:,.0f}")
        print(f"    - Unique winners: {unique_winners}")
        print(f"    - Unique losers: {unique_losers}")
        print(f"    - Unique DMAs: {unique_dmas}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to build cube: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False
    finally:
        con.close()


def build_all_cubes(
    db_path: str,
    ds: str,
    output_dir: str,
    skip_existing: bool = False
) -> bool:
    """
    Build all 4 cubes for a given dataset.
    
    Args:
        db_path: Path to DuckDB database
        ds: Data source to process
        output_dir: Output directory
        skip_existing: Skip if cube file already exists
        
    Returns:
        True if all cubes built successfully
    """
    print(f"\n{'='*70}")
    print(f"Building cubes for dataset: {ds}")
    print(f"Database: {db_path}")
    print(f"Output directory: {output_dir}")
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
        output_file = os.path.join(output_dir, f"{ds}_{metric}_{mover_str}_cube.parquet")
        
        if skip_existing and os.path.exists(output_file):
            print(f"[SKIP] {output_file} already exists")
            results.append(True)
            continue
        
        success = build_cube(db_path, ds, mover_ind, metric, output_dir)
        results.append(success)
        print()  # Blank line between cubes
    
    success_count = sum(results)
    total_count = len(results)
    
    print(f"\n{'='*70}")
    print(f"Summary: {success_count}/{total_count} cubes built successfully")
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


def parse_args(argv=None):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Build pre-aggregated cubes from DuckDB database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build cubes for default dataset (gamoshi)
  uv run build_cubes_from_db.py
  
  # Build cubes for specific dataset
  uv run build_cubes_from_db.py --ds gamoshi
  
  # Build cubes for all datasets in the database
  uv run build_cubes_from_db.py --all
  
  # Custom database and output location
  uv run build_cubes_from_db.py --db custom.db --ds gamoshi -o my_cubes/
  
  # Skip existing cubes (incremental build)
  uv run build_cubes_from_db.py --skip-existing
        """
    )
    
    parser.add_argument(
        "--db",
        default=os.path.join(os.getcwd(), "duck_suppression.db"),
        help="Path to DuckDB database (default: ./duck_suppression.db)"
    )
    
    parser.add_argument(
        "--ds",
        default="gamoshi",
        help="Dataset to process (default: gamoshi)"
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Build cubes for all datasets in the database"
    )
    
    parser.add_argument(
        "-o", "--output",
        default=os.path.join(os.getcwd(), "cubes"),
        help="Output directory for cube parquet files (default: ./cubes)"
    )
    
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip building cubes that already exist"
    )
    
    parser.add_argument(
        "--list-datasets",
        action="store_true",
        help="List available datasets and exit"
    )
    
    return parser.parse_args(argv)


def main(argv=None):
    """Main entry point"""
    args = parse_args(argv)
    
    # Check database exists
    if not os.path.exists(args.db):
        print(f"[ERROR] Database not found: {args.db}", file=sys.stderr)
        print(f"[ERROR] Run: uv run build_suppression_db.py <preagg.parquet>", file=sys.stderr)
        return 1
    
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
    
    # Build cubes for all datasets or just one
    if args.all:
        datasets = get_available_datasets(args.db)
        if not datasets:
            print("[ERROR] No datasets found in database", file=sys.stderr)
            return 1
        
        print(f"[INFO] Building cubes for {len(datasets)} dataset(s): {', '.join(datasets)}\n")
        
        all_success = True
        for ds in datasets:
            success = build_all_cubes(args.db, ds, args.output, args.skip_existing)
            if not success:
                all_success = False
                print(f"[WARNING] Some cubes failed for dataset: {ds}\n")
        
        return 0 if all_success else 1
    else:
        # Build cubes for single dataset
        success = build_all_cubes(args.db, args.ds, args.output, args.skip_existing)
        return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
