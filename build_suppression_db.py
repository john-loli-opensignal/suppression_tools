#!/usr/bin/env python3
"""
Build a persistent DuckDB database from pre-aggregated parquet files.

This script loads pre-agg data with display rules and geo enrichment into a 
persistent DuckDB file optimized for repeated querying. Significantly faster 
than scanning partitioned parquet files for interactive dashboards.

Usage:
    uv run build_suppression_db.py <base_preagg_path> [-o output.db]
    
Example:
    uv run build_suppression_db.py /path/to/preagg.parquet -o duck_suppression.db
"""
import os
import sys
import glob
import argparse
import shutil
import atexit
from typing import Optional


# Track temporary files for cleanup
_temp_files = []


def register_temp_cleanup():
    """Register cleanup handler for temporary files"""
    atexit.register(_cleanup_temp_files)


def _cleanup_temp_files():
    """Clean up all registered temporary files"""
    for f in _temp_files:
        try:
            if os.path.exists(f):
                os.remove(f)
                print(f"[CLEANUP] Removed temporary file: {f}")
        except Exception as e:
            print(f"[WARNING] Could not remove {f}: {e}", file=sys.stderr)


def _parquet_glob(path: str) -> str:
    """Convert path to parquet glob pattern"""
    if os.path.isdir(path):
        return os.path.join(path, "**", "*.parquet")
    if path.endswith(".parquet"):
        return path
    return os.path.join(path, "*.parquet")


def build_suppression_db(
    base: str,
    rules: str,
    geo: str,
    output_db: str,
    overwrite: bool = True,
    create_indexes: bool = True,
    optimize: bool = True
) -> bool:
    """
    Build a persistent DuckDB database from pre-agg parquet files.
    
    Args:
        base: Path to platform pre-agg parquet (dir or file)
        rules: Path to display rules parquet (dir or file)
        geo: Path to geo parquet (dir or file)
        output_db: Path to output DuckDB file (e.g., duck_suppression.db)
        overwrite: Whether to overwrite existing database
        create_indexes: Whether to create indexes on key columns
        optimize: Whether to run ANALYZE after loading
        
    Returns:
        True if successful, False otherwise
    """
    try:
        import duckdb
    except Exception as e:
        print("[ERROR] DuckDB not installed. Please: pip install duckdb", file=sys.stderr)
        return False

    # Convert paths to globs
    base_glob = _parquet_glob(base)
    rules_glob = _parquet_glob(rules)
    geo_glob = _parquet_glob(geo)

    # Preflight: check all sources exist
    missing = []
    if not glob.glob(base_glob, recursive=True):
        missing.append(f"base: {base_glob}")
    if not glob.glob(rules_glob, recursive=True):
        missing.append(f"rules: {rules_glob}")
    if not glob.glob(geo_glob, recursive=True):
        missing.append(f"geo: {geo_glob}")
    if missing:
        print("[ERROR] No parquet files found for: " + ", ".join(missing), file=sys.stderr)
        return False

    # Handle existing database
    if os.path.exists(output_db):
        if not overwrite:
            print(f"[ERROR] Database already exists: {output_db}", file=sys.stderr)
            print("[ERROR] Use --overwrite to replace it", file=sys.stderr)
            return False
        try:
            os.remove(output_db)
            print(f"[INFO] Removed existing database: {output_db}")
        except Exception as e:
            print(f"[ERROR] Failed to remove existing database: {e}", file=sys.stderr)
            return False

    print("[INFO] Starting DuckDB database build...")
    print(f"[INFO] base -> {base_glob}")
    print(f"[INFO] rules -> {rules_glob}")
    print(f"[INFO] geo -> {geo_glob}")
    print(f"[INFO] output -> {output_db}")

    con = duckdb.connect(output_db)
    try:
        # Set optimal configuration
        con.execute("PRAGMA enable_progress_bar = true;")
        con.execute("PRAGMA threads = 4;")  # Adjust based on system
        con.execute("PRAGMA memory_limit = '4GB';")  # Adjust based on system

        # Inspect geo schema to handle alternate column names
        print("[INFO] Inspecting geo schema...")
        geo_cols = set()
        try:
            df_cols = con.execute(f"DESCRIBE SELECT * FROM parquet_scan('{geo_glob}') LIMIT 0").df()
            geo_cols = set(df_cols['column_name'].astype(str).tolist())
        except Exception:
            geo_cols = set()
        
        blockid_candidates = [
            'census_blockid', 'serv_terr_blockid', 'acs_2017_blockid', 'popstats_blockid'
        ]
        state_candidates = [
            'state_name', 'state', 'state_abbr'
        ]
        blockid_col = next((c for c in blockid_candidates if c in geo_cols), None)
        state_col = next((c for c in state_candidates if c in geo_cols), None)
        
        if not blockid_col:
            raise RuntimeError(
                "Geo parquet missing a recognizable blockid column "
                "(tried: census_blockid, serv_terr_blockid, acs_2017_blockid, popstats_blockid)"
            )

        # Build enriched dataset query
        state_sel = f", {state_col} AS state" if state_col else ", NULL::VARCHAR AS state"
        
        print("[INFO] Creating carrier_data table with enrichments...")
        create_table_query = f"""
        CREATE TABLE carrier_data AS
        WITH base AS (
            SELECT * FROM parquet_scan('{base_glob}')
        ),
        rules_w AS (
            SELECT sp_dim_id AS w_sp_dim_id, sp_reporting_name_group AS winner
            FROM parquet_scan('{rules_glob}')
        ),
        rules_l AS (
            SELECT sp_dim_id AS l_sp_dim_id, sp_reporting_name_group AS loser
            FROM parquet_scan('{rules_glob}')
        ),
        geo AS (
            SELECT {blockid_col} AS census_blockid, dma, dma_name{state_sel}
            FROM parquet_scan('{geo_glob}')
        ),
        enriched AS (
            SELECT 
                b.*,
                w.winner,
                l.loser,
                g.dma,
                g.dma_name,
                g.state,
                COALESCE(CAST(b.the_date AS DATE), DATE '1970-01-01') AS the_date_clean,
                COALESCE(CAST(b.ds AS VARCHAR), 'unknown') AS ds_clean,
                COALESCE(CAST(b.mover_ind AS BOOLEAN), FALSE) AS mover_ind_clean
            FROM base b
            LEFT JOIN rules_w w ON b.primary_sp_group = w.w_sp_dim_id
            LEFT JOIN rules_l l ON b.secondary_sp_group = l.l_sp_dim_id
            LEFT JOIN geo g ON b.primary_geoid = g.census_blockid
        )
        SELECT 
            the_date_clean AS the_date,
            ds_clean AS ds,
            mover_ind_clean AS mover_ind,
            winner,
            loser,
            dma,
            dma_name,
            state,
            adjusted_wins,
            adjusted_losses,
            primary_geoid,
            primary_sp_group,
            secondary_sp_group,
            CAST(strftime('%Y', the_date_clean) AS INTEGER) AS year,
            CAST(strftime('%m', the_date_clean) AS INTEGER) AS month,
            CAST(strftime('%d', the_date_clean) AS INTEGER) AS day,
            CAST(strftime('%w', the_date_clean) AS INTEGER) AS day_of_week
        FROM enriched
        WHERE winner IS NOT NULL 
          AND loser IS NOT NULL 
          AND dma_name IS NOT NULL
          AND the_date_clean IS NOT NULL
          AND ds_clean IS NOT NULL
        """
        
        con.execute(create_table_query)
        
        # Get row count
        row_count = con.execute("SELECT COUNT(*) FROM carrier_data").fetchone()[0]
        print(f"[INFO] Loaded {row_count:,} rows into carrier_data table")
        
        if row_count == 0:
            print("[WARNING] No rows loaded. Check your join conditions and source data.", file=sys.stderr)
            return False

        # Data quality checks
        print("[INFO] Running data quality checks...")
        
        # Check for nulls in key columns
        null_check_query = """
        SELECT
            SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS null_winner,
            SUM(CASE WHEN loser IS NULL THEN 1 ELSE 0 END) AS null_loser,
            SUM(CASE WHEN dma_name IS NULL THEN 1 ELSE 0 END) AS null_dma_name,
            SUM(CASE WHEN the_date IS NULL THEN 1 ELSE 0 END) AS null_date
        FROM carrier_data
        """
        null_counts = con.execute(null_check_query).fetchone()
        
        if any(count > 0 for count in null_counts):
            print(f"[WARNING] Found NULL values in key columns:", file=sys.stderr)
            if null_counts[0] > 0: print(f"  - winner: {null_counts[0]} rows", file=sys.stderr)
            if null_counts[1] > 0: print(f"  - loser: {null_counts[1]} rows", file=sys.stderr)
            if null_counts[2] > 0: print(f"  - dma_name: {null_counts[2]} rows", file=sys.stderr)
            if null_counts[3] > 0: print(f"  - the_date: {null_counts[3]} rows", file=sys.stderr)
        else:
            print("[INFO] ✓ No NULL values in key columns")

        # Get date range
        date_range = con.execute(
            "SELECT MIN(the_date) as min_date, MAX(the_date) as max_date FROM carrier_data"
        ).fetchone()
        print(f"[INFO] Date range: {date_range[0]} to {date_range[1]}")
        
        # Get distinct counts
        stats_query = """
        SELECT
            COUNT(DISTINCT ds) as ds_count,
            COUNT(DISTINCT winner) as winner_count,
            COUNT(DISTINCT loser) as loser_count,
            COUNT(DISTINCT dma_name) as dma_count,
            COUNT(DISTINCT state) as state_count
        FROM carrier_data
        """
        stats = con.execute(stats_query).fetchone()
        print(f"[INFO] Distinct values:")
        print(f"  - Data sources (ds): {stats[0]}")
        print(f"  - Winners: {stats[1]}")
        print(f"  - Losers: {stats[2]}")
        print(f"  - DMAs: {stats[3]}")
        print(f"  - States: {stats[4]}")

        # Create indexes for query performance
        if create_indexes:
            print("[INFO] Creating indexes on key columns...")
            indexes = [
                ("idx_ds", "ds"),
                ("idx_mover_ind", "mover_ind"),
                ("idx_the_date", "the_date"),
                ("idx_winner", "winner"),
                ("idx_loser", "loser"),
                ("idx_dma_name", "dma_name"),
                ("idx_state", "state"),
                ("idx_year_month", "year, month"),
            ]
            
            for idx_name, columns in indexes:
                try:
                    con.execute(f"CREATE INDEX {idx_name} ON carrier_data({columns})")
                    print(f"  ✓ Created index: {idx_name}")
                except Exception as e:
                    print(f"  ✗ Failed to create index {idx_name}: {e}", file=sys.stderr)

        # Optimize database
        if optimize:
            print("[INFO] Optimizing database (running ANALYZE)...")
            con.execute("ANALYZE carrier_data")
            print("[INFO] ✓ Database optimized")

        # Create helpful views for common queries
        print("[INFO] Creating convenience views...")
        
        # National daily aggregates view
        con.execute("""
        CREATE VIEW IF NOT EXISTS national_daily AS
        SELECT 
            the_date,
            ds,
            mover_ind,
            winner,
            SUM(adjusted_wins) as total_wins,
            SUM(adjusted_losses) as total_losses,
            COUNT(DISTINCT dma_name) as dma_count
        FROM carrier_data
        GROUP BY the_date, ds, mover_ind, winner
        """)
        print("  ✓ Created view: national_daily")
        
        # DMA daily aggregates view
        con.execute("""
        CREATE VIEW IF NOT EXISTS dma_daily AS
        SELECT 
            the_date,
            ds,
            mover_ind,
            dma_name,
            state,
            winner,
            loser,
            SUM(adjusted_wins) as total_wins,
            SUM(adjusted_losses) as total_losses
        FROM carrier_data
        GROUP BY the_date, ds, mover_ind, dma_name, state, winner, loser
        """)
        print("  ✓ Created view: dma_daily")

        # Get final database size
        db_size_mb = os.path.getsize(output_db) / (1024 * 1024)
        print(f"\n[SUCCESS] Database created: {output_db}")
        print(f"[INFO] Database size: {db_size_mb:.2f} MB")
        print(f"[INFO] Total rows: {row_count:,}")
        
        return True

    except Exception as e:
        print(f"[ERROR] Failed to build database: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False
    finally:
        con.close()


def parse_args(argv=None):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Build a persistent DuckDB database for carrier suppression analysis.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build database from pre-agg parquet
  uv run build_suppression_db.py /path/to/preagg.parquet
  
  # Custom output location
  uv run build_suppression_db.py /path/to/preagg.parquet -o my_db.duckdb
  
  # Use custom reference data
  uv run build_suppression_db.py /path/to/preagg.parquet \\
      --rules /custom/rules.parquet \\
      --geo /custom/geo.parquet
        """
    )
    
    parser.add_argument(
        "base",
        help="Path to platform pre-agg parquet (directory or file)"
    )
    
    # Default to project-local reference locations
    base_dir = os.path.dirname(__file__)
    default_rules = os.path.join(base_dir, "ref", "display_rules")
    default_geo = os.path.join(base_dir, "ref", "cb_cw_2020")
    
    parser.add_argument(
        "--rules",
        default=default_rules,
        help=f"Path to display rules parquet (default: {default_rules})"
    )
    
    parser.add_argument(
        "--geo",
        default=default_geo,
        help=f"Path to geo parquet (default: {default_geo})"
    )
    
    parser.add_argument(
        "-o", "--output",
        default=os.path.join(os.getcwd(), "duck_suppression.db"),
        help="Output database file path (default: ./duck_suppression.db)"
    )
    
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Do not overwrite existing database file"
    )
    
    parser.add_argument(
        "--no-indexes",
        action="store_true",
        help="Skip creating indexes (faster load, slower queries)"
    )
    
    parser.add_argument(
        "--no-optimize",
        action="store_true",
        help="Skip running ANALYZE optimization"
    )
    
    return parser.parse_args(argv)


def main(argv=None):
    """Main entry point"""
    register_temp_cleanup()
    
    args = parse_args(argv)
    
    ok = build_suppression_db(
        base=args.base,
        rules=args.rules,
        geo=args.geo,
        output_db=args.output,
        overwrite=not args.no_overwrite,
        create_indexes=not args.no_indexes,
        optimize=not args.no_optimize,
    )
    
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
