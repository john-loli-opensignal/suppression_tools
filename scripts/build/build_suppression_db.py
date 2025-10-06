#!/usr/bin/env python3
"""
Build a persistent DuckDB database from pre-aggregated parquet files.

This script loads pre-agg data with display rules and geo enrichment into a 
persistent DuckDB file optimized for repeated querying. Significantly faster 
than scanning partitioned parquet files for interactive dashboards.

SUPPORTS MULTIPLE PRE-AGG VERSIONS:
- v15.0: 2020 census blocks (primary_geoid, has ds column)
- v0.3: 2010 census blocks (census_blockid, ds from path)

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
import re
import json
from datetime import datetime
from typing import Optional, Dict


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


def extract_ds_from_path(base: str) -> Optional[str]:
    """
    Extract dataset name from v0.3 path pattern.
    
    Pattern: ~/tmp/platform_pre_aggregate_v_0_3/{ds}/{date}/{uuid}/
    
    Args:
        base: Path to pre-agg data
        
    Returns:
        Dataset name (e.g., 'gamoshi') or None if not found
    """
    # Expand ~ to full path
    expanded = os.path.expanduser(base)
    
    # Look for v0.3 pattern
    pattern = r'platform_pre_aggregate_v_0_3/([^/]+)/'
    match = re.search(pattern, expanded)
    
    if match:
        ds_name = match.group(1)
        print(f"[INFO] Detected dataset from path: {ds_name}")
        return ds_name
    
    return None


def detect_preagg_version(base: str, con) -> Dict[str, any]:
    """
    Detect pre-agg version by inspecting schema and path structure.
    
    Args:
        base: Path to pre-agg parquet files
        con: DuckDB connection
        
    Returns:
        Dictionary with version metadata:
        {
            'version': 'v15.0' | 'v0.3',
            'blockid_col': 'primary_geoid' | 'census_blockid',
            'has_ds_column': bool,
            'ds_from_path': Optional[str],
            'crosswalk_join_key': str,
            'recommended_crosswalk': str,
            'date_type': 'INT32' | 'BYTE_ARRAY'
        }
    """
    base_glob = _parquet_glob(base)
    
    try:
        # Inspect schema
        schema_df = con.execute(f"""
            SELECT column_name, column_type 
            FROM (DESCRIBE SELECT * FROM parquet_scan('{base_glob}') LIMIT 0)
        """).df()
        
        columns = set(schema_df['column_name'].str.lower().tolist())
        
        # Get column type map
        type_map = dict(zip(
            schema_df['column_name'].str.lower(),
            schema_df['column_type'].str.upper()
        ))
        
    except Exception as e:
        raise RuntimeError(f"Failed to inspect pre-agg schema: {e}")
    
    # Detect version based on schema
    has_primary_geoid = 'primary_geoid' in columns
    has_census_blockid = 'census_blockid' in columns
    has_ds_column = 'ds' in columns
    
    # Check date format
    date_type = None
    if 'the_date' in type_map:
        date_type_raw = type_map['the_date']
        if 'INT' in date_type_raw:
            date_type = 'INT32'
        elif 'VARCHAR' in date_type_raw or 'BYTE_ARRAY' in date_type_raw:
            date_type = 'BYTE_ARRAY'
    
    # Determine version
    if has_primary_geoid and has_ds_column:
        version = 'v15.0'
        blockid_col = 'primary_geoid'
        crosswalk_join_key = 'census_blockid'
        recommended_crosswalk = 'ref/cb_cw_2020'
        ds_from_path = None
        
    elif has_census_blockid:
        version = 'v0.3'
        blockid_col = 'census_blockid'
        crosswalk_join_key = 'serv_terr_blockid'
        recommended_crosswalk = 'ref/d_census_block_crosswalk'
        
        # Try to extract ds from path
        ds_from_path = extract_ds_from_path(base)
        
        if not has_ds_column and not ds_from_path:
            print("[WARNING] ⚠️  v0.3 data missing 'ds' column and could not extract from path!", file=sys.stderr)
            print("[WARNING] Dataset name will default to 'unknown'", file=sys.stderr)
        elif not has_ds_column and ds_from_path:
            print(f"[INFO] ⚠️  v0.3 data missing 'ds' column - using path-derived value: '{ds_from_path}'")
        
    else:
        raise RuntimeError(
            f"Unable to detect pre-agg version. Found columns: {sorted(columns)}\n"
            "Expected either 'primary_geoid' (v15.0) or 'census_blockid' (v0.3)"
        )
    
    metadata = {
        'version': version,
        'blockid_col': blockid_col,
        'has_ds_column': has_ds_column,
        'ds_from_path': ds_from_path,
        'crosswalk_join_key': crosswalk_join_key,
        'recommended_crosswalk': recommended_crosswalk,
        'date_type': date_type,
        'column_count': len(columns)
    }
    
    return metadata


def build_suppression_db(
    base: str,
    rules: str,
    geo: str,
    output_db: str,
    overwrite: bool = True,
    create_indexes: bool = True,
    optimize: bool = True,
    detect_only: bool = False
) -> bool:
    """
    Build a persistent DuckDB database from pre-agg parquet files.
    
    Auto-detects pre-agg version (v15.0 or v0.3) and uses appropriate schema.
    
    Args:
        base: Path to platform pre-agg parquet (dir or file)
        rules: Path to display rules parquet (dir or file)
        geo: Path to geo parquet (dir or file)
        output_db: Path to output DuckDB file (e.g., duck_suppression.db)
        overwrite: Whether to overwrite existing database
        create_indexes: Whether to create indexes on key columns
        optimize: Whether to run ANALYZE after loading
        detect_only: Only detect version and print info, don't build
        
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

    # Detect pre-agg version
    print("[INFO] Detecting pre-agg version...")
    con_temp = duckdb.connect()
    try:
        version_info = detect_preagg_version(base, con_temp)
    except Exception as e:
        print(f"[ERROR] Failed to detect pre-agg version: {e}", file=sys.stderr)
        return False
    finally:
        con_temp.close()
    
    # Print version info
    print(f"\n{'='*70}")
    print(f"PRE-AGG VERSION DETECTED: {version_info['version']}")
    print(f"{'='*70}")
    print(f"  Block ID column:      {version_info['blockid_col']}")
    print(f"  Has DS column:        {version_info['has_ds_column']}")
    print(f"  DS from path:         {version_info['ds_from_path'] or 'N/A'}")
    print(f"  Crosswalk join key:   {version_info['crosswalk_join_key']}")
    print(f"  Recommended XWalk:    {version_info['recommended_crosswalk']}")
    print(f"  Date format:          {version_info['date_type']}")
    print(f"  Total columns:        {version_info['column_count']}")
    print(f"{'='*70}\n")
    
    # If detect_only, exit here
    if detect_only:
        print("[INFO] Detect-only mode. Exiting.")
        return True

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
        
        # Use version-detected crosswalk join key
        expected_join_key = version_info['crosswalk_join_key']
        if expected_join_key not in geo_cols:
            raise RuntimeError(
                f"Geo parquet missing expected join key '{expected_join_key}' for {version_info['version']}. "
                f"Found columns: {sorted(geo_cols)}"
            )
        
        state_candidates = [
            'state_name', 'state', 'state_abbr'
        ]
        state_col = next((c for c in state_candidates if c in geo_cols), None)
        state_sel = f", {state_col} AS state" if state_col else ", NULL::VARCHAR AS state"
        
        print(f"[INFO] Using crosswalk join key: {expected_join_key}")
        print("[INFO] Creating carrier_data table with enrichments...")
        
        # ===============================================================
        # VERSION-SPECIFIC QUERY GENERATION
        # ===============================================================
        
        if version_info['version'] == 'v15.0':
            # v15.0: primary_geoid, ds column exists, date is INT32
            print("[INFO] Using v15.0 (2020 census blocks) schema...")
            
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
                SELECT {expected_join_key} AS census_blockid, dma, dma_name{state_sel}
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
                primary_geoid AS census_blockid,
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
        
        elif version_info['version'] == 'v0.3':
            # v0.3: census_blockid, ds from path, date is BYTE_ARRAY
            print("[INFO] Using v0.3 (2010 census blocks) schema...")
            
            # Determine ds value
            ds_value = version_info['ds_from_path'] or 'unknown'
            if not version_info['has_ds_column']:
                print(f"[INFO] ⚠️  Injecting ds column with value: '{ds_value}'")
            
            create_table_query = f"""
            CREATE TABLE carrier_data AS
            WITH base AS (
                SELECT 
                    *,
                    -- Inject ds column if missing
                    {f"'{ds_value}'" if not version_info['has_ds_column'] else "ds"} AS ds_injected
                FROM parquet_scan('{base_glob}')
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
                SELECT {expected_join_key} AS census_blockid, dma, dma_name{state_sel}
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
                    -- v0.3: the_date is BYTE_ARRAY in 'YYYY-MM-DD' format
                    COALESCE(TRY_CAST(b.the_date AS DATE), DATE '1970-01-01') AS the_date_clean,
                    COALESCE(CAST(b.ds_injected AS VARCHAR), 'unknown') AS ds_clean,
                    COALESCE(CAST(b.mover_ind AS BOOLEAN), FALSE) AS mover_ind_clean
                FROM base b
                LEFT JOIN rules_w w ON b.primary_sp_group = w.w_sp_dim_id
                LEFT JOIN rules_l l ON b.secondary_sp_group = l.l_sp_dim_id
                LEFT JOIN geo g ON b.census_blockid = g.census_blockid
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
                census_blockid,
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
        
        else:
            raise RuntimeError(f"Unsupported pre-agg version: {version_info['version']}")

        
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
        
        # Save build configuration for reproducibility
        config_path = output_db.replace('.db', '_build_config.json').replace('.duckdb', '_build_config.json')
        build_config = {
            'version': version_info['version'],
            'build_date': datetime.utcnow().isoformat() + 'Z',
            'source_path': base,
            'rules_path': rules,
            'crosswalk_path': geo,
            'output_db': output_db,
            'row_count': row_count,
            'date_range': [str(date_range[0]), str(date_range[1])],
            'datasets': stats[0],
            'winners': stats[1],
            'losers': stats[2],
            'dmas': stats[3],
            'states': stats[4],
            'version_info': version_info,
            'command': ' '.join(sys.argv)
        }
        
        try:
            with open(config_path, 'w') as f:
                json.dump(build_config, f, indent=2, default=str)
            print(f"[INFO] Build config saved: {config_path}")
        except Exception as e:
            print(f"[WARNING] Failed to save build config: {e}", file=sys.stderr)
        
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
    
    # Default to project-local reference locations (relative to project root, not script)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    default_rules = os.path.join(project_root, "ref", "display_rules")
    default_geo = os.path.join(project_root, "ref", "cb_cw_2020")
    
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
        default=os.path.join(os.getcwd(), "data/databases/duck_suppression.db"),
        help="Output database file path (default: ./data/databases/duck_suppression.db)"
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
    
    parser.add_argument(
        "--detect-only",
        action="store_true",
        help="Only detect pre-agg version and print info, don't build database"
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
        detect_only=args.detect_only,
    )
    
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
