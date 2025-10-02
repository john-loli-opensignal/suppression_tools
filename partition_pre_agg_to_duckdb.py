#!/usr/bin/env python3
import os
import sys
import glob
import argparse
import shutil


def _parquet_glob(path: str) -> str:
    if os.path.isdir(path):
        return os.path.join(path, "**", "*.parquet")
    if path.endswith(".parquet"):
        return path
    return os.path.join(path, "*.parquet")


def build_partitioned_dataset(base: str, rules: str, geo: str, output_dir: str, overwrite: bool = True) -> bool:
    try:
        import duckdb
    except Exception as e:
        print("[ERROR] DuckDB not installed. Please: pip install duckdb", file=sys.stderr)
        return False

    base_glob = _parquet_glob(base)
    rules_glob = _parquet_glob(rules)
    geo_glob = _parquet_glob(geo)

    # Preflight
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

    # Overwrite handling
    if overwrite and os.path.isdir(output_dir):
        try:
            shutil.rmtree(output_dir)
        except Exception as e:
            print(f"[ERROR] Failed to clear output directory '{output_dir}': {e}", file=sys.stderr)
            return False
    os.makedirs(output_dir, exist_ok=True)

    print("[INFO] Starting DuckDB build...")
    print(f"[INFO] base -> {base_glob}")
    print(f"[INFO] rules -> {rules_glob}")
    print(f"[INFO] geo -> {geo_glob}")
    print(f"[INFO] output -> {output_dir}")

    con = duckdb.connect()
    try:
        con.execute("PRAGMA enable_progress_bar = true;")

        # Inspect geo schema to tolerate alternate column names
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
            raise RuntimeError("Geo parquet missing a recognizable blockid column (tried census_blockid, serv_terr_blockid, acs_2017_blockid, popstats_blockid)")

        # Build query with detected geo columns
        state_sel = f", {state_col} AS state" if state_col else ", NULL::VARCHAR AS state"
        final_query = f"""
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
        enr AS (
            SELECT b.*, w.winner, l.loser, g.dma, g.dma_name, g.state
            FROM base b
            LEFT JOIN rules_w w ON b.primary_sp_group = w.w_sp_dim_id
            LEFT JOIN rules_l l ON b.secondary_sp_group = l.l_sp_dim_id
            LEFT JOIN geo g ON b.primary_geoid = g.census_blockid
        ),
        final AS (
            SELECT
                enr.*,
                COALESCE(CAST(the_date AS DATE), DATE '1970-01-01') AS the_date,
                COALESCE(CAST(ds AS VARCHAR), 'unknown') AS ds,
                COALESCE(CAST(mover_ind AS BOOLEAN), FALSE) AS mover_ind,
                -- Derive Y/M/D as strings for partitioning (even if originals exist, ensure they are populated)
                CAST(strftime('%Y', the_date) AS VARCHAR) AS year,
                CAST(strftime('%m', the_date) AS VARCHAR) AS month,
                CAST(strftime('%d', the_date) AS VARCHAR) AS day,
                CASE WHEN COALESCE(CAST(mover_ind AS BOOLEAN), FALSE) THEN 'True' ELSE 'False' END AS p_mover_ind
            FROM enr
        )
        SELECT * FROM final
        """

        # Register the final CTE as a temporary view for checks
        con.execute(f"CREATE TEMPORARY VIEW final_data AS {final_query}")

        # Check 1: Null keys
        null_counts_query = """
        SELECT
            SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS null_winner,
            SUM(CASE WHEN loser IS NULL THEN 1 ELSE 0 END) AS null_loser,
            SUM(CASE WHEN dma_name IS NULL THEN 1 ELSE 0 END) AS null_dma_name
        FROM final_data
        """
        null_counts = con.execute(null_counts_query).fetch_first()

        if null_counts and (null_counts[0] > 0 or null_counts[1] > 0 or null_counts[2] > 0):
            print(f"[ERROR] Found NULL keys in final data:", file=sys.stderr)
            if null_counts[0] > 0: print(f"  - winner: {null_counts[0]} rows", file=sys.stderr)
            if null_counts[1] > 0: print(f"  - loser: {null_counts[1]} rows", file=sys.stderr)
            if null_counts[2] > 0: print(f"  - dma_name: {null_counts[2]} rows", file=sys.stderr)
            # Sample problematic rows
            sample_query = """
            SELECT winner, loser, dma_name
            FROM final_data
            WHERE winner IS NULL OR loser IS NULL OR dma_name IS NULL
            LIMIT 5
            """
            sample_df = con.execute(sample_query).df()
            print("  Sample problematic rows:", file=sys.stderr)
            print(sample_df.to_string(), file=sys.stderr)
            return False
        print("[INFO] Null key check passed.", file=sys.stderr)

        # Check 2: DMA join coverage
        coverage_query = """
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN dma_name IS NOT NULL THEN 1 ELSE 0 END) AS mapped_dma_rows
        FROM enr
        """
        coverage_counts = con.execute(coverage_query).fetch_first()

        if coverage_counts and coverage_counts[0] > 0 and coverage_counts[1] < coverage_counts[0]:
            unmapped_count = coverage_counts[0] - coverage_counts[1]
            print(f"[ERROR] DMA join coverage failed: {unmapped_count} primary_geoid unmapped.", file=sys.stderr)
            return False
        print("[INFO] DMA join coverage check passed.", file=sys.stderr)

        copy_stmt = f"""
        COPY (
          SELECT * FROM (
            {final_query}
          ) WHERE ds IS NOT NULL AND p_mover_ind IS NOT NULL AND the_date IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
        ) TO '{output_dir}' (FORMAT PARQUET, PARTITION_BY (ds, p_mover_ind, year, month, day, the_date));
        """

        con.execute(copy_stmt)
        print("[INFO] Done. Partitioned dataset written.")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to write partitioned dataset: {e}", file=sys.stderr)
        return False
    finally:
        con.close()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Build a partitioned parquet dataset (DuckDB) for the carrier dashboard.")
    parser.add_argument("base", help="Path to platform pre-agg parquet (dir or file)")
    # Default to project-local reference locations; override as needed
    base_dir = os.path.dirname(__file__)
    default_rules = os.path.join(base_dir, "ref", "display_rules")
    default_geo = os.path.join(base_dir, "ref", "cb_cw_2020")
    parser.add_argument("--rules", default=default_rules, help="Path to display rules parquet (dir or file)")
    parser.add_argument("--geo", default=default_geo, help="Path to geo parquet (dir or file)")
    parser.add_argument("-o", "--output", default=os.path.join(os.getcwd(), "duckdb_partitioned_store"), help="Output directory (default: ./duckdb_partitioned_store)")
    parser.add_argument("--no-overwrite", action="store_true", help="Do not overwrite existing output directory")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    ok = build_partitioned_dataset(
        base=args.base,
        rules=args.rules,
        geo=args.geo,
        output_dir=args.output,
        overwrite=not args.no_overwrite,
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
