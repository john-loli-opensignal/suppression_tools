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
        query = f"""
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
            SELECT census_blockid, dma, dma_name, state_name AS state
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

        copy_stmt = f"""
        COPY (
          SELECT * FROM (
            {query}
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
