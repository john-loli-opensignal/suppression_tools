Suppression Tools

Overview
- This package provides a focused, self‑contained flow to:
  1) Inspect base (unsuppressed) data in a date window
  2) Detect positive outliers (DOW‑partitioned, 14→28 fallback)
  3) Build an auto‑suppression plan directly from those base outliers
  4) Preview QA columns (national and pair/DMA baselines) before applying
  5) Apply (write a plan CSV into the suppressions folder)
  6) Optionally build a new suppressed dataset for visuals

Structure
- main.py: Streamlit dashboard entry point
- src/
  - plan.py: duckdb helpers to scan outliers and build plans
  - util.py: small helpers (paths, formatting)
- sql/
  - outliers_base.sql: base outlier scan (positive z only)
  - plan_winner_dates.sql: plan for a winner on specific dates (with nat_* and pair_* QA)

Typical Flow
1) Start the dashboard: streamlit run suppression_tools/main.py
2) Pick store dir, ds, mover_ind, and Graph Window (view‑only)
3) Click “Scan base outliers (view)” to list all positive outliers in the window
4) Click “Build plan from these outliers” to preview the plan with QA columns
5) If it looks right, “Save plan to suppressions folder” to apply
6) When satisfied, click “Build suppressed dataset” (wraps the existing builder)

Notes
- Plans are saved to ./suppressions/<round_name>.csv within this project directory
- Round configs (when exporting from the simulator) are saved under
  ./suppressions/rounds/<round_name>/
- QA columns are ignored when applying; only the key + remove_units are used
- The builder writes a new dataset to ./duckdb_partitioned_store_suppressed

Paths and Defaults
- Suppressions directory (applied plans): ./suppressions
- Suppression rounds directory: ./suppressions/rounds
- Partitioned dataset input (parquet glob): ./duckdb_partitioned_store/**/*.parquet
- Suppressed dataset output: ./duckdb_partitioned_store_suppressed

TODO
- Create a hosted Git remote (GitHub/Bitbucket) and update the local `origin` to the hosted URL. Then open PRs from `codex-agent` to `main` for merges.
