Title: feat(cube): use shared cube_outliers; default to outlier days

Base: codex-agent
Head: feature/cube-shared-T6

Summary
- Refactor build_win_cube.py to call suppression_tools.src.outliers.cube_outliers instead of embedded SQL.
- Preserve CLI flags; default to national-outlier days; support --all-rows to include all rows.
- Keep data-quality gates as in unified SQL (exclude NULL winner/loser/dma_name; pair_wins_current > 0).

Changes
- build_win_cube.py: replace inline SQL; add safe default date resolution; preserve output writing.
- suppression_tools/src/outliers.py: minor debug/robustness logs around template rendering (non-functional).

Validation
- Unit tests: uv run --python .venv pytest -q (passed).
- Smoke: make smoke (passed).
- Cube smoke:
  - Default (outlier days only): 83,749 rows, columns include `the_date,winner,loser,dma_name,pair_wins_current,nat_share_current,nat_outlier_pos`.
  - With --all-rows: 912,600 rows (expanded), same schema; `nat_outlier_pos` present.

Repro
- uv run --python .venv python build_win_cube.py --store ./duckdb_partitioned_store/**/*.parquet --ds gamoshi --mover-ind True -o ./current_run_duckdb/win_cube_T6_default.csv
- uv run --python .venv python build_win_cube.py --store ./duckdb_partitioned_store/**/*.parquet --ds gamoshi --mover-ind True --all-rows -o ./current_run_duckdb/win_cube_T6_all.csv

Rollback
- Single-commit revertable after merge (`git revert <merge-sha>`), or revert individual commits listed in PR_T6.txt.

Notes
- Keeps output schema stable for existing consumers; defaults tuned for performance by limiting to national outlier days.

