Title: fix(partition): fail fast on null keys and unmapped geo

Base: codex-agent
Head: feature/partition-guards-T7

Summary
- Add explicit data-quality guards to the partition builder (`partition_pre_agg_to_duckdb.py`).
- Guard 1: Fail if any of `winner`, `loser`, or `dma_name` are NULL; report counts and sample rows.
- Guard 2: Report DMA join coverage and fail if any `primary_geoid` remains unmapped (NULL `dma_name`).
- Keeps tolerant geo schema detection and COPY layout intact.

Changes
- `partition_pre_agg_to_duckdb.py`: 48 insertions, 2 deletions; adds guard queries and clear `[ERROR]` messages before COPY; fixes `fetch_first` to `fetchone`.

Validation
- Unit tests: `uv run --python .venv pytest -q` — green.
- Smoke: `make smoke` — green.
- Manual builder (failure path):
  - Command: `uv run --python .venv python partition_pre_agg_to_duckdb.py ./duckdb_partitioned_store -o ./temp_output_dir`
  - Result: exits non-zero with `[ERROR] Found NULL keys in final data` and sample rows (e.g., `dma_name: 2207587 rows`).

Repro
- `uv run --python .venv python partition_pre_agg_to_duckdb.py <base> --rules ./ref/display_rules --geo ./ref/cb_cw_2020 -o ./duckdb_partitioned_store`

Rollback
- Single-commit revertable after merge (`git revert <merge-sha>`), or revert individual commits listed in PR_T7.txt.

Notes
- Success path validation requires a clean crosswalk; failure path confirmed and actionable.

