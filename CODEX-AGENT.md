# Codex Agent Plan: Suppression Tools Alignment

## Mission
Enable rapid, correct iteration from current market state → outlier detection → plan simulation → applied suppressions, with shared core logic, strong data quality guarantees, and project‑local reproducibility.

## Roles and Names (Proposed)
- Viewer Dashboard: `carrier_dashboard_duckdb.py` (aka “Market Viewer”)
  - Purpose: Inspect current state (filters, date window, comparisons).
- Auto Suppression Tool: `main.py` (aka “Auto Suppressor”)
  - Purpose: Scan base outliers and propose a first‑pass plan.
- Suppression Simulator: `carrier_suppression_dashboard.py` (aka “Simulator”)
  - Purpose: Preview national impact; iterate additional outlier rounds; export rounds.
- Cube Builder: `build_win_cube.py`
  - Purpose: Produce compact outlier‑day pair metrics for faster UX.
- Partition Builder: `partition_pre_agg_to_duckdb.py`
  - Purpose: Materialize partitioned parquet store with complete mappings.

Note: Keep current filenames; adopt the role names in docs/UI. Rename only after code alignment.

## Shared Core (Design Targets)
Create a single source of truth for metrics and outlier logic under `suppression_tools/src`:
- `metrics.py`
  - Market totals; national series; pair aggregates; competitor views.
- `outliers.py`
  - DOW‑partitioned baselines (μ/σ); z‑scores; outlier flags.
- `util.py`
  - Project‑local paths; date helpers; small I/O utilities.

Dashboards and cube builder call these modules; SQL lives in one place.

## Data Quality Guarantees
- No NULL keys: winner, loser, dma_name are non‑null in store, cube, and simulator inputs.
- Positive volume: aggregated pair_wins_current > 0 for all cube rows.
- Outliers‑only cube: rows limited to national outlier days by default (flag present).
- Full crosswalk coverage: 100% of `primary_geoid` mapped to DMA via the ref crosswalk.

## Environment & Paths
- Project‑local data only (no `~` expansion):
  - Store: `./duckdb_partitioned_store/**/*.parquet`
  - Cubes: `./current_run_duckdb/*.csv`
  - Suppressions: `./suppressions/*.csv`, rounds under `./suppressions/rounds`.
- uv env: `uv venv .venv`; run with `uv run --python .venv ...`.
- Never commit generated data; `.gitignore` excludes stores/cubes/suppressions.

## Branching & Process
- Work on `codex-agent`; open PRs to `main`.
- Conventional commits; one logical change per PR.
- Small, reviewable diffs; include rationale and validation notes.

### PR Workflow and Review
- Agents must submit PRs from `codex-agent` (feature branches allowed) into `main`.
- Reviewer of record: the supervising Codex assistant (this agent) — the “harbinger of truth”.
- The supervising agent will validate data‑quality gates, run smoke tests, and check parity snapshots before approval.
- Human owner gives the go‑ahead for the supervising agent to review/approve the PR.

## Success Criteria (Acceptance)
- Shared modules:
  - Both dashboards and cube builder import metrics/outliers from `suppression_tools/src`.
  - No duplicated SQL for national/pair metrics or outliers.
- Data quality:
  - Store has 0 NULL in winner/loser/dma_name.
  - Cube has 0 NULL in keys and pair_wins_current > 0 for 100% rows.
  - Cube row count aligns with outlier days only (sanity threshold documented in PR).
- Tests & tooling:
  - `make smoke` and `make smoke-cube` pass.
  - Unit tests for metrics/outliers pass (pytest).
  - `ruff check .` passes (or issues documented and deferred).
- UX parity:
  - Viewer and Simulator produce identical charts before/after refactor for a fixed slice (snapshot attached in PR).

## Work Breakdown (Tickets)
1) Extract core metrics
- Implement `src/metrics.py` with: market totals, national series, pair aggregates, competitor metrics.
- Tests: golden SQL equivalence to current outputs on a small fixture.
- AC: Both dashboards render with `metrics.py` without behavior change.

2) Extract outlier logic
- Implement `src/outliers.py` with DOW μ/σ/z and flags.
- Replace ad‑hoc z‑score SQL in dashboards and builder.
- AC: Outlier days/points match prior results on fixture; z deltas explained if any.

3) Align cube builder
- Make `build_win_cube.py` call `metrics/outliers`; enforce outliers‑only and data‑quality gates.
- AC: No NULL keys; pair_wins_current > 0; row count in expected bounds.

4) Harden partition builder
- Validate crosswalk coverage; fail fast if unmapped geoids > 0.
- AC: Store null checks for keys all zero on rebuild.

5) Testing & linting
- Add pytest with minimal CSV/Parquet fixtures (small, checked‑in).
- Add `ruff` and `make lint`.
- AC: CI or local script runs lint + tests + smoke.

6) UX polish (low risk)
- Add “Reset to full range” and include date range in chart titles.
- AC: Tested on both dashboards; no logic changes.

7) Docs
- Update README with roles, run commands, and expected data guarantees.
- AC: Clear guidance for new users; matches implementation.

### Assignment Batches
- Batch 1 (Agent A)
  - T1) Extract core metrics (src/metrics.py) and wire Viewer to it.
  - T2) Extract outlier logic (src/outliers.py) and wire Viewer to it.
  - T3) Author SQL templates (see Unified SQL) and add renderer helper.
  - T4) Add unit tests for metrics/outliers; extend smoke tests.
  - Deliverable: Viewer running solely on shared modules; tests + smoke passing.

- Batch 2 (Agent B)
  - T5) Wire Simulator to shared modules; preserve plan preview UX.
  - T6) Align cube builder to shared modules; enforce data‑quality gates.
  - T7) Harden partition builder null checks; fail fast if any missing mappings.
  - Deliverable: Simulator + cube parity with pre‑refactor; no NULL keys; outliers‑only cube.

- Batch 3 (Agent C)
  - T8) UX polish (date reset, titles); docs refresh.
  - T9) Add lint (ruff) + Make targets; optional CI script.
  - Deliverable: Documentation complete; lint/tests/smoke green; PR guides included.

## Validation Checklist (PR template)
- What changed and why (module, caller, tests).
- Data quality diffs: key NULL counts, cube size, sample day comparison.
- How to reproduce (commands; seeds; fixture files).
- Rollback plan (one‑commit revertable).

## Non‑Goals (for this pass)
- Algorithmic changes to suppression itself.
- Performance tuning beyond obvious wins.
- File renames; adopt role names in docs first.

## Risks & Mitigations
- Drift between dashboards: eliminated by shared modules.
- Hidden path dependencies: centralized in `util.py`; smoke tests catch regressions.
- Large diffs: sequence PRs per ticket; keep changes surgical.

## Run Commands (reference)
- Viewer: `uv run --python .venv streamlit run carrier_dashboard_duckdb.py`
- Auto Suppressor: `uv run --python .venv streamlit run main.py`
- Simulator: `uv run --python .venv streamlit run carrier_suppression_dashboard.py`
- Smoke: `make smoke` / `make smoke-cube`
- Lint: `ruff check .` (after installing `ruff` in `.venv`).

## Unified SQL (Single Source of Truth)
Goal: one canonical set of SQL templates powering metrics, outliers, competitor views, and the cube.

- Structure: Standard CTE pipeline used everywhere
  - `ds`: parquet_scan(store_glob)
  - `filt`: ds filtered by ds, mover_ind, optional date range, and optional UI filters
  - `market`: per-day market totals (wins/losses)
  - `nat`: per-day winner totals; join to `market` for shares
  - `pair`: per-day winner×loser×dma aggregates (wins)
  - `dow_part`: CASE Sat/Sun/Weekday partitioning helper
  - Windows: DOW μ/σ with fallback policy documented (28→14 or 14 only)
- Outliers:
  - National outlier flag: z over DOW partition; explicit window and z-threshold params
  - Pair z and auxiliary flags (pct jump, rare/new)
- Data-quality gates:
  - Exclude NULL winner/loser/dma_name; enforce adjusted_wins > 0 before pair agg
  - Cube: restrict to national outlier days by default (switch optional)
- Implementation:
  - Templates in `suppression_tools/sql/` (Jinja2 or format strings)
  - Thin wrappers in `src/metrics.py` and `src/outliers.py` render+execute
  - No duplicate SQL in dashboards/builders
- Acceptance:
  - Dashboards and cube produce identical results to pre-unification on a pinned slice
  - Golden query tests compare output schemas and key aggregates (row counts, sums)
  - Smoke asserts: no NULL keys; pair_wins_current > 0; cube size within outlier-day bounds
- Tickets:
  1) Author templates: `national_timeseries.sql`, `pair_metrics.sql`, `nat_outliers.sql`, `competitor_view.sql`, `cube_outliers.sql`
  2) Add template renderer + param schema
  3) Wire Viewer → metrics/outliers
  4) Wire Simulator → metrics/outliers
  5) Wire cube builder → unified templates
  6) Golden tests + smoke extensions

## Vector Index (Jump Table)
- Data model: base pre-agg fields → see partition builder `partition_pre_agg_to_duckdb.py` (CTEs: base, rules_w/l, geo, enr, final)
- Store layout: `./duckdb_partitioned_store/**` (partitions: `ds`, `p_mover_ind`, `year`, `month`, `day`, `the_date`)
- Viewer logic: `carrier_dashboard_duckdb.py` (compute_national_pdf/compute_competitor_pdf)
- Simulator logic: `carrier_suppression_dashboard.py` (national + pair QA, proportional removal preview)
- Auto suppressor: `main.py` (base outliers, plan rows, stages: auto, distributed)
- Cube builder: `build_win_cube.py` (outlier‑day pair metrics)
- Shared helpers: `suppression_tools/src/plan.py` (base series, scan), `suppression_tools/src/util.py`
- Smoke tests: `tools/smoke_test_dashboard.py`
- Crosswalk: `ref/cb_cw_2020/**` (full DMA mapping)

## Data Model & Types
- Base pre‑agg parquet (input):
  - Keys: `the_date` (DATE/TIMESTAMP), `ds` (VARCHAR), `mover_ind` (BOOLEAN), `primary_geoid` (VARCHAR), `primary_sp_group` (INT/VARCHAR), `secondary_sp_group` (INT/VARCHAR)
  - Measures: `wins`, `losses`, optional `adjusted_wins`, `adjusted_losses` (DOUBLE)
- Display rules parquet: maps `sp_dim_id` → `sp_reporting_name_group` (winner/loser names)
- Crosswalk parquet: maps blockid (`serv_terr_blockid` or similar) → `dma`, `dma_name`, `state`
- Partitioned store (output): enriches with winner, loser, DMA; derived: `year`, `month`, `day`, `p_mover_ind`
- Quality gates (store): 0 NULL in winner/loser/dma_name; `the_date`, `ds`, `mover_ind` not NULL

## Core Metrics (Formulas)
- Market totals (per day): `T_wins = Σ adjusted_wins`; `T_losses = Σ adjusted_losses`
- National winner totals (per day, per winner): `W = Σ adjusted_wins`; `L = Σ adjusted_losses`
- Shares (per day):
  - `win_share = W / NULLIF(T_wins, 0)`
  - `loss_share = L / NULLIF(T_losses, 0)`
  - `wins_per_loss = W / NULLIF(L, 0)`
- Pair totals (per day, per winner×loser×dma): `pair_wins_current = Σ adjusted_wins`
- Day‑of‑week partition: `day_type = CASE strftime('%w', the_date) WHEN '6' THEN 'Sat' WHEN '0' THEN 'Sun' ELSE 'Weekday' END`

## Outlier Scoring
- Window policy: DOW‑partitioned rolling; prefer 28→14 fallback; current code uses 14 with configurable `window`
- National z: `z_nat = (win_share − μ_dow) / σ_dow` with guards (σ>0), else 0
- Pair z: `z_pair = (pair_wins_current − μ_dow_pair) / σ_dow_pair` (guards)
- Aux flags:
  - `pct_outlier_pos`: `pair_wins_current > 1.3 × μ_dow_pair`
  - `rare_pair`: `μ_dow_pair < 2.0`; `new_pair`: `μ_dow_pair IS NULL OR = 0`
- Thresholds (default): `z_nat > 2.5` for national day; `z_pair > 2.0` for pair spikes (used in auto stage)

## Cube Semantics (Fast UX)
- Scope: Only national outlier days (default) for `ds`, `mover_ind` (and optional view window)
- Rows: per `the_date, winner, loser, dma_name`
- Columns: pair metrics (wins_current, μ, σ, z, flags) + national metrics (W, T, share_current, μ, σ, z, window)
- Data quality: no NULL keys; `pair_wins_current > 0`

## Suppression Approach
- Stage 0 (Detection): identify national outlier days and candidate pairs (pair z / pct / rare/new)
- Stage 1 (Auto): remove excess at pair level where baseline exists or remove all when baseline tiny
  - Robust removal per row: `rm_pair = ceil(pair_wins_current)` if `μ_pair < 5` else `ceil(max(0, pair_wins_current − μ_pair))`
  - Greedy until national need satisfied: `need = ceil(max((W − μ*T) / max(1e−12, (1 − μ)), 0))`
- Stage 2 (Distributed): equalize remaining `need_after` across pairs with caps (≤ current) and allocate extras to largest residuals
- Simulator Preview: proportional removal within winner×loser×dma group by `adjusted_wins / group_wins` (never negative); recompute national series for comparison

## End‑to‑End Flow
1) Build partitioned store (`partition_pre_agg_to_duckdb.py`) with full crosswalk → 0 NULL keys
2) View current state (Market Viewer): filters, date window, comparisons
3) Auto Suppressor (main.py): scan base outliers → propose plan (auto + distributed)
4) Simulator: preview impact, iterate rounds, export CSVs (`./suppressions`)
5) Build suppressed dataset (`tools/build_suppressed_dataset.py`) for downstream visuals

## QA & Thresholds
- Store: winner/loser/dma_name NULL count == 0
- Cube: NULL keys == 0; `pair_wins_current > 0`
- Outlier days count sane vs window (document expectation per `ds`)
- Snapshot tests: fixed window + winners produce identical series pre/post refactor (within float tolerance)

## Performance Notes
- Use `parquet_scan` and CTEs; push filters early (`WHERE ds = ? AND mover_ind = ? AND date BETWEEN ...`)
- Avoid exploding joins; pre‑filter before pair aggregates; use partition pruning via store layout
- Prefer smaller cubes scoped to outlier days for interactivity

## Repro & Env
- uv env: `.venv` with duckdb, pandas, numpy, streamlit, plotly
- Run: `uv run --python .venv streamlit run <dashboard.py>`
- Smoke: `make smoke`, `make smoke-cube`
