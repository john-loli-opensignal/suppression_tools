# Cleanup Plan

## Files/Directories to Remove

### 1. Temporary Directories
- `temp_output_dir/` - empty temp directory
- `current_run_duckdb/` - old CSV files (385MB), replaced by DuckDB tables

### 2. Old CSV cube files
- All files in `current_run_duckdb/` (win_cube_*.csv)

### 3. Obsolete Scripts (One-off analysis, already executed)
- `analyze_cb_new_appearances_and_suppression.py` - one-off analysis
- `analyze_census_block_anomalies.py` - one-off analysis
- `detect_census_block_anomalies.py` - one-off analysis
- `generate_anomaly_report.py` - one-off analysis
- `visualize_census_block_anomalies.py` - one-off analysis
- `visualize_surgical_suppression.py` - one-off analysis
- `create_overlay_visualizations.py` - one-off, created graphs already
- `test_census_block_performance.py` - POC test
- `test_suppression_approach.py` - one-off test
- `build_win_cube.py` - replaced by build_cubes_in_db.py

### 4. Old PR text files (archive)
- `PR_T5.txt`
- `PR_T6.txt`
- `PR_T7.txt`

### 5. Python cache
- `__pycache__/`

### 6. IDE config (should be in .gitignore)
- `.idea/`

## Files to KEEP

### Core Tools
- `build_cubes_in_db.py` - core cube building
- `build_census_block_cubes.py` - census block cube builder
- `build_suppression_db.py` - DB initialization
- `partition_pre_agg_to_duckdb.py` - data loading
- `auto_suppression.py` - suppression tool
- `main.py` - main entry point

### Dashboards
- `carrier_dashboard_duckdb.py` - main dashboard
- `carrier_suppression_dashboard.py` - suppression dashboard
- `census_block_outlier_dashboard.py` - census block dashboard

### Core Modules
- `suppression_tools/` - package
- `tests/` - test suite
- `tools/` - utilities

### Documentation (consolidate later)
- All .md files (will consolidate in separate task)

### Data
- `duck_suppression.db` - main database
- `suppressions/` - suppression data
- `census_block_analysis_results/` - analysis results (keep for reference)
- `suppression_analysis_results/` - recent analysis results

