# Suppression Tools

## Overview

A comprehensive toolset for detecting and suppressing data outliers in telecom win/loss data using a hierarchical, census block-level approach.

### Key Features
- **Multi-level outlier detection:** National → H2H pairs → State → DMA → Census Block
- **DuckDB-powered analytics:** Fast queries on 6-10GB datasets using pre-aggregated cube tables
- **DOW-aware analysis:** Day-of-week partitioning to avoid false positives
- **Surgical precision:** Census block-level suppression preserves legitimate data
- **Interactive dashboards:** Real-time analysis and visualization

## Project Structure

```
suppression_tools/
├── carrier_dashboard_duckdb.py        # Main dashboard for win/loss analysis
├── carrier_suppression_dashboard.py   # Legacy suppression dashboard
├── census_block_outlier_dashboard.py  # Census block analysis dashboard
├── main.py                            # Original suppression tools UI
│
├── tools/                             # Core library (formerly suppression_tools)
│   ├── db.py                         # DuckDB connection & query utilities
│   ├── src/
│   │   ├── metrics.py                # Metrics computation (national, H2H, etc.)
│   │   ├── outliers.py               # Outlier detection algorithms
│   │   ├── plan.py                   # Suppression plan builders
│   │   └── util.py                   # Helper utilities
│   └── sql/                          # SQL templates
│
├── scripts/
│   ├── build/                        # Database & cube building scripts
│   │   ├── build_suppression_db.py   # Load pre-agg data into DuckDB
│   │   ├── build_cubes_in_db.py      # Create aggregated cube tables
│   │   ├── build_census_block_cubes.py  # Census block cube tables
│   │   └── partition_pre_agg_to_duckdb.py  # Partition & load data
│   ├── analysis/                     # Analysis & testing scripts
│   │   ├── auto_suppression.py       # Automated suppression pipeline
│   │   └── regenerate_overlay_graphs.py  # Graph generation
│   └── legacy/                       # Deprecated scripts
│
├── data/
│   └── databases/
│       └── duck_suppression.db       # Main DuckDB database
│
├── analysis_results/                 # Analysis outputs
│   ├── census_block/                # Census block analysis results
│   └── suppression/                 # Suppression analysis results
│
├── docs/                            # Documentation
│   ├── QUICKSTART_DB.md             # Getting started guide
│   ├── DATABASE_GUIDE.md            # Database structure & usage
│   ├── CUBES_GUIDE.md               # Cube tables reference
│   ├── OUTLIER_METHODS.md           # Outlier detection methods
│   ├── REMOVE_OUTLIERS.md           # Suppression analysis report
│   └── MIGRATION_GUIDE.md           # Migration from parquet to DuckDB
│
├── suppressions/                    # Suppression plans & configs
│   └── rounds/                      # Round-specific configs
│
├── tests/                           # Test suite
└── ref/                             # Reference data
```

## Quick Start

### 1. Build the Database

```bash
# Load pre-aggregated data into DuckDB
uv run scripts/build/build_suppression_db.py --input /path/to/preagg/data

# Build cube tables for a dataset
uv run scripts/build/build_cubes_in_db.py --ds gamoshi

# Or build cubes for all datasets
uv run scripts/build/build_cubes_in_db.py --all
```

### 2. Launch Dashboards

```bash
# Main carrier analysis dashboard (recommended)
streamlit run carrier_dashboard_duckdb.py

# Census block outlier analysis
streamlit run census_block_outlier_dashboard.py

# Legacy suppression tools
streamlit run main.py
```

### 3. Run Analysis

```bash
# Automated suppression pipeline
uv run scripts/analysis/auto_suppression.py --ds gamoshi --dates 2025-08-15 2025-08-16
```

## Database Structure

The DuckDB database (`data/databases/duck_suppression.db`) contains:

### Raw Tables
- `{dataset}_wins_raw` - Raw win data with census block IDs
- `{dataset}_losses_raw` - Raw loss data with census block IDs

### Cube Tables (Pre-aggregated)
- `{dataset}_win_mover_cube` - Movers wins aggregated by date/state/DMA/winner/loser
- `{dataset}_win_non_mover_cube` - Non-movers wins
- `{dataset}_loss_mover_cube` - Movers losses
- `{dataset}_loss_non_mover_cube` - Non-movers losses
- `{dataset}_win_mover_cb_cube` - Census block-level movers wins
- `{dataset}_win_non_mover_cb_cube` - Census block-level non-movers wins

**Performance:** Cube tables enable sub-second queries on 6-10GB datasets.

## Outlier Detection Methods

### 1. National-Level (DOW-Aware)
- Rolling 14-day window grouped by day-of-week
- Z-score threshold: 2.5
- Detects carriers with abnormal win share

### 2. H2H Pair-Level
- Analyzes winner-loser-DMA triplets
- Z-score threshold: 2.0
- Flags new pairs, rare pairs, percentage spikes

### 3. Census Block-Level
- Surgical precision suppression
- Multiple criteria:
  - Z-score > 3.0
  - Spike ratio > 5.0x baseline
  - First appearance (high-volume blocks)
  - Concentration > 80% of DMA total

## Typical Workflow

1. **Build Database** → Load raw data and create cube tables
2. **National Analysis** → Identify carriers with suspicious activity
3. **H2H Drill-Down** → Find specific winner-loser pairs
4. **Census Block Analysis** → Pinpoint exact locations
5. **Generate Suppression Plan** → Create list of records to suppress
6. **Apply & Validate** → Suppress outliers, verify impact

## Configuration

Database path can be configured via:
- Default: `data/databases/duck_suppression.db`
- Override in dashboards via sidebar
- Set via `--db` flag in CLI scripts

## Development

### Running Tests
```bash
uv run pytest tests/
```

### Adding a New Dataset
```bash
# 1. Load data
uv run scripts/build/build_suppression_db.py --input /path/to/data --dataset mydataset

# 2. Build cubes
uv run scripts/build/build_cubes_in_db.py --ds mydataset

# 3. Verify in dashboard
streamlit run carrier_dashboard_duckdb.py
```

## Documentation

- **[Quickstart Guide](docs/QUICKSTART_DB.md)** - Get up and running
- **[Database Guide](docs/DATABASE_GUIDE.md)** - Schema and querying
- **[Cubes Guide](docs/CUBES_GUIDE.md)** - Cube table reference
- **[Outlier Methods](docs/OUTLIER_METHODS.md)** - Detection algorithms
- **[Suppression Analysis](docs/REMOVE_OUTLIERS.md)** - Case study results

## Notes

- Always work on the `codex-agent` branch for development
- Use conventional commit messages for easy reverts
- Database file is ~6-10GB depending on dataset size
- Cube tables significantly speed up queries (100x+ faster)
- Census block analysis enables surgical precision suppression

## TODO

- [ ] Implement real-time monitoring dashboard
- [ ] Add ML-based anomaly prediction
- [ ] Create feedback loop for suppression refinement
- [ ] Automated daily suppression pipeline
- [ ] API for programmatic access
