# Pre-Aggregated Cubes Guide

## Overview

Pre-aggregated cubes are **extremely fast** to query because they're already grouped by all indexed dimensions. They live **inside the DuckDB database** as tables, not separate files.

## Cube Structure

Each dataset (ds) produces **4 cube tables** inside the database:

```
{ds}_win_mover_cube       # Wins for movers
{ds}_win_non_mover_cube   # Wins for non-movers  
{ds}_loss_mover_cube      # Losses for movers
{ds}_loss_non_mover_cube  # Losses for non-movers
```

**Benefits of tables vs. separate files:**
- ✅ All in one database file (no extra files)
- ✅ ACID transactions (atomic updates)
- ✅ Incremental updates possible
- ✅ DuckDB can optimize queries across tables
- ✅ Still extremely fast with indexes

## Aggregation Dimensions

Each cube is pre-aggregated on **all indexed columns**:

- **Time**: `the_date`, `year`, `month`, `day`, `day_of_week`
- **Carriers**: `winner`, `loser`
- **Geography**: `dma`, `dma_name`, `state`

**Metric columns**:
- `total_wins` or `total_losses` - sum of adjusted values
- `record_count` - number of source records aggregated

## Building Cubes

### Build Cube Tables in Database (Recommended)
```bash
# Build for default dataset (gamoshi)
uv run build_cubes_in_db.py

# Build for all datasets
uv run build_cubes_in_db.py --all

# List existing cube tables
uv run build_cubes_in_db.py --list

# Skip existing tables (incremental)
uv run build_cubes_in_db.py --skip-existing
```

### Alternative: Build as Parquet Files (Legacy)
```bash
# If you need portable cube files
uv run build_cubes_from_db.py
```

## Performance

### Speed Comparison

| Operation | Direct DB Query | Cube Query | Improvement |
|-----------|----------------|------------|------------:|
| National aggregation | 0.5-1.0 sec | 0.01-0.05 sec | **20-50x** |
| State aggregation | 0.3-0.5 sec | 0.005-0.01 sec | **50-100x** |
| DMA aggregation | 0.2-0.3 sec | 0.003-0.008 sec | **60-100x** |
| Time series slicing | 0.1-0.2 sec | 0.002-0.005 sec | **50x** |

### Size Comparison

For typical dataset (30-60 days, ~50 carriers, ~200 DMAs):

- **Database (raw data)**: ~600-800 MB
- **Database with 4 cube tables**: ~650-900 MB (**~50-100MB overhead**)
- **Alternative: 4 separate parquet files**: ~50-120 MB (but requires managing separate files)

## Querying Cubes

### Using Helper Functions (Easiest)

```python
from suppression_tools import db

# Get national daily wins for non-movers
df = db.get_national_from_cube(
    ds='gamoshi',
    metric='win',
    mover_ind=False,
    start_date='2025-01-01',
    end_date='2025-12-31'
)

# Query cube with custom filter
df = db.query_cube(
    ds='gamoshi',
    metric='win',
    mover_ind=False,
    sql_filter="winner = 'AT&T' AND state = 'CA'"
)

# List all cube tables
cubes = db.list_cube_tables()
print(cubes)
```

### Direct SQL Queries

```python
from suppression_tools import db

# National wins per day for AT&T
df = db.query("""
    SELECT the_date, SUM(total_wins) as daily_wins
    FROM gamoshi_win_non_mover_cube
    WHERE winner = 'AT&T'
    GROUP BY the_date
    ORDER BY the_date
""")
```

### With DuckDB Connection

```python
from suppression_tools import db

con = db.connect()
try:
    df = con.execute("""
        SELECT the_date, winner, SUM(total_wins) as wins
        FROM gamoshi_win_non_mover_cube
        GROUP BY the_date, winner
    """).df()
finally:
    con.close()
```

## Common Use Cases

### 1. National Daily Wins by Carrier

```python
from suppression_tools import db

df = db.query("""
    SELECT 
        the_date,
        winner,
        SUM(total_wins) as national_wins
    FROM gamoshi_win_non_mover_cube
    WHERE the_date >= DATE '2025-01-01'
    GROUP BY the_date, winner
    ORDER BY the_date, winner
""")
```

### 2. Win Share by Carrier

```python
from suppression_tools import db

df = db.query("""
    WITH daily_totals AS (
        SELECT 
            the_date,
            winner,
            SUM(total_wins) as wins
        FROM gamoshi_win_non_mover_cube
        GROUP BY the_date, winner
    ),
    market_totals AS (
        SELECT 
            the_date,
            SUM(wins) as market_total
        FROM daily_totals
        GROUP BY the_date
    )
    SELECT 
        d.the_date,
        d.winner,
        d.wins,
        m.market_total,
        d.wins / m.market_total as win_share
    FROM daily_totals d
    JOIN market_totals m USING (the_date)
    ORDER BY the_date, winner
""")
```

### 3. State-Level Aggregation

```python
from suppression_tools import db

df = db.query("""
    SELECT 
        state,
        winner,
        SUM(total_wins) as total_wins
    FROM gamoshi_win_non_mover_cube
    WHERE state IN ('CA', 'TX', 'FL', 'NY')
    GROUP BY state, winner
    ORDER BY state, total_wins DESC
""")
```

### 4. Head-to-Head by DMA

```python
from suppression_tools import db

df = db.query("""
    SELECT 
        dma_name,
        state,
        SUM(total_wins) as att_wins_vs_vzw
    FROM gamoshi_win_non_mover_cube
    WHERE winner = 'AT&T' AND loser = 'Verizon'
    GROUP BY dma_name, state
    ORDER BY att_wins_vs_vzw DESC
    LIMIT 20
""")
```

### 5. Day of Week Patterns

```python
from suppression_tools import db

df = db.query("""
    SELECT 
        day_of_week,
        CASE day_of_week
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END as day_name,
        winner,
        AVG(daily_wins) as avg_wins
    FROM (
        SELECT 
            day_of_week,
            the_date,
            winner,
            SUM(total_wins) as daily_wins
        FROM gamoshi_win_non_mover_cube
        GROUP BY day_of_week, the_date, winner
    )
    GROUP BY day_of_week, winner
    ORDER BY day_of_week, winner
""")
```

## When to Use Cubes vs Database

### Use Cube Tables When:
- ✅ You need repeated queries on the same dimensions
- ✅ You're building dashboards with multiple views
- ✅ You want maximum query speed (<10ms)
- ✅ You're analyzing specific metric types (wins or losses)
- ✅ Your queries filter by indexed dimensions
- ✅ You want everything in one database file

### Use Raw carrier_data Table When:
- ✅ You need the raw, unaggregated data
- ✅ You need dimensions not in the cube (e.g., `primary_geoid`)
- ✅ You're doing one-off exploratory queries
- ✅ You need to join with other tables/data
- ✅ You need record-level detail

## Workflow

```
Raw Parquet 
    ↓
    uv run build_suppression_db.py
    ↓
DuckDB Database (duck_suppression.db)
  ├─ carrier_data table (raw data)
  └─ (optional) cube tables
         ↓
         uv run build_cubes_in_db.py
         ↓
  ├─ {ds}_win_mover_cube
  ├─ {ds}_win_non_mover_cube
  ├─ {ds}_loss_mover_cube
  └─ {ds}_loss_non_mover_cube
         ↓
Fast Queries & Dashboards
```

## Rebuilding Cubes

Rebuild cube tables whenever:
- Database is updated with new data
- You need cubes for a new dataset
- Cube schema changes

```bash
# Rebuild all cube tables
uv run build_cubes_in_db.py --all

# Or rebuild just one dataset
uv run build_cubes_in_db.py --ds gamoshi

# Check what cube tables exist
uv run build_cubes_in_db.py --list
```

## Best Practices

1. **Keep cubes fresh**: Rebuild after database updates
2. **Use appropriate cube**: Win vs Loss cubes for different analyses
3. **Mover segmentation**: Use mover/non-mover cubes for different behaviors
4. **Combine cubes**: UNION queries when you need both movers and non-movers
5. **Cache results**: For dashboards, cache frequently-used cube queries

## Troubleshooting

### Cube Table Not Found
```
Table gamoshi_win_non_mover_cube does not exist
```
**Solution**: Run `uv run build_cubes_in_db.py` to generate cube tables

### Cube Data Looks Stale
**Solution**: Rebuild cube tables after database updates
```bash
uv run build_cubes_in_db.py --ds gamoshi
```

### Want to See All Cubes
```bash
uv run build_cubes_in_db.py --list
```

## Example: Complete Analysis Pipeline

```python
from suppression_tools import db

# 1. National trend
national = db.query("""
    SELECT the_date, winner, SUM(total_wins) as wins
    FROM gamoshi_win_non_mover_cube
    GROUP BY the_date, winner
""")

# 2. State breakdown
states = db.query("""
    SELECT state, winner, SUM(total_wins) as wins
    FROM gamoshi_win_non_mover_cube
    GROUP BY state, winner
""")

# 3. Head-to-head
h2h = db.query("""
    SELECT 
        the_date,
        SUM(total_wins) as att_wins
    FROM gamoshi_win_non_mover_cube
    WHERE winner = 'AT&T' AND loser = 'Verizon'
    GROUP BY the_date
""")

# Now you have 3 DataFrames ready for visualization!
```

## Additional Resources

- [DATABASE_GUIDE.md](./DATABASE_GUIDE.md) - Database setup and querying
- [QUICKSTART_DB.md](./QUICKSTART_DB.md) - Quick reference for database
- DuckDB Parquet: https://duckdb.org/docs/data/parquet.html
