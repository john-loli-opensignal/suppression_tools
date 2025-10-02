# Pre-Aggregated Cubes Guide

## Overview

Pre-aggregated cubes are **extremely fast** to query because they're already grouped by all indexed dimensions. Think of them as materialized views optimized for specific metrics.

## Cube Structure

Each dataset (ds) produces **4 cubes**:

```
{ds}_win_mover_cube.parquet       # Wins for movers
{ds}_win_non_mover_cube.parquet   # Wins for non-movers  
{ds}_loss_mover_cube.parquet      # Losses for movers
{ds}_loss_non_mover_cube.parquet  # Losses for non-movers
```

## Aggregation Dimensions

Each cube is pre-aggregated on **all indexed columns**:

- **Time**: `the_date`, `year`, `month`, `day`, `day_of_week`
- **Carriers**: `winner`, `loser`
- **Geography**: `dma`, `dma_name`, `state`

**Metric columns**:
- `total_wins` or `total_losses` - sum of adjusted values
- `record_count` - number of source records aggregated

## Building Cubes

### Build for Default Dataset (gamoshi)
```bash
uv run build_cubes_from_db.py
```

### Build for All Datasets
```bash
uv run build_cubes_from_db.py --all
```

### Custom Options
```bash
# Specific dataset
uv run build_cubes_from_db.py --ds gamoshi

# Custom output directory
uv run build_cubes_from_db.py -o my_cubes/

# Skip existing cubes (incremental)
uv run build_cubes_from_db.py --skip-existing
```

### List Available Datasets
```bash
uv run build_cubes_from_db.py --list-datasets
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

- **Database**: ~600-800 MB (all raw data)
- **Win Mover Cube**: ~5-15 MB
- **Win Non-Mover Cube**: ~20-40 MB  
- **Loss Mover Cube**: ~5-15 MB
- **Loss Non-Mover Cube**: ~20-40 MB
- **Total Cubes**: ~50-120 MB (**10-15x smaller**)

## Querying Cubes

### Direct with DuckDB

```python
import duckdb

# National wins per day for AT&T
con = duckdb.connect()
df = con.execute("""
    SELECT the_date, SUM(total_wins) as daily_wins
    FROM 'cubes/gamoshi_win_non_mover_cube.parquet'
    WHERE winner = 'AT&T'
    GROUP BY the_date
    ORDER BY the_date
""").df()
con.close()
```

### With Pandas

```python
import pandas as pd

# Load cube
df = pd.read_parquet('cubes/gamoshi_win_non_mover_cube.parquet')

# Filter and aggregate
att_wins = df[df['winner'] == 'AT&T'].groupby('the_date')['total_wins'].sum()
```

## Common Use Cases

### 1. National Daily Wins by Carrier

```python
import duckdb

con = duckdb.connect()
df = con.execute("""
    SELECT 
        the_date,
        winner,
        SUM(total_wins) as national_wins
    FROM 'cubes/gamoshi_win_non_mover_cube.parquet'
    WHERE the_date >= DATE '2025-01-01'
    GROUP BY the_date, winner
    ORDER BY the_date, winner
""").df()
```

### 2. Win Share by Carrier

```python
con = duckdb.connect()
df = con.execute("""
    WITH daily_totals AS (
        SELECT 
            the_date,
            winner,
            SUM(total_wins) as wins
        FROM 'cubes/gamoshi_win_non_mover_cube.parquet'
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
""").df()
```

### 3. State-Level Aggregation

```python
con = duckdb.connect()
df = con.execute("""
    SELECT 
        state,
        winner,
        SUM(total_wins) as total_wins
    FROM 'cubes/gamoshi_win_non_mover_cube.parquet'
    WHERE state IN ('CA', 'TX', 'FL', 'NY')
    GROUP BY state, winner
    ORDER BY state, total_wins DESC
""").df()
```

### 4. Head-to-Head by DMA

```python
con = duckdb.connect()
df = con.execute("""
    SELECT 
        dma_name,
        state,
        SUM(total_wins) as att_wins_vs_vzw
    FROM 'cubes/gamoshi_win_non_mover_cube.parquet'
    WHERE winner = 'AT&T' AND loser = 'Verizon'
    GROUP BY dma_name, state
    ORDER BY att_wins_vs_vzw DESC
    LIMIT 20
""").df()
```

### 5. Day of Week Patterns

```python
con = duckdb.connect()
df = con.execute("""
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
        FROM 'cubes/gamoshi_win_non_mover_cube.parquet'
        GROUP BY day_of_week, the_date, winner
    )
    GROUP BY day_of_week, winner
    ORDER BY day_of_week, winner
""").df()
```

## When to Use Cubes vs Database

### Use Cubes When:
- ✅ You need repeated queries on the same dimensions
- ✅ You're building dashboards with multiple views
- ✅ You want maximum query speed (<10ms)
- ✅ You're analyzing specific metric types (wins or losses)
- ✅ Your queries filter by indexed dimensions

### Use Database Directly When:
- ✅ You need the raw, unaggregated data
- ✅ You need dimensions not in the cube (e.g., `primary_geoid`)
- ✅ You're doing one-off exploratory queries
- ✅ You need to join with other tables/data

## Workflow

```
Raw Parquet 
    ↓
    uv run build_suppression_db.py
    ↓
DuckDB Database (duck_suppression.db)
    ↓
    uv run build_cubes_from_db.py
    ↓
Pre-aggregated Cubes (cubes/*.parquet)
    ↓
Fast Queries & Dashboards
```

## Rebuilding Cubes

Rebuild cubes whenever:
- Database is updated with new data
- You need cubes for a new dataset
- Cube schema changes

```bash
# Rebuild all cubes
uv run build_cubes_from_db.py --all

# Or rebuild just one dataset
uv run build_cubes_from_db.py --ds gamoshi
```

## Best Practices

1. **Keep cubes fresh**: Rebuild after database updates
2. **Use appropriate cube**: Win vs Loss cubes for different analyses
3. **Mover segmentation**: Use mover/non-mover cubes for different behaviors
4. **Combine cubes**: UNION queries when you need both movers and non-movers
5. **Cache results**: For dashboards, cache frequently-used cube queries

## Troubleshooting

### Cube File Not Found
```
FileNotFoundError: cubes/gamoshi_win_non_mover_cube.parquet
```
**Solution**: Run `uv run build_cubes_from_db.py` to generate cubes

### Cube Data Looks Stale
**Solution**: Rebuild cubes after database updates
```bash
uv run build_cubes_from_db.py --ds gamoshi
```

### Out of Memory Loading Cube
**Solution**: Query the cube with DuckDB instead of loading into Pandas
```python
# Instead of: df = pd.read_parquet('cube.parquet')
# Use:
con = duckdb.connect()
df = con.execute("SELECT * FROM 'cube.parquet' WHERE ...").df()
```

## Example: Complete Analysis Pipeline

```python
import duckdb

# Connect once
con = duckdb.connect()

# 1. National trend
national = con.execute("""
    SELECT the_date, winner, SUM(total_wins) as wins
    FROM 'cubes/gamoshi_win_non_mover_cube.parquet'
    GROUP BY the_date, winner
""").df()

# 2. State breakdown
states = con.execute("""
    SELECT state, winner, SUM(total_wins) as wins
    FROM 'cubes/gamoshi_win_non_mover_cube.parquet'
    GROUP BY state, winner
""").df()

# 3. Head-to-head
h2h = con.execute("""
    SELECT 
        the_date,
        SUM(total_wins) as att_wins
    FROM 'cubes/gamoshi_win_non_mover_cube.parquet'
    WHERE winner = 'AT&T' AND loser = 'Verizon'
    GROUP BY the_date
""").df()

con.close()

# Now you have 3 DataFrames ready for visualization!
```

## Additional Resources

- [DATABASE_GUIDE.md](./DATABASE_GUIDE.md) - Database setup and querying
- [QUICKSTART_DB.md](./QUICKSTART_DB.md) - Quick reference for database
- DuckDB Parquet: https://duckdb.org/docs/data/parquet.html
