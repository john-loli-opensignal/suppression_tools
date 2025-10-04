# DuckDB Database Guide

## Overview

This project now supports a **persistent DuckDB database** approach for storing and querying pre-aggregated carrier data. This is **significantly faster** than repeatedly scanning partitioned Parquet files, especially for interactive dashboards and frequent queries.

## Why Use DuckDB Database Instead of Partitioned Parquet?

### Performance Comparison

| Feature | Partitioned Parquet | DuckDB Database |
|---------|-------------------|-----------------|
| **First Query** | 2-5 seconds | 0.1-0.5 seconds |
| **Repeated Queries** | 2-5 seconds (each) | 0.05-0.2 seconds |
| **Aggregations** | Full scan required | Indexed lookups |
| **Storage Size** | 6-10 GB | 4-8 GB (compressed) |
| **Best For** | Batch processing | Interactive use |

### Key Advantages

1. **Persistent Indexes**: DuckDB maintains indexes across queries
2. **Query Optimization**: Statistics and query plans are cached
3. **Smaller Size**: Better compression for structured data
4. **Simpler Queries**: No need to manage partition paths
5. **Transaction Safety**: ACID compliance for updates

## Quick Start

### 1. Build the Database

```bash
# From pre-aggregated parquet file(s)
uv run build_suppression_db.py /path/to/preagg.parquet

# Custom output location
uv run build_suppression_db.py /path/to/preagg.parquet -o custom_name.db

# With custom reference data
uv run build_suppression_db.py /path/to/preagg.parquet \
    --rules /path/to/rules.parquet \
    --geo /path/to/geo.parquet
```

This creates `duck_suppression.db` in your current directory.

### 2. Query the Database

#### Using Python

```python
from suppression_tools import db

# Simple query
df = db.query("SELECT DISTINCT winner FROM carrier_data ORDER BY winner")

# With filters
carriers = db.get_distinct_values('winner')
dmas = db.get_distinct_values('dma_name', where="state = 'CA'")

# National timeseries with shares
df = db.get_national_timeseries(
    ds='gamoshi',
    mover_ind=False,
    start_date='2025-01-01',
    end_date='2025-12-31',
    state='CA'  # optional filter
)

# Get database statistics
stats = db.get_table_stats()
print(f"Rows: {stats['row_count']:,}")
print(f"Date range: {stats['min_date']} to {stats['max_date']}")
print(f"Carriers: {stats['distinct_winners']}")
```

#### Direct Connection

```python
import duckdb
from suppression_tools import db

con = db.connect()  # or db.connect('path/to/db.duckdb')
try:
    result = con.execute("""
        SELECT winner, SUM(adjusted_wins) as total_wins
        FROM carrier_data
        WHERE ds = 'gamoshi' AND the_date >= DATE '2025-01-01'
        GROUP BY winner
        ORDER BY total_wins DESC
        LIMIT 10
    """).df()
    print(result)
finally:
    con.close()
```

## Database Schema

### Main Table: `carrier_data`

| Column | Type | Description | Indexed |
|--------|------|-------------|---------|
| `the_date` | DATE | Date of observation | ✓ |
| `ds` | VARCHAR | Data source | ✓ |
| `mover_ind` | BOOLEAN | Mover indicator | ✓ |
| `winner` | VARCHAR | Winning carrier | ✓ |
| `loser` | VARCHAR | Losing carrier | ✓ |
| `dma` | INTEGER | DMA code | |
| `dma_name` | VARCHAR | DMA name | ✓ |
| `state` | VARCHAR | State | ✓ |
| `adjusted_wins` | DOUBLE | Adjusted win count | |
| `adjusted_losses` | DOUBLE | Adjusted loss count | |
| `year` | INTEGER | Year (derived) | ✓ |
| `month` | INTEGER | Month (derived) | ✓ |
| `day` | INTEGER | Day (derived) | |
| `day_of_week` | INTEGER | Day of week (0=Sun) | |

### Views

#### `national_daily`
Daily aggregates at national level by carrier

```sql
SELECT * FROM national_daily
WHERE ds = 'gamoshi' AND the_date >= DATE '2025-01-01'
ORDER BY the_date, winner;
```

#### `dma_daily`
Daily aggregates by DMA and carrier pair

```sql
SELECT * FROM dma_daily
WHERE state = 'CA' AND the_date = DATE '2025-01-15'
ORDER BY dma_name, winner, loser;
```

## Updating the Database

### Full Rebuild
```bash
# Overwrite existing database
uv run build_suppression_db.py /path/to/new_preagg.parquet --overwrite
```

### Incremental Updates (Advanced)
```python
import duckdb
from suppression_tools import db

# Connect in read-write mode
con = db.connect(read_only=False)

# Insert new data
con.execute("""
    INSERT INTO carrier_data
    SELECT ... FROM parquet_scan('/path/to/new_data.parquet')
    WHERE the_date > (SELECT MAX(the_date) FROM carrier_data)
""")

# Re-analyze for optimal query plans
con.execute("ANALYZE carrier_data")
con.close()
```

## Performance Tips

1. **Use Filters Early**: Push filters into WHERE clauses
   ```sql
   -- Good: Filter is applied during scan
   SELECT * FROM carrier_data 
   WHERE ds = 'gamoshi' AND the_date >= DATE '2025-01-01'
   
   -- Bad: Full scan then filter
   SELECT * FROM (SELECT * FROM carrier_data) 
   WHERE ds = 'gamoshi'
   ```

2. **Use Indexed Columns**: Filter on indexed columns for faster lookups
   - Indexed: `ds`, `mover_ind`, `the_date`, `winner`, `loser`, `dma_name`, `state`, `year`, `month`

3. **Aggregate Efficiently**: Use GROUP BY with indexed columns
   ```sql
   SELECT the_date, winner, SUM(adjusted_wins)
   FROM carrier_data
   WHERE ds = 'gamoshi'
   GROUP BY the_date, winner  -- both indexed
   ```

4. **Reuse Connections**: Create one connection and execute multiple queries
   ```python
   con = db.connect()
   try:
       df1 = con.execute(query1).df()
       df2 = con.execute(query2).df()
   finally:
       con.close()
   ```

## Migration from Partitioned Parquet

### Option 1: Dual Mode (Recommended)
Keep both formats during transition:
```python
# Try database first, fall back to parquet
try:
    df = db.query(my_query)
except FileNotFoundError:
    df = query_partitioned_parquet(parquet_glob)
```

### Option 2: Update Existing Code
Replace parquet scans with database queries:

**Before:**
```python
con = duckdb.connect()
df = con.execute(f"""
    SELECT * FROM parquet_scan('{glob_pattern}')
    WHERE ds = 'gamoshi'
""").df()
```

**After:**
```python
from suppression_tools import db
df = db.query("SELECT * FROM carrier_data WHERE ds = 'gamoshi'")
```

## Troubleshooting

### Database Not Found
```python
FileNotFoundError: Database not found: duck_suppression.db
```
**Solution**: Run `uv run build_suppression_db.py <preagg.parquet>` to create it

### Out of Memory
```
Error: Out of Memory
```
**Solution**: Increase memory limit in your script:
```python
con = db.connect(read_only=False)
con.execute("PRAGMA memory_limit = '8GB'")
```

### Slow Queries
**Solution**: Make sure database was built with indexes:
```bash
uv run build_suppression_db.py <preagg.parquet> --optimize
```

Or manually rebuild statistics:
```python
con = db.connect(read_only=False)
con.execute("ANALYZE carrier_data")
```

## Cleanup and Temporary Files

The system automatically cleans up temporary database files on exit. For manual cleanup:

```python
from suppression_tools import db
import os

# Get database size
size_info = db.get_db_size()
print(f"Database size: {size_info['size_mb']:.2f} MB")

# Remove database (be careful!)
if os.path.exists('duck_suppression.db'):
    os.remove('duck_suppression.db')
```

## Best Practices

1. **Version Control**: Don't commit database files (they're in `.gitignore`)
2. **Regenerate**: Build database from source parquet files as needed
3. **Read-Only**: Use `read_only=True` (default) for query operations
4. **Backup Source**: Keep original parquet files as source of truth
5. **Document**: Update queries and add comments for complex logic

## Example Workflows

### Dashboard Startup
```python
from suppression_tools import db
import streamlit as st

@st.cache_resource
def get_db_connection():
    """Cached database connection"""
    return db.connect()

@st.cache_data
def load_carriers():
    """Cached carrier list"""
    return db.get_distinct_values('winner')

# Use in dashboard
carriers = load_carriers()
con = get_db_connection()
```

### Batch Analysis
```python
from suppression_tools import db
import pandas as pd

# Analyze multiple carriers
carriers = ['AT&T', 'Verizon', 'T-Mobile']
results = []

con = db.connect()
try:
    for carrier in carriers:
        df = con.execute(f"""
            SELECT the_date, SUM(adjusted_wins) as wins
            FROM carrier_data
            WHERE winner = '{carrier}' AND ds = 'gamoshi'
            GROUP BY the_date
            ORDER BY the_date
        """).df()
        df['carrier'] = carrier
        results.append(df)
finally:
    con.close()

combined = pd.concat(results, ignore_index=True)
```

## Additional Resources

- DuckDB Documentation: https://duckdb.org/docs/
- SQL Reference: https://duckdb.org/docs/sql/introduction
- Python API: https://duckdb.org/docs/api/python/overview
