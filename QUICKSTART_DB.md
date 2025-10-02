# Quick Start: DuckDB Database

## TL;DR

**DuckDB database is 5-10x faster than partitioned Parquet for repeated queries.**

## Build Once

```bash
# Build the database from your pre-agg parquet
uv run build_suppression_db.py /path/to/preagg.parquet
```

This creates `duck_suppression.db` (~4-8GB, compressed).

## Query Often

### In Python Scripts

```python
from suppression_tools import db

# Simple query
carriers = db.query("SELECT DISTINCT winner FROM carrier_data ORDER BY winner")

# Get distinct values
winners = db.get_distinct_values('winner')
dmas = db.get_distinct_values('dma_name', where="state = 'CA'")

# National timeseries with shares
df = db.get_national_timeseries(
    ds='gamoshi',
    mover_ind=False,
    start_date='2025-01-01',
    end_date='2025-12-31'
)
```

### In Dashboards (Streamlit)

```python
from suppression_tools import db
import streamlit as st

@st.cache_data
def load_data(filters):
    sql = """
        SELECT the_date, winner, SUM(adjusted_wins) as wins
        FROM carrier_data
        WHERE ds = ? AND mover_ind = ?
        GROUP BY the_date, winner
    """
    return db.query(sql, params={'ds': filters['ds'], 'mover_ind': filters['mover']})

# Use it
df = load_data({'ds': 'gamoshi', 'mover': False})
```

## Why Use This?

| Metric | Partitioned Parquet | DuckDB Database |
|--------|-------------------|-----------------|
| First query | 2-5 seconds | 0.1-0.5 seconds |
| Repeated queries | 2-5 seconds | 0.05-0.2 seconds |
| File size | 6-10 GB | 4-8 GB |

## Main Table: `carrier_data`

Key columns (all indexed for fast filtering):
- `the_date`: DATE - observation date
- `ds`: VARCHAR - data source
- `mover_ind`: BOOLEAN - mover indicator
- `winner`, `loser`: VARCHAR - carrier names
- `dma_name`: VARCHAR - DMA name
- `state`: VARCHAR - state code
- `adjusted_wins`, `adjusted_losses`: DOUBLE - metrics
- `year`, `month`: INTEGER - date partitions

## Common Queries

### Get all carriers
```sql
SELECT DISTINCT winner FROM carrier_data ORDER BY winner;
```

### Daily wins by carrier
```sql
SELECT the_date, winner, SUM(adjusted_wins) as total_wins
FROM carrier_data
WHERE ds = 'gamoshi' AND the_date >= DATE '2025-01-01'
GROUP BY the_date, winner
ORDER BY the_date, winner;
```

### Top DMAs for a carrier
```sql
SELECT dma_name, SUM(adjusted_wins) as total_wins
FROM carrier_data
WHERE winner = 'AT&T' AND ds = 'gamoshi'
GROUP BY dma_name
ORDER BY total_wins DESC
LIMIT 10;
```

### Head-to-head comparison
```sql
SELECT the_date, SUM(adjusted_wins) as wins
FROM carrier_data
WHERE winner = 'AT&T' AND loser = 'Verizon'
  AND ds = 'gamoshi' AND mover_ind = FALSE
GROUP BY the_date
ORDER BY the_date;
```

## Rebuild When Data Updates

```bash
# Replace existing database with new data
uv run build_suppression_db.py /path/to/new_preagg.parquet
```

## Full Documentation

See [DATABASE_GUIDE.md](./DATABASE_GUIDE.md) for:
- Complete schema reference
- Performance optimization tips
- Migration from partitioned parquet
- Troubleshooting guide
- Advanced usage patterns
