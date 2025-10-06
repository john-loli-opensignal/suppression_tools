# Outlier Detection Differences: carrier_dashboard_duckdb.py vs main.py

## Investigation Date: 2025-10-06

## Summary

The two dashboards use **different outlier detection algorithms**, which explains why they show different results even with the same parameters.

---

## carrier_dashboard_duckdb.py

**Function**: `db.national_outliers_from_cube()`

### Characteristics:
- **Day Type Grouping**: 3 buckets (Saturday, Sunday, Weekday)
- **Fixed Window**: Uses whatever window you set (e.g., 14 days)
- **Rolling Calculation**: Within the specified window only
- **Simpler Logic**: Single window size, straightforward z-score calculation
- **Scope**: All carriers in the dataset

### SQL Logic:
```sql
CASE 
    WHEN day_of_week = 6 THEN 'Sat'
    WHEN day_of_week = 0 THEN 'Sun'
    ELSE 'Weekday'
END as day_type

AVG(win_share) OVER (
    PARTITION BY winner, day_type 
    ORDER BY the_date 
    ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING
) as mu
```

### Use Case:
- Quick exploratory analysis
- Simple outlier detection
- All carriers included
- Fast visualization

---

## main.py (Suppression Tool)

**Function**: `scan_base_outliers()`

### Characteristics:
- **DOW Partitioning**: 7 buckets (Sunday through Saturday)
- **Tiered Rolling Windows**: Tries 28d → 14d → 4d based on data availability
- **Entire Time Series**: Calculates rolling metrics from beginning, filters to window at end
- **Advanced Logic**: Adaptive window selection based on DOW and sample count
- **Scope**: Top N carriers + egregious outliers outside top N
- **Share Filtering**: Optional minimum share % threshold

### SQL Logic:
```sql
-- Self-join for DOW-partitioned rolling windows
LEFT JOIN national_daily hist
    ON curr.winner = hist.winner
    AND curr.dow = hist.dow  -- 1=Sunday, 7=Saturday
    AND hist.the_date < curr.the_date

-- Tiered selection
CASE 
    -- Try 28d first
    WHEN dow IN (1, 7) AND n_28d >= 2 THEN avg_28d  -- Weekend
    WHEN dow NOT IN (1, 7) AND n_28d >= 4 THEN avg_28d  -- Weekday
    -- Fall back to 14d
    WHEN dow IN (1, 7) AND n_14d >= 2 THEN avg_14d
    WHEN dow NOT IN (1, 7) AND n_14d >= 4 THEN avg_14d
    -- Fall back to 4d
    ...
END

-- Filter to graph window at the end
WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
    AND selected_window IS NOT NULL
```

### Use Case:
- Production suppression planning
- Focused on actionable carriers (top N)
- More robust to data sparsity (adaptive windows)
- Requires sufficient historical data

---

## Key Differences Table

| Feature | carrier_dashboard_duckdb.py | main.py |
|---------|----------------------------|---------|
| Day Grouping | 3 types (Sat/Sun/Weekday) | 7 types (DOW 1-7) |
| Window Strategy | Fixed single window | Tiered (28d/14d/4d) |
| Rolling Scope | Within window only | Entire time series |
| Carrier Filter | All carriers | Top N + egregious |
| Share Filter | None | Optional min % |
| Complexity | Simple, fast | Advanced, robust |
| Purpose | Exploration | Production suppression |

---

## Why They Show Different Outliers

### Scenario: 14-day window, z-score = 2.5

**carrier_dashboard_duckdb.py**:
- Groups by 3 day types
- Averages last 14 days of each day type
- Shows outliers for ALL carriers

**main.py**:
- Groups by 7 DOWs (more granular)
- May use 28-day or 4-day window depending on data availability
- Only shows top 25 carriers (default) + egregious outliers (impact > 40)
- Can filter by minimum share % (e.g., 0.5%)

### Example:
- **Carrier "XYZ"** might show as outlier in carrier_dashboard_duckdb.py but not main.py because:
  - It's not in top 25
  - Impact < 40
  - Or it's below minimum share threshold

---

## Recommendations

### For Exploration:
- **Use carrier_dashboard_duckdb.py**
  - Fast, simple outlier detection
  - See all carriers
  - Good for initial investigation

### For Production Suppression:
- **Use main.py**
  - More robust algorithm
  - Focused on actionable carriers
  - Handles data sparsity better
  - Tiered windows provide more context

### Alignment (if needed):
If you want them to match more closely:

1. **Option A**: Add top N filter to carrier_dashboard_duckdb.py
2. **Option B**: Use same day grouping in both (3 types vs 7)
3. **Option C**: Keep them different - they serve different purposes

**Recommended**: **Option C** - They're designed for different use cases.

---

## Current Status ✅

Both dashboards are working as designed. The differences are **intentional** and serve different analytical needs.

- carrier_dashboard_duckdb.py = **Exploratory tool**
- main.py = **Suppression production tool**
