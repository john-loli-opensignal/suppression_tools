# Database Consolidation Summary

## Problem Identified
The project had **3 duplicate database files** in different locations:
1. `/home/jloli/codebase-comparison/suppression_tools/duck_suppression.db` (EMPTY)
2. `/home/jloli/codebase-comparison/suppression_tools/data/duck_suppression.db` (EMPTY)
3. `/home/jloli/codebase-comparison/suppression_tools/data/databases/duck_suppression.db` (ACTIVE - contains all cube tables)

This caused repeated issues where views and tables were being created in the wrong database.

## Solution Implemented

### 1. Database Consolidation
- **Deleted** the two empty database files at root and `data/` level
- **Kept** the single database at: `data/databases/duck_suppression.db`
- **Verified** this database contains all 12 cube tables:
  - `gamoshi_win_mover_cube`, `gamoshi_win_non_mover_cube`
  - `gamoshi_loss_mover_cube`, `gamoshi_loss_non_mover_cube`
  - `all_win_mover_cube`, `all_win_non_mover_cube`
  - `all_loss_mover_cube`, `all_loss_non_mover_cube`
  - Plus 4 census block cubes

### 2. Code Updates
Updated the following files to use the correct path:
- `tests/test_metrics_outliers.py`: Changed `duck_suppression.db` → `data/databases/duck_suppression.db`

All other files already correctly referenced `data/databases/duck_suppression.db`:
- `tools/db.py` (DEFAULT_DB_PATH)
- `carrier_dashboard_duckdb.py`
- `census_block_outlier_dashboard.py`
- All scripts in `scripts/build/`
- All scripts in `scripts/`

### 3. Day-of-Week Fix
Fixed the DOW calculation in the rolling views:
- **Before**: Used DuckDB's DAYOFWEEK() which returns 1=Sunday, 7=Saturday
- **After**: Now uses `DAYOFWEEK(the_date) - 1` to get 0=Sunday, 6=Saturday
- **Verified**: August 3, 2025 correctly shows as Sunday (day_of_week=0)

## Rolling Views Created

Created `gamoshi_win_mover_rolling` view with:

### Rolling Metrics (DOW-aware)
- **28-day window**: ~4 occurrences of same day of week
- **14-day window**: ~2 occurrences of same day of week
- Calculates: mean, stddev, z-scores, percentage changes

### Outlier Detection Flags
1. **Z-score based**: current > 1.5 std deviations from mean
2. **Percentage based**: current > 30% above mean
3. **First appearance**: New winner-loser pair in DMA (on same DOW)
4. **Rare pair**: Pair appeared < 5 times before (on same DOW)

### View Statistics
```
Total rows: 1,878,560
Date range: 2025-02-19 to 2025-09-04
First appearances: 513,408 (27.3%)
Rare pairs (< 5 appearances): 1,035,519 (55.1%)
Outliers (28d window): 120,442 (6.4%)
Outliers (14d window): 162,438 (8.6%)
Outliers (any method): 192,745 (10.3%)
```

## Sample Outliers from 2025-06-19

Top outliers detected (sorted by z-score):

| State | DMA | Winner | Loser | Current | Avg (28d) | Z-Score | % Change |
|-------|-----|--------|-------|---------|-----------|---------|----------|
| Massachusetts | Boston, MA | Comcast | Spectrum | 29 | 12.25 | 38.68 | +136.7% |
| New Jersey | New York, NY | Comcast | Spectrum | 16 | 6.00 | 14.14 | +166.7% |
| New Jersey | Philadelphia, PA | Comcast | Verizon | 32 | 12.75 | 13.02 | +150.9% |
| Kansas | Wichita-Hutchinson | Spectrum | Comcast | 18 | 3.25 | 11.35 | +453.8% |
| Alabama | Birmingham, AL | Spectrum | Comcast | 10 | 2.00 | 11.31 | +400.0% |

## Next Steps

1. ✅ **Database consolidation complete** - No more duplicate database issues
2. ✅ **DOW calculation fixed** - Rolling windows now work correctly by day of week
3. ✅ **Rolling view created** - Ready for outlier detection
4. 🔄 **Next**: Create similar views for:
   - `gamoshi_win_non_mover_rolling`
   - `gamoshi_loss_mover_rolling`
   - `gamoshi_loss_non_mover_rolling`
5. 🔄 **Next**: Implement suppression logic using these rolling views
6. 🔄 **Next**: Integrate into dashboards for real-time outlier detection

## Performance Benefits

Using database views instead of CSV cubes:
- **Faster queries**: Direct SQL on indexed tables
- **Less disk space**: No duplicate data in CSVs
- **Real-time updates**: Views always reflect latest cube data
- **Flexible thresholds**: Can adjust z-score/percentage params without rebuilding
- **DOW-aware**: More accurate baselines by comparing same day of week

## Validation

To verify the database and view:
```bash
# Check database location
find . -name "*.db" -type f

# Should show only: ./data/databases/duck_suppression.db

# Query the rolling view
duckdb data/databases/duck_suppression.db << 'EOF'
SELECT COUNT(*) as total, 
       COUNT(DISTINCT the_date) as dates,
       SUM(CASE WHEN is_outlier_any THEN 1 ELSE 0 END) as outliers
FROM gamoshi_win_mover_rolling;
EOF
```
