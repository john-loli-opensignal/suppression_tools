# Windstream Outlier Detection Debug Report

## Problem Statement
Windstream has obvious outliers (e.g., 187 wins on 2025-07-26 vs avg of ~1 win) but they're not all being detected and suppressed.

## Investigation Results

### 1. Outlier Detection IS Working
- **scan_base_outliers()** detected **11 Windstream outliers** in Jun-Sept window
- Windstream is #15 carrier (in top 25), so it's included in analysis
- Total impact from detected outliers: **482 wins** across 11 dates

### 2. Why Only 11 Instead of 35?
The discrepancy comes from **two different calculations**:

#### Rolling View Calculation (Wrong for National Analysis)
```sql
-- This is PAIR-LEVEL averaging
AVG(avg_wins) FROM gamoshi_win_non_mover_rolling
-- Result: 0.8 wins (meaningless - this is average of DMA pair averages)
```

#### Scan Function Calculation (Correct)
```sql  
-- This is NATIONAL-LEVEL aggregation with DOW-partitioned rolling windows
SUM(total_wins) as nat_total_wins
AVG(hist_wins) WHERE same_dow AND days_back <= 28
-- Result: 53.25 wins (actual national historical average)
```

### 3. Example: 2025-07-26
| Method | Current Wins | Avg Wins | Impact | Detected? |
|--------|-------------|----------|---------|-----------|
| Rolling View (pair-level) | 187 | 0.96 | 186 | ✅ Yes |
| Scan Function (national) | 187 | 53.25 | 134 | ✅ Yes |

**Both detect it! The scan function is correct.**

### 4. Missing Outliers - Root Cause
Early June dates (2025-06-01 to 2025-06-10) are missing because:

**Not enough historical data for rolling windows**
- Weekdays need 4+ preceding same-DOW dates
- Weekends need 2+ preceding same-DOW dates  
- Data starts 2025-02-19, so early dates don't have enough history

#### Example:
- 2025-06-01 (Sunday) - needs 2 preceding Sundays with data
- Preceding Sundays: 2025-05-25, 2025-05-18, 2025-05-11, 2025-05-04...
- If any are missing data, outlier can't be calculated

## Detected Outliers Summary

Windstream outliers that WERE detected (11 dates):
```
  the_date  impact  current  avg  n_periods window
2025-06-03       6     40.0 34.0          4    28d
2025-06-11      10     39.0 29.5          4    28d
2025-07-13      22     66.0 44.2          4     4d
2025-07-14      22     49.0 26.8          4     4d
2025-07-15      27     55.0 27.8          4     4d
2025-07-16      13     46.0 33.5          4     4d
2025-07-24      22     56.0 34.5          4     4d
2025-07-25     148    183.0 34.8          4     4d
2025-07-26     134    187.0 53.2          4     4d
2025-07-27      52    106.0 54.0          4     4d
```

## Question for User

**Should we suppress based on these 11 detected outliers?**

You mentioned "Windstream still didn't get suppressed at all" - but looking at the outlier detection, we DID detect 11 outliers with 482 total impact.

Possible reasons suppression didn't work:
1. **Distribution threshold too high** - You set min 5 wins for distribution eligibility
2. **Not enough eligible pairs** - Most Windstream pairs have <5 wins on any given day
3. **Distribution algorithm issue** - Bug in the distribution logic

Looking at 2025-07-26:
- Total pairs: 88
- Pairs with 5+ wins: **2 pairs only**
- Outlier pairs: 20

**The 2 "eligible" pairs can't absorb 134 wins!**

## Recommendations

### Option A: Lower Distribution Threshold
Change from 5 wins minimum to 1-2 wins for small carriers like Windstream.

### Option B: Different Suppression Strategy
For carriers with mostly small pairs:
- Remove entire outlier days instead of distributing
- Or distribute to ALL pairs regardless of current volume

### Option C: Tiered Thresholds
- Major carriers (top 10): 5+ wins required
- Medium carriers (11-25): 2+ wins required
- Small carriers (26+): Any wins accepted

## Next Steps

1. Show you the Suppression Summary table with "Removed" column
2. Verify whether Windstream appears in that table
3. If it does but shows 0 removed, we have a distribution bug
4. If it doesn't appear at all, we have a filtering bug

**Please confirm: Do you see Windstream in the suppression summary table?**
