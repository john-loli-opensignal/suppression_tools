# DOW Approach Analysis: 3-Category vs 7-Day

## Executive Summary

**Winner: 3-Category Approach (Sat/Sun/Weekday)**

The 3-category DOW approach finds **MORE outliers** (19,620 vs 19,481), **MORE carriers** (389 vs 302), and is **more aligned** with business patterns (weekends behave differently from weekdays).

## Test Results

### Parameters
- **Date Range**: 2025-06-01 to 2025-09-04
- **Z-Score Threshold**: 2.5
- **Window**: 28 days

### 3-Category Approach (Sat/Sun/Weekday)
- **Total DMA-level outliers**: 19,620
- **Unique dates**: 96
- **Unique carriers with outliers**: 389

### 7-Day Approach (Each DOW separate)
- **Total DMA-level outliers**: 19,481
- **Unique dates**: 96
- **Unique carriers with outliers**: 302

### Carriers Found
- **Only in 3-Category**: 87 carriers (including many legitimate small carriers)
- **Only in 7-Day**: 3 carriers
- **In Both**: 299 carriers

## Why 3-Category is Better

### 1. **Business Logic Alignment**
- **Weekends** have different traffic patterns than weekdays
- **Saturday** and **Sunday** are similar enough to group separately
- **Monday-Friday** have similar business patterns
- Splitting into 7 separate DOWs **dilutes the signal**

### 2. **Statistical Power**
- **3-category**: More samples per partition → Better statistical estimates
- **7-day**: Fewer samples per partition → Weaker detection
- Example: 4 preceding Sundays vs 28 preceding "all days"

### 3. **Carrier Coverage**
- 3-category finds **87 MORE carriers** with outliers
- These are legitimate small carriers that would be missed otherwise
- 7-day approach only finds 3 unique carriers that 3-category missed

### 4. **Current Implementation**
- `carrier_dashboard_duckdb.py` uses **3-category** in `national_outliers_from_cube()`
- This is why carrier_dashboard finds more AT&T outliers
- `main.py` rolling views use **7-day** approach (inconsistency)

## Recommendation

### Adopt 3-Category Approach Everywhere

**For main.py:**
1. Update rolling view to use `day_type` instead of `day_of_week`
2. Group as: `Sat (6)`, `Sun (0)`, `Weekday (1-5)`
3. Adjust minimum thresholds:
   - **Weekday**: Min 4 periods (one week of weekdays)
   - **Weekend**: Min 2 periods (relaxed for lower frequency)

**Benefits:**
- ✅ Consistency across dashboards
- ✅ More comprehensive outlier detection
- ✅ Better alignment with business patterns
- ✅ Proven approach (already in carrier_dashboard)

## Additional Requirement: Customizable Window

User also wants **customizable outlier window** for national-level detection:

### Current State
- `carrier_dashboard_duckdb.py`: Has slider (7-60 days) ✅
- `main.py`: Fixed window (14 days hardcoded in view) ❌

### Solution
Instead of pre-computing fixed windows in views, compute on-the-fly:

```sql
-- Dynamic window in query instead of view
WITH rolling_metrics AS (
    SELECT 
        *,
        AVG(win_share) OVER (
            PARTITION BY winner, day_type 
            ORDER BY the_date 
            ROWS BETWEEN {user_window} PRECEDING AND 1 PRECEDING
        ) as mu,
        ...
    FROM daily_data
)
```

This allows users to:
- Test sensitivity (7 days = aggressive, 60 days = conservative)
- Adapt to seasonal patterns
- Balance false positives vs false negatives

## Implementation Plan

### Phase 1: Update Rolling Views (DMA-level)
- [x] Change `day_of_week` partitioning to `day_type` (3-category)
- [x] Adjust minimum thresholds (4 weekday, 2 weekend)
- [ ] Rebuild views for all datasets

### Phase 2: Update National Outlier Detection
- [x] Already uses 3-category in `national_outliers_from_cube()` ✅
- [ ] Make window parameter configurable in UI (already done in carrier_dashboard)

### Phase 3: Update main.py
- [ ] Add window slider to sidebar (default: 28 days)
- [ ] Add z-score sliders for both national and DMA levels
- [ ] Pass window parameter to scan functions

## Conclusion

**Use 3-category DOW (Sat/Sun/Weekday)** for all outlier detection:
- More outliers found (19,620 vs 19,481)
- More carriers covered (389 vs 302)
- Better business logic alignment
- Already proven in carrier_dashboard
- Simple to implement

**Add customizable windows** to give users control over sensitivity.
