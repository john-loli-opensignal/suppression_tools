# Dashboard Outlier Detection Discrepancy Analysis

## Executive Summary

You're absolutely right - there are **significant differences** in how the two dashboards detect outliers, causing them to show different results even with the same settings. Here's what I found:

---

## Key Differences

### 1. **Rolling Window Logic** 🔴 CRITICAL DIFFERENCE

**carrier_dashboard_duckdb.py:**
- Uses **simple row-based rolling window** via `ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING`
- This means: "Look at the previous N rows regardless of date gaps"
- Window = 14 means "previous 14 data points"
- **Does NOT account for DOW partitioning**
- **Does NOT use tiered fallback (28d → 14d → 4d)**

**main.py (via scan_base_outliers in plan.py):**
- Uses **DOW-partitioned tiered rolling windows**
- Computes history across **entire time series**, then filters to graph window
- Window = 28d/14d/4d means "28/14/4 calendar days of same DOW"
- **Requires minimum 4 samples for weekdays, 2 for weekends**
- **Falls back through tiers**: Try 28d → fallback 14d → fallback 4d
- Much more sophisticated and accurate

**Why This Matters:**
```
Example: June 19, 2025 (Thursday)

carrier_dashboard: 
  - Looks at previous 14 Thursdays (simple row count)
  - No minimum sample requirement
  - Could compare to very sparse history

main.py:
  - Looks at 28 calendar days of Thursdays
  - Requires at least 4 Thursday samples (or falls back to 14d)
  - Much more stable baseline
```

---

### 2. **Top N Filtering** 🔴 CRITICAL DIFFERENCE

**carrier_dashboard_duckdb.py:**
- **No top N filtering** on outlier detection
- Shows outliers for **all carriers in the data**
- Only filters display in the graph (for visualization)

**main.py:**
- **Filters outlier detection to Top N carriers** (default: 25)
- Only analyzes carriers in `top_carriers` list
- **Plus egregious outliers** (impact > 40) outside top N
- More focused analysis

---

### 3. **Entire Series vs. Window Calculation** 🔴 CRITICAL DIFFERENCE

**carrier_dashboard_duckdb.py:**
```sql
WHERE 1=1 AND w.the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
```
- Calculates rolling metrics **only within the graph window**
- If your window is June 1 - Aug 31, it **only looks at data in that range**
- **Limited historical context** for rolling calculations
- Early dates in the window have very sparse baselines

**main.py:**
```sql
-- Computes rolling across ENTIRE series (from first date to last date)
-- Then filters results to window:
WHERE curr.the_date BETWEEN '{start_date}' AND '{end_date}'
```
- Calculates rolling metrics **from beginning of time series**
- Graph window (June 1 - Aug 31) only filters **which outliers to show**
- **Full historical context** for every date
- Much more stable and accurate

**Analogy:**
- carrier_dashboard: Looking through a narrow window and only seeing what's visible
- main.py: Computing everything, then showing you the relevant subset

---

### 4. **Share % Minimum Threshold**

**carrier_dashboard_duckdb.py:**
- **No built-in share threshold**
- Shows all carriers, regardless of market share
- Could flag outliers for tiny carriers (0.01% share)

**main.py:**
- **Optional min_share_pct filter** (default: 0.5%)
- Only analyzes carriers with >= X% of total wins
- Focuses on carriers that actually matter

---

### 5. **Impact Calculation**

**carrier_dashboard_duckdb.py:**
- Does not calculate "impact" (current - baseline in absolute wins)
- Only flags outliers based on z-score

**main.py:**
- Calculates **impact = current_wins - avg_wins**
- Uses this to flag "egregious" outliers
- Helps prioritize which outliers to address

---

## Why They Show Different Results

### Scenario: 28-day window, z-score = 2.5, June 1 - Aug 31

**carrier_dashboard_duckdb.py:**
1. Filters data to June 1 - Aug 31
2. For each date in that range, looks at previous 28 **rows** (not days)
3. For June 1, this might only be 5-10 data points (early in window)
4. Flags outliers for **all carriers**
5. Shows limited outliers because baseline is unstable

**main.py:**
1. Uses **entire time series** (Feb 19 - Sept 4)
2. For June 1, looks at 28 calendar days of **same DOW** before June 1
3. This could be 4-6 data points of actual Thursdays (depending on DOW)
4. Falls back to 14d if not enough samples, then 4d
5. Only analyzes **top 25 carriers** (+ egregious outliers)
6. Shows more outliers because baseline is stable and focused

---

## Recommendations

### For carrier_dashboard_duckdb.py:

**Option A: Quick Fix (Add DOW Partitioning)**
- Update `tools/db.py` `national_outliers_from_cube()` to use DOW partitioning
- Add tiered fallback (28d → 14d → 4d)
- Calculate over entire series, filter to window

**Option B: Unified Approach**
- Make carrier_dashboard use the same `scan_base_outliers()` function from `plan.py`
- Ensure both dashboards use identical logic
- One source of truth

### For main.py:

**Add Configurable Thresholds (REQUESTED):**
1. ✅ **Z-score threshold slider** - Already exists (default: 2.5)
2. ❌ **DMA-level z-score slider** - NEEDS TO BE ADDED
3. ❌ **DMA-level % change threshold** - NEEDS TO BE ADDED
4. ❌ **First appearance threshold** - NEEDS TO BE ADDED
5. ✅ **Top N slider** - Already exists (default: 25)
6. ✅ **Min share %** - Already exists (default: 0.5%)
7. ✅ **Egregious impact** - Already exists (default: 40)

---

## What Needs to Change in main.py

### Missing Sliders to Add:

```python
st.sidebar.header('DMA-Level Outlier Thresholds')

# DMA Z-score threshold
dma_z_threshold = st.sidebar.slider(
    'DMA Z-Score Threshold', 
    min_value=0.5, max_value=5.0, value=1.5, step=0.1,
    help='Z-score threshold for DMA-level outliers (default: 1.5)'
)

# DMA % change threshold
dma_pct_threshold = st.sidebar.slider(
    'DMA % Change Threshold', 
    min_value=10, max_value=200, value=30, step=5,
    help='Percentage increase threshold for DMA outliers (default: 30%)'
)

# First appearance lookback
first_appearance_days = st.sidebar.slider(
    'First Appearance Lookback (days)', 
    min_value=7, max_value=90, value=28, step=7,
    help='How far back to check for first appearances (default: 28 days)'
)
```

These would be passed to:
- `build_enriched_cube()` - for DMA-level outlier detection
- Distribution logic - for first appearance detection

### Where These Are Used:

**DMA Z-score & % change:**
- In `build_enriched_cube()` when detecting DMA-level outliers
- Currently hardcoded in the view definition (`gamoshi_win_mover_rolling`)
- Need to make these parameterized

**First appearance:**
- In distribution logic when identifying "rare pairs"
- Currently uses a fixed lookback window
- Should be configurable

---

## Action Plan

### Phase 1: Document Current State ✅ (This Document)

### Phase 2: Fix carrier_dashboard_duckdb.py
1. Update `national_outliers_from_cube()` to match `scan_base_outliers()` logic
2. Add DOW partitioning
3. Add tiered window fallback
4. Calculate over entire series
5. Test with June 19 data to verify consistency

### Phase 3: Add Missing Sliders to main.py
1. Add DMA z-score slider
2. Add DMA % change slider
3. Add first appearance lookback slider
4. Wire these into `build_enriched_cube()` and distribution logic
5. Update views to be parameterized instead of hardcoded

### Phase 4: Validate Consistency
1. Run both dashboards with identical settings
2. Verify they show the same outliers
3. Document any remaining differences

---

## Answers to Your Questions

### Q1: Why do carrier_dashboard and main.py show different outliers with same settings?

**A:** Three main reasons:
1. **carrier_dashboard uses simple row-based windows** (no DOW partitioning)
2. **carrier_dashboard calculates only within the graph window** (limited history)
3. **main.py filters to top N carriers** (more focused analysis)

### Q2: Should main.py have sliders for DMA z-score and % change?

**A:** **YES**, absolutely! Currently these are hardcoded in the rolling view:
- DMA z-score threshold: Hardcoded to 2.5 in view
- DMA % change: Hardcoded to 30% in view

Making these configurable would allow users to:
- Tighten/loosen DMA-level outlier detection
- Experiment with different thresholds
- Match settings between national and DMA levels

### Q3: What other thresholds should be configurable?

**A:** All of these should be sliders (some already are):
- ✅ National z-score (already exists)
- ❌ DMA z-score (needs to be added)
- ❌ DMA % change (needs to be added)
- ❌ First appearance lookback (needs to be added)
- ✅ Top N carriers (already exists)
- ✅ Min share % (already exists)
- ✅ Egregious impact (already exists)
- ✅ Auto suppression min wins (already exists)
- ✅ Distributed min wins (already exists)

---

## Technical Details

### Current View Definition (Hardcoded):
```sql
CREATE OR REPLACE VIEW gamoshi_win_mover_rolling AS
...
CASE 
    WHEN best_sigma > 0 AND best_n >= min_required_samples
         AND ABS(nat_total_wins - best_mean) / best_sigma > 2.5  -- HARDCODED Z-SCORE
    THEN TRUE
    ELSE FALSE
END as is_nat_outlier,

CASE 
    WHEN best_mean > 0 
         AND (nat_total_wins - best_mean) / best_mean > 0.30  -- HARDCODED 30%
    THEN TRUE
    ELSE FALSE
END as is_first_appearance
...
```

### How to Make Parameterized:

**Option A: Dynamic View Creation**
- Drop and recreate view with user-specified thresholds
- Fast, but requires write access to database

**Option B: Filter Results After Query**
- Query view with all metrics (z-score, % change)
- Apply thresholds in Python
- Slower, but no database changes needed

**Option C: Inline Query (Recommended)**
- Don't use view at all
- Embed the rolling calculation in the query with parameters
- Most flexible, moderate performance

---

## Summary

The discrepancy exists because:
1. **Different rolling window logic** (simple rows vs. DOW-partitioned tiered)
2. **Different calculation scope** (window-only vs. entire series)
3. **Different filtering** (all carriers vs. top N)

To fix:
1. **Align carrier_dashboard to use same logic as main.py**
2. **Add missing sliders to main.py** (DMA z-score, DMA %, first appearance)
3. **Make view thresholds parameterized** instead of hardcoded
4. **Test and validate consistency**

This is a significant but fixable architectural difference. The main.py approach is more sophisticated and accurate, so carrier_dashboard should adopt its logic.

