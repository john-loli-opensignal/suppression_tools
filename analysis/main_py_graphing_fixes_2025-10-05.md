# Main.py Graphing Fixes - October 5, 2025

## Issues Fixed

### 1. ❌ Outliers Appearing in Wrong Places
**Problem**: Outlier markers were not aligning with the carrier lines on the graph.

**Root Cause**: Re-smoothing only the outlier points caused misalignment. The outlier subset was being smoothed independently from the main line, creating different values.

**Solution**: 
- Pre-compute smoothed series for each carrier
- Store in dictionary indexed by carrier name
- Extract outlier marker positions from pre-computed smooth series
- Never re-smooth a subset of data

```python
# BEFORE (Wrong - causes misalignment)
pos_series = pos_out['win_share'] * 100
if len(pos_series) >= 3:
    pos_y = pos_series.rolling(window=3, center=True, min_periods=1).mean()

# AFTER (Correct - uses pre-computed smooth)
smooth_series = carrier_smoothed[w]  # Already computed for full series
pos_y = [smooth_series.get(d, fallback) for d in pos_out['the_date']]
```

### 2. ❌ Colors Not Ranked Properly
**Problem**: Legend and colors were not ordered by carrier size (total wins).

**Root Cause**: Carriers were sorted alphabetically instead of by rank.

**Solution**:
- Rank carriers by total wins (descending order)
- Assign colors based on rank
- Apply consistent ranking to both base graph and outliers graph

```python
# Rank carriers by total wins
carrier_totals = ts.groupby('winner')['total_wins'].sum().sort_values(ascending=False)
carriers_ranked = carrier_totals.index.tolist()

# Assign colors by rank
palette = px.colors.qualitative.Dark24
color_map = {c: palette[i % len(palette)] for i, c in enumerate(carriers_ranked)}
```

### 3. ✅ Outliers ARE National-Level (No Issue)
**Clarification**: User thought outliers were DMA-level when they should be national. 

**Reality**: Outliers WERE national-level all along - the scan_base_outliers() function correctly aggregates across DMAs. The confusion was due to poor display, not incorrect calculation.

**Verification**:
```python
# Test showed correct national aggregation
outliers = scan_base_outliers(ds='gamoshi', mover_ind=False, ...)
# Returns: 66 outliers, 22 unique dates, 33 unique winners
# Columns: the_date, winner, nat_z_score, impact, nat_total_wins, ...
```

## Changes Made

### File: `main.py`

**Lines 43-127**: Base graph section
- Added carrier ranking by total wins
- Updated color mapping to use ranked order
- Same formatting as carrier_dashboard_duckdb.py

**Lines 164-330**: Outliers graph section
- Pre-compute smoothed series for each carrier
- Store in `carrier_smoothed` dict
- Use pre-computed values for marker positioning
- Split positive/negative outliers correctly
- Apply carrier ranking for color consistency

## Testing Checklist

- [x] main.py imports without errors
- [x] Git commit created with clear message
- [ ] User to test: Base graph displays with proper ranking
- [ ] User to test: Outliers graph shows markers aligned with lines
- [ ] User to test: Colors consistent across both graphs
- [ ] User to test: Legend ordered by carrier size (descending)

## Key Takeaways

### For Future Development

1. **Never re-smooth subsets**: Always compute full smoothed series first, then extract points
2. **Rank before coloring**: Sort carriers by metric (wins, share, etc.) before assigning colors
3. **Verify calculations separately from display**: Outlier detection was correct; display was broken
4. **Use carrier_dashboard_duckdb.py as reference**: It has the proven graphing logic

### Updated in .agent_memory.json

- Documented graphing fixes
- Added color ranking standard
- Confirmed national-level outlier detection works correctly
- Key insight: Display bugs != calculation bugs

## What's Next

User should test the dashboard and verify:
1. Base graph colors/legend ranked by total wins
2. Outlier markers properly aligned on carrier lines
3. Yellow stars for positive z-scores, red minus for negative
4. Hover tooltips show correct z-score and impact values

If issues persist, check:
- Are markers visible? (size=11 for stars, size=12 for minus)
- Are colors consistent between base and outliers?
- Does smoothing window (3 periods) make sense for your data?

---

**Commit**: `f73cb25` - fix(main.py): correct outlier display and color ranking
**Branch**: `codex-agent`
**Date**: 2025-10-05
