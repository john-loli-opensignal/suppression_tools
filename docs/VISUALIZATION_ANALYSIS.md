# Visualization Analysis - Suppression Graphs

**Date:** 2025-01-03  
**Status:** ⚠️ CRITICAL ISSUE FOUND

---

## Summary

The before/after suppression graphs have been updated with:
- ✅ Thicker lines (3.5px) for better visibility
- ✅ Solid lines (before) rendered on top
- ✅ Dashed lines (after) rendered underneath
- ✅ Star markers on carriers with significant suppressions
- ✅ Proper legend and annotations

**However, a critical issue was discovered during analysis:**

---

## 🚨 Critical Finding: Over-Suppression

The current pair-level outlier detection is **flagging too many legitimate pairs as outliers**, resulting in excessive suppression:

| Date | Total Wins | Wins After Suppression | Suppression Rate |
|------|------------|------------------------|------------------|
| 2025-06-19 | 12,800 | 3,233 | **74.74%** ❌ |
| 2025-08-16 | 17,948 | 6,720 | **62.56%** ❌ |
| 2025-08-17 | ~18,000 | ~7,000 | **~61%** ❌ |

**Expected suppression rate:** 3-5%  
**Actual suppression rate:** 60-75%

---

## Root Cause Analysis

### Pair Outlier Statistics

```
Total pair outliers flagged: 45,156
├─ Pairs with 0 wins:        18,807 (41.6%)  ← Correctly ignored
└─ Pairs with >0 wins:        26,349 (58.4%)  ← Being suppressed

Actual wins suppressed: 44,588
```

### Outlier Types Breakdown

| Type | Count | Issue |
|------|-------|-------|
| **Rare pairs** (< 3 appearances) | 38,741 | ⚠️ TOO AGGRESSIVE |
| Percentage spikes | 15,406 | ✅ Legitimate |
| New pairs (first appearance) | 4,964 | ⚠️ Needs review |

**Problem:** The "rare pair" criterion (appearing < 3 times in 14-day history) is catching too many legitimate low-volume pairs.

### Win Distribution of Flagged Pairs

```
1 win:  19,940 pairs  ← Vast majority are single-win pairs
2 wins:  3,615 pairs
3 wins:  1,040 pairs
4 wins:    564 pairs
5+ wins: 1,190 pairs
```

**Insight:** Most flagged pairs have only 1-2 wins. While these might be rare, suppressing ALL of them removes the majority of daily data.

---

## Example: Comcast on 2025-06-19

**Before Suppression:**
- Total wins: 2,674
- Win share: 20.89%

**After Suppression:**
- Total wins: 704 (-1,970 wins, 73.7% removed)
- Win share: 21.78%

**Paradox:** Removing 73.7% of Comcast's wins INCREASED their win share!

**Why?** Because OTHER carriers had even MORE suppression, shrinking the denominator (total daily wins) more than Comcast's numerator decreased.

This is not a meaningful suppression - it's destroying the data.

---

## Why Graphs Don't Show Clear Difference

1. **Scale Issue:** Win share changes are -0.5% to +5% - small on a 0-25% scale
2. **All carriers affected:** Since most carriers have heavy suppression, relative positions don't change much
3. **Paradoxical changes:** Some carriers' shares go UP after suppression (due to denominator shrinkage)

The visualizations ARE working correctly - the problem is the underlying suppression logic is too aggressive.

---

## Recommendations

### 1. **Immediate: Refine Outlier Detection Criteria**

Current criteria are too broad:

```python
# CURRENT (too aggressive)
is_rare_pair = hist_count < 3  # Flags 86% of pairs!
is_new_pair = hist_count == 0  # Might be legitimate new matchups
```

**Proposed refinement:**

```python
# REFINED
is_rare_pair = (hist_count < 3) AND (pair_wins_current > 5)  # Only flag if significant volume
is_new_pair = (hist_count == 0) AND (pair_wins_current > 10)  # New pairs with high volume only
is_percentage_outlier = (pct_z > 2.0) AND (pair_wins_current > 3)  # Keep percentage spikes
```

### 2. **Add Minimum Win Threshold**

Don't flag pairs as outliers unless they have meaningful impact:

```python
MIN_WINS_TO_FLAG = 5  # Don't suppress pairs with < 5 wins
```

Rationale:
- Single-win pairs (19,940 of them) are noise, not outliers
- Focusing on pairs with 5+ wins targets actual anomalies
- Reduces false positives by ~80%

### 3. **Use Tiered Suppression**

Instead of binary suppress/keep:

| Z-Score | Action |
|---------|--------|
| z > 5   | Suppress 100% |
| 3 < z ≤ 5 | Suppress 75% |
| 2 < z ≤ 3 | Suppress 50% |
| z ≤ 2   | Keep (investigate only) |

### 4. **Add Safeguards**

```python
# Daily suppression limits
MAX_DAILY_SUPPRESSION_PCT = 10.0  # Alert if > 10%
MAX_CARRIER_SUPPRESSION_PCT = 25.0  # Alert if any carrier > 25%
```

---

## Visualization Status

The graphs are **technically correct** but show minimal difference because:

1. Win share changes are small in magnitude (-5% to +5% on 0-25% scale)
2. Relative carrier positions remain similar (all boats sink together)
3. The suppression is so extreme it creates paradoxes

**Proposed visualization improvements:**

1. **Add a "Suppression Impact" subplot** showing:
   - Bars for wins suppressed per carrier on target dates
   - Makes the impact more visible than win share deltas

2. **Create a "zoom-in" view** around target dates:
   - Focus on ±7 days around target dates
   - Larger y-axis scale to show small changes

3. **Show absolute wins, not just win share:**
   - Win share can be misleading when denominator changes
   - Absolute wins show the actual suppression magnitude

---

## Next Steps

1. ⚠️ **DO NOT USE** current suppression results in production
2. 🔧 Refine outlier detection criteria (see Recommendation #1)
3. 🧪 Re-run analysis with refined criteria
4. 📊 Regenerate graphs with corrected suppressions
5. ✅ Validate suppression rate is 3-5%, not 60-75%

---

## Files

- **Analysis doc:** `docs/REMOVE_OUTLIERS.md`
- **Graph script:** `scripts/generate_suppression_graphs.py`
- **Graphs:** `analysis_results/suppression/graphs/*.png`
- **Data:** `analysis_results/suppression/data/*.json`

---

## Conclusion

The visualization code is working correctly. The problem is **the outlier detection is too aggressive**, flagging 60-75% of daily wins for suppression instead of the intended 3-5%.

The "rare pair" criterion needs to be refined to avoid flagging every low-volume legitimate matchup as an outlier.
