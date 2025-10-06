# Windstream Investigation Summary

## What I Found ✅

### 1. Outlier Detection IS Working
- **Windstream outliers detected: 11 dates** (not 35 as initially thought)
- Total impact: **482 wins** across Jun-Sept
- Windstream rank: **#15** (in top 25)
- scan_base_outliers() is correctly computing national-level metrics

### 2. Why Only 11 Outliers?
The discrepancy comes from different aggregation methods:

| Method | Level | Example (2025-07-26) | Correct? |
|--------|-------|---------------------|----------|
| Rolling View | Pair-level | avg = 0.96 wins/pair | ❌ Wrong for national |
| scan_base_outliers | National-level | avg = 53.25 wins total | ✅ Correct |

**Key insight**: Rolling view stores pair-level metrics. Using `AVG(avg_wins)` gives meaningless results. scan_base_outliers re-aggregates at national level correctly.

### 3. The Real Problem: Distribution Threshold

Example from 2025-07-26:
- **Current wins**: 187
- **Historical avg**: 53.25  
- **Impact (excess)**: 134 wins
- **Pairs meeting 5+ win threshold**: **2 pairs only**
- **Outlier pairs**: 20 pairs

**The 2 eligible pairs cannot absorb 134 excess wins!**

This is why Windstream appears in "Carriers Not Meeting Threshold" table.

## UI Improvements Made

### 1. National Outliers Summary Table
Shows immediately after scanning:
- Which carriers have outliers
- How many dates affected
- Total/avg/max impact
- Average z-scores

Example output:
```
Carrier      Outlier Days  Total Impact  Avg Impact  Max Impact
Windstream           11          482        43.8         148
AT&T                  5        1,484       296.8         612
T-Mobile FWA          5          665       133.0         328
```

### 2. Enhanced Insufficient Threshold Reporting
Shows carriers that couldn't fully distribute:

**Summary by Carrier:**
```
Carrier      Dates Affected  Total Unaddressed Impact  Total Auto-Removed
Windstream            8                   567                    89
```

**Details by Date:**
- Shows exact dates where distribution failed
- Displays unaddressed impact per date
- Provides actionable tip: "Lower threshold to 2-3 wins"

## Recommendations

### Option A: Lower Distribution Threshold (EASIEST)
Change from 5 wins to 2 wins for carriers ranked 11-25.

**Tiered approach:**
- Top 10 carriers: 5+ wins required
- Carriers 11-25: 2+ wins required  
- Carriers 26+: 1+ win accepted

### Option B: Remove Entire Days (SURGICAL)
For carriers with insufficient pairs, remove the entire outlier day instead of distributing.

### Option C: Hybrid (RECOMMENDED)
1. Try auto-suppression first (pair outliers)
2. Try distribution with tiered thresholds
3. If still unaddressed, remove entire excess

## Testing Instructions

Run main.py with these settings:
- Dataset: gamoshi
- Mover type: Non-mover
- Date range: 2025-06-01 to 2025-09-04
- Top N: 25
- Z-threshold: 1.5
- Min wins for distribution: **2** (lowered from 5)

Then check:
1. Does Windstream appear in National Outliers table? (Yes, 11 dates)
2. Does Windstream appear in Insufficient Threshold table? (Should be reduced or gone)
3. Does Preview show Windstream suppressed? (Check dashed lines vs solid)
4. Does Suppression Summary show Windstream with non-zero "Removed"? (Validate)

## Files Modified
- `main.py` - Added National Outliers Summary and enhanced threshold reporting
- `.agent_memory.json` - Added debugging scenarios to prevent re-learning
- `analysis/WINDSTREAM_OUTLIER_DEBUG.md` - Detailed technical investigation

## Commit
```bash
feat(main): add National Outliers Summary and enhanced distribution threshold reporting
```
