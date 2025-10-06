# Dashboard Outlier Detection - Questions Answered ✅

**Date**: 2025-10-06  
**Status**: Complete

---

## ❓ Your Questions

### 1. Why do carrier_dashboard_duckdb.py and main.py show different outliers with same settings?

**Answer**: They use **different outlier detection algorithms** - this is **intentional and by design**.

#### carrier_dashboard_duckdb.py (Exploratory Tool)
- **Purpose**: Fast, simple outlier detection for exploration
- **Algorithm**: `db.national_outliers_from_cube()`
- **Day Grouping**: 3 types (Saturday, Sunday, Weekday)
- **Window**: Fixed single window (whatever you set, e.g., 14 days)
- **Scope**: ALL carriers in dataset
- **Speed**: Very fast (< 1 second)

#### main.py (Production Suppression Tool)
- **Purpose**: Robust suppression planning for production
- **Algorithm**: `scan_base_outliers()`
- **Day Grouping**: 7 types (DOW 1-7, each day of week)
- **Window**: Tiered (tries 28d → 14d → 4d based on data availability)
- **Scope**: Top N carriers + egregious outliers (impact > threshold)
- **Additional Filters**: Optional minimum share % threshold
- **Speed**: Still fast (< 5 seconds)

#### Why This Makes Sense
- **Different use cases** = different needs
- carrier_dashboard: "Show me everything quickly"
- main.py: "Show me actionable items for production"

**Recommendation**: ✅ **Keep them different** - they serve complementary purposes.

---

### 2. Should main.py have sliders for DMA-level z-score and percent change?

**Answer**: ✅ **YES - Now implemented!**

Previously, DMA-level thresholds were hardcoded:
```python
# OLD - hardcoded values
CASE WHEN pct_change > 30 THEN true ELSE false END  -- 30% threshold
AND zscore > 1.5  -- 1.5 z-score threshold
AND (total_wins - avg_wins) > 15  -- 15 impact for rare pairs
```

Now, users can configure these via sliders:

#### New Sliders Added to main.py Sidebar

**National Level (existing - improved labels)**:
- 🔹 **Top N Carriers**: 10-100 (default: 25)
- 🔹 **National Z-Score Threshold**: 0.5-5.0 (default: 2.5)
- 🔹 **Egregious Impact**: 10-100 (default: 40)

**DMA (Pair) Level (NEW! ✨)**:
- 🆕 **DMA Z-Score Threshold**: 0.5-5.0 (default: 1.5)
- 🆕 **DMA % Change Threshold**: 10-100% (default: 30%)
- 🆕 **Rare Pair Impact**: 5-50 (default: 15)

---

## 📊 Implementation Details

### Code Changes

#### 1. Updated `tools/src/plan.py::build_enriched_cube()`

Added parameters:
```python
def build_enriched_cube(
    ds: str,
    mover_ind: bool,
    start_date: str,
    end_date: str,
    dma_z_threshold: float = 1.5,  # NEW
    dma_pct_threshold: float = 30.0,  # NEW
    rare_pair_impact_threshold: int = 15,  # NEW
    db_path: Optional[str] = None
) -> pd.DataFrame:
```

Replaced hardcoded SQL with parameters:
```sql
-- Before
CASE WHEN pct_change > 30 THEN true ELSE false END

-- After
CASE WHEN pct_change > {dma_pct_threshold} THEN true ELSE false END
```

#### 2. Updated `main.py`

Added sliders to sidebar:
```python
st.sidebar.caption('**DMA (Pair) Level**')
dma_z_threshold = st.sidebar.slider('DMA Z-Score Threshold', ...)
dma_pct_threshold = st.sidebar.slider('DMA % Change Threshold', ...)
rare_pair_impact = st.sidebar.slider('Rare Pair Impact', ...)
```

Pass to function:
```python
enriched = build_enriched_cube(
    ds=ds,
    mover_ind=mover_ind,
    start_date=str(view_start),
    end_date=str(view_end),
    dma_z_threshold=dma_z_threshold,  # NEW
    dma_pct_threshold=dma_pct_threshold,  # NEW
    rare_pair_impact_threshold=rare_pair_impact,  # NEW
    db_path=db_path
)
```

---

## 🧪 Testing Recommendations

### To Verify National vs DMA Detection:

1. **Set different thresholds**:
   - National Z-Score: 2.5 (default)
   - DMA Z-Score: 1.5 (default)
   
2. **Scan outliers** - you should see:
   - National outliers (broader patterns)
   - DMA pair outliers (granular issues)
   
3. **Try stricter DMA settings**:
   - DMA Z-Score: 2.0
   - DMA % Change: 50%
   - Should flag fewer pair-level outliers

4. **Try looser DMA settings**:
   - DMA Z-Score: 1.0
   - DMA % Change: 20%
   - Should flag more pair-level outliers

### Expected Behavior:

- **National outliers**: Affect overall carrier performance
- **DMA pair outliers**: Affect specific market matchups
- **Rare pairs**: New or infrequent matchups with high impact
- **Distribution**: Should spread suppression to non-outliers

---

## 📝 Documentation Added

### 1. Code Documentation
- ✅ Updated function signatures
- ✅ Added parameter descriptions
- ✅ Inline comments explain logic

### 2. Analysis Documents
- ✅ `analysis/outlier_detection_differences.md` - Detailed comparison
- ✅ `analysis/DASHBOARD_SLIDERS_UPDATE.md` - This document
- ✅ `.agent_memory.json` - Machine-readable context

---

## 🎯 User Benefits

### Before:
- ❌ Couldn't adjust DMA-level sensitivity
- ❌ Had to edit code to change thresholds
- ❌ Didn't understand why dashboards showed different results

### After:
- ✅ Full control over both national and DMA thresholds
- ✅ Real-time tuning via sliders (no code changes)
- ✅ Clear documentation of algorithm differences
- ✅ Can experiment to find optimal settings

---

## 🚀 Next Steps

### Immediate:
1. **Test the new sliders** in main.py
2. **Verify** outlier detection behaves as expected
3. **Tune** thresholds for your use case

### Future (Optional):
- Consider adding slider presets ("Conservative", "Balanced", "Aggressive")
- Add tooltips showing how many outliers each threshold change affects
- Log slider settings in suppression metadata for reproducibility

---

## Summary

✅ **Task A**: Documented why carrier_dashboard_duckdb.py and main.py show different outliers (intentional design)  
✅ **Task B**: Added DMA-level threshold sliders to main.py (3 new sliders)  
✅ **Bonus**: Updated agent memory to prevent re-learning this context

**Result**: Users now have full control over outlier detection sensitivity at both national and DMA levels! 🎉
