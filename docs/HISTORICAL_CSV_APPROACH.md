# Historical CSV-Based Suppression Approach: Key Findings

## Executive Summary

After reviewing the git history of `main.py`, `carrier_suppression_dashboard.py`, and `carrier_dashboard_duckdb.py`, I've identified the key components of the original CSV cube-based suppression system that made it highly effective. This document summarizes those findings.

---

## Core Architecture (Pre-DuckDB Migration)

### 1. **CSV Cube Pre-computation**
   - **Location**: `build_win_cube.py` (commit 4ec96a2 and earlier)
   - **Storage**: CSV files stored in `current_run_duckdb/` directory
     - `win_cube_mover.csv` (mover_ind=True)
     - `win_cube_non_mover.csv` (mover_ind=False)
   - **Key Feature**: All rolling metrics pre-computed and stored in CSV

### 2. **Rolling Window Metrics** (The Critical Difference)

The CSV cubes included **14-row lookback windows** partitioned by:
- **Winner** (for national metrics)
- **Winner + Loser + DMA** (for pair metrics)  
- **Day of Week Type**: `Sat`, `Sun`, `Weekday` (DOW partitioning)

#### Rolling Metrics Computed:

**National Level (per winner):**
```sql
-- Partitioned by: winner, day_type
-- Window: 14 PRECEDING rows (prior-only, excluding current)
AVG(nat_share) AS nat_mu_share
STDDEV_SAMP(nat_share) AS nat_sigma_share
```

**Pair Level (per winner-loser-DMA):**
```sql
-- Partitioned by: winner, loser, dma_name, day_type  
-- Window: 14 PRECEDING rows (prior-only, excluding current)
AVG(pair_wins) AS pair_mu_wins
STDDEV_SAMP(pair_wins) AS pair_sigma_wins
```

**Critical Insight**: The window was **row-count based** (14 prior rows), not date-based. This means:
- Weekdays had ~14 days of history
- Saturdays had ~14 Saturdays of history  
- Sundays had ~14 Sundays of history

This **automatically handled volume differences** across days without explicit DOW adjustments.

---

## 3. **Multi-Stage Outlier Detection**

### Stage 1: Automatic Detection (DMA-Level Pair Outliers)

The system identified outliers at the **DMA level** using multiple triggers:

#### Trigger Conditions (any one triggers outlier):
1. **Z-Score Outlier**: `pair_z > 2.5` (z-score based on DOW-partitioned rolling baseline)
2. **Percentage Jump**: `pair_wins_current > 1.3 * pair_mu_wins` (30% increase)
3. **Rare Pair**: Pairs that appeared infrequently in history
4. **New Pair**: First appearance of a winner-loser-DMA combination

#### Volume Filter:
- **Minimum Volume**: `pair_wins_current > 5` (avoid noise from low-volume pairs)

#### Removal Calculation:
```python
# If baseline is weak (< 5 wins average), remove everything
if pair_mu_wins < 5.0:
    remove_units = pair_wins_current  # Remove all
else:
    remove_units = max(0, pair_wins_current - pair_mu_wins)  # Remove excess
```

### Stage 2: Distributed Suppression

After Stage 1 removed obvious outliers, if more suppression was needed to bring national share back to baseline:

1. **Calculate Remaining Need**:
   ```python
   # National level target
   W = nat_total_wins (current)
   T = market_total_wins (current)
   mu = nat_mu_share (baseline)
   
   need = max(0, (W - mu*T) / (1 - mu))
   need_after_stage1 = need - sum(stage1_removals)
   ```

2. **Equalized Distribution**:
   - Distribute remaining suppressions **equally** across all remaining winner-loser-DMA pairs
   - **Base allocation**: `need_after / num_pairs` (rounded down)
   - **Residual allocation**: Distribute remainder 1 unit at a time to pairs with highest residual capacity

3. **Stage Labeling**:
   - Stage 1 suppressions: `stage = 'auto'`
   - Stage 2 suppressions: `stage = 'distributed'`

---

## 4. **Visualization & Validation**

### Before/After Overlay Graphs

The dashboard provided **in-memory preview** of suppression effects:

```python
# Step 5: Preview graph with plan (from main.py commit dd1d183)
- Load base data (unsuppressed)
- Apply plan in-memory via DuckDB
- Calculate adjusted wins:
  adjusted_wins_new = max(0, adjusted_wins - proportional_removal)
  
- Plot overlays:
  * Solid lines: Original (unsuppressed) win share
  * Dashed lines: Suppressed win share
```

**Key Insight**: The user could see **exactly what would be removed** before applying changes.

---

## 5. **Plan Storage & Application**

### Plan CSV Format:
```csv
date,winner,loser,mover_ind,dma_name,remove_units,impact,stage,
nat_share_current,nat_mu_share,nat_sigma_share,nat_mu_window,
pair_wins_current,pair_mu_wins,pair_sigma_wins,pair_mu_window,pair_z,
dma_wins,pair_share,pair_share_mu
```

### Application Flow:
1. **Save Plan**: `suppressions/{round_name}.csv`
2. **Reload Dashboard**: Loads all plans from `suppressions/` folder
3. **Apply Suppressions**: 
   - Join plan with raw data on `(date, winner, loser, mover_ind, dma_name)`
   - Proportionally distribute `remove_units` across records in each group
   - Write suppressed dataset to new location

---

## 6. **Why It Worked So Well**

### ✅ Strengths:

1. **DOW-Aware Baselines**: Rolling windows partitioned by day type prevented false positives from normal Sat/Sun volume spikes

2. **Row-Count Windows**: Using 14 prior rows (not 14 days) ensured consistent history depth regardless of data density

3. **Multi-Trigger Detection**: Combined statistical (z-score), relative (30% jump), and temporal (rare/new) signals

4. **Surgical Precision**: Operated at DMA-level pairs, allowing targeted removal of specific problematic matchups

5. **Two-Stage Approach**: 
   - Stage 1 caught obvious outliers with context-aware triggers
   - Stage 2 ensured national-level targets were met without over-suppressing

6. **Transparent QA**: Users could see:
   - Which pairs triggered which rules
   - How much was removed at each stage
   - Before/after comparison graphs
   - All baseline metrics for validation

7. **Incremental Application**: Plans saved as CSVs allowed for:
   - Version control
   - Rollback
   - Iterative refinement

---

## 7. **What We Lost in the DuckDB Migration**

### ❌ Current Gaps:

1. **No Pre-computed Rolling Metrics**: Database cube tables lack `pair_mu_wins`, `pair_sigma_wins`, `nat_mu_share`, etc.

2. **No DOW Partitioning**: Z-scores computed globally, not partitioned by day type

3. **No Multi-Trigger Detection**: Lost rare/new pair detection, percentage jump triggers

4. **No Two-Stage Distribution**: Current approach doesn't have the surgical auto-removal + equalized distribution pattern

5. **No Census Block Granularity in Plans**: Old approach worked at DMA level; census blocks weren't part of the distribution mechanism (they were just aggregation keys)

6. **Visualization Gap**: Current graphs don't clearly show the delta between original and suppressed

---

## 8. **Recommended Path Forward**

### Option A: Recreate CSV Approach in Database

**Steps:**
1. Add rolling metric columns to cube tables:
   - `nat_mu_share`, `nat_sigma_share`, `nat_mu_window`
   - `pair_mu_wins`, `pair_sigma_wins`, `pair_mu_window`
   - `pair_z`, `nat_z`
   - `rare_pair`, `new_pair`, `pct_outlier_pos` flags

2. Rebuild `build_cubes_in_db.py` to compute these metrics with DOW-partitioned windows

3. Update `tools/outliers.py` to query these pre-computed metrics

4. Restore two-stage distribution logic in dashboards

### Option B: Enhanced Real-Time Computation

**Steps:**
1. Create specialized functions in `tools/outliers.py` that compute rolling metrics on-the-fly with DOW partitioning

2. Implement multi-trigger detection (z-score, %, rare, new)

3. Rebuild distribution planner with auto + distributed stages

4. Fix visualization to show overlays properly

### Option C: Hybrid Approach (Recommended)

**Rationale**: Get speed of pre-computation with flexibility of real-time adjustments

**Steps:**
1. **Pre-compute in cubes**: Rolling baselines (mu, sigma) by DOW
2. **Compute on query**: Z-scores, outlier flags, rare/new detection (fast on indexed cubes)
3. **Distribution engine**: Standalone module that takes cube + filters and generates plans
4. **Visualization**: Reusable overlay graph component

---

## 9. **Key Code References**

### Historical Commits:
- **CSV cube builder**: `4ec96a2` (`build_win_cube.py`)
- **Distribution logic**: `dd1d183` (`main.py`, lines ~170-280)
- **Dashboard overlays**: `6ed5582` (`main.py`, Step 5 preview)
- **Rolling window logic**: `93236b1` (DOW-partitioned row-count windows)

### Original Formula for Removal Target:
```python
# How many units to remove to bring winner back to baseline?
W = nat_total_wins          # Current winner total
T = market_total_wins       # Current market total  
mu = nat_mu_share           # Baseline winner share

need = ceil(max(0, (W - mu*T) / (1 - mu)))
```

**Interpretation**: 
- `W - mu*T` = excess wins above baseline
- `(1 - mu)` = share of market held by losers
- Division gives units to remove such that removing them brings winner share back to `mu`

---

## Conclusion

The original CSV cube approach was **highly sophisticated** and balanced:
- **Speed**: Pre-computed metrics meant instant outlier detection
- **Accuracy**: DOW-partitioned rolling windows prevented false positives
- **Control**: Two-stage approach (auto + distributed) was surgical yet comprehensive
- **Transparency**: Rich metadata and visualizations enabled validation

Our current DuckDB approach has **excellent query performance** but lacks the **statistical rigor** and **multi-stage distribution logic** that made the CSV system effective.

**Next step**: Implement Option C (hybrid) to get the best of both worlds.
