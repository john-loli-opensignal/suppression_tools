# Outlier Detection Methods

## Overview

The project has **multiple levels** of outlier detection that work together to identify anomalous data points for suppression. Each method serves a different purpose in the pipeline.

---

## 1. National-Level Outlier Detection

### Purpose
Identify days when a **carrier's national market share** is anomalously high.

### Method: **Day-of-Week Partitioned Z-Score**

```python
# From nat_outliers.sql and outliers.py
national_outliers(store_glob, ds, mover_ind, start_date, end_date, 
                 window=14, z_thresh=2.5, metric='win_share')
```

### Algorithm

1. **Calculate metric** (win_share, loss_share, or wins_per_loss):
   ```sql
   metric = carrier_wins / market_total_wins
   ```

2. **Partition by day type**:
   - Weekday (Mon-Fri)
   - Saturday
   - Sunday

3. **Rolling window calculation** (per carrier, per day type):
   - Look back N days (default: 14)
   - Calculate mean (μ) and standard deviation (σ)
   - Only uses historical data (excludes current day)

4. **Z-score calculation**:
   ```
   z = |current_value - μ| / σ
   ```

5. **Flag as outlier if**:
   ```
   z > z_threshold (default: 2.5)
   ```

### Key Features
- ✅ **Day-of-week awareness**: Compares Saturdays to Saturdays, Sundays to Sundays
- ✅ **Historical only**: Uses only past data for baseline
- ✅ **Configurable window**: 14-day default, falls back to smaller if insufficient data
- ✅ **Multiple metrics**: Can detect on win_share, loss_share, or wins_per_loss

### Output Columns
- `the_date`, `winner`
- `z`: Z-score value
- `nat_outlier_pos`: Boolean flag (TRUE if outlier)

### Used By
- Carrier dashboard outlier markers
- Suppression planning (identifies target dates)

---

## 2. Pair-Level (DMA) Outlier Detection

### Purpose
Identify specific **winner-loser-DMA combinations** with anomalous volume.

### Method: **Multiple Detection Strategies**

```python
# From cube_outliers.sql
cube_outliers(store_glob, ds, mover_ind, start_date, end_date,
             window=14, z_nat=2.5, z_pair=2.0)
```

### Algorithm

**A. Z-Score Method** (Similar to national)
1. Calculate pair wins: `winner-loser-DMA` daily wins
2. Partition by day type (Weekday/Sat/Sun)
3. Rolling window (14 days default)
4. Z-score: `z = (current - μ) / σ`
5. Flag if `z > z_pair` (default: 2.0)

**B. Percentage Jump Method**
- Flag if current > 1.3 × historical mean
- Catches "30% spikes" even if variance is high
- Column: `pct_outlier_pos`

**C. New Pair Detection**
- Flag if this winner-loser-DMA combination has **no history**
- Column: `new_pair` (mu IS NULL OR mu = 0)

**D. Rare Pair Detection**
- Flag if historical mean < 2.0 wins/day
- Column: `rare_pair`
- Catches pairs with low typical volume

### Key Features
- ✅ **Multi-strategy**: Combines z-score, percentage, and volume heuristics
- ✅ **Granular**: DMA-level detection
- ✅ **Context-aware**: Knows what's "normal" for each specific pairing
- ✅ **Linked to national**: Only includes days where national is an outlier

### Output Columns
- `the_date`, `winner`, `loser`, `dma_name`
- `pair_wins_current`: Current day wins
- `pair_mu_wins`, `pair_sigma_wins`: Historical stats
- `pair_z`: Z-score for pair
- `pct_outlier_pos`: Boolean (30% jump)
- `new_pair`: Boolean (no history)
- `rare_pair`: Boolean (low volume)
- **Plus all national-level columns**

### Used By
- Suppression plan builder (Stage 1: auto suppressions)
- Win cube generation

---

## 3. Competitor Mode Outliers (Dashboard)

### Purpose
Detect anomalies in **head-to-head matchups** for competitor analysis.

### Method: **Day-Type Grouped Z-Score on H2H Metrics**

```python
# From carrier_dashboard_duckdb.py compute_competitor_pdf()
# Applied per competitor in a 1-vs-many analysis
```

### Algorithm

1. **Calculate H2H metrics**:
   - `h2h_wins / primary_total_wins` (win_share)
   - `h2h_losses / primary_total_losses` (loss_share)
   - `h2h_wins / h2h_losses` (wins_per_loss)

2. **Group by**:
   - Winner (primary carrier)
   - Competitor (loser)
   - Day type (Weekday/Sat/Sun)

3. **Rolling window** (default: 14 days):
   - Calculate μ and σ per group
   - Shift by 1 to exclude current day

4. **Z-score**:
   ```python
   z = (current_metric - μ) / σ
   ```

5. **Flag outliers**:
   ```python
   is_outlier = (z > z_threshold)
   ```

### Key Features
- ✅ **Head-to-head focused**: Analyzes specific matchups
- ✅ **Interactive dashboard**: Real-time visualization
- ✅ **Configurable parameters**: Window and threshold adjustable in UI

### Output
- DataFrame with `zscore` and `is_outlier` columns
- Visualized as star markers (positive) and minus signs (negative) on charts

---

## 4. Combined Cube Outliers (Full Pipeline)

### Purpose
Generate a **complete cube** of all potential suppression targets with QA columns.

### Method: **National + Pair Combined**

```python
# From cube_outliers.sql
# Joins national outliers with pair-level details
```

### Workflow

1. **Identify national outlier days**:
   - Run national z-score detection
   - Filter to `nat_outlier_pos = TRUE`

2. **For each national outlier day**:
   - Get all winner-loser-DMA pairs
   - Calculate pair-level statistics
   - Apply multiple detection strategies

3. **Enrich with context**:
   - National share baseline
   - Pair-level baselines
   - Multiple outlier flags

4. **Optional filtering**:
   - Can return only outlier days (`only_outliers=True`)
   - Or full cube with all data (`only_outliers=False`)

### Output
Full cube with columns from both levels:
- **National**: nat_zscore, nat_outlier_pos, nat_mu_share, etc.
- **Pair**: pair_z, pct_outlier_pos, new_pair, rare_pair, etc.
- **Data**: All wins, losses, DMAs, etc.

### Used By
- `build_win_cube.py`: Pre-compute outlier cube to CSV
- Suppression plan builder: Stage 1 auto-suppression logic

---

## Detection Parameters

### Common Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `window` | 14 | Rolling window size (days) |
| `z_nat` / `z_thresh` | 2.5 | National-level z-score threshold |
| `z_pair` | 2.0 | Pair-level z-score threshold |
| `metric` | 'win_share' | Metric to analyze (win_share, loss_share, wins_per_loss) |

### Dashboard Parameters

| Parameter | Default | Adjustable In UI |
|-----------|---------|------------------|
| `outlier_window` | 14 | ✅ Slider (7-60) |
| `outlier_z` | 3.0 | ✅ Slider (1.0-4.0) |
| `outlier_show` | 'All' | ✅ Radio (All/Positive only) |

---

## Suppression Stage Logic

### Stage 1: Auto-Suppression

Targets pairs that meet **any** of:
1. **Z-score outlier**: `pair_z > 2.0`
2. **Percentage spike**: `pct_outlier_pos = TRUE` (>30% above baseline)
3. **Rare pair**: `rare_pair = TRUE` (baseline < 2.0)
4. **New pair**: `new_pair = TRUE` (no history)

**Plus volume minimum**: Current day wins must exceed historical mean

### Stage 2: Distributed Suppression

If Stage 1 doesn't remove enough:
- Take remaining pairs by capacity (descending)
- Distribute remaining needed suppressions
- No outlier flagging needed (just capacity-based)

---

## Visualization

### Carrier Dashboard

**Positive Outliers** (z > threshold):
- ⭐ Yellow star markers
- Hover shows: date, metric value, z-score, day type

**Negative Outliers** (z < -threshold):
- ➖ Red minus sign markers  
- Hover shows: date, metric value, z-score, day type

**Configuration**:
- Toggle "Show outliers: All / Positive only"
- Adjust window and z-threshold with sliders

---

## SQL Templates

| File | Purpose |
|------|---------|
| `nat_outliers.sql` | National-level z-score detection |
| `cube_outliers.sql` | Combined national + pair detection |
| `national_timeseries.sql` | Raw metrics (no outlier detection) |
| `competitor_view.sql` | H2H metrics (no outlier detection) |
| `pair_metrics.sql` | Pair-level data (no outlier detection) |

---

## Python Functions

### In `suppression_tools/src/outliers.py`

```python
# National outliers only
national_outliers(store_glob, ds, mover_ind, start_date, end_date,
                 window=14, z_thresh=2.5, state=None, dma_name=None, 
                 metric='win_share')

# Full cube with pair-level details
cube_outliers(store_glob, ds, mover_ind, start_date, end_date,
             window=14, z_nat=2.5, z_pair=2.0, only_outliers=True,
             state=None, dma_name=None)
```

### In Dashboards

**Carrier Dashboard** (`carrier_dashboard_duckdb.py`):
```python
compute_national_pdf()  # Calls national_outliers()
compute_competitor_pdf()  # Computes outliers inline
```

**Suppression Tools** (`main.py`):
```python
scan_base_outliers()  # Wrapper for national_outliers()
```

---

## Performance Considerations

### Current (Parquet Scans)
- National outliers: ~1-3 seconds
- Full cube outliers: ~5-10 seconds (large datasets)

### With DuckDB + Cubes
- **From cube tables**: <100ms 🚀
- **Why**: Data already aggregated at the right granularity

### Optimization Strategy
1. Use cube tables for pair-level data
2. Pre-compute rolling stats in cube
3. Query cube with simple filters instead of complex CTEs

---

## Decision Tree

```
Is national share an outlier?
├─ NO → Day is normal, no suppression needed
└─ YES → Check pair-level details
    ├─ Pair z-score > threshold? → Auto-suppress
    ├─ Pair has 30% spike? → Auto-suppress
    ├─ Pair is new/rare? → Auto-suppress
    └─ Otherwise → Consider for Stage 2 (distributed)
```

---

## Next Steps: Cube-Based Outlier Detection

With cube tables in the database, we can:

1. **Pre-compute rolling stats** in cube build
2. **Add outlier columns** to cube tables
3. **Query instantly** instead of calculating on-the-fly
4. **Update incrementally** as new data arrives

Example new cube columns:
- `nat_mu_share_14d`, `nat_sigma_share_14d`
- `nat_zscore`, `nat_outlier_pos`
- `pair_mu_wins_14d`, `pair_sigma_wins_14d`  
- `pair_zscore`, `pair_outlier_pos`

This would make outlier detection **orders of magnitude faster**! 🚀
