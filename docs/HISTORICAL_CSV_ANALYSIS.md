# Analysis of Historical CSV Cube Approach and Distribution Strategy

## Date: 2025-01-XX
**Analyzed commits:** 6ed5582, 5e4d75c, and related dashboard implementations

---

## Key Findings from Historical Implementation

### 1. **CSV Cube + Parquet Scanning Approach**

The original implementation (commit `6ed5582`) used:
- **Data Source:** Parquet files scanned directly via DuckDB (`parquet_scan`)
- **Storage:** `duckdb_partitioned_store/` directory with 6,014 parquet files (~938MB)
- **Performance Issue:** Full scans on every page load were expensive

**What Made It Work:**
```python
# They computed outliers at NATIONAL level first (aggregated)
compute_outliers_duckdb(ds_glob, filters, winners, window, z_thresh, start_date, end_date)

# This used DOW-partitioned rolling stats with lookback buffer
# Default: 56-day lookback for proper DOW statistics
# Window: 14 or 28 days (2 or 4 weeks of same DOW)
```

### 2. **The Distribution Algorithm (The Missing Piece!)**

This is what we've been missing! The historical approach had a **2-stage distribution**:

#### **Stage 1: Targeted Auto-Suppression**
Remove outliers based on multiple triggers:
- Z-score based (pair_z > threshold)
- 30% jump detection (pct_outlier_pos)
- Rare pairs (low historical volume)
- **New pairs (first appearance!)**

```python
auto = sub[(sub.get('pair_outlier_pos')==True) |
           (sub.get('pct_outlier_pos')==True) |
           (sub.get('rare_pair')==True) |
           (sub.get('new_pair')==True)].copy()

# Enforce minimum volume (>5 wins on the day)
auto = auto[auto['pair_wins_current'] > 5]

# Remove calculation:
# - If baseline < 5: remove ALL (new/rare pair)
# - Else: remove EXCESS (current - baseline)
pw = current_wins
mu_eff = baseline_wins
remove_all = mu_eff < 5.0
rm_excess = np.ceil(np.maximum(0.0, pw - mu_eff))
auto['rm_pair'] = np.where(remove_all, np.ceil(pw), rm_excess).astype(int)

# Budget constraint: stop when you've removed enough
auto['cum'] = auto['rm_pair'].cumsum()
auto['rm1'] = np.where(auto['cum']<=need, auto['rm_pair'], 
                       np.maximum(0, need - auto['cum'].shift(fill_value=0))).astype(int)
```

#### **Stage 2: Equalized Distribution**
If Stage 1 didn't remove enough, spread remaining evenly:

```python
# Calculate how much still needs to be removed
need_after = need - auto['rm1'].sum()

# Get all pair-DMA combinations
caps = sub[['loser','dma_name','pair_wins_current']].copy()

# Distribute evenly across all pairs
m = len(caps)  # number of pair-DMA combos
base = need_after // m  # floor division

# Each gets base amount (capped at their volume)
caps['rm_base'] = np.minimum(caps['pair_wins_current'], base).astype(int)

# Remaining units distributed 1-by-1 to pairs with highest residual
remaining = need_after - caps['rm_base'].sum()
caps = caps.sort_values(['residual','pair_wins_current'], ascending=[False, False])
caps['extra'] = 0
if remaining > 0:
    idx = caps.index[caps['residual']>0][:remaining]
    caps.loc[idx, 'extra'] = 1

caps['rm2'] = (caps['rm_base'] + caps['extra']).astype(int)
```

### 3. **The "Need" Calculation**

The target removal amount was calculated from national share statistics:

```python
# National totals
W = nat_total_wins       # Winner's total wins
T = nat_market_wins      # Market total wins
mu = nat_mu_share        # Winner's historical share

# Calculate how many wins to remove to get back to mu*T
need = int(np.ceil(max((W - mu*T) / max(1e-12, (1-mu)), 0)))
```

This formula comes from:
```
target_wins = mu * T
current_wins = W
excess = W - mu*T

# But we also need to account for market shrinking
# If we remove X from winner, total becomes T-X
# We want: (W-X) / (T-X) = mu
# Solve for X: X = (W - mu*T) / (1 - mu)
```

### 4. **Visualization Approach**

The dashboard showed before/after with dashed overlay:

```python
# Base series (solid lines)
fig = create_plot(base_view, metric, analysis_mode, primary, label="Base")

# Overlay outliers as yellow stars
for w in sorted(out_view['winner'].unique()):
    ow = out_view[out_view['winner']==w]
    # Get y-values from base_view at outlier dates
    ys = base_view[base_view['winner']==w][['the_date', metric]]
    m = ow.merge(ys, on='the_date', how='left').dropna(subset=['y'])
    fig.add_trace(go.Scatter(
        x=m['the_date'], y=m['y'], mode='markers', name=f"{w} outlier",
        marker=dict(symbol='star', color='yellow', size=12),
        showlegend=False
    ))

# Suppressed series (dashed lines, different traces)
f2 = create_plot(supp_view, metric, analysis_mode, primary, label="Suppressed")
for tr in f2.data:
    tr.line['dash'] = 'dash'  # Make it dashed
    fig.add_trace(tr)
```

**Key insight:** They created separate traces for base and suppressed, both visible, so you could see the delta.

---

## What We've Been Doing Wrong

### Problem 1: No Distribution Strategy
Our current `REMOVE_OUTLIERS.md` just removes at the top level without distributing:
- We detect outliers ✓
- We calculate excess ✓
- But we don't distribute the removal across pair-DMA combinations ✗

**Impact:** Small or no visible change because we're not actually modifying the granular data.

### Problem 2: Not Using the Right Formula
We've been using simple `current - avg` which doesn't account for market dynamics:
```python
# Wrong (what we've been doing):
remove = current - avg

# Right (what the original did):
need = (W - mu*T) / (1 - mu)  # Accounts for market shrinking
```

### Problem 3: Missing First Appearance Detection
The original system had `new_pair` flag to catch pairs that never appeared before.
- We discussed this but never implemented it!
- This is critical for catching data quality issues

### Problem 4: Visualization Approach
Our graphs show no difference because:
- We're not creating separate dashed traces
- We're not ensuring suppressed data is visibly lower
- We may not be recalculating win_share after removal

---

## Recommendations: What We Need to Implement

### Immediate Actions

1. **Implement 2-Stage Distribution Algorithm**
   - Stage 1: Target specific outliers (z-score, 30% jump, rare, new)
   - Stage 2: Equalize remaining across all pairs
   - Use proper `need` formula accounting for market dynamics

2. **Add First Appearance Detection**
   - Track pair-DMA combos that never appeared before
   - Flag them as high priority for Stage 1 removal
   - Use census block for the finest granularity

3. **Fix the Suppression Flow**
   ```
   National Outliers → Calculate Need → 
   Stage 1 (targeted) → Stage 2 (equalized) → 
   Apply to pair-DMA level → Recalculate metrics → 
   Visualize with dashed overlay
   ```

4. **Proper Visualization**
   - Create separate traces for base (solid) and suppressed (dashed)
   - Ensure both are visible simultaneously
   - Add yellow stars for outlier markers
   - Show win_share recalculated after suppression

### Technical Implementation

#### Step 1: Detect National Outliers (✓ We have this)
```python
outliers = national_outliers(ds, mover_ind, start_date, end_date)
```

#### Step 2: Calculate Need per Outlier (NEED TO ADD)
```python
for date, winner in outliers:
    W = current_total_wins
    T = market_total_wins
    mu = historical_share
    need = int(np.ceil(max((W - mu*T) / (1 - mu), 0)))
```

#### Step 3: Build Suppression Plan with Distribution (NEED TO ADD)
```python
# Get all pair-DMA combinations for this winner/date
pairs = get_all_pairs_for_winner(ds, mover_ind, date, winner)

# Stage 1: Auto-suppress outlier pairs
auto = pairs[
    (pairs['pair_z'] > z_thresh) |
    (pairs['pct_change'] > 0.30) |
    (pairs['pair_mu_wins'] < 5) |  # Rare
    (pairs['first_appearance'] == True)  # New!
]
auto['rm'] = np.where(
    auto['pair_mu_wins'] < 5,
    auto['pair_wins_current'],  # Remove all for new/rare
    np.ceil(auto['pair_wins_current'] - auto['pair_mu_wins'])  # Remove excess
)
auto = auto.sort_values('pair_z', ascending=False)
auto['cum'] = auto['rm'].cumsum()
auto = auto[auto['cum'] <= need]

# Stage 2: Distribute remaining
need_after = need - auto['rm'].sum()
if need_after > 0:
    remaining_pairs = pairs[~pairs.index.isin(auto.index)]
    m = len(remaining_pairs)
    base_removal = need_after // m
    # ... (full algorithm from above)
```

#### Step 4: Apply and Visualize (NEED TO FIX)
```python
# Apply suppressions at pair-DMA-census_block level
suppressed_data = apply_suppression_plan(raw_data, plan)

# Recalculate win_share
suppressed_agg = recalculate_national_metrics(suppressed_data)

# Plot both
fig = go.Figure()
# Base (solid)
for carrier in base_agg['winner'].unique():
    df = base_agg[base_agg['winner']==carrier]
    fig.add_trace(go.Scatter(x=df['the_date'], y=df['win_share'],
                            mode='lines', name=carrier, line=dict(dash='solid')))

# Suppressed (dashed)
for carrier in suppressed_agg['winner'].unique():
    df = suppressed_agg[suppressed_agg['winner']==carrier]
    fig.add_trace(go.Scatter(x=df['the_date'], y=df['win_share'],
                            mode='lines', name=f"{carrier} (suppressed)",
                            line=dict(dash='dash'), showlegend=False))

# Outlier markers
for _, row in outliers.iterrows():
    # Get y-value from base
    y = base_agg[(base_agg['the_date']==row['the_date']) & 
                 (base_agg['winner']==row['winner'])]['win_share'].values[0]
    fig.add_trace(go.Scatter(x=[row['the_date']], y=[y], mode='markers',
                            marker=dict(symbol='star', color='yellow', size=14),
                            showlegend=False))
```

---

## Performance Considerations

### Why CSV Cubes Were Slow
- 6,014 files needed globbing/scanning
- DuckDB had to open each file
- Metadata queries expensive

### Why Database Cubes Are Fast
- Single file (duck_suppression.db)
- Indexed columns
- Query planner optimizes
- Already aggregated at right granularity

### Our Advantage
We have the best of both worlds:
- ✓ Fast database cubes
- ✓ Pre-aggregated data
- ✓ Indexed properly
- ✗ Missing: Distribution algorithm
- ✗ Missing: First appearance detection
- ✗ Missing: Proper visualization

---

## Next Steps Priority

1. **HIGH:** Implement 2-stage distribution algorithm
2. **HIGH:** Add first appearance detection (census block level)
3. **HIGH:** Fix visualization to show clear before/after
4. **MEDIUM:** Add 30% jump detection
5. **MEDIUM:** Add rare pair detection (baseline < 5)
6. **LOW:** Optimize further with caching

---

## Code References

- Distribution algorithm: commit `5e4d75c:main.py` lines ~400-450
- Outlier detection: commit `6ed5582:carrier_suppression_dashboard.py` 
- Visualization: commit `6ed5582` search for `tr.line['dash'] = 'dash'`
- Need calculation: commit `5e4d75c:main.py` search for `W - mu*T`

---

## Conclusion

The original CSV approach was **algorithmically superior** in terms of:
1. **Distribution strategy** - 2-stage targeted + equalized
2. **Outlier triggers** - Multiple detection methods including first appearance
3. **Need calculation** - Accounts for market dynamics
4. **Visualization** - Clear before/after with dashed overlay

Our current database approach is **architecturally superior** in terms of:
1. **Performance** - 50-200x faster queries
2. **Maintainability** - Single database file
3. **Scalability** - Proper indexing and optimization

**We need to port the algorithmic intelligence to our new architecture!**
