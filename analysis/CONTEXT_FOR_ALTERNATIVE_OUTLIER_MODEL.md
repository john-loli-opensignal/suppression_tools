# Context for Alternative Outlier Detection Model

## Problem Domain: Carrier Win/Loss Suppression

### Business Context
We analyze "churn" data showing which mobile carriers won or lost customers in head-to-head (H2H) competitions. The goal is to detect and suppress anomalous data points that would distort market share analysis and competitive intelligence dashboards.

### Key Stakeholders & Use Cases
- **Product Teams**: Need clean national and state-level win share trends
- **Market Analysts**: Focus on DMA (Designated Market Area) level carrier pair performance
- **Data Quality Teams**: Identify data collection issues at source

### Hierarchical Analysis Structure
Outliers can originate at multiple levels and propagate up:

```
National Level (ds, mover_ind)
  ├─> H2H National (winner vs loser pairs)
      ├─> State Level H2H
          ├─> DMA Level H2H
              └─> Census Block Level (most granular)
```

**Critical Point**: Outliers detected at lower granularities (DMA, census block) often explain anomalies at higher levels (state, national). Our suppression strategy works top-down to identify issues but may need bottom-up investigation.

## Data Characteristics

### Scale & Granularity
- **Primary Analysis Level**: DMA × Carrier Pairs = **~175,000 time series**
- **Secondary Level**: Census Block × Carrier Pairs = **~millions of series** (too large for routine analysis)
- **Time Period**: 198 days (Feb 19, 2025 - Sep 4, 2025)
- **Daily Volume**: ~12,000 wins/day (avg), 40% coefficient of variation

### Hierarchical Series Counts
| Level | Series Count | Example |
|-------|--------------|---------|
| National | 2 | gamoshi_mover, gamoshi_non_mover |
| H2H National | 38,498 | "AT&T vs Verizon" (mover segment) |
| State H2H | 108,882 | "AT&T vs Verizon in California" |
| **DMA Pairs** | **174,953** | **"AT&T vs Verizon in Los Angeles DMA"** |
| Census Block | Millions | "AT&T vs Verizon in block 123..." |

### Carrier Distribution
- **Total Carriers**: 629
- **Highly Skewed**: Top carrier: 410K wins, Median: 192 wins
- **Long Tail**: Many carriers with minimal activity
- **Focus Strategy**: Top 50 carriers by volume account for 80%+ of wins

### Temporal Patterns
1. **Strong Weekly Seasonality**: 
   - Weekend multiplier: 1.38x weekday volume
   - Seasonality strength: 0.339 (moderate-high)
   - Saturday/Sunday show 38% higher activity
   
2. **No Missing Data**: 100% date coverage (all 198 days present)

3. **High Volatility**:
   - Day-to-day change: ~26% average
   - CV: 39.34%
   - Driven by: weekend cycles, data collection batch effects, real market events

### Sparsity & First Appearances
- **First Appearance Rate**: 528 per 100 observations
- This is VERY high because:
  - New DMA × carrier pairs emerge constantly
  - Carriers expand into new markets
  - Data collection coverage improves over time
- **Challenge**: Distinguish "legitimate new market entry" from "data quality issue"

## Current Outlier Detection Approach

### Method: Rolling Z-Score with DOW-Aware Windows
We use a 28-day rolling window (minimum 4 prior same-DOW observations) to compute:
- `rolling_avg_28d`: Mean of prior 28 days (same DOW)
- `rolling_stddev_28d`: Std dev of prior 28 days
- `zscore`: (current - rolling_avg) / rolling_stddev
- `pct_change`: (current - rolling_avg) / rolling_avg

### Outlier Criteria
A DMA × carrier pair × date is flagged if:
1. **Z-Score Spike**: `zscore > 1.5` AND `current_wins > 10`
2. **Percent Change**: `pct_change > 30%` AND `current_wins > 10`
3. **First Appearance**: `appearance_rank = 1` AND `current_wins > 10`

**Minimum Threshold**: We ignore observations with ≤10 wins to avoid noise from low-volume pairs.

### Observed Anomaly Rates
- **Spike Rate**: 59.25 per 100 observations (high!)
- **First Appearances**: 528 per 100 observations
- **Average Z-Score for Spikes**: 2.15

### Why So Many Anomalies?
1. **High Natural Variability**: Marketing campaigns, competitive actions cause real spikes
2. **Data Collection Artifacts**: Batch processing, delayed reporting, regional outages
3. **Sparse Time Series**: Many DMA pairs have intermittent activity
4. **Seasonality Mismatch**: Some carriers have non-weekly patterns (monthly billing cycles)

## Suppression Strategy

### Current Approach: "Hierarchical Impact-Based"
1. **Detect at DMA Level**: Find DMA × carrier pairs with outliers
2. **Calculate Impact**: `impact = current_wins - rolling_avg_28d`
3. **Suppress Impact Only**: Remove excess wins, keep baseline
4. **Distribute Suppressed Wins**: Redistribute to other carriers proportionally (controversial!)
5. **Validate at National Level**: Check if national H2H win shares are now stable

### Key Insight: We Don't Remove All Data
- If AT&T typically wins 100 in a DMA but has 200 today → remove 100, keep 100
- This preserves "normal" activity while suppressing anomalies
- Redistribution ensures total win counts remain consistent

### Multi-Round Refinement
- We run 3+ rounds of suppression
- Each round: detect outliers → suppress → recompute aggregates → detect again
- Goal: Converge to stable national/state level metrics

## What We Need from Your Model

### Primary Goal
**Reduce False Positives** while maintaining high recall for data quality issues.

Our current approach flags ~60% of observations as anomalies, which is too high. We need better discrimination between:
- **True Anomalies**: Data quality issues, collection errors, impossible values
- **Real Events**: Legitimate market changes, campaigns, expansions

### Specific Improvements Needed

1. **Better Seasonality Handling**
   - Current: Simple DOW adjustment
   - Needed: Handle monthly billing cycles, holiday effects, promotional periods

2. **Sparse Series Handling**
   - Many DMA pairs have <30 observations total
   - Current rolling window often insufficient
   - Consider: Hierarchical borrowing (use state/national patterns as priors)

3. **First Appearance Discrimination**
   - Current: Flag all first appearances
   - Needed: Distinguish "new market entry" from "data error"
   - Hint: Check if carrier is active in adjacent DMAs, if growth is gradual

4. **Collective Anomaly Detection**
   - Current: Point-wise only
   - Needed: Detect multi-day runs, sudden shifts, regime changes
   - Example: Carrier launches major campaign → sustained 2-week increase is normal

5. **Cross-Series Correlation**
   - Current: Each series independent
   - Needed: Use correlation between related carriers
   - Example: If all carriers in a DMA spike → likely data issue, not individual carrier problem

### Success Metrics

**Primary**: Reduce false positive rate from 60% to <10% while maintaining:
- Catch all dates flagged in manual review (Feb 19, June 19, Aug 15-18)
- Maintain stable national win shares after suppression
- Keep computational time <30 seconds for full dataset

**Secondary**:
- Detect rare "fraud" patterns (same census block, impossible volume)
- Early warning for data collection failures (many first appearances same day)
- Confidence scores for each anomaly (helps prioritize manual review)

## Critical Constraints

1. **Latency**: Results needed within seconds for interactive dashboard
2. **Explainability**: Analysts must understand WHY something is flagged
3. **Stability**: Small data changes shouldn't flip classifications
4. **Minimum Threshold**: Must respect `current_wins > 10` rule (business requirement)
5. **Hierarchical Validation**: Suppression at DMA level must fix state/national anomalies

## Multivariate Features Available

Beyond raw win counts, we have:
1. **total_wins**: Raw count (primary signal)
2. **win_share**: % of total wins in that DMA on that date
3. **h2h_ratio**: Winner wins / (winner wins + loser wins) for the pair
4. **dma_concentration**: How concentrated wins are (Gini coefficient)
5. **opposite_metric**: Loser wins in the reverse matchup (consistency check)

**Correlation Structure**:
- Related carriers (e.g., "AT&T Wireless", "AT&T Mobility"): ~0.7-0.9 correlation
- Unrelated carriers: ~0.1-0.2 correlation
- Same carrier across DMAs: ~0.3-0.5 (regional differences)

## Example Problematic Dates

### June 19, 2025
- Multiple carriers showed first appearances across many DMAs simultaneously
- Suggests: Data backfill or new collection source activated
- Current approach: Flags all as anomalies (probably wrong)

### August 15-18, 2025
- Sustained multi-day spikes for specific carrier pairs
- Could be: Real promotional campaign OR data duplication
- Current approach: Flags all 4 days (maybe correct, maybe not)

### February 19, 2025
- Data start date → many first appearances expected
- Current approach: Correctly flags, but should we suppress?

## Questions for Your Model

1. Can you distinguish "coordinated anomalies" (many series spike together → data issue) from "independent anomalies" (single carrier spikes → real event)?

2. Can you model the "censored" nature of the data? We only observe wins when a customer switches carriers. Zero wins doesn't mean zero market presence.

3. Can you provide uncertainty estimates? A spike of 200% with only 15 observations is less reliable than same spike with 1000 observations.

4. Can your model explain: "Anomaly because: spike of 2.5σ above 28-day DOW-adjusted baseline, inconsistent with state-level aggregates"?

5. Can you handle the computational constraint? 175K series × 198 days = 34M observations to score in <30 seconds?

## Files & Database Access

- **Database**: `data/databases/duck_suppression.db`
- **Tables**: 
  - `gamoshi_win_mover_cube`: Base aggregated data
  - `gamoshi_win_mover_rolling`: Rolling metrics and current outlier flags
  - `gamoshi_win_mover_census_cube`: Census block level (use sparingly, very large)
- **Scripts**: See `analysis/` directory for exploration scripts
- **Dashboard**: `carrier_dashboard_duckdb.py` shows current visualizations

## Additional Notes

- **"Movers" vs "Non-Movers"**: Two segments in the data. Movers change address + carrier, Non-Movers just change carrier. Analyzed separately because behavior differs.

- **"Distribution" Strategy**: After suppression, we redistribute removed wins to other carriers. This is controversial but maintains total consistency. Alternative models might avoid this.

- **Census Block Level**: We're exploring this for "surgical" suppression (identify exact records to remove) but computational cost is very high.

- **Why DuckDB**: Need sub-second query response for interactive dashboards with complex aggregations. Pandas too slow for this scale.

Let me know if you need any clarification or additional data profiling!
