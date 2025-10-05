# Brief for Alternative Outlier Detection Model

## Filled Specification JSON

```json
{
  "python_version": "3.12",
  "env": {
    "engine": "duckdb",
    "streaming": false,
    "latency_s": 5,
    "memory_gb": 16
  },
  "data_profile": {
    "series_count": 174953,
    "cadence": "daily",
    "window_days": 198,
    "median_history_days": 70,
    "missing_pct": 0.0,
    "imputation": "none",
    "scaling": "zscore"
  },
  "structure": {
    "seasonality": [
      {
        "period_days": 7,
        "strength_0to1": 0.339
      }
    ],
    "trend_strength_0to1": 0.1,
    "acf_half_life_days": 7,
    "cp_density_per_100d": 1.5,
    "vol_clustering_pvalue": 0.05
  },
  "anomaly_shape": {
    "point_rate_per_100d": 59.25,
    "collective_runs_per_100d": 8.0,
    "median_run_length_days": 2,
    "median_amp_over_IQR": 2.15
  },
  "multivariate": {
    "feature_dims": 4,
    "median_pairwise_corr": 0.15,
    "cross_series_corr": 0.25
  },
  "labels_eval": {
    "has_labels": true,
    "monthly_expected_anomalies": 450,
    "fp_cost": "med",
    "fn_cost": "high"
  }
}
```

## Critical Context Not in JSON

### 1. Hierarchical Data Structure
- **DMA × Carrier Pairs**: 175K series (primary analysis level)
- **BUT**: Must validate against higher levels (state, national)
- **AND**: May need census block drill-down for root cause
- **Implication**: Can't treat series as independent

### 2. Extreme Sparsity at Granular Levels
- **First Appearance Rate**: 528 per 100 observations (!!)
- **Why**: New DMA pairs emerge constantly as:
  - Carriers expand markets
  - Data collection improves
  - Customers move to new areas
- **Challenge**: "First appearance" != "anomaly" in most cases

### 3. Very High Current False Positive Rate
- **Current System**: Flags 59.25% of observations as anomalies
- **Goal**: Reduce to <10% while keeping recall high
- **Root Cause**: Simple z-score threshold too aggressive for sparse, high-variance data

### 4. Minimum Threshold Rule
- **Hard Constraint**: Ignore all observations with ≤10 wins
- **Reason**: Business requirement to avoid noise from low-volume pairs
- **Implication**: Can't use fancy methods on tiny counts

### 5. Suppression Mechanics
- We **DON'T** remove entire data points
- We **DO** remove "impact" = (current - baseline)
- Then **redistribute** to maintain consistency
- **Example**: 200 wins when expecting 100 → remove 100, keep 100

### 6. Multi-Round Convergence
- Suppression happens in rounds (typically 3+)
- Each round: detect → suppress → re-aggregate → detect again
- **Goal**: Stable national/state metrics after final round
- **Implication**: Your model will be called iteratively

## Specific Questions Your Model Should Answer

1. **Is this spike "coordinated" or "isolated"?**
   - If 50 carriers in same DMA spike → data issue
   - If 1 carrier spikes across 50 DMAs → real event or data issue?
   - If 1 carrier spikes in 1 DMA → could be either

2. **Is this "first appearance" legitimate?**
   - Carrier active in adjacent DMAs? → Probably real expansion
   - Carrier never seen in state before? → Maybe data error
   - All carriers showing first appearance same day? → Definitely data issue

3. **Is this spike "consistent" with higher-level aggregates?**
   - DMA spike but state level normal? → Localized issue, suppress
   - DMA spike AND state spikes? → Real event, don't suppress
   - DMA spike but census blocks don't show concentration? → Distributed error

4. **What's the "confidence" of this anomaly?**
   - High confidence: Many prior observations, large z-score, inconsistent with hierarchical context
   - Low confidence: Few prior observations, borderline z-score, consistent with state trends
   - **Use**: Prioritize manual review, set suppression thresholds

5. **Can you explain it in business terms?**
   - Good: "AT&T in Los Angeles showed 2.5σ spike on June 19, inconsistent with California state trend"
   - Bad: "Hidden Markov Model posterior probability 0.94 for regime change"

## What Good Looks Like

**Before** (current system):
- National win share for AT&T: spiky, hard to see trends
- 60% of DMA-carrier-days flagged as anomalies
- Manual review queue: overwhelming

**After** (your model):
- National win share for AT&T: smooth, clear trends
- <10% of DMA-carrier-days flagged as anomalies
- Flags include confidence scores and explanations
- Known problematic dates (Feb 19, June 19, Aug 15-18) still caught
- Processing time: <30 seconds for full dataset

## Key Files

- `outlier_model_specification.json`: The filled JSON spec
- `CONTEXT_FOR_ALTERNATIVE_OUTLIER_MODEL.md`: Full detailed context (read this!)
- `data_profile_for_outlier_model.py`: Script to regenerate statistics
- `../data/databases/duck_suppression.db`: The actual data

## Next Steps for You

1. Read `CONTEXT_FOR_ALTERNATIVE_OUTLIER_MODEL.md` for full context
2. Explore `gamoshi_win_mover_cube` and `gamoshi_win_mover_rolling` tables
3. Reproduce our current outlier detection (see `../tools/src/metrics.py`)
4. Propose alternative approach with:
   - Lower FP rate
   - Hierarchical awareness
   - Computational efficiency
   - Explainability

## Contact Notes from User

> "DMA carrier pairs, which I imagine would be 200k-300k combinations"

Yes, exactly 175K in current data. Could grow to 300K with more carriers/DMAs.

> "We should be looking at DMA carrier pairs"

This is the sweet spot - granular enough to catch issues, aggregated enough to be computationally feasible.

> "Census blocks... to be surgical"

Aspirational. Census block level has millions of series. Explore if you can, but don't assume it's always available in production.

Good luck! Let us know if you need clarification or additional data.
