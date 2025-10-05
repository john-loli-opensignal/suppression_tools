# Additional Critical Context for Outlier Detection Model

## What the Other Model Needs to Know About Suppression

### 1. DMA-Carrier Pairs Are The Key Unit of Analysis

**Why 200K-300K combinations matter:**
- Current dataset: 175K DMA × carrier pairs (mover segment only)
- With non-mover segment: ~350K pairs total
- With future growth: Could reach 500K+ pairs

**This is computationally significant because:**
- Each pair needs rolling metrics computed daily
- 175K pairs × 198 days = 34.6M data points to analyze
- Must complete in <30 seconds for dashboard interactivity
- This rules out certain computationally expensive methods (ARIMA per series, deep learning, etc.)

### 2. The Suppression Goal Is NOT Anomaly Detection Alone

**Current misconception**: "Detect all anomalies"
**Actual goal**: "Detect anomalies that **distort higher-level aggregates**"

**Example Scenario:**
- AT&T shows 3σ spike in "Topeka, KS" DMA (small market)
- But Topeka contributes 0.01% of national volume
- This spike doesn't affect national win share
- **Decision**: Don't suppress (not impactful)

**Contrast:**
- AT&T shows 1.5σ spike in "Los Angeles" DMA (huge market)
- LA contributes 8% of national volume
- This spike makes national win share look volatile
- **Decision**: Suppress (high impact)

**Implication**: Anomaly score should incorporate:
- Magnitude of deviation (z-score)
- Volume significance (current_wins count)
- Hierarchical impact (affects state/national?)

### 3. First Appearance Is Overweighted in Current System

**The Problem:**
- 528 first appearances per 100 observations
- Current system flags ALL first appearances
- But most are legitimate market expansion

**Why so many first appearances?**
1. **Data collection rollout**: New DMAs added over time
2. **Carrier expansion**: Small carriers enter new markets gradually
3. **Customer movement**: People move to new areas with their carrier
4. **Sparse pairs**: "Carrier X vs Carrier Y" might not occur in every DMA every day

**What makes a first appearance suspicious?**
- **Coordinated**: Many carriers show first appearance in same DMA on same date → data backfill
- **Impossible geography**: Carrier appears in remote DMA before appearing in nearby DMAs → data error
- **Volume inconsistency**: First appearance has 1000+ wins → should have appeared before
- **Temporal inconsistency**: First appearance early in time series → probably data start, not real

**What makes a first appearance legitimate?**
- **Gradual expansion**: Carrier appeared in adjacent DMAs in prior weeks
- **Low volume**: First appearance has 10-50 wins → plausible market entry
- **Consistent with state trends**: State-level data shows gradual increase
- **Single series**: Only this DMA pair shows first appearance, not coordinated

### 4. We Care About Win Share Stability More Than Win Count Accuracy

**Business stakeholders don't ask:**
- "Did AT&T win 1,234 or 1,256 customers in LA on June 19?"

**They ask:**
- "What is AT&T's national win share trend over Q2?"
- "Is AT&T gaining or losing ground vs Verizon?"
- "Which markets show competitive threats?"

**Implication for suppression:**
- If we suppress 100 wins from AT&T in LA, we should ideally give them to someone else
- Total market size should remain roughly constant
- But relative shares (AT&T vs Verizon) should be corrected

**This is why we redistribute!**
- Suppressing without redistribution would make market look smaller than it is
- Redistributing proportionally maintains market structure
- Alternative: Don't redistribute, but then totals become unstable

### 5. The "Top 50 Carriers" Filter Is Critical

**Why we focus on top 50:**
- Top 50 carriers account for ~80% of total wins
- Remaining 579 carriers are long tail (small regional players)
- Stakeholders don't care about "Bob's Cellular" with 12 wins nationwide

**Computational benefit:**
- 50 carriers × 211 DMAs × 50 common competitors = ~10K series (not 175K)
- Focused analysis is 17x faster
- Can use more sophisticated methods

**Challenge for your model:**
- "Top 50" is dynamic (changes over time)
- A carrier in top 50 nationally might be #100 in certain states
- Should your model still track them in those states? (Probably yes, for consistency)

### 6. Census Block Level: Aspiration vs Reality

**Why census blocks are interesting:**
- **Surgical precision**: Identify exactly which records are problematic
- **Fraud detection**: Impossible for one block to have 1000+ carrier switches
- **Geographic patterns**: Outliers clustered in specific neighborhoods indicate data quality issues

**Why census blocks are impractical:**
- **Scale**: Millions of series (exact count unknown, but huge)
- **Sparsity**: Most blocks have 0-5 wins per day
- **Computation**: Can't compute rolling metrics for all blocks in real-time
- **Storage**: Full census block cube would be 100+ GB

**Compromise approach:**
- Use DMA-level detection to identify problematic dates/carriers
- **Then** drill down to census block for those specific cases
- This is "lazy evaluation" - only compute what's needed

**Your model should:**
- Primarily operate at DMA × carrier pair level
- Optionally accept census block data for drill-down
- Provide "drill-down recommended" flag when census block analysis would help

### 7. Outlier Types We Care About (Prioritized)

**Tier 1 (Must Catch)**:
1. **Data duplication**: Same wins counted multiple times
2. **Batch processing errors**: Entire day's data for a region arrives late, gets counted twice
3. **Impossible values**: Census block has 10,000 wins (more than population)
4. **Coordinated spikes**: All carriers in a DMA spike simultaneously

**Tier 2 (Should Catch)**:
5. **First appearance storms**: 100+ new DMA pairs appear same day
6. **Sustained anomalies**: Carrier maintains 3σ spike for 7+ days (likely data issue, not real event)
7. **Inconsistent aggregates**: DMA sum ≠ state sum for same carrier/date
8. **Reverse mismatches**: "AT&T beats Verizon" count ≠ "Verizon loses to AT&T" count

**Tier 3 (Nice to Catch, But Low Priority)**:
9. **Mild spikes**: 1.5-2σ deviations that don't affect national metrics
10. **Low-volume anomalies**: Spikes in small markets with <50 wins
11. **Legitimate campaigns**: Real marketing events (we actually want to keep these!)

### 8. Explainability Examples

**Good explanations** (what stakeholders understand):
- "AT&T in Los Angeles: 2.5σ spike (200 wins vs 80 expected), first appearance in this market, not seen in nearby DMAs"
- "Verizon nationwide: First appearance storms across 50 DMAs on June 19, suggests data backfill event"
- "T-Mobile in Texas: Sustained 3σ elevation for 8 days (June 15-22), inconsistent with national trend staying flat"

**Bad explanations** (too technical):
- "Series 12,394: Hidden Markov Model detected regime change with posterior probability 0.94"
- "Anomaly score: 0.87 from ensemble of LSTM, Isolation Forest, and Bayesian changepoint detection"
- "Mahalanobis distance of 3.2 in 5-dimensional feature space"

**Your model should output:**
- Primary reason (spike/first_appearance/sustained/coordinated)
- Magnitude in business terms (X wins vs Y expected, Z% change)
- Context (how it compares to state/national trend)
- Confidence (based on observation count, history length, z-score)

### 9. The Multi-Round Problem

**Why suppression happens in rounds:**
1. **Round 1**: Detect outliers at DMA level, suppress impact
2. **Recompute**: Aggregate suppressed data to state/national level
3. **Round 2**: Detect NEW outliers (some were masked by Round 1 outliers)
4. **Repeat**: Until state/national metrics are stable OR 5 rounds reached

**Challenge**: "Stable" is subjective
- No formal convergence criterion
- Currently: "National win share max deviation < 2%" (rule of thumb)
- Better: "95% of carrier-date pairs have <1% win share change between rounds"

**Implication for your model:**
- Must be **deterministic** (same input → same output)
- Must **not** oscillate (suppress in Round 1, unsuppress in Round 2)
- Should **converge** quickly (ideally 2-3 rounds, max 5)
- Should provide **round-over-round diagnostics** (how much changed?)

### 10. Null Hypothesis Considerations

**Current approach assumes:**
- H0: "This DMA-carrier pair follows its own 28-day rolling distribution"
- H1: "This observation is an outlier from that distribution"

**Problems with this:**
- Assumes stationarity (distribution doesn't change over time)
- Ignores external factors (seasonality, campaigns, competitor actions)
- Treats each pair independently (ignores correlation)

**Alternative null hypotheses to consider:**
- H0: "This pair's deviation is consistent with state-level changes" (hierarchical)
- H0: "This pair's deviation is consistent with correlated carriers' changes" (multivariate)
- H0: "This pair is experiencing a regime shift to a new steady state" (changepoint-aware)
- H0: "This pair has seasonal pattern X, and this observation fits that pattern" (seasonality-aware)

**Your model might benefit from:**
- Multiple null hypotheses tested hierarchically
- Bonferroni or FDR correction for multiple testing (175K series!)
- Posterior probability of each hypothesis rather than binary decision

---

## Summary: Key Insights for Your Model

1. **Scale**: 175K series, but focus on top 50 carriers (10K series) for practical reasons
2. **Goal**: Reduce FP rate from 60% to <10%, keep high recall for data quality issues
3. **Hierarchy**: DMA detection must validate against state/national aggregates
4. **First Appearances**: Highly overweighted in current system, need better discrimination
5. **Impact-Based**: Weight anomalies by their effect on higher-level metrics
6. **Multi-Round**: Model will be called iteratively, must converge
7. **Census Blocks**: Aspirational drill-down, not primary analysis level
8. **Explainability**: Business language, not just statistical jargon
9. **Redistribution**: Suppressed wins get redistributed, affects downstream analysis
10. **Speed**: <30 seconds for 34M data points (175K series × 198 days)

## Questions to Ask Us

1. Can you provide labeled examples of "definitely real events" vs "definitely data errors"?
2. What's the acceptable trade-off between FP and FN? (Currently: prefer FN to minimize FP)
3. Should the model automatically adjust thresholds, or use fixed thresholds?
4. Do you want probabilistic scores or binary decisions?
5. Should we suppress data we're uncertain about (conservative) or keep it (aggressive)?

Let us know if you need more context!
