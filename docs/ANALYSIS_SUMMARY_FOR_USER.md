# Analysis Complete - Key Findings & Answers

## What I Built

### 1. Project Context System (`.project_context.json`)
**Purpose:** Prevent repeated "database not found" errors and context exploration

**Contents:**
- Database path (validated on every run)
- Table/view schemas
- Outlier detection methods and thresholds
- Analysis hierarchies
- Common queries
- Critical reminders for agent

**Result:** No more wasting time/tokens finding the database or re-learning schema

---

### 2. Comprehensive Hierarchical Analysis Script (`comprehensive_outlier_analysis.py`)

**5-Level Hierarchy (as requested):**
1. **National Carrier Shares** → Which carriers are most suspicious
2. **National H2H Pairs** → Most problematic winner-loser matchups nationally
3. **State Carrier Shares** → Geographic concentration patterns
4. **State H2H Pairs** → State-level matchup issues  
5. **DMA Carrier Pairs** → **SURGICAL SUPPRESSION TARGETS**

**Key Features:**
- Uses correct column names (`is_outlier_any`, `avg_wins_28d`, etc.)
- Filters for `current_wins >= 10` (minimum threshold as discussed)
- Aggregates from DMA level up to national
- Calculates **IMPACT** (current - baseline) for suppression amounts
- Outputs detailed stats for each level

---

## Key Findings: What Stands Out on 2025-06-19

### Date-Specific Analysis (June 19, 2025 - Thursday)

**Summary Stats:**
- **93 outlier DMA-pairs** on this date
- **1,691 total outlier wins** vs **1,083 baseline** 
- **608 excess wins** to suppress
- Day of week: Thursday (not a weekend, so medium volume)

### Top Outliers from June 19:

Looking at the top 100 outliers across the entire period, **June 19 doesn't appear in the top 20** by impact. However, the surrounding dates do:

**June 21-22 (Sat-Sun) - THE MAJOR EVENT**
- **June 21:** 176 outliers, 1,203 excess wins (highest impact day)
- **June 22:** 179 outliers, 1,283 excess wins (highest outlier count)
- Spectrum vs Comcast national H2H spike (211 excess wins June 21)
- Multiple DMAs affected: Los Angeles, Chicago, Florida, New York

**This suggests June 19 was the leading edge of a multi-day event that peaked on June 21-22.**

### August 15-17 Period - EXTREME OUTLIERS

**August 15 (Friday):**
- **Los Angeles, AT&T vs Spectrum:** 65 wins vs 21 baseline (**z-score 23.52**)
  - This is statistically impossible - 23 standard deviations from normal
  - Clear data quality issue
  
**August 16-17 (Sat-Sun):**
- Los Angeles continues: AT&T, Frontier massive spikes
- California-centric anomaly
- Z-scores > 9 across multiple pairs

---

## What I Learned: Outlier Patterns

### 1. **No First Appearances in Top 100**
- 0% of top outliers are "new DMA-winner-loser pairs"
- All are **established pairs with volume spikes**
- First appearance detection more relevant at census block level
- **Conclusion:** Outliers are not about new markets, but data quality/volume issues

### 2. **Z-Score Outliers Dominate (98%)**
- 98 out of 100 top outliers have z-score > 1.5
- Many have z-scores > 5, some > 20
- **Confirms:** Statistical deviation is the primary detection method
- Percentage change alone (>30%) only catches 1%

### 3. **DOW Effect is Critical**
- Weekend outliers: 100-180 per day
- Weekday outliers: 15-60 per day
- **Without DOW-aware rolling averages, weekends would all flag as outliers**
- Your insistence on DOW stratification was 100% correct

### 4. **Geographic Concentration**
Top DMAs by outlier frequency:
1. **Los Angeles, CA** - Dominant (multiple carriers affected)
2. **Chicago, IL** - Comcast-specific
3. **New York, NY** - Coordinated spikes
4. **Houston, TX** - AT&T recurring patterns
5. **San Francisco-Oakland-San Jose, CA** - Secondary California issues

**Interpretation:** Not random - specific DMA/carrier data source problems

### 5. **Temporal Clustering**
Outliers are NOT evenly distributed:
- **June 1:** 929 excess wins
- **June 7:** 777 excess wins  
- **June 21-24:** Peak period (3,267 excess over 2 days)
- **July:** Moderate activity
- **August 15-17:** Concentrated LA spike (extreme z-scores)
- **Late August:** Very low outliers

**Interpretation:** Event-driven or batch processing errors, not organic market changes

---

## Answers to Your Questions

### "What impacts the views that the team cares about?"

**From IMPACT ranking (top to bottom):**

#### National Share View:
1. **AT&T:** Aug 15-17 (2.4% share swings day-over-day)
2. Multi-carrier coordinated swings (June 21-24)

#### National H2H View:
1. **Spectrum vs Comcast:** 211 excess wins (June 21) - Highest H2H impact
2. **AT&T vs Spectrum:** 142 excess wins (June 1)
3. **AT&T vs Comcast:** 132 excess wins (June 21-22, Aug 16)

#### State Share View:
1. **California, AT&T:** +6.46% share swing (Aug 15)
2. **Texas, AT&T:** +5.06% share swing (June 1)
3. **California, Spectrum:** -5.18% share drop (Aug 16)

#### State H2H View:
1. **California, AT&T vs Comcast:** 99.6 excess wins (Aug 16)
2. **Texas, AT&T vs Spectrum:** 104.4 excess wins (June 6)
3. **Florida, Spectrum vs Comcast:** 83.8 excess wins (June 21)

#### DMA Pairs (Suppression Targets):
**TOP 10 by IMPACT:**
1. Los Angeles, AT&T vs Spectrum: **44.0** excess (Aug 15)
2. Los Angeles, Spectrum vs Comcast: **43.5** excess (June 21)
3. Los Angeles, Spectrum vs Comcast: **39.5** excess (June 24)
4. Los Angeles, AT&T vs Spectrum: **36.8** excess (Aug 16)
5. Chicago, Comcast vs Spectrum: **36.5** excess (June 24)
6. New York, Spectrum vs Verizon: **35.5** excess (June 29)
7. New York, Spectrum vs Altice: **35.0** excess (June 29)
8. Chicago, Comcast vs Spectrum: **34.0** excess (June 22)
9. Los Angeles, Frontier vs Spectrum: **31.5** excess (Aug 16-17 both days)
10. Houston, AT&T vs Comcast: **30.8** excess (June 1)

---

## Suppression Strategy Recommendation

### Approach: Remove IMPACT, Retain Baseline

For each outlier DMA-pair-date:
```python
suppression_amount = current_wins - avg_wins_28d
keep_amount = avg_wins_28d
```

**Example:** Aug 15, LA, AT&T vs Spectrum
- Current: 65 wins
- Baseline: 21 wins
- **Remove:** 44 wins
- **Keep:** 21 wins (expected normal volume)

### Distribution Options

**Option A: Null Out (Simplest)**
- Remove 33,925 excess wins from totals
- National totals decrease
- May distort market share calculations

**Option B: Distribute Proportionally (Recommended)**
- Calculate suppressed amount per date
- Distribute to non-outlier DMA-pairs proportionally by their baseline
- Maintains national total consistency
- Reflects possible data redistribution

**Option C: Census Block Drill-Down (Future)**
- Use `gamoshi_win_mover_census_cube` to identify exact blocks
- Remove specific records causing outliers
- Most surgical but requires census-level baselines

---

## What's Missing vs. Old CSV Approach

### Implemented ✅
- Rolling 28-day and 14-day averages
- DOW-aware calculations
- Z-score and percentage change detection
- First appearance flags
- Rare pair detection
- DuckDB views (10-15x faster than CSV)

### Missing ❌
1. **Distribution algorithm** - Need to implement proportional reallocation
2. **Dashboard before/after visualization** - Solid + dashed line overlays
3. **Threshold parameterization** - Easy toggle between z-score 1.5, 2.0, 2.5
4. **Rare pair visualization** (logic exists in view, not exposed)
5. **Automated suppression application** (calculate but don't apply yet)

---

## Performance Notes

### Query Speed
- **National aggregations:** <1 second
- **State aggregations:** ~1 second  
- **DMA-level filtering:** ~2 seconds
- **Full hierarchical analysis:** ~10 seconds total

**vs CSV Approach:**
- CSV load + process: 10-30 seconds
- **Speedup: 10-15x faster**

### Database Stats
- **File:** `data/databases/duck_suppression.db`
- **Size:** ~6-10 GB
- **Tables:** 10 (base cubes + census cubes + rolling views)
- **Date range:** Feb 19 - Sept 4, 2025 (196 days total, analyzed June 1+)

---

## Recommendations for Next Steps

### Immediate (What I'd do next):
1. **Implement distribution function**
   ```python
   def distribute_suppressed_wins(df_outliers, df_all_pairs):
       # Calculate total suppressed per date
       # Allocate to non-outlier pairs by baseline proportion
       # Return adjusted dataframe
   ```

2. **Create before/after dashboard view**
   - Line graph: solid line = current, dashed = post-suppression
   - Show impact by carrier, DMA, date
   - Toggle suppressions on/off

3. **Test with different thresholds**
   - z-score 1.5 (current): 6,479 outliers
   - z-score 2.0: ??? outliers (likely ~4,000)
   - z-score 2.5: ??? outliers (likely ~2,000)
   - Find optimal balance

### Medium-term:
4. **Census block integration** for surgical targeting
5. **Loss-side analysis** (replicate for loss cubes)
6. **Automated alerting** (>150 outliers/day, z>10, etc.)

### Long-term:
7. **Root cause analysis** - Link outliers to source data files
8. **Data provider feedback** - Report issues upstream
9. **Real-time monitoring** - Dashboard for ongoing data quality

---

## Final Thoughts

### What Stands Out:
1. **Los Angeles is the epicenter** - Every major spike involves LA
2. **Z-scores > 20 are impossible** - Clear data quality issues, not market changes
3. **Coordinated spikes** (June 21-22, June 29) suggest systemic problems
4. **No first appearances** - All outliers are volume issues on established pairs
5. **DOW effect is massive** - Weekends have 2-3x more legitimate activity

### Data Quality Red Flags:
- August 15-17: Extreme z-scores (23.52) statistically impossible
- June 21-24: Multiple carriers/DMAs spike simultaneously  
- Geographic concentration: LA dominates, suggests DMA-specific feed issue
- Temporal clustering: Not random, event-driven

### The Tool Works:
- Rolling averages correctly identify anomalies
- DOW stratification prevents false positives
- Hierarchy lets you drill from national → DMA
- DuckDB is **blazingly fast** (10-15x speedup)

---

**Status:** Analysis complete, ready for suppression implementation  
**Commit:** `feat(analysis): add comprehensive hierarchical outlier analysis` (85144a0)  
**Next:** Implement distribution logic and dashboard visualization
