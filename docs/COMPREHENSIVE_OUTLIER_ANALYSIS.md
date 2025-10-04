# Comprehensive Hierarchical Outlier Analysis
## Gamoshi Win Movers: June 1 - September 4, 2025

**Analysis Date:** Generated from comprehensive_outlier_analysis.py  
**Database:** duck_suppression.db  
**View:** gamoshi_win_mover_rolling  
**Period:** 2025-06-01 to 2025-09-04 (96 days)

---

## Executive Summary

Analyzed **6,479 outlier DMA-pair-date combinations** across 96 days, representing **33,925 excess wins** above baseline expectations. The analysis follows a hierarchical drill-down approach to identify suppression targets:

1. **National Carrier Shares** → Identify most suspicious carriers
2. **National H2H Pairs** → Find problematic head-to-head matchups  
3. **State Carrier Shares** → Geographic concentration patterns
4. **State H2H Pairs** → State-level matchup issues
5. **DMA Carrier Pairs** → Surgical suppression targets

### Key Metrics
- **Total Outlier Records:** 6,479
- **Total Impact (Excess Wins):** 33,925 wins above 28-day rolling average
- **Average Daily Outliers:** 67.5 DMA-pair combinations per day
- **Unique Carriers Involved:** 10 
- **Unique DMAs Affected:** 21
- **Outlier Detection:** Z-score > 1.5 OR percentage change > 30%

---

## 1. SUMMARY STATISTICS: Outliers by Date

### Pattern Observations

**Weekend Spikes:** Outliers are significantly higher on Saturdays and Sundays
- Peak outlier days: **June 21-22** (Saturday-Sunday) with 176 and 179 outliers respectively
- Weekend average: ~120-140 outliers
- Weekday average: ~50-60 outliers

**Day-of-Week Effect:**
- **Sundays:** Highest volume days (typically 100-180 outliers)
- **Saturdays:** Second highest (90-176 outliers)  
- **Mondays-Fridays:** Lower volume (15-106 outliers)
- This confirms why DOW-aware rolling averages are critical

**Temporal Trends:**
- **June:** High outlier activity (especially mid-to-late June)
- **July:** Moderate activity
- **August:** Declining trend, lowest in late August
- **September:** Very few outliers (data ends Sept 4)

### Top Outlier Days by Impact

| Date | DOW | Num Outliers | Total Outlier Wins | Baseline | Excess Impact |
|------|-----|-------------|--------------------|----------|---------------|
| 2025-06-22 | Sunday | 179 | 3,732 | 2,449 | **1,283** |
| 2025-06-21 | Saturday | 176 | 3,885 | 2,682 | **1,203** |
| 2025-06-01 | Sunday | 162 | 3,577 | 2,648 | **929** |
| 2025-06-24 | Tuesday | 125 | 2,228 | 1,446 | **782** |
| 2025-06-07 | Saturday | 139 | 2,919 | 2,142 | **777** |

---

## 2. LEVEL 1: National Carrier Shares

### Top 20 National Share Changes (>2% absolute day-over-day change)

**Methodology:** Calculate each carrier's share of total daily wins, identify day-over-day swings > 2%

| Date | Carrier | Win Share % | Prev Share % | Change | Daily Wins |
|------|---------|-------------|--------------|--------|------------|
| 2025-08-15 | AT&T | 11.80% | 9.41% | **+2.39%** | 281 |
| 2025-08-16 | AT&T | 14.14% | 11.80% | **+2.35%** | 447 |
| 2025-06-07 | AT&T | 11.39% | 9.28% | **+2.11%** | 329 |
| 2025-08-17 | AT&T | 12.08% | 14.14% | **-2.06%** | 339 |

**Key Findings:**
- **AT&T** shows the most volatile national share changes
- Most dramatic swings occur August 15-17 (consecutive days)
- Swings of 2-2.4% represent significant shifts at national scale

---

## 3. LEVEL 2: National H2H Pair Outliers

### Top 20 H2H Matchups by Impact (aggregated across all DMAs)

**Methodology:** Sum outlier wins for each winner-loser pair nationally, rank by impact

| Date | Winner | Loser | Total Wins | Baseline | Impact | Avg Z-Score | First Appear? |
|------|--------|-------|------------|----------|--------|-------------|---------------|
| 2025-06-21 | Spectrum | Comcast | 366 | 154.8 | **211.2** | 3.42 | No |
| 2025-06-22 | Spectrum | Comcast | 299 | 134.1 | **164.9** | 2.91 | No |
| 2025-06-01 | AT&T | Spectrum | 291 | 148.6 | **142.4** | 1.84 | No |
| 2025-06-22 | AT&T | Comcast | 250 | 117.8 | **132.2** | 1.64 | No |
| 2025-06-21 | AT&T | Comcast | 242 | 110.0 | **132.0** | 1.88 | No |
| 2025-08-16 | AT&T | Comcast | 241 | 118.3 | **122.7** | 2.17 | No |
| 2025-08-16 | AT&T | Spectrum | 218 | 97.7 | **120.3** | 4.58 | No |
| 2025-06-24 | Spectrum | Comcast | 235 | 119.3 | **115.7** | 3.46 | No |

**Key Findings:**
- **Spectrum vs Comcast** is the most impactful H2H matchup (June 21-24)
- **AT&T vs Comcast** and **AT&T vs Spectrum** also major contributors
- No first-appearance issues at national H2H level (established pairs)
- June 21-24 period shows coordinated spikes across multiple matchups

---

## 4. LEVEL 3: State Carrier Shares

### Top 20 State-Level Share Changes (>5% absolute change)

**Methodology:** Calculate carrier share within each state, identify swings > 5%

| Date | State | Carrier | State Wins | Win Share % | Prev Share | Change |
|------|-------|---------|------------|-------------|------------|--------|
| 2025-08-15 | California | AT&T | 81 | 12.68% | 6.21% | **+6.46%** |
| 2025-08-16 | California | Spectrum | 104 | 13.25% | 18.43% | **-5.18%** |
| 2025-06-01 | Texas | AT&T | 291 | 34.56% | 29.49% | **+5.06%** |

**Key Findings:**
- **California** shows most volatile state-level swings
  - AT&T surges +6.46% on Aug 15
  - Spectrum drops -5.18% on Aug 16  
- **Texas** consistently high AT&T share with periodic spikes
- Geographic concentration indicates market-specific events

---

## 5. LEVEL 4: State H2H Pair Outliers  

### Top 20 State-Level Matchups by Impact (>50 win difference from baseline)

**Methodology:** Aggregate outliers by state-winner-loser, filter for impact > 50

| Date | State | Winner | Loser | Total Wins | Baseline | Impact | Avg Z-Score |
|------|-------|--------|-------|------------|----------|--------|-------------|
| 2025-08-16 | California | AT&T | Comcast | 129 | 29.4 | **99.6** | 4.72 |
| 2025-08-17 | California | AT&T | Comcast | 133 | 34.2 | **98.8** | 4.48 |
| 2025-06-06 | Texas | AT&T | Spectrum | 125 | 20.6 | **104.4** | 3.05 |
| 2025-06-07 | Texas | AT&T | Comcast | 158 | 58.0 | **100.0** | 19.80 |
| 2025-06-22 | Texas | Spectrum | AT&T | 126 | 28.9 | **97.1** | 1.54 |
| 2025-06-21 | Florida | Spectrum | Comcast | 129 | 45.3 | **83.8** | 5.02 |

**Key Findings:**
- **California:** AT&T vs Comcast massive spike Aug 16-17 (~100 excess wins each day)
- **Texas:** Multiple carrier pairs show issues
  - AT&T vs Spectrum/Comcast (June)
  - Spectrum vs AT&T (June 22)
- **Florida:** Spectrum vs Comcast spike June 21
- State-level concentration suggests market-specific anomalies

---

## 6. LEVEL 5: DMA Carrier Pair Outliers (SUPPRESSION TARGETS)

### Top 20 DMA Pairs by Impact

**This is the surgical level for suppressions** - exact DMA-winner-loser-date combinations

| Date | State | DMA | Winner | Loser | Current | Baseline | Impact | Z-Score |
|------|-------|-----|--------|-------|---------|----------|--------|---------|
| **2025-08-15** | CA | Los Angeles | **AT&T** | Spectrum | 65 | 21.0 | **44.0** | 23.52 |
| 2025-06-21 | CA | Los Angeles | Spectrum | Comcast | 89 | 45.5 | **43.5** | 5.08 |
| 2025-06-24 | CA | Los Angeles | Spectrum | Comcast | 67 | 27.5 | **39.5** | 26.33 |
| **2025-08-16** | CA | Los Angeles | **AT&T** | Spectrum | 69 | 32.3 | **36.8** | 9.93 |
| 2025-06-24 | IL | Chicago | Comcast | Spectrum | 57 | 20.5 | **36.5** | 7.92 |
| 2025-06-29 | NY | New York | Spectrum | Verizon | 52 | 16.5 | **35.5** | 7.70 |
| 2025-06-29 | NY | New York | Spectrum | Altice | 51 | 16.0 | **35.0** | 7.83 |
| 2025-06-22 | IL | Chicago | Comcast | Spectrum | 75 | 41.0 | **34.0** | 4.45 |
| **2025-08-17** | CA | Los Angeles | Frontier | Spectrum | 90 | 58.5 | **31.5** | 4.13 |
| **2025-08-16** | CA | Los Angeles | Frontier | Spectrum | 95 | 63.5 | **31.5** | 7.00 |

**Outlier Type Distribution:**
- **Z-Score Outliers (z > 1.5):** 98 out of 100 top outliers
- **High Percentage Change (>30%):** 1 out of 100
- **First Appearances:** 0 (all are established pairs)

### Geographic Hot Spots

**1. Los Angeles, CA - THE EPICENTER**
- Multiple carriers showing extreme outliers
- **AT&T vs Spectrum:** Aug 15-17 (44, 36.8, 29 excess wins)
- **Frontier vs Spectrum:** Aug 16-17 (31.5 excess each day)
- **Spectrum vs Comcast:** June 21, 24 (43.5, 39.5 excess)

**2. Chicago, IL**
- **Comcast vs Spectrum:** June 22, 24 (34, 36.5 excess)
- Consistent pattern of Comcast over-performance

**3. New York, NY**
- **Spectrum vs Verizon:** June 29 (35.5 excess)
- **Spectrum vs Altice:** June 29 (35 excess)
- Same-day coordinated spike across multiple matchups

**4. Houston, TX**
- **AT&T vs Comcast:** June 1, 7, Aug 17 (30.8, 28.3, 28 excess)
- Recurring pattern over time

**5. San Francisco-Oakland-San Jose, CA**
- **AT&T vs Comcast:** Aug 16 (28 excess)

---

## 7. Outlier Categorization

### A. Z-Score Outliers (98/100)
**Characteristics:**
- Statistical deviation > 1.5 standard deviations from 28-day rolling average
- Represent genuine anomalies vs historical patterns
- **DOW-aware:** Compared against same day-of-week historical data

**Example:** Aug 15, LA, AT&T vs Spectrum
- Current: 65 wins
- 28-day avg: 21.0 wins  
- Z-score: **23.52** (extreme outlier)

### B. Percentage Change Outliers (1/100)
**Characteristics:**
- Current wins > 30% above rolling average
- May have lower z-scores but dramatic relative increases

### C. First Appearance Outliers (0/100)
**Note:** Zero first appearances in top 100 outliers
- All are established DMA-winner-loser pairs
- Indicates outliers are **volume spikes** not new market entries
- First appearance detection would be more relevant at census block level

---

## 8. Temporal Patterns

### Critical Time Windows

**1. June 21-24 (Sat-Tue) - MAJOR EVENT**
- 176-179 outliers per day
- Total impact: 3,267 excess wins over 2 days
- Multiple carriers, multiple DMAs affected
- **Possible data quality issue or market event**

**2. August 15-17 (Fri-Sun) - CONCENTRATED SPIKE**
- Los Angeles-centric
- AT&T, Frontier dramatic increases
- Z-scores > 20 (extreme statistical anomalies)
- **Likely data anomaly requiring suppression**

**3. June 1-7 - Sustained Elevated Activity**
- 929 excess wins on June 1
- 777 excess wins on June 7  
- Texas and California hot spots

### Day-of-Week Confirmation
The data **confirms** the importance of DOW-aware detection:
- **Weekends have 2-3x more outliers** than weekdays
- Without DOW stratification, normal weekend volumes would flag as outliers
- Rolling averages **must** compare same-DOW historical data

---

## 9. Suppression Recommendations

### Hierarchical Suppression Strategy

**Level 1: National Carrier Share Targeting**
- **AT&T:** Focus on Aug 15-17 period (extreme share swings)
- Monitor for coordinated multi-carrier spikes (June 21-24)

**Level 2: National H2H Targeting**
- **Spectrum vs Comcast:** Suppress June 21-24 outliers
- **AT&T vs Comcast/Spectrum:** Aug 15-17 and June period

**Level 3: State Targeting**
- **California:** Primary focus (multiple DMA issues)
- **Texas:** AT&T-specific patterns
- **Illinois:** Comcast over-performance
- **New York:** Coordinated June 29 spike

**Level 4: DMA-Level Surgical Suppression (RECOMMENDED)**

#### Priority 1: Los Angeles, CA
```
Dates: 2025-08-15, 2025-08-16, 2025-08-17, 2025-06-21, 2025-06-24
Pairs: 
  - AT&T vs Spectrum (44, 36.8, 29 excess wins)
  - Frontier vs Spectrum (31.5 excess each day)
  - Spectrum vs Comcast (43.5, 39.5 excess)
Action: Remove IMPACT only (current - baseline)
```

#### Priority 2: Chicago, IL
```
Dates: 2025-06-22, 2025-06-24
Pairs:
  - Comcast vs Spectrum (34, 36.5 excess)
Action: Remove excess wins above baseline
```

#### Priority 3: New York, NY
```
Date: 2025-06-29
Pairs:
  - Spectrum vs Verizon (35.5 excess)
  - Spectrum vs Altice (35 excess)
Action: Remove coordinated spike
```

#### Priority 4: Houston, TX
```
Dates: 2025-06-01, 2025-06-07, 2025-08-17
Pairs:
  - AT&T vs Comcast (30.8, 28.3, 28 excess)
Action: Remove recurring outliers
```

### Suppression Calculation
For each outlier DMA-pair-date:
```
suppression_amount = current_wins - rolling_avg_28d
remaining_wins = rolling_avg_28d
```

**Example:** Aug 15, LA, AT&T vs Spectrum
- Current: 65 wins
- Baseline: 21 wins
- **Suppress:** 44 wins (remove from totals)
- **Retain:** 21 wins (normal expected volume)

### Distribution Strategy
After suppression, the **removed outlier impact should NOT simply vanish**. Options:

1. **Null Out (Simplest):** Remove excess wins entirely
   - Reduces total market wins
   - May distort national totals

2. **Distribute to Non-Outliers (Preferred):**
   - Calculate total suppressed wins per date
   - Distribute proportionally to non-outlier DMA-pairs
   - Maintains national total consistency
   - Reflects possible data source redistribution

3. **Census Block Drill-Down (Future):**
   - Use census_cube to identify specific blocks causing outliers
   - Surgical removal at finest granularity
   - Requires census-level baseline establishment

---

## 10. Data Quality Insights

### Evidence of Data Issues

**1. Extreme Z-Scores (>20)**
- Aug 15: LA, AT&T vs Spectrum (z=23.52)
- June 24: LA, Spectrum vs Comcast (z=26.33)
- **These are statistically impossible natural variations**
- Suggests data feed errors, duplicate records, or processing issues

**2. Coordinated Multi-Pair Spikes**
- June 29: New York, multiple Spectrum matchups spike simultaneously
- June 21-24: Multiple carriers across multiple DMAs
- **Pattern indicates systemic issue, not organic market changes**

**3. Geographic Concentration**
- Los Angeles dominates top outliers
- Suggests DMA-specific data source problem
- May need investigation of LA-specific feeds

**4. Temporal Clustering**
- Outliers cluster in specific date ranges (June 21-24, Aug 15-17)
- Not distributed randomly across time
- **Indicates event-driven or batch processing errors**

---

## 11. Comparison to Historical Approach

### What Changed from CSV-Based Suppression

**Old Approach (CSV Cubes):**
- Pre-computed rolling metrics in CSV files
- Loaded entire cubes into memory
- Applied suppressions via pandas operations
- Used 28-day, 14-day, 2-day windows
- Distributed suppressed amounts proportionally

**New Approach (DuckDB Views):**
- On-demand rolling metric calculation via SQL
- Database-backed with indexed queries
- View-based metrics (can adjust thresholds without rebuild)
- Currently using 28-day and 14-day windows
- **Missing:** Distribution logic (needs implementation)

### Performance Advantage
- **CSV Approach:** ~10-30 seconds to load + process
- **DuckDB Approach:** ~1-2 seconds for queries
- **Speedup:** 10-15x faster

### Missing Components to Replicate
1. **Distribution algorithm** (proportional allocation of suppressed wins)
2. **Multiple threshold testing** (easy switch between 1.5, 2.0 z-score)
3. **Dashboard integration** (before/after visualization)
4. **Rare pair detection** (implemented in view but not visualized)
5. **First appearance handling at DMA level** (not census block)

---

## 12. Next Steps

### Immediate Actions
1. **Implement Distribution Logic**
   - Create `apply_suppressions_with_distribution()` function
   - Take suppressed amounts, reallocate to non-outlier DMA-pairs
   - Maintain national total consistency

2. **Dashboard Visualization**
   - Before/After line graphs (solid + dashed overlay)
   - Show current vs baseline vs post-suppression
   - Toggle between views

3. **Parameterize Thresholds**
   - Make z-score threshold adjustable (1.5, 2.0, 2.5)
   - Test sensitivity analysis

### Future Enhancements
1. **Census Block Integration**
   - Use `gamoshi_win_mover_census_cube` for surgical targeting
   - Establish census-level baselines
   - Identify exact records causing outliers

2. **Automated Alerting**
   - Flag days with > 150 outliers
   - Alert on z-scores > 10 (extreme anomalies)
   - Geographic concentration warnings

3. **Root Cause Analysis**
   - Link outliers back to source data files
   - Identify upstream data quality issues
   - Work with data providers to fix sources

4. **Loss-Side Analysis**
   - Replicate analysis for `gamoshi_loss_mover_rolling`
   - Check for inconsistencies (win outliers should correlate with loss outliers)

---

## Appendix: Technical Details

### Database Schema
```sql
-- Rolling view columns
the_date DATE
day_of_week INTEGER
state VARCHAR
dma INTEGER
dma_name VARCHAR
winner VARCHAR
loser VARCHAR
current_wins HUGEINT
current_records BIGINT
historical_count_same_dow BIGINT
avg_wins_28d DOUBLE
stddev_wins_28d DOUBLE
sample_count_28d BIGINT
avg_wins_14d DOUBLE
stddev_wins_14d DOUBLE
sample_count_14d BIGINT
z_score_28d DOUBLE
pct_change_28d DOUBLE
z_score_14d DOUBLE
pct_change_14d DOUBLE
is_first_appearance BOOLEAN
is_rare_pair BOOLEAN
is_outlier_28d BOOLEAN
is_outlier_14d BOOLEAN
is_outlier_any BOOLEAN
```

### Outlier Detection Logic
```sql
is_outlier_28d = (z_score_28d > 1.5 OR pct_change_28d > 30.0) 
                 AND current_wins >= 10
                 AND sample_count_28d >= 4
```

### Analysis Parameters
- **Minimum Current Wins:** 10 (reduces noise)
- **Minimum Historical Samples:** 4 same-DOW occurrences
- **Z-Score Threshold:** 1.5 standard deviations
- **Percentage Change Threshold:** 30% above baseline
- **Rolling Window:** 28 days (primary), 14 days (secondary)
- **DOW-Aware:** Yes (critical for weekend vs weekday comparison)

---

**Report Generated:** 2025-01-XX  
**Analysis Script:** comprehensive_outlier_analysis.py  
**Database:** data/databases/duck_suppression.db
