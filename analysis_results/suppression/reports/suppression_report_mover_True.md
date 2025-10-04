wh# Suppression Analysis Report - Mover: True

**Dataset:** gamoshi
**Dates Analyzed:** 2025-06-19, 2025-08-15, 2025-08-16, 2025-08-17, 2025-08-18
**Generated:** 2025-10-03 21:13:28

---

## 📊 Executive Summary

- **National outliers detected:** 105
- **H2H pair outliers detected:** 45156
- **Census blocks analyzed:** 104
- **Blocks flagged for suppression:** 95
- **Data retention rate:** 8.7%
- **Wins suppressed:** 124 / 133

---

## 🚨 Step 1: National-Level Outliers

Detected **105** national-level outlier events.

### Top 10 Outliers by Z-Score

| Date | Carrier | Z-Score |
|------|---------|---------|
| 2025-08-16 | Pavlov Media | 18.21 |
| 2025-08-16 | Apogee Telecom | 14.34 |
| 2025-08-17 | WhiteSky Communications | 14.00 |
| 2025-08-17 | Pavlov Media | 12.25 |
| 2025-08-16 | WhiteSky Communications | 10.31 |
| 2025-08-18 | WhiteSky Communications | 7.92 |
| 2025-08-16 | Single Digits | 7.36 |
| 2025-08-17 | Apogee Telecom | 6.51 |
| 2025-08-18 | Pavlov Media | 6.40 |
| 2025-08-16 | VNET Fiber | 6.36 |

---

## 🔍 Step 2: H2H Pair Outliers

Detected **45156** H2H pair outlier records.

### Outlier Types

- **New pairs (first appearance):** 4964
- **Rare pairs (< 3 appearances):** 38741
- **Percentage spikes:** 15417

### Top 10 Pair Outliers

| Date | Winner | Loser | DMA | Z-Score | Wins |
|------|--------|-------|-----|---------|------|
| 2025-08-16 | Spectrum | Packerland Broadband | Traverse City-Cadillac, MI | 16.49 | 15 |
| 2025-08-17 | Apogee Telecom | AT&T | Dallas-Ft. Worth, TX | 11.71 | 6 |
| 2025-08-15 | Apogee Telecom | Comcast | Atlanta, GA | 10.96 | 3 |
| 2025-06-19 | Spectrum | Comcast | Wichita-Hutchinson, KS Plus | 10.69 | 18 |
| 2025-08-15 | AT&T | Comcast | Fresno-Visalia, CA | 10.60 | 22 |
| 2025-08-17 | Pavlov Media | AT&T | Houston, TX | 10.29 | 6 |
| 2025-08-16 | Ozarks Electric Cooperative | Windstream | Ft. Smith-Fayetteville-Springdale-Rogers, AR | 9.39 | 5 |
| 2025-08-16 | Apogee Telecom | AT&T | Dallas-Ft. Worth, TX | 9.17 | 3 |
| 2025-08-15 | Spectrum | T-Mobile Fiber | Tampa-St. Petersburg, FL | 8.89 | 4 |
| 2025-08-16 | Spectrum | Hughes | Grand Rapids-Kalamazoo-Battle Creek, MI | 8.67 | 5 |

---

## 🎯 Step 3: Census Block Drill-Down

Analyzed **104** census blocks.

Flagged **95** blocks for suppression (91.3%).

**Data retention rate: 8.7%** ✅

### Suppression Reasons

- **Z-score outliers:** 94
- **Spike ratio outliers:** 75
- **First appearances:** 62
- **Rare appearances:** 32
- **Concentration outliers:** 6

### Suppression by Date

| Date | Total Blocks | Suppressed | Retention % | Wins Suppressed |
|------|--------------|------------|-------------|-----------------|
| 2025-06-19 | 10 | 10 | 0.0% | 10 |
| 2025-08-15 | 24 | 21 | 12.5% | 26 |
| 2025-08-16 | 37 | 36 | 2.7% | 54 |
| 2025-08-17 | 32 | 27 | 15.6% | 33 |
| 2025-08-18 | 1 | 1 | 0.0% | 1 |

---

## 📈 Step 4: Before/After Visualization


### Win Share Time Series

![Win Share Before/After](graphs/win_share_before_after_mover_True.png)


### Target Dates Comparison

![Target Dates Comparison](graphs/target_dates_comparison_mover_True.png)


---

## 💡 Recommendations

1. **Review flagged census blocks** - Validate that suppression reasons are legitimate

2. **Adjust thresholds** - Fine-tune Z-score, spike ratio, and concentration thresholds

3. **Implement surgical suppression** - Use census block IDs for precise removal

4. **Monitor retention rate** - Aim for >70% data retention while removing outliers

5. **Track suppression impact** - Compare product metrics before/after suppression
