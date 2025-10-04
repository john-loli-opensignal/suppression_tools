# Suppression Analysis Report - Mover: False

**Dataset:** gamoshi
**Dates Analyzed:** 2025-06-19, 2025-08-15, 2025-08-16, 2025-08-17, 2025-08-18
**Generated:** 2025-10-03 21:14:48

---

## 📊 Executive Summary

- **National outliers detected:** 60
- **H2H pair outliers detected:** 13950
- **Census blocks analyzed:** 263
- **Blocks flagged for suppression:** 222
- **Data retention rate:** 15.6%
- **Wins suppressed:** 352 / 443

---

## 🚨 Step 1: National-Level Outliers

Detected **60** national-level outlier events.

### Top 10 Outliers by Z-Score

| Date | Carrier | Z-Score |
|------|---------|---------|
| 2025-08-16 | AT&T | 15.25 |
| 2025-08-15 | Central Utah Telephone | 11.51 |
| 2025-08-16 | CenturyLink | 9.22 |
| 2025-08-17 | AT&T | 8.78 |
| 2025-08-16 | Antietam Broadband | 6.83 |
| 2025-08-18 | Rock Solid Internet & Telephone | 5.17 |
| 2025-08-16 | Arbuckle Communications | 5.16 |
| 2025-08-17 | CenturyLink | 5.01 |
| 2025-08-17 | Frontier | 4.97 |
| 2025-08-15 | Pocketinet Communications | 4.77 |

---

## 🔍 Step 2: H2H Pair Outliers

Detected **13950** H2H pair outlier records.

### Outlier Types

- **New pairs (first appearance):** 682
- **Rare pairs (< 3 appearances):** 12695
- **Percentage spikes:** 4782

### Top 10 Pair Outliers

| Date | Winner | Loser | DMA | Z-Score | Wins |
|------|--------|-------|-----|---------|------|
| 2025-08-16 | AT&T | Cox Communications | Los Angeles, CA | 18.75 | 22 |
| 2025-08-16 | AT&T | Comcast | San Francisco-Oakland-San Jose, CA | 14.78 | 99 |
| 2025-08-17 | AT&T | Cox Communications | Los Angeles, CA | 14.61 | 21 |
| 2025-08-16 | CenturyLink | Comcast | Denver, CO | 13.17 | 42 |
| 2025-08-15 | Central Utah Telephone | Frontier | Salt Lake City, UT | 13.11 | 9 |
| 2025-08-17 | AT&T | Spectrum | Los Angeles, CA | 12.51 | 153 |
| 2025-08-15 | AT&T | Cox Communications | Los Angeles, CA | 12.27 | 14 |
| 2025-08-17 | AT&T | Comcast | San Francisco-Oakland-San Jose, CA | 11.66 | 115 |
| 2025-08-17 | Frontier | Spectrum | San Francisco-Oakland-San Jose, CA | 10.68 | 6 |
| 2025-08-15 | AT&T | Spectrum | Los Angeles, CA | 10.61 | 113 |

---

## 🎯 Step 3: Census Block Drill-Down

Analyzed **263** census blocks.

Flagged **222** blocks for suppression (84.4%).

**Data retention rate: 15.6%** ✅

### Suppression Reasons

- **Z-score outliers:** 222
- **Spike ratio outliers:** 144
- **First appearances:** 108
- **Rare appearances:** 102
- **Concentration outliers:** 5

### Suppression by Date

| Date | Total Blocks | Suppressed | Retention % | Wins Suppressed |
|------|--------------|------------|-------------|-----------------|
| 2025-06-19 | 12 | 12 | 0.0% | 16 |
| 2025-08-15 | 73 | 65 | 11.0% | 108 |
| 2025-08-16 | 91 | 78 | 14.3% | 108 |
| 2025-08-17 | 84 | 64 | 23.8% | 114 |
| 2025-08-18 | 3 | 3 | 0.0% | 6 |

---

## 📈 Step 4: Before/After Visualization


### Win Share Time Series

![Win Share Before/After](graphs/win_share_before_after_mover_False.png)


### Target Dates Comparison

![Target Dates Comparison](graphs/target_dates_comparison_mover_False.png)


---

## 💡 Recommendations

1. **Review flagged census blocks** - Validate that suppression reasons are legitimate

2. **Adjust thresholds** - Fine-tune Z-score, spike ratio, and concentration thresholds

3. **Implement surgical suppression** - Use census block IDs for precise removal

4. **Monitor retention rate** - Aim for >70% data retention while removing outliers

5. **Track suppression impact** - Compare product metrics before/after suppression
