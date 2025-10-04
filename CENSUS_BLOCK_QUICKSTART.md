# Census Block Anomaly Detection - Quick Start Guide

## 🚀 Quick Start

### Run Complete Analysis (5 Detection Methods)

```bash
# Analyze specific dates for a dataset
uv run python detect_census_block_anomalies.py \
  --ds gamoshi \
  --dates "2025-06-19,2025-08-15,2025-08-16,2025-08-17,2025-08-18" \
  --output census_block_analysis_results

# Generate visualizations and report
uv run python generate_anomaly_report.py
```

**Output:**
- 5 CSV files with detailed anomalies
- 7 PNG visualizations
- Comprehensive markdown report
- Summary statistics JSON

**Runtime:** ~2 minutes for 5 dates

---

## 📊 Detection Methods

### 1. Statistical Outliers (with DOW adjustment)
```bash
--z-threshold 3.0        # Z-score threshold (default: 3.0)
--iqr-multiplier 1.5     # IQR multiplier (default: 1.5)
```

### 2. First Appearances
Automatically detects new census block + carrier combinations.

### 3. Volume Spikes
```bash
--spike-multiplier 5.0   # Volume spike threshold (default: 5.0x)
```

### 4. Geographic Concentrations
Flags blocks contributing >80% of carrier's daily state-level activity.

### 5. Impossible Metrics
Automatically checks for data quality issues.

---

## 🗄️ Prerequisites

### 1. Build Census Block Cubes
```bash
# Build cubes for one dataset
uv run python build_census_block_cubes.py --ds gamoshi

# Build cubes for all datasets
uv run python build_census_block_cubes.py --all

# List available datasets
uv run python build_census_block_cubes.py --list
```

### 2. Verify Database
```bash
# Check tables exist
uv run python -c "import duckdb; con = duckdb.connect('duck_suppression.db'); \
print(con.execute('SHOW TABLES').df())"
```

**Required tables:**
- `{ds}_win_mover_census_cube`
- `{ds}_win_non_mover_census_cube`
- `{ds}_loss_mover_census_cube`
- `{ds}_loss_non_mover_census_cube`

---

## 📈 Example Workflow

### Standard Analysis
```bash
# Step 1: Build cubes (if not already built)
uv run python build_census_block_cubes.py --ds gamoshi

# Step 2: Run anomaly detection
uv run python detect_census_block_anomalies.py \
  --ds gamoshi \
  --dates "2025-08-15,2025-08-16,2025-08-17" \
  --output results

# Step 3: Generate report
uv run python generate_anomaly_report.py
```

### Custom Thresholds
```bash
# More sensitive detection
uv run python detect_census_block_anomalies.py \
  --ds gamoshi \
  --dates "2025-08-15" \
  --z-threshold 2.5 \
  --spike-multiplier 3.0 \
  --output results_sensitive
```

### Single Date Analysis
```bash
# Quick check for one date
uv run python detect_census_block_anomalies.py \
  --ds gamoshi \
  --dates "2025-08-16" \
  --output quick_check
```

---

## 📁 Output Files

### CSV Files (in `census_block_analysis_results/`)
- `gamoshi_statistical_outliers.csv` - Z-score & IQR outliers with DOW adjustment
- `gamoshi_first_appearances.csv` - New census block + carrier combinations
- `gamoshi_volume_spikes.csv` - Dramatic volume increases (>5x historical)
- `gamoshi_geographic_concentrations.csv` - Suspicious geographic clustering
- `gamoshi_summary_stats.json` - Aggregated statistics

### Visualizations (in `census_block_analysis_results/visualizations/`)
- `anomaly_type_distribution.png` - Overall distribution chart
- `anomalies_by_date.png` - Temporal breakdown
- `top_carriers_statistical_outliers.png` - Carrier rankings (outliers)
- `top_carriers_first_appearances.png` - Carrier rankings (new activity)
- `top_states_statistical_outliers.png` - Geographic distribution
- `dow_analysis.png` - Day-of-week patterns
- `mover_distribution.png` - Mover vs. non-mover breakdown

### Reports
- `CENSUS_BLOCK_ANOMALY_REPORT.md` - Full detailed report with all findings
- `CENSUS_BLOCK_ANALYSIS_SUMMARY.md` - Executive summary

---

## 🔍 Interpreting Results

### Statistical Outliers
**Columns to check:**
- `z_score` - How many standard deviations from mean (>3.0 = outlier)
- `mean_value` - Historical average for this DOW
- `metric_value` - Actual value on target date

**High Z-scores (>100):** Usually first appearances with mean=0

### First Appearances
**Significance:**
- High `metric_value` + new combination = potential concern
- Low `metric_value` + new combination = likely legitimate expansion

**Action:** Cross-reference with business records to validate.

### Volume Spikes
**Columns to check:**
- `spike_ratio` - Current / historical average (>5.0x = spike)
- `avg_historical` - Baseline expectation
- `metric_value` - Current value

**Action:** Investigate ratios >10x immediately.

### Geographic Concentrations
**Columns to check:**
- `contribution_pct` - Percentage of daily state-level activity (>0.8 = flag)
- `block_metric` - This block's contribution
- `total_daily` - Total daily activity in state

**Action:** Review blocks >90% contribution for data quality issues.

---

## ⚙️ Advanced Usage

### Adjust Lookback Period
Edit `detect_census_block_anomalies.py`:
```python
def get_dow_baseline(self, mover_ind, metric_type, lookback_days=90):
    # Change 90 to desired days
```

### Change Concentration Threshold
```python
def detect_geographic_concentrations(self, mover_ind, metric_type, concentration_threshold=0.8):
    # Change 0.8 to desired threshold (e.g., 0.9 for 90%)
```

### Add Custom Detection Method
Extend `CensusBlockAnomalyDetector` class in `detect_census_block_anomalies.py`:
```python
def detect_custom_anomaly(self, mover_ind, metric_type):
    """Your custom detection logic here."""
    # Query census cube tables
    # Apply your logic
    # Return list of anomaly records
```

---

## 🐛 Troubleshooting

### Error: "Table does not exist"
**Solution:** Build census block cubes first:
```bash
uv run python build_census_block_cubes.py --ds gamoshi
```

### Error: "No baseline data available"
**Cause:** Target dates are too early (no 90-day lookback)
**Solution:** Use dates with sufficient history or reduce lookback period.

### Empty Results
**Possible causes:**
1. No anomalies on those dates (good news!)
2. Thresholds too strict (try lowering z-threshold or spike-multiplier)
3. Dataset doesn't have data for those dates

**Check data availability:**
```sql
SELECT MIN(the_date), MAX(the_date) 
FROM gamoshi_win_mover_census_cube;
```

### Extremely High Z-Scores
**Expected behavior:** Z-scores can be billions when mean=0 (first appearances)
**Not a bug:** These are valid extreme outliers

---

## 📚 Related Documentation

- **[CENSUS_BLOCK_ANOMALY_REPORT.md](CENSUS_BLOCK_ANOMALY_REPORT.md)** - Full analysis report with findings
- **[CENSUS_BLOCK_ANALYSIS_SUMMARY.md](CENSUS_BLOCK_ANALYSIS_SUMMARY.md)** - Executive summary
- **[OUTLIER_METHODS.md](OUTLIER_METHODS.md)** - Overview of all outlier detection methods
- **[DATABASE_GUIDE.md](DATABASE_GUIDE.md)** - Database structure and schema
- **[CUBES_GUIDE.md](CUBES_GUIDE.md)** - Cube tables documentation

---

## 🎯 Use Cases

### Daily Monitoring
```bash
# Add to cron/scheduled task
0 6 * * * cd /path/to/suppression_tools && \
  uv run python detect_census_block_anomalies.py \
  --ds gamoshi \
  --dates $(date -d yesterday +%Y-%m-%d) \
  --output daily_results
```

### Quality Assurance Check
```bash
# Check new data load
uv run python detect_census_block_anomalies.py \
  --ds new_dataset \
  --dates "2025-10-01" \
  --output qa_check
```

### Fraud Investigation
```bash
# Deep dive on specific carrier
# After detection, filter results:
grep "SuspiciousCarrier" census_block_analysis_results/gamoshi_*.csv
```

---

## 💡 Tips

1. **Start with default thresholds** - They're well-calibrated
2. **Review first appearances carefully** - Most common anomaly type
3. **Cross-reference with business events** - Validate against known launches/changes
4. **Watch geographic concentrations** - Often indicate data quality issues
5. **Use DOW analysis** - Identify if anomalies cluster on specific days
6. **Combine with other tools** - Use with carrier_dashboard_duckdb.py for drill-down

---

## 🤝 Contributing

Found a bug or have a suggestion? Please open an issue or submit a PR.

**Common enhancements:**
- Additional detection methods
- Custom visualization templates
- Integration with alerting systems
- Machine learning models
- Automated suppression rules

---

**Questions?** See the full documentation or contact the team.
