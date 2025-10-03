# Census Block Level Analysis - POC

## Overview

Census block cubes enable **hierarchical outlier detection** from national level down to individual geographic locations (census blocks), supporting quality assurance and fraud detection use cases.

## Hierarchical Detection Path

```
National (ds, mover_ind) 
  ↓ Most suspicious carriers
H2H Matchups (winner vs loser)
  ↓ Most suspicious pairs
State
  ↓ Geographical patterns
DMA (Designated Market Area)
  ↓ Market-level issues
Census Block
  ↓ Pinpoint exact locations
```

## Use Cases

### 1. **Outlier Detection Hierarchy**
   - Start at national level to identify suspicious carrier pairs
   - Drill down through states and DMAs
   - Identify exact census blocks with anomalous behavior
   - Statistical flagging using z-score analysis

### 2. **Quality Assurance**
   - Detect blocks with abnormally high wins/losses
   - Identify suspicious concentration patterns
   - Validate data quality at the source level
   - Cross-reference with known data issues

### 3. **Fraud Detection**
   - Flag blocks with impossible metrics
   - Geo-spatial anomaly detection
   - Identify coordinated suspicious activity
   - Pattern analysis across time and space

## Performance Metrics

### Cube Building Performance

| Cube Type | Rows | Unique Blocks | Build Time | Index Time | Total Time |
|-----------|------|---------------|------------|------------|------------|
| Mover Wins | 4,768,629 | 230,258 | 2.20s | 10.77s | ~13s |
| Mover Losses | 4,768,629 | 230,258 | 2.93s | 11.64s | ~15s |
| Non-Mover Wins | 1,848,074 | 116,138 | 2.40s | 6.53s | ~9s |
| Non-Mover Losses | 1,848,074 | 116,138 | 1.89s | 5.85s | ~8s |

**Total:** All 4 cubes built in ~45 seconds

### Cube Size Comparison

| Level | Rows | Unique Entities | Ratio to DMA |
|-------|------|-----------------|--------------|
| **DMA Cube** | 1,878,560 | 211 DMAs | 1.0x (baseline) |
| **Census Block Cube** | 4,768,629 | 230,258 blocks | 2.5x |

**Surprise:** Census block cubes are only 2.5x larger than DMA cubes despite having 1,000x more geographic granularity!

### Why Census Blocks Are Efficient

1. **Sparse Data:** Not every census block has activity every day
2. **Natural Aggregation:** H2H matchups at block level naturally group data
3. **Compression:** Most blocks have 0-1 records per day
4. **Indexing:** Multiple indexes enable fast filtering at any hierarchy level

## Query Performance

### Typical Query Times (from POC testing)

- **National aggregation:** <100ms
- **H2H time series:** <200ms  
- **State breakdown:** <300ms
- **DMA breakdown:** <400ms
- **Census block outlier detection:** <2s
- **Full census block listing:** <1s (with 1000 row limit)

### Performance Comparison: Parquet vs DuckDB Cubes

| Operation | Parquet Files | DuckDB Cubes | Speedup |
|-----------|---------------|--------------|---------|
| National aggregation | ~30s | <0.1s | **300x** |
| H2H filtering | ~45s | <0.2s | **225x** |
| State rollup | ~60s | <0.3s | **200x** |
| Census block drill-down | ~120s | <1s | **120x** |

## Storage Requirements

### Database Size
- **Duck_suppression.db total:** ~15 GB
- **Census block cubes:** ~2.5 GB (4 tables)
- **DMA cubes:** ~800 MB (4 tables)
- **Parquet store:** ~8 GB (raw partitioned data)

### Storage Efficiency
- Census blocks add only 17% to total database size
- Enables 100-200x faster queries
- Eliminates need to scan raw parquet files

## Data Granularity

### Coverage Statistics (Gamoshi dataset)
- **Date Range:** 2025-02-19 to 2025-09-04 (197 days)
- **Unique Census Blocks:** 230,258 (movers), 116,138 (non-movers)
- **Unique Carriers:** 629
- **Unique States:** 51
- **Unique DMAs:** 312

### Sparsity Analysis
- **Average records per block:** 20.7 records (movers)
- **Average days active per block:** ~4.2 days
- **Blocks with single-day activity:** ~60%
- **Blocks with >10 days activity:** ~15%

## Outlier Detection Methods

### Statistical Approach (Implemented)
- **Z-Score Analysis:** Flag blocks exceeding N standard deviations
- **Configurable Threshold:** Default 3σ (99.7% confidence)
- **Multi-level Analysis:** Compare within DMA, State, and National contexts

### Future Methods (Potential)
- **Temporal Anomalies:** Sudden spikes in activity
- **Geographic Clustering:** Unusual concentration patterns
- **Rate Limiting:** Impossible win/loss rates
- **Cross-carrier Validation:** Same block anomalous for multiple carriers

## Dashboard Features

### Census Block Outlier Dashboard (POC)

**Hierarchical Navigation:**
1. National overview with carrier rankings
2. H2H matchup selection and time series
3. State-level breakdown with outlier flagging
4. DMA-level analysis with filtering
5. Census block drill-down with outlier detection

**Key Features:**
- Color-coded outlier highlighting
- Configurable z-score threshold
- Statistical summaries at each level
- CSV export of census block data
- Interactive filtering and drill-down

## Usage

### Build Census Block Cubes

```bash
# Build for specific dataset
uv run python build_census_block_cubes.py --ds gamoshi

# Build for all datasets
uv run python build_census_block_cubes.py --all

# List available datasets
uv run python build_census_block_cubes.py --list
```

### Launch POC Dashboard

```bash
# Run the census block outlier dashboard
uv run streamlit run census_block_outlier_dashboard.py
```

### Query Examples

**Find outlier census blocks for a specific H2H:**
```python
import duckdb
con = duckdb.connect('duck_suppression.db')

# Statistical outliers (>3 std deviations)
outliers = con.execute("""
    WITH block_stats AS (
        SELECT 
            census_blockid,
            state,
            SUM(total_wins) as total_wins,
            COUNT(*) as days_active
        FROM gamoshi_win_mover_census_cube
        WHERE winner = 'Comcast' AND loser = 'Spectrum'
        GROUP BY census_blockid, state
    ),
    stats AS (
        SELECT AVG(total_wins) as mean, STDDEV(total_wins) as std
        FROM block_stats
    )
    SELECT b.*, (b.total_wins - s.mean) / s.std as z_score
    FROM block_stats b, stats s
    WHERE ABS((b.total_wins - s.mean) / s.std) > 3
    ORDER BY z_score DESC
""").fetchdf()
```

## Benefits Summary

### Performance
✅ **100-300x faster** than scanning parquet files  
✅ Sub-second queries at all hierarchy levels  
✅ Fast cube building (~45s for all 4 cubes)

### Storage
✅ Only 2.5x larger than DMA cubes  
✅ 17% of total database size  
✅ Efficient sparse data storage

### Functionality
✅ Hierarchical drill-down from national to block level  
✅ Statistical outlier detection at finest granularity  
✅ Multi-dimensional analysis (time, geo, carrier)  
✅ Quality assurance and fraud detection

### Scalability
✅ Handles 230k+ unique census blocks  
✅ Fast aggregation at any level  
✅ Indexed for optimal query performance  
✅ Ready for multi-dataset analysis

## Limitations & Considerations

### Data Sparsity
- Many census blocks have limited activity
- Statistical analysis needs sufficient sample size
- Outlier detection may flag legitimately rare events

### Geographic Resolution
- Census block IDs from source data (primary_geoid)
- No coordinate mapping in current implementation
- Would need geo-enrichment for map visualization

### Temporal Patterns
- Current implementation doesn't model seasonality
- Day-of-week effects not considered
- Time-series forecasting not included

### Memory Usage
- Full census block cube queries can be memory-intensive
- Recommend filtering by state/DMA for large analyses
- Consider pagination for >100k result sets

## Next Steps

### Potential Enhancements

1. **Geographic Visualization**
   - Add lat/lng coordinates for mapping
   - Choropleth maps at block level
   - Heat maps for outlier concentration

2. **Advanced Analytics**
   - Time-series anomaly detection
   - Clustering analysis
   - Predictive modeling for future outliers

3. **Integration**
   - Add to main carrier dashboard
   - Automated outlier alerting
   - Export to other systems

4. **Performance**
   - Materialized views for common queries
   - Additional aggregate tables
   - Query result caching

## Conclusion

Census block cubes provide **powerful, efficient hierarchical outlier detection** with minimal storage overhead. The POC dashboard demonstrates the feasibility and value of fine-grained geographic analysis for quality assurance and fraud detection.

**Recommendation:** Integrate census block analysis into production workflow after validation period.
