# Pre-Agg v0.3 Support Migration Plan

## Executive Summary

**Objective**: Add support for v0.3 pre-aggregated data format while maintaining compatibility with existing v15.0 pipeline.

**Key Differences**:
- v0.3 uses 2010 census blocks, v15.0 uses 2020 census blocks
- v0.3 column names differ slightly from v15.0
- v0.3 is missing some intermediate processing columns

**Strategy**: Normalize v0.3 data into existing database schema during load phase.

---

## 1. Data Format Comparison

### v0.3 Pre-Agg Schema (21 columns)
```
Source: ~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/063c37f4-d6ba-4d7c-8a9c-4a0d67ed8689/
```

**Key columns**:
1. `element_id` - Array of element IDs
2. `event_type` - "bb_churn"
3. `census_blockid` - **2010 census block ID** (vs v15.0's `primary_geoid`)
4. `zip_cd`, `state`, `bta`, `cbsa`, `dma`, `cma` - Geographic codes
5. `secondary_census_blockid` - Secondary location
6. `secondary_sp_group` - Secondary service provider
7. `primary_sp_group` - Primary service provider  
8. `mover_ind` - Boolean, mover indicator
9. `adjusted_wins`, `adjusted_losses` - Metrics
10. `network`, `brand`, `wireless_sp_group` - Provider details
11. `r` - Record version
12. `the_date` - Date
13. `ds` - **Data source** (already present in v0.3!)

### v15.0 Pre-Agg Schema (35 columns)
```
Source: ~/tmp/platform_pre_agg_v_15_0/2025-09-26/471f025d-11d5-4368-be19-dca7ebeb381d/
```

**Key columns**:
1. `the_date`, `element_id`, `churn_id` - Event identifiers
2. `primary_geoid` - **2020 census block ID**
3. `primary_location`, `primary_location_gps`, `primary_location_sink` - Location details
4. `secondary_geoid`, `secondary_location`, etc. - Secondary location
5. `mover_ind`, `move_distance_m`, `move_distance_km` - Move details
6. `wireless_network_sp_group`, `wireless_brand_sp_group`, `network`, `brand`, `mcc`, `mnc` - Provider
7. `filter_reasons` - Processing metadata
8. `wins`, `losses` - Raw metrics (structs)
9. `adjusted_wins`, `adjusted_losses` - Final metrics
10. `primary_sp_group`, `secondary_sp_group` - Service providers
11. `r` - Record version
12. `ds` - Data source
13. `country_iso3`, `year`, `month`, `day` - Partitioning columns

### Census Block Crosswalk Schema

#### 2010 Crosswalk (v0.3)
```
Source: ~/codebase-comparison/suppression_tools/ref/d_census_block_crosswalk/5bb9481d-6e03-4802-a965-8a5242e74d65/
```

**Key columns** (37 total):
- `serv_terr_blockid` - **Join key for v0.3**
- `popstats_blockid`, `acs_2017_blockid` - Alternate block IDs
- `state_fips`, `state`, `state_name` - State info
- `dma`, `dma_name` - **Target columns we need**
- `county_fips`, `county_name`, `cbsa_fips`, `cbsa_name`, etc. - Geography
- `bta`, `bta_name`, `cma`, `cma_name`, `pea`, `pea_name` - Markets

#### 2020 Crosswalk (v15.0) 
```
Source: ~/codebase-comparison/suppression_tools/ref/cb_cw_2020/ (MISSING - need to find)
```

---

## 2. Required Changes

### 2.1 Modify `build_suppression_db.py`

**Current behavior**: 
- Hardcoded to use `ref/cb_cw_2020` for geo crosswalk
- Expects v15.0 schema with `primary_geoid`

**Changes needed**:
1. **Add `--version` argument** to specify pre-agg version (v0.3 or v15.0)
2. **Auto-detect pre-agg version** by inspecting schema:
   - If `census_blockid` column exists → v0.3
   - If `primary_geoid` column exists → v15.0
3. **Use correct crosswalk**:
   - v0.3 → `ref/d_census_block_crosswalk/5bb9481d-6e03-4802-a965-8a5242e74d65/`
   - v15.0 → `ref/cb_cw_2020/`
4. **Use correct join key**:
   - v0.3 → join on `census_blockid = serv_terr_blockid`
   - v15.0 → join on `primary_geoid = census_blockid`
5. **Normalize column names** in the query:
   - v0.3: `census_blockid` → `primary_geoid` (aliased)
   - v15.0: `primary_geoid` → `primary_geoid` (passthrough)
6. **Handle missing columns**:
   - v0.3 doesn't have `year`, `month`, `day` → derive from `the_date`
   - v0.3 doesn't have `ds` in some versions → use filename or default

### 2.2 Update `partition_pre_agg_to_duckdb.py` (if still used)

**Status**: May be obsolete, but update for completeness.

**Changes**:
- Same schema detection logic as build_suppression_db.py
- Support both crosswalk versions

### 2.3 Cube Building Scripts

**Status**: No changes needed! ✅

The cube building scripts (`build_cubes_in_db.py`, `build_census_block_cubes.py`) operate on the `carrier_data` table which has a normalized schema. As long as we properly normalize v0.3 data during the load phase, the cube builders work unchanged.

### 2.4 Reference Data Organization

**Current structure**:
```
ref/
├── cb_cw_2020/          # 2020 census blocks (v15.0)
├── d_census_block_crosswalk/
│   └── 5bb9481d-6e03-4802-a965-8a5242e74d65/  # 2010 census blocks (v0.3)
└── display_rules/       # Carrier name mapping
```

**Recommendation**: Keep both crosswalks, auto-detect which to use.

---

## 3. Implementation Plan

### Phase 1: Schema Detection & Normalization

**File**: `scripts/build/build_suppression_db.py`

**Steps**:
1. Add `detect_preagg_version()` function:
   ```python
   def detect_preagg_version(con: duckdb.Connection, base_glob: str) -> str:
       """
       Detect pre-agg version by inspecting schema.
       Returns: 'v0.3' or 'v15.0'
       """
       schema = con.execute(f"DESCRIBE SELECT * FROM '{base_glob}' LIMIT 0").df()
       columns = set(schema['column_name'].tolist())
       
       if 'census_blockid' in columns and 'primary_geoid' not in columns:
           return 'v0.3'
       elif 'primary_geoid' in columns:
           return 'v15.0'
       else:
           raise ValueError("Cannot determine pre-agg version from schema")
   ```

2. Add `get_schema_config()` function:
   ```python
   def get_schema_config(version: str, base_dir: str) -> dict:
       """Get schema-specific configuration"""
       if version == 'v0.3':
           return {
               'blockid_col': 'census_blockid',
               'geo_path': 'ref/d_census_block_crosswalk/5bb9481d-6e03-4802-a965-8a5242e74d65',
               'geo_join_key': 'serv_terr_blockid',
               'needs_partitioning_cols': True,
               'has_ds_column': True,  # v0.3 has ds!
           }
       elif version == 'v15.0':
           return {
               'blockid_col': 'primary_geoid',
               'geo_path': 'ref/cb_cw_2020',
               'geo_join_key': 'census_blockid',
               'needs_partitioning_cols': False,
               'has_ds_column': True,
           }
       else:
           raise ValueError(f"Unsupported version: {version}")
   ```

3. Modify `build_suppression_db()` to use config:
   ```python
   # Detect version
   version = detect_preagg_version(con, base_glob)
   config = get_schema_config(version, base_dir)
   
   print(f"[INFO] Detected pre-agg version: {version}")
   
   # Use config for geo path
   geo_glob = _parquet_glob(os.path.join(base_dir, config['geo_path']))
   
   # Build query with version-specific columns
   if config['needs_partitioning_cols']:
       partition_cols = """
           CAST(strftime('%Y', the_date) AS INTEGER) AS year,
           CAST(strftime('%m', the_date) AS INTEGER) AS month,
           CAST(strftime('%d', the_date) AS INTEGER) AS day,
       """
   else:
       partition_cols = "year, month, day,"
   
   # Normalize blockid column name
   blockid_select = f"{config['blockid_col']} AS primary_geoid"
   
   # Build geo join
   geo_join = f"LEFT JOIN geo g ON b.{config['blockid_col']} = g.{config['geo_join_key']}"
   ```

### Phase 2: Testing

**Test with v0.3 data**:
```bash
cd /home/jloli/codebase-comparison/suppression_tools

# Build database from v0.3 pre-agg
uv run scripts/build/build_suppression_db.py \
    ~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/063c37f4-d6ba-4d7c-8a9c-4a0d67ed8689 \
    -o data/databases/duck_suppression_v03.db

# Verify carrier_data table schema
uv run python3 << 'EOF'
import duckdb
con = duckdb.connect('data/databases/duck_suppression_v03.db', read_only=True)
print("=== Schema ===")
schema = con.execute("DESCRIBE carrier_data").fetchall()
for row in schema:
    print(f"  {row[0]}: {row[1]}")
    
print("\n=== Sample Row ===")
sample = con.execute("SELECT * FROM carrier_data LIMIT 1").df()
print(sample.T)

print("\n=== Stats ===")
stats = con.execute("""
    SELECT 
        COUNT(*) as total_rows,
        MIN(the_date) as min_date,
        MAX(the_date) as max_date,
        COUNT(DISTINCT ds) as ds_count,
        COUNT(DISTINCT winner) as winner_count
    FROM carrier_data
""").fetchone()
print(f"  Rows: {stats[0]:,}")
print(f"  Date range: {stats[1]} to {stats[2]}")
print(f"  Data sources: {stats[3]}")
print(f"  Winners: {stats[4]}")
con.close()
EOF

# Build cube tables
uv run scripts/build/build_cubes_in_db.py \
    --db data/databases/duck_suppression_v03.db \
    --ds gamoshi

# Test dashboard with v0.3 data
uv run streamlit run carrier_dashboard_duckdb.py
# → In dashboard, change DB path to data/databases/duck_suppression_v03.db
```

**Test with v15.0 data** (regression test):
```bash
# Ensure v15.0 still works
uv run scripts/build/build_suppression_db.py \
    ~/tmp/platform_pre_agg_v_15_0/2025-09-26/471f025d-11d5-4368-be19-dca7ebeb381d \
    -o data/databases/duck_suppression_v150.db
```

### Phase 3: Documentation Updates

1. Update `README.md`:
   - Document v0.3 support
   - Show example commands for both versions

2. Update `AGENTS.md`:
   - Add v0.3 paths to vector memory
   - Document version detection logic

3. Create `.agent_memory.json` entry:
   ```json
   {
     "preagg_versions": {
       "v0.3": {
         "path_pattern": "~/tmp/platform_pre_aggregate_v_0_3/{ds}/{date}/{uuid}/",
         "census_year": "2010",
         "crosswalk_path": "ref/d_census_block_crosswalk/5bb9481d-6e03-4802-a965-8a5242e74d65/",
         "blockid_column": "census_blockid",
         "geo_join_key": "serv_terr_blockid",
         "key_differences": [
           "Uses 2010 census blocks",
           "Missing year/month/day columns (derived from the_date)",
           "Has ds column in data"
         ]
       },
       "v15.0": {
         "path_pattern": "~/tmp/platform_pre_agg_v_15_0/{date}/{uuid}/",
         "census_year": "2020",
         "crosswalk_path": "ref/cb_cw_2020/",
         "blockid_column": "primary_geoid",
         "geo_join_key": "census_blockid",
         "key_differences": [
           "Uses 2020 census blocks",
           "Has year/month/day columns",
           "Has extensive location metadata"
         ]
       }
     }
   }
   ```

---

## 4. Database Naming Strategy

**Problem**: Multiple databases for different versions → confusion

**Solution**: Prefix cube tables with version or keep databases separate

### Option A: Separate Databases (Recommended)
```
data/databases/
├── duck_suppression.db           # Current v15.0 data
├── duck_suppression_v03.db       # v0.3 data
└── duck_suppression_legacy.db    # Backups
```

**Pros**:
- Clean separation
- Easy to switch between versions
- No cube table name collisions

**Cons**:
- Need to specify DB path in dashboards
- Can't compare across versions easily

### Option B: Single Database with Prefixed Tables
```
Tables:
- v03_gamoshi_win_mover_cube
- v03_gamoshi_win_non_mover_cube
- v150_gamoshi_win_mover_cube
- v150_gamoshi_win_non_mover_cube
```

**Pros**:
- All data in one place
- Can compare versions

**Cons**:
- Table name explosion
- Requires updates to all query logic
- More complex cube building

**Recommendation**: Use **Option A** for now. We can consolidate later if needed.

---

## 5. Open Questions

### Q1: What about the display_rules?
**Answer**: Same for both versions. `ref/display_rules/` maps `sp_dim_id` → `sp_reporting_name_group` regardless of census block version.

### Q2: Do we need to handle different `ds` values?
**Answer**: v0.3 already has `ds = 'gamoshi'` in the data. No special handling needed.

### Q3: What if v0.3 is missing `ds` column in some datasets?
**Answer**: Add fallback logic:
```python
# In build_suppression_db.py
ds_select = "COALESCE(b.ds, 'gamoshi') AS ds_clean"  # Default to 'gamoshi' if missing
```

### Q4: Should we update `partition_pre_agg_to_duckdb.py`?
**Answer**: That script creates partitioned parquet files. We moved away from that approach. **Mark as deprecated** or delete it.

### Q5: Census block migration - do we need to map 2010 → 2020 blocks?
**Answer**: **No**. The crosswalk tables already contain `dma` and `dma_name` for their respective census vintages. We don't need to translate between vintages - we just need to use the right crosswalk for the right data.

---

## 6. Risk Assessment

### Low Risk ✅
- **Display rules**: Same for both versions
- **Cube building**: Operates on normalized `carrier_data` table
- **Dashboard logic**: No changes needed if schema is normalized

### Medium Risk ⚠️
- **Date partitioning**: v0.3 doesn't have year/month/day columns
  - **Mitigation**: Derive from `the_date` during load
- **NULL handling**: Crosswalk joins might fail for unmapped blocks
  - **Mitigation**: Add preflight checks (already exists in current code)

### High Risk 🔴
- **Census block mismatch**: 2010 vs 2020 blocks are different geographies
  - **Impact**: Can't directly compare v0.3 and v15.0 data at block level
  - **Mitigation**: Document this limitation clearly
  - **Workaround**: Aggregate to DMA level for cross-version comparisons

---

## 7. Success Criteria

### Must Have
- [ ] Auto-detect v0.3 vs v15.0 from schema
- [ ] Load v0.3 data into normalized `carrier_data` table
- [ ] Build cube tables from v0.3 data
- [ ] Dashboards work with v0.3 database (no code changes)
- [ ] Display rules correctly map carriers
- [ ] DMA crosswalk produces non-NULL dma_name

### Nice to Have
- [ ] Side-by-side comparison dashboard (v0.3 vs v15.0)
- [ ] Automated testing for both versions
- [ ] Migration guide for users

### Out of Scope
- Census block 2010 → 2020 translation
- Schema migration for existing databases
- Unified multi-version database

---

## 8. Estimated Effort

| Phase | Effort | Notes |
|-------|--------|-------|
| Schema detection | 1 hour | Simple column inspection |
| Query modification | 2 hours | Update JOINs and SELECT clauses |
| Testing v0.3 | 1 hour | Load and validate |
| Testing v15.0 regression | 30 min | Ensure no breakage |
| Documentation | 1 hour | README, AGENTS.md |
| **Total** | **5.5 hours** | Conservative estimate |

---

## 9. Next Steps

**Immediate**:
1. Review this plan with user
2. Answer open questions
3. Get approval to proceed

**Implementation**:
1. Implement Phase 1 (schema detection)
2. Test with v0.3 data
3. Test v15.0 regression
4. Commit and push
5. Update documentation
6. Merge to codex-agent

**Follow-up**:
1. Add automated tests
2. Document known limitations
3. Create migration guide for users switching versions

---

## 10. Clarifying Questions for User

Before I proceed with implementation, please clarify:

1. **Database strategy**: Should I use separate databases (Option A) or prefixed tables (Option B)?
   - **Recommendation**: Separate databases for cleaner separation

2. **Default version**: What should be the default if no version is specified?
   - **Recommendation**: Auto-detect from schema

3. **Backward compatibility**: Should I maintain support for legacy parquet partitioning (`partition_pre_agg_to_duckdb.py`)?
   - **Recommendation**: Mark as deprecated, don't update it

4. **Testing scope**: Should I test with your actual v0.3 data or create synthetic test data?
   - **Recommendation**: Use actual data: `~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/063c37f4-d6ba-4d7c-8a9c-4a0d67ed8689`

5. **Cube table prefixes**: If using separate databases, do cube tables need version prefixes?
   - **Recommendation**: No prefixes needed if databases are separate

6. **Rollout strategy**: Should this go straight to `codex-agent` or do you want to test in the feature branch first?
   - **Recommendation**: Test in `feature/preagg-v03-support` first, then merge after validation

Please let me know your preferences and I'll proceed with implementation! 🚀
