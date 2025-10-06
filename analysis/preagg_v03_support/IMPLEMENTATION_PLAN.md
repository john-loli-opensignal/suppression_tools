# Pre-Agg v0.3 Support - Implementation Plan
## Based on User Feedback (2025-10-06)

---

## ✅ User Decisions & Requirements

### Database Strategy
- ✅ **Separate database**: `duck_suppression_v_2_1_prod.db` (not `v03` - this is v2.1 production)
- ✅ **Location**: `data/databases/duck_suppression_v_2_1_prod.db`
- ✅ **No cube name prefixes**: Same cube names (`gamoshi_win_mover_cube`), database file identifies version
- ✅ **Configuration backup**: Store build config for reproducibility

### Data Sources
- ✅ **v0.3 (2.1) pre-agg**: `~/tmp/platform_pre_aggregate_v_0_3/{ds}/{date}/{uuid}/`
- ✅ **v0.3 crosswalk**: `ref/d_census_block_crosswalk/5bb9481d-6e03-4802-a965-8a5242e74d65/`
- ✅ **v15.0 crosswalk**: `ref/cb_cw_2020/90226c5d-f3bf-47eb-963a-e03545c972bd/` (verified exists)
- ✅ **Display rules**: `ref/display_rules/` (version-agnostic)

### Missing `ds` Column Handling
- ✅ **If `ds` missing**: Infer from directory pattern (e.g., `/gamoshi/` → `ds='gamoshi'`)
- ✅ **Log warning**: Terminal warning showing assumed ds value
- ✅ **Format**: `[WARNING] ds column missing in pre-agg data. Inferred from path: 'gamoshi'`

### Schema Detection
- ✅ **Auto-detect version** by inspecting schema:
  - Has `census_blockid` column? → v0.3 (2010 blocks)
  - Has `primary_geoid` column? → v15.0 (2020 blocks)
- ✅ **Log detected version**: `[INFO] Detected pre-agg version: v0.3 (2010 census blocks)`

### Merge Strategy
- ✅ **Feature branch**: `feature/preagg-v03-support` (CURRENT BRANCH)
- ✅ **Merge target**: `codex-agent` (after user validation)
- ✅ **Not yet**: Don't merge to `main` until approved

### Deprecation
- ✅ **Deprecate**: `partition_pre_agg_to_duckdb.py` (partitioned parquet approach)
- ✅ **Reason**: Project moved to persistent DuckDB databases

---

## 📋 Implementation Checklist

### Phase 1: Schema Detection & Configuration
- [ ] Add `detect_preagg_version()` function to `build_suppression_db.py`
  - [ ] Inspect pre-agg schema for `census_blockid` vs `primary_geoid`
  - [ ] Return version identifier ('v0.3' or 'v15.0')
  - [ ] Log detected version to terminal

- [ ] Add `get_schema_config()` function
  - [ ] Return version-specific paths:
    - `crosswalk_path`: v0.3 → `d_census_block_crosswalk`, v15.0 → `cb_cw_2020`
    - `blockid_col`: v0.3 → `census_blockid`, v15.0 → `primary_geoid`
    - `join_key`: v0.3 → `serv_terr_blockid`, v15.0 → `census_blockid`
  - [ ] Return partitioning strategy:
    - v0.3: Must derive `year`, `month`, `day` from `the_date`
    - v15.0: Use existing `year`, `month`, `day` columns

- [ ] Add `infer_ds_from_path()` function
  - [ ] Extract ds from path pattern (e.g., `/gamoshi/` → `'gamoshi'`)
  - [ ] Log warning if ds was inferred vs. present in data
  - [ ] Fallback to 'unknown' if pattern not found

### Phase 2: Update `build_suppression_db.py`
- [ ] Modify query builder to use schema config
  - [ ] Use `config['blockid_col']` instead of hardcoded `primary_geoid`
  - [ ] Use `config['crosswalk_path']` for geo join
  - [ ] Use `config['join_key']` for crosswalk join condition
  - [ ] Add conditional logic for `year`/`month`/`day`:
    ```sql
    -- v0.3: derive from the_date
    CAST(strftime('%Y', the_date) AS INTEGER) AS year,
    CAST(strftime('%m', the_date) AS INTEGER) AS month,
    CAST(strftime('%d', the_date) AS INTEGER) AS day,
    
    -- v15.0: use existing columns
    COALESCE(year, CAST(strftime('%Y', the_date) AS INTEGER)) AS year,
    ...
    ```

- [ ] Add ds column handling
  - [ ] Check if `ds` column exists in schema
  - [ ] If missing: call `infer_ds_from_path(base_path)`
  - [ ] Add to query:
    ```sql
    COALESCE(CAST(b.ds AS VARCHAR), '{inferred_ds}') AS ds_clean
    ```

- [ ] Update default paths in argument parser
  - [ ] Keep default geo as `cb_cw_2020` (most common case)
  - [ ] Add `--detect-version` flag (default: True)
  - [ ] Allow manual `--crosswalk` override

### Phase 3: Configuration Backup
- [ ] Create `build_config.json` alongside database
  - [ ] Save build parameters:
    ```json
    {
      "version": "v0.3",
      "pre_agg_path": "~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/...",
      "crosswalk_path": "ref/d_census_block_crosswalk/...",
      "display_rules_path": "ref/display_rules/...",
      "build_date": "2025-10-06T13:45:00",
      "row_count": 123456,
      "date_range": ["2025-02-19", "2025-09-04"],
      "datasets": ["gamoshi"]
    }
    ```
  - [ ] Store in `data/databases/duck_suppression_v_2_1_prod_config.json`
  - [ ] Log path to config file

### Phase 4: Testing
- [ ] Test v0.3 build
  ```bash
  uv run scripts/build/build_suppression_db.py \
      ~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/063c37f4-d6ba-4d7c-8a9c-4a0d67ed8689 \
      -o data/databases/duck_suppression_v_2_1_prod.db
  ```
  - [ ] Verify correct version detection
  - [ ] Verify correct crosswalk used
  - [ ] Verify ds inferred if missing
  - [ ] Verify row count > 0
  - [ ] Verify no NULL values in key columns
  - [ ] Verify config backup created

- [ ] Test v0.3 cube building
  ```bash
  uv run scripts/build/build_cubes_in_db.py \
      --db data/databases/duck_suppression_v_2_1_prod.db \
      --ds gamoshi
  ```
  - [ ] Verify cubes created successfully
  - [ ] Verify rolling views work
  - [ ] Verify sample queries return data

- [ ] Test v15.0 regression (ensure we didn't break existing)
  ```bash
  uv run scripts/build/build_suppression_db.py \
      ~/tmp/platform_pre_agg_v_15_0/2025-09-26/471f025d-11d5-4368-be19-dca7ebeb381d \
      -o data/databases/duck_suppression_regression_test.db
  ```
  - [ ] Verify v15.0 still works
  - [ ] Compare row counts with existing production DB
  - [ ] Verify same carriers/DMAs detected

- [ ] Test dashboard with v0.3 database
  ```bash
  # Temporarily point dashboard to v0.3 DB (manual code change for testing)
  uv run streamlit run carrier_dashboard_duckdb.py
  ```
  - [ ] Verify graphs render
  - [ ] Verify outlier detection works
  - [ ] Verify carrier list populated
  - [ ] Compare results with v15.0 (should differ due to census vintage)

### Phase 5: Documentation
- [ ] Update `README.md`
  - [ ] Add section on pre-agg version support
  - [ ] Document how to build v0.3 vs v15.0 databases
  - [ ] Explain census vintage differences

- [ ] Update `AGENTS.md`
  - [ ] Add v0.3 support to critical project rules
  - [ ] Document separate database approach
  - [ ] Add section on configuration backups

- [ ] Update `analysis/preagg_v03_support/MIGRATION_PLAN.md`
  - [ ] Mark as "✅ IMPLEMENTED"
  - [ ] Add actual implementation notes
  - [ ] Document any deviations from plan

- [ ] Create `docs/PRE_AGG_VERSION_GUIDE.md`
  - [ ] User guide for working with different versions
  - [ ] When to use v0.3 vs v15.0
  - [ ] Limitations of cross-version comparisons
  - [ ] How to switch between databases in dashboards

### Phase 6: Deprecation
- [ ] Mark `partition_pre_agg_to_duckdb.py` as deprecated
  - [ ] Add deprecation notice at top of file
  - [ ] Reference new `build_suppression_db.py` approach
  - [ ] Move to `scripts/legacy/` directory

- [ ] Update any scripts that reference partitioned approach
  - [ ] Search for references to `duckdb_partitioned_store`
  - [ ] Update to use database path instead

### Phase 7: Cleanup & Commit
- [ ] Remove any test/temporary files created
- [ ] Verify `.gitignore` includes temporary databases
- [ ] Update `.agent_memory.json` with new implementation details
- [ ] Create clear, focused commits:
  ```bash
  git add scripts/build/build_suppression_db.py
  git commit -m "feat(preagg): add v0.3 pre-agg support with auto-detection"
  
  git add docs/ analysis/preagg_v03_support/
  git commit -m "docs(preagg): add v0.3 support documentation and guides"
  
  git add scripts/legacy/partition_pre_agg_to_duckdb.py
  git commit -m "chore(scripts): deprecate partitioned parquet approach"
  ```

---

## 🔍 Key Technical Details

### Schema Detection Logic
```python
def detect_preagg_version(base_path: str) -> str:
    """
    Auto-detect pre-agg version by inspecting schema.
    
    Returns:
        'v0.3' for 2010 census blocks
        'v15.0' for 2020 census blocks
    
    Raises:
        RuntimeError if unable to determine version
    """
    con = duckdb.connect()
    try:
        base_glob = _parquet_glob(base_path)
        desc = con.execute(f"DESCRIBE SELECT * FROM parquet_scan('{base_glob}') LIMIT 0").df()
        columns = set(desc['column_name'].astype(str).tolist())
        
        if 'census_blockid' in columns and 'primary_geoid' not in columns:
            return 'v0.3'
        elif 'primary_geoid' in columns:
            return 'v15.0'
        else:
            raise RuntimeError("Unable to determine pre-agg version from schema")
    finally:
        con.close()
```

### Schema Configuration
```python
def get_schema_config(version: str, base_dir: str) -> dict:
    """
    Get version-specific paths and column names.
    
    Args:
        version: 'v0.3' or 'v15.0'
        base_dir: Project base directory
    
    Returns:
        Configuration dictionary with paths and column mappings
    """
    if version == 'v0.3':
        return {
            'version': 'v0.3',
            'census_vintage': 2010,
            'crosswalk_path': os.path.join(base_dir, 'ref', 'd_census_block_crosswalk', '5bb9481d-6e03-4802-a965-8a5242e74d65'),
            'blockid_col': 'census_blockid',
            'crosswalk_join_key': 'serv_terr_blockid',
            'has_partitioning_cols': False,
            'requires_ds_inference': True,  # May need to infer from path
        }
    elif version == 'v15.0':
        return {
            'version': 'v15.0',
            'census_vintage': 2020,
            'crosswalk_path': os.path.join(base_dir, 'ref', 'cb_cw_2020', '90226c5d-f3bf-47eb-963a-e03545c972bd'),
            'blockid_col': 'primary_geoid',
            'crosswalk_join_key': 'census_blockid',
            'has_partitioning_cols': True,
            'requires_ds_inference': False,
        }
    else:
        raise ValueError(f"Unsupported version: {version}")
```

### DS Inference Logic
```python
def infer_ds_from_path(path: str) -> Optional[str]:
    """
    Infer dataset name from path pattern.
    
    Expected patterns:
        ~/tmp/platform_pre_aggregate_v_0_3/{ds}/...
        ~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/...
    
    Returns:
        Dataset name or None if pattern not recognized
    """
    import re
    pattern = r'/platform_pre_aggregate_v_0_3/([^/]+)/'
    match = re.search(pattern, path)
    if match:
        return match.group(1)
    return None
```

### Query Template (Pseudo-code)
```sql
-- Dynamically construct based on config
WITH base AS (
    SELECT * FROM parquet_scan('{base_glob}')
),
rules_w AS (
    SELECT sp_dim_id AS w_sp_dim_id, sp_reporting_name_group AS winner
    FROM parquet_scan('{rules_glob}')
),
rules_l AS (
    SELECT sp_dim_id AS l_sp_dim_id, sp_reporting_name_group AS loser
    FROM parquet_scan('{rules_glob}')
),
geo AS (
    -- Use version-specific columns
    SELECT {config['crosswalk_join_key']} AS join_key, 
           dma, dma_name, state
    FROM parquet_scan('{config['crosswalk_path']}')
),
enriched AS (
    SELECT 
        b.*,
        w.winner,
        l.loser,
        g.dma,
        g.dma_name,
        g.state
    FROM base b
    LEFT JOIN rules_w w ON b.primary_sp_group = w.w_sp_dim_id
    LEFT JOIN rules_l l ON b.secondary_sp_group = l.l_sp_dim_id
    -- Version-specific join
    LEFT JOIN geo g ON b.{config['blockid_col']} = g.join_key
)
SELECT 
    the_date,
    -- Handle ds column (may need inference)
    COALESCE(CAST(ds AS VARCHAR), '{inferred_ds}', 'unknown') AS ds,
    mover_ind,
    winner,
    loser,
    dma,
    dma_name,
    state,
    adjusted_wins,
    adjusted_losses,
    {config['blockid_col']} AS primary_geoid,  -- Normalize column name
    -- Partitioning columns (version-specific)
    {partitioning_logic} AS year,
    {partitioning_logic} AS month,
    {partitioning_logic} AS day
FROM enriched
WHERE winner IS NOT NULL AND loser IS NOT NULL AND dma_name IS NOT NULL
```

---

## ⚠️ Known Limitations & Considerations

### Census Vintage Mismatch
- **v0.3 uses 2010 census blocks, v15.0 uses 2020 census blocks**
- Census blocks changed between 2010 and 2020 (boundaries redrawn)
- **Cannot directly compare at census block level**
- **Safe comparisons**:
  - DMA level (DMAs didn't change)
  - State level
  - National level
  - Carrier-level metrics

### Data Coverage
- v0.3 may have different carriers than v15.0 (time period difference)
- v0.3 may have different date ranges
- Crosswalk join success rate may differ (2010 vs 2020 blocks)

### Performance
- v0.3 has fewer columns (21 vs 35) → faster to load
- v0.3 crosswalk has more rows → join may be slower
- Both should have similar query performance after loading

### Dashboard Compatibility
- **Dashboards are version-agnostic** - they operate on normalized `carrier_data` table
- **No code changes needed** - just point to different database file
- **Results will differ** due to census vintage and time period

---

## 🎯 Success Criteria

### Build Phase
- ✅ v0.3 database builds without errors
- ✅ Version auto-detected correctly
- ✅ Correct crosswalk used automatically
- ✅ ds column handled (present or inferred)
- ✅ No NULL values in key columns (winner, loser, dma_name)
- ✅ Date range matches input data
- ✅ Row count > 0 and reasonable
- ✅ Config backup created

### Cube Phase
- ✅ Cube tables created successfully
- ✅ Rolling views work
- ✅ Sample queries return expected data

### Regression Phase
- ✅ v15.0 still builds correctly
- ✅ v15.0 results match existing production DB

### Dashboard Phase
- ✅ Dashboard loads v0.3 database
- ✅ Graphs render
- ✅ Outlier detection works
- ✅ No errors in terminal

### Documentation Phase
- ✅ User guide created
- ✅ Technical docs updated
- ✅ AGENTS.md updated with new rules

---

## 📅 Timeline Estimate

| Phase | Time | Description |
|-------|------|-------------|
| Phase 1: Schema Detection | 1 hour | Implement detection functions |
| Phase 2: Query Builder | 1.5 hours | Update build_suppression_db.py |
| Phase 3: Config Backup | 0.5 hours | JSON export |
| Phase 4: Testing | 2 hours | Build, test, validate |
| Phase 5: Documentation | 1 hour | User guides + updates |
| Phase 6: Deprecation | 0.5 hours | Move old scripts |
| Phase 7: Cleanup & Commit | 0.5 hours | Git commits |
| **Total** | **7 hours** | End-to-end implementation |

---

## 🚀 Ready to Start?

**User confirmation needed**:
1. ✅ Database naming confirmed: `duck_suppression_v_2_1_prod.db`
2. ✅ Separate database approach approved
3. ✅ Auto-detection approach approved
4. ✅ ds inference approach approved
5. ✅ Deprecation of partitioned approach approved
6. ✅ Merge to codex-agent after validation

**Once you say "proceed", I will**:
1. Implement Phase 1 & 2 (core functionality)
2. Test with your v0.3 data
3. Report back with results before moving to documentation phase
4. Await your approval for each major phase

---

**Status**: ⏸️ AWAITING USER APPROVAL TO PROCEED
