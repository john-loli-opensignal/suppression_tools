# Phase 1 Implementation: Version-Aware Build Script

## Status: ✅ READY TO IMPLEMENT

## Objective
Create a version-aware build script that automatically detects pre-agg version and builds the appropriate database with correct crosswalk joins.

## Key Schema Differences

### v15.0 (2020 Census Blocks) - CURRENT
- **Path**: `~/tmp/platform_pre_agg_v_15_0/{date}/{uuid}/`
- **Columns**: 1197 (nested structure with year/month/day already present)
- **Blockid**: `primary_geoid` (BYTE_ARRAY)
- **DS column**: ✅ Present
- **Crosswalk**: `ref/cb_cw_2020/` → join on `census_blockid`
- **Date columns**: `the_date` (INT32), `year`, `month`, `day` already present

### v0.3 (2010 Census Blocks) - NEW TARGET
- **Path**: `~/tmp/platform_pre_aggregate_v_0_3/{ds}/{date}/{uuid}/`
- **Columns**: 44 (flat structure, simpler)
- **Blockid**: `census_blockid` (BYTE_ARRAY)
- **DS column**: ⚠️ **MISSING** (must extract from path: `platform_pre_aggregate_v_0_3/gamoshi/...`)
- **Crosswalk**: `ref/d_census_block_crosswalk/` → join on `serv_terr_blockid`
- **Date columns**: `the_date` (BYTE_ARRAY in 'YYYY-MM-DD' format) - must derive year/month/day

## Critical Column Mappings

| Field | v15.0 | v0.3 |
|-------|-------|------|
| Census Block | `primary_geoid` | `census_blockid` |
| Dataset | `ds` (column) | **PATH-DERIVED** (gamoshi) |
| Date | `the_date` (INT32) | `the_date` (BYTE_ARRAY 'YYYY-MM-DD') |
| Mover | `mover_ind` (BOOLEAN) | `mover_ind` (BOOLEAN) |
| Wins | `adjusted_wins` (INT32) | `adjusted_wins` (INT32) |
| Losses | `adjusted_losses` (INT32) | `adjusted_losses` (INT32) |
| Primary SP | `primary_sp_group` (INT32) | `primary_sp_group` (INT32) |
| Secondary SP | `secondary_sp_group` (INT32) | `secondary_sp_group` (INT32) |

## Crosswalk Join Differences

### v15.0 Join:
```sql
LEFT JOIN geo g ON b.primary_geoid = g.census_blockid
```

### v0.3 Join:
```sql
LEFT JOIN geo g ON b.census_blockid = g.serv_terr_blockid
```

## Implementation Strategy

### 1. Auto-Detection
```python
def detect_preagg_version(base_path: str) -> dict:
    """
    Detect pre-agg version by inspecting schema and path structure.
    
    Returns:
        {
            'version': 'v15.0' | 'v0.3',
            'blockid_col': 'primary_geoid' | 'census_blockid',
            'has_ds_column': bool,
            'ds_from_path': Optional[str],  # For v0.3
            'crosswalk_join_key': str,
            'recommended_crosswalk': str,
            'date_format': 'INT32' | 'BYTE_ARRAY'
        }
    """
```

### 2. Version-Specific SQL Generation
- **v15.0**: Use existing logic (already works)
- **v0.3**: Modified logic with:
  - Extract `ds` from path pattern
  - Cast `the_date` from BYTE_ARRAY to DATE
  - Join on `serv_terr_blockid`
  - Derive year/month/day from date string

### 3. Database Naming Convention
- **v15.0**: `data/databases/duck_suppression.db` (default)
- **v0.3**: `data/databases/duck_suppression_v03.db`
- **Custom**: User can specify via `--output`

### 4. Build Config Backup
Save metadata to `data/databases/build_config_{db_name}.json`:
```json
{
    "version": "v0.3",
    "build_date": "2025-10-06T10:30:00Z",
    "source_path": "/path/to/preagg",
    "rules_path": "ref/display_rules",
    "crosswalk_path": "ref/d_census_block_crosswalk",
    "row_count": 1234567,
    "date_range": ["2025-02-19", "2025-09-04"],
    "datasets": ["gamoshi"],
    "command": "uv run build_suppression_db.py ..."
}
```

## Code Changes Required

### File: `scripts/build/build_suppression_db.py`

#### 1. Add version detection function (lines 52-100)
```python
def detect_preagg_version(base: str, con) -> dict:
    """Inspect parquet schema and path to determine version"""
    # Implementation here
```

#### 2. Add DS extraction from path (lines 101-120)
```python
def extract_ds_from_path(base: str) -> Optional[str]:
    """Extract dataset name from v0.3 path pattern"""
    # Pattern: ~/tmp/platform_pre_aggregate_v_0_3/{ds}/{date}/{uuid}/
    import re
    pattern = r'platform_pre_aggregate_v_0_3/([^/]+)/'
    match = re.search(pattern, base)
    return match.group(1) if match else None
```

#### 3. Modify `build_suppression_db()` function (lines 53-349)
- Add version detection at start
- Branch SQL generation based on version
- Log version info to console
- Save build config JSON

#### 4. Update query generation (lines 154-211)
- For v0.3:
  - Use `census_blockid` instead of `primary_geoid`
  - Add `ds` column with literal value from path
  - Cast `the_date` from BYTE_ARRAY to DATE
  - Derive year/month/day from date string
  - Join on `serv_terr_blockid`

## Testing Plan

### Test 1: Detect v15.0 (existing data)
```bash
uv run scripts/build/build_suppression_db.py \
    ~/tmp/platform_pre_agg_v_15_0/2025-09-26/471f025d-11d5-4368-be19-dca7ebeb381d \
    --detect-only
```
**Expected**: Version=v15.0, blockid=primary_geoid, has_ds=true

### Test 2: Detect v0.3 (new data)
```bash
uv run scripts/build/build_suppression_db.py \
    ~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/063c37f4-d6ba-4d7c-8a9c-4a0d67ed8689 \
    --detect-only
```
**Expected**: Version=v0.3, blockid=census_blockid, has_ds=false, ds_from_path=gamoshi

### Test 3: Build v0.3 database
```bash
uv run scripts/build/build_suppression_db.py \
    ~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/063c37f4-d6ba-4d7c-8a9c-4a0d67ed8689 \
    --geo ref/d_census_block_crosswalk \
    -o data/databases/duck_suppression_v03.db \
    --overwrite
```
**Expected**: 
- Database created with correct schema
- `ds` column populated with 'gamoshi'
- DMA joins work correctly
- Date range detected
- Build config saved

### Test 4: Verify cube compatibility
```bash
uv run scripts/build/build_cubes_in_db.py \
    --db data/databases/duck_suppression_v03.db \
    --ds gamoshi \
    --list
```
**Expected**: Cube builder recognizes v0.3 database

## Acceptance Criteria

✅ **MUST HAVE**:
1. Auto-detects version from schema
2. Extracts `ds` from path for v0.3
3. Uses correct crosswalk join key
4. Normalizes to same `carrier_data` table structure
5. Saves build config JSON
6. Logs warnings for missing ds column

✅ **NICE TO HAVE**:
7. `--detect-only` flag for inspection
8. Validates join coverage (warns if < 95% blocks matched)
9. Backwards compatible (v15.0 still works)

## Migration Path

1. ✅ Implement version detection
2. ✅ Add v0.3 SQL generation
3. ✅ Test with sample data
4. ✅ Validate cube compatibility
5. ✅ Update AGENTS.md with new instructions
6. ✅ Merge to `codex-agent` branch

## Questions Resolved

| Question | Answer |
|----------|--------|
| Separate DB or same? | **Separate**: `duck_suppression_v03.db` |
| Missing census blocks? | Test join coverage, log warnings |
| DS column missing? | **Extract from path**, log warning |
| Deprecate parquet approach? | **Yes** (already removed) |
| Merge strategy? | **Feature branch → codex-agent after validation** |

## Files to Create/Modify

### Modified:
- `scripts/build/build_suppression_db.py` (version-aware)

### Created:
- `analysis/preagg_v03_support/IMPLEMENTATION_PHASE1.md` (this file)
- `data/databases/build_config_duck_suppression_v03.json` (at build time)

### Updated:
- `AGENTS.md` (add v0.3 build instructions)
- `.agent_memory.json` (add version detection context)

---

## Ready to Proceed?

All research complete. Implementation can begin immediately.

**Estimated time**: 2-3 hours
**Risk level**: Low (backwards compatible, feature branch)
**Validation method**: Compare cube queries between v15.0 and v0.3 databases

---

*Created: 2025-10-06*
*Branch: feature/preagg-v03-support*
*Status: APPROVED - PROCEED WITH IMPLEMENTATION*
