# Pre-Agg v0.3 Support - Quick Summary

## TL;DR

**Goal**: Support v0.3 pre-agg data (2010 census blocks) alongside existing v15.0 support (2020 census blocks).

**Solution**: Normalize v0.3 data into existing database schema during load. No changes needed to cubes, dashboards, or analysis tools.

---

## Key Findings

### ✅ Good News
1. **v0.3 already has `ds` column** - No need to infer from filename
2. **Display rules are version-agnostic** - Same carrier mappings work for both
3. **Cube builders are schema-agnostic** - Operate on normalized `carrier_data` table
4. **Dashboards need zero changes** - Just point to different database

### ⚠️ Key Differences

| Aspect | v0.3 | v15.0 |
|--------|------|-------|
| **Census blocks** | 2010 vintage | 2020 vintage |
| **Blockid column** | `census_blockid` | `primary_geoid` |
| **Crosswalk** | `ref/d_census_block_crosswalk/5bb9481d-6e03-4802-a965-8a5242e74d65/` | `ref/cb_cw_2020/` (missing?) |
| **Crosswalk join key** | `serv_terr_blockid` | `census_blockid` |
| **Partitioning cols** | None (derive from `the_date`) | `year`, `month`, `day` |
| **Total columns** | 21 | 35 |

---

## Implementation Strategy

### 1-File Change Approach ✨

**File**: `scripts/build/build_suppression_db.py`

**Changes**:
1. Add `detect_preagg_version()` - Inspect schema to determine version
2. Add `get_schema_config()` - Return version-specific paths and column names
3. Modify query builder to use config:
   - Use correct blockid column
   - Use correct crosswalk
   - Use correct join key
   - Derive partitioning columns if needed

**That's it!** Everything else just works.

---

## Testing Plan

```bash
# 1. Build v0.3 database
uv run scripts/build/build_suppression_db.py \
    ~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/063c37f4-d6ba-4d7c-8a9c-4a0d67ed8689 \
    -o data/databases/duck_suppression_v03.db

# 2. Build v0.3 cubes
uv run scripts/build/build_cubes_in_db.py \
    --db data/databases/duck_suppression_v03.db \
    --ds gamoshi

# 3. Test dashboard
uv run streamlit run carrier_dashboard_duckdb.py
# → Change DB path to data/databases/duck_suppression_v03.db in UI

# 4. Regression test v15.0
uv run scripts/build/build_suppression_db.py \
    ~/tmp/platform_pre_agg_v_15_0/2025-09-26/471f025d-11d5-4368-be19-dca7ebeb381d \
    -o data/databases/duck_suppression_v150_test.db
```

---

## Database Strategy: Separate Files

**Recommended structure**:
```
data/databases/
├── duck_suppression.db        # Current production (v15.0)
├── duck_suppression_v03.db    # v0.3 data
└── duck_suppression_test.db   # Testing/sandbox
```

**Why?**
- ✅ Clean separation between versions
- ✅ No cube table name collisions
- ✅ Easy to switch between versions
- ✅ Can delete/rebuild one without affecting the other

**Alternative (NOT recommended)**: Prefix cube tables (`v03_gamoshi_win_mover_cube`) → too complex.

---

## Estimated Effort

- **Implementation**: 2-3 hours
- **Testing**: 1 hour
- **Documentation**: 30 minutes
- **Total**: 3.5-4.5 hours

---

## Open Questions for User

1. **Do you have the 2020 crosswalk file?** (`ref/cb_cw_2020/`)
   - My test failed because it's missing
   - May have been deleted in cleanup
   - Need to restore or locate it

2. **Should I proceed with separate databases?**
   - Recommended: Yes
   - Alternative: Prefixed tables (more complex)

3. **What should I do with `partition_pre_agg_to_duckdb.py`?**
   - We moved away from partitioned parquet approach
   - Recommended: Mark as deprecated or delete

4. **Test with real v0.3 data or synthetic?**
   - Recommended: Use your actual v0.3 data
   - Path: `~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/063c37f4-d6ba-4d7c-8a9c-4a0d67ed8689`

5. **Should cubes have version prefixes if we use separate DBs?**
   - Recommended: No, just use `gamoshi_win_mover_cube` in both
   - The database file itself identifies the version

6. **Merge strategy?**
   - Test in `feature/preagg-v03-support` branch first
   - Merge to `codex-agent` after you validate
   - Or go straight to `codex-agent` if you're confident

---

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| Missing 2020 crosswalk | 🔴 HIGH | Find and restore `cb_cw_2020/` |
| NULL dma_name from join | 🟡 MEDIUM | Preflight checks (already exist) |
| Breaking v15.0 support | 🟡 MEDIUM | Regression test before merge |
| Census vintage mismatch | 🟢 LOW | Document limitation clearly |

---

## Next Steps

**Awaiting your approval**:
1. Answer the 6 questions above
2. Confirm you want me to proceed
3. Clarify if I should find the 2020 crosswalk or use a different approach

**Once approved, I will**:
1. Implement schema detection in `build_suppression_db.py`
2. Test with your v0.3 data
3. Test v15.0 regression
4. Update documentation
5. Commit to feature branch
6. Report results for your validation

---

## Full Details

See `analysis/preagg_v03_support/MIGRATION_PLAN.md` for comprehensive technical details.
