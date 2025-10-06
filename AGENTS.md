Agent Workflow and Branching

Summary
- Always work on and push to the `codex-agent` branch.
- Keep commits small and focused (one logical change per commit).
- Use clear, conventional commit messages to enable easy reverts.

Branches
- `main`: stable baseline, reviewed merges only.
- `codex-agent`: active development by the agent. Default working branch.

Commit Style
- Format: `<type>(scope): short description`
  - Examples: `feat(dashboard): add carrier multiselect`, `chore(paths): update suppressions dir`
- One change per commit to simplify `git revert`.

Working Rules
- Do not commit directly to `main`.
- Open PRs from `codex-agent` to `main` for review/merge.
- If a change spans multiple files, split into logically separate commits.

Suggested Local Config
- To default to pushing to `codex-agent` from this repo:
  - `git config branch.codex-agent.merge refs/heads/codex-agent`
  - `git config push.default current`

## Critical Project Rules

### Database Management
- **SINGLE DATABASE ONLY**: `data/databases/duck_suppression.db`
- ⚠️ **NEVER** create databases in project root or other locations
- Always use `tools.db.get_db_path()` to get the database path
- All scripts must validate database path before operations
- If you find multiple .db files, DELETE extras immediately

### Analysis and Temporary Files
- **ALL analysis outputs go in `analysis/` directory**:
  - Images → `analysis/images/`
  - Output files → `analysis/outputs/`
  - Test scripts → `analysis/`
  - Markdown reports → `analysis/`
- **NEVER** create temporary files in project root
- **ALWAYS** create a subdirectory in `analysis/` for each analysis task
  - Example: `analysis/ezee_fiber_houston/` for specific analysis
  - Example: `analysis/outlier_test_june/` for outlier testing
- Clean up temporary files on exit or crash
- Move completed analysis reports to `docs/` if they are final

### Project Organization
- Keep project root minimal: only essential config files and main dashboards
- Use subdirectories for organization:
  - `tools/` - Core utility modules
  - `scripts/` - Operational scripts
  - `data/` - All data files
  - `analysis/` - All analysis work
  - `tests/` - Test files

### Git Workflow Reminders
- **ALWAYS work on `codex-agent` branch**
- Check current branch before making commits: `git branch --show-current`
- Commit frequently with clear, focused commits
- Use conventional commit format: `<type>(scope): description`
- Push regularly to keep remote updated

### TODO - Future Enhancements
- [ ] Census block level outlier detection (current: DMA level only)
- [ ] Docker containerization for n8n agent integration
- [ ] API endpoint exposure for external tool integration


## Pre-Agg Version Support (v0.3 and v15.0)

### Overview
The project supports two versions of pre-aggregated data with different census block vintages:

- **v0.3**: 2010 census blocks, 21 columns
  - Path pattern: `~/tmp/platform_pre_aggregate_v_0_3/{ds}/{date}/{uuid}/`
  - Crosswalk: `ref/d_census_block_crosswalk/5bb9481d-6e03-4802-a965-8a5242e74d65/`
  
- **v15.0**: 2020 census blocks, 35 columns (current default)
  - Path pattern: `~/tmp/platform_pre_agg_v_15_0/{date}/{uuid}/`
  - Crosswalk: `ref/cb_cw_2020/`

### Key Differences

| Aspect | v0.3 | v15.0 |
|--------|------|-------|
| Census vintage | 2010 | 2020 |
| Blockid column | `census_blockid` | `primary_geoid` |
| Crosswalk join key | `serv_terr_blockid` | `census_blockid` |
| Date format | BYTE_ARRAY ('YYYY-MM-DD') | INT32 |
| Has `ds` column | ✅ Yes (or extract from path) | ✅ Yes |
| Partitioning columns | Derive from `the_date` | Has `year`, `month`, `day` |

### Database Build Process
**The build script auto-detects version!** No manual configuration needed.

```bash
# Detect version only (no build)
uv run python scripts/build/build_suppression_db.py <path> --detect-only

# Build v15.0 database (auto-detected)
uv run python scripts/build/build_suppression_db.py \
    ~/tmp/platform_pre_agg_v_15_0/2025-09-26/{uuid} \
    -o data/databases/duck_suppression.db

# Build v0.3 database (auto-detected)
uv run python scripts/build/build_suppression_db.py \
    ~/tmp/platform_pre_aggregate_v_0_3/gamoshi/2025-10-06/{uuid} \
    --geo ref/d_census_block_crosswalk \
    -o data/databases/duck_suppression_v03.db
```

**Build Process Steps:**
1. Auto-detect version from schema (inspect columns)
2. Extract `ds` from path if missing (v0.3 fallback)
3. Use version-specific crosswalk and join keys
4. Normalize into common `carrier_data` schema
5. Build version-specific database (e.g., `duck_suppression_v03.db`)
6. Save build config JSON for reproducibility

### Important Notes
- Cube builders, dashboards, and analysis tools are version-agnostic
- They operate on normalized `carrier_data` table
- Can't directly compare v0.3 and v15.0 at census block level (different geographies)
- Aggregate to DMA level for cross-version comparisons
- Each build creates a `*_build_config.json` with metadata

### Related Documentation
- `analysis/preagg_v03_support/IMPLEMENTATION_PHASE1.md` - Implementation details
- `analysis/preagg_v03_support/MIGRATION_PLAN.md` - Full technical details
- `analysis/preagg_v03_support/QUICK_SUMMARY.md` - Executive summary

