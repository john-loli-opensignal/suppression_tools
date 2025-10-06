# Main.py Restoration Project Plan

## Executive Summary
This document outlines the plan to restore `main.py` functionality by migrating from CSV-based cubes to DuckDB database-backed operations, while preserving the proven suppression workflow.

---

## Current State Analysis

### What main.py Does (Historical)
1. **Data Source**: Reads from `duckdb_partitioned_store/**/*.parquet` files
2. **Cube Building**: Builds CSV cubes (`win_cube_mover.csv`, `win_cube_non_mover.csv`)
3. **Workflow**:
   - Step 0: Preview base graph (unsuppressed timeseries)
   - Step 1: Scan base outliers (national level, positive only)
   - Step 2: Build suppression plan from outliers (2-stage approach)
   - Step 3: Save plan as CSV to `suppressions/` folder
   - Step 4: Build suppressed dataset (optional)

### What We Have Now (Database Infrastructure)
1. **Database**: `data/databases/duck_suppression.db` (6-10 GB)
2. **Pre-aggregated Cube Tables**:
   - `{ds}_win_{mover}_cube` (e.g., `gamoshi_win_mover_cube`)
   - `{ds}_loss_{mover}_cube`
   - `all_win_{mover}_cube` (combined dataset)
3. **Rolling Views** for Outlier Detection:
   - `gamoshi_win_mover_rolling`
   - `gamoshi_win_non_mover_rolling`
   - Contains: 28d avg/stddev, z-scores, pct_change, is_first_appearance, is_outlier
4. **Suppression Tables** (experimental, from recent analysis):
   - `gamoshi_win_mover_round1/2/3`
   - `suppression_round_1/2_mover`
5. **Utility Modules**:
   - `tools.db` - Database connection management
   - `tools.src.metrics` - National timeseries, ranked winners, etc.
   - `tools.src.outliers` - Outlier detection functions
   - `tools.src.plan` - Plan building logic (CURRENTLY USES PARQUET)
   - `tools.src.suppress` - Suppression application logic

### Key Differences: CSV Workflow vs Database Workflow

| Aspect | Old (CSV) | New (Database) |
|--------|-----------|----------------|
| Data Source | Parquet files via glob | Database cube tables |
| Cube Storage | CSV files (100-500 MB each) | Database tables (indexed, fast) |
| Outlier Detection | Calculated during scan | Pre-calculated in rolling views |
| Speed | Slow (~30-60 seconds) | Blazing fast (<1 second) |
| Rolling Metrics | Calculated on-the-fly | Pre-computed in views |
| DOW Handling | Manual calculation | Built into rolling views |
| Suppression Storage | CSV files only | Can use database tables |

---

## Migration Challenges Identified

### 1. tools.src.plan Module
**Issue**: Currently reads from parquet files using duckdb.connect() with parquet_scan()

**Functions to Update**:
- `base_national_series()` - Get timeseries for preview
- `scan_base_outliers()` - Find national outliers
- `build_plan_for_winner_dates()` - Core planning logic (2-stage approach)

**What They Need**:
- Replace parquet_scan with database table queries
- Use rolling views for outlier detection instead of calculating on-the-fly
- Leverage pre-calculated metrics (z-score, pct_change, first_appearance)

### 2. Cube Building in main.py
**Issue**: Calls `build_win_cube.py` script which creates CSV files

**Current Behavior**:
- User clicks "Build mover cube (True)" button
- Subprocess calls `build_win_cube.py --store <glob> --ds <ds> --mover-ind True -o <csv_path>`
- Creates CSV file with outlier columns pre-calculated

**New Approach**:
- Cubes already exist in database (built via `build_cubes_in_db.py`)
- Remove cube building buttons OR 
- Replace with "Refresh rolling views" if needed
- Auto-check if tables exist, error if missing

### 3. Suppression Plan Schema
**Issue**: Current CSVs have these columns but database needs a schema

**Current CSV Columns** (from main.py line 172-190):
```python
{
    'date', 'winner', 'mover_ind', 'loser', 'dma_name',
    'remove_units', 'impact', 'stage',
    'nat_share_current', 'nat_mu_share', 'nat_sigma_share', 'nat_mu_window',
    'pair_wins_current', 'pair_mu_wins', 'pair_sigma_wins', 'pair_mu_window', 'pair_z',
    'dma_wins', 'pair_share', 'pair_share_mu'
}
```

**Proposed Database Schema**:
```sql
CREATE SCHEMA IF NOT EXISTS suppressions;

CREATE TABLE suppressions.plans (
    plan_id INTEGER,
    plan_name VARCHAR,
    created_at TIMESTAMP,
    ds VARCHAR,
    mover_ind BOOLEAN,
    the_date DATE,
    winner VARCHAR,
    loser VARCHAR,
    dma_name VARCHAR,
    state VARCHAR,
    remove_units INTEGER,
    impact DOUBLE,
    stage VARCHAR,  -- 'auto' or 'distributed'
    -- National metrics
    nat_share_current DOUBLE,
    nat_mu_share DOUBLE,
    nat_sigma_share DOUBLE,
    nat_mu_window INTEGER,
    -- Pair metrics
    pair_wins_current DOUBLE,
    pair_mu_wins DOUBLE,
    pair_sigma_wins DOUBLE,
    pair_mu_window INTEGER,
    pair_z DOUBLE,
    -- DMA metrics
    dma_wins DOUBLE,
    pair_share DOUBLE,
    pair_share_mu DOUBLE,
    PRIMARY KEY (plan_id, the_date, winner, loser, dma_name)
);
```

### 4. Configuration Management
**Issue**: main.py hardcodes paths and uses text inputs

**Current Config**:
- `default_store()` returns `duckdb_partitioned_store/**/*.parquet`
- Cube paths hardcoded to `current_run_duckdb/win_cube_mover.csv`
- Suppression dir hardcoded to `suppressions/`

**Proposed Config**:
- Use `tools.db.get_default_db_path()` for database
- Remove store_glob input (no longer needed)
- Remove cube path inputs (tables always in database)
- Keep suppression dir but add "Save to Database" option

---

## Migration Strategy

### Phase 1: Update tools.src.plan Module
**Priority**: HIGH (blocks everything else)

**Changes Required**:
1. **Add new function**: `base_national_series_db()` 
   - Use `tools.src.metrics.national_timeseries()` (already exists!)
   - Returns same format as old function
   
2. **Add new function**: `scan_base_outliers_db()`
   - Query rolling view: `{ds}_win_{mover}_rolling`
   - Filter WHERE `is_outlier = TRUE AND the_date BETWEEN ... `
   - Return (the_date, winner) pairs
   
3. **Add new function**: `build_plan_from_rolling_view()`
   - Core planning logic using rolling view data
   - 2-stage approach:
     - **Stage 1 (Auto)**: Target pairs flagged as outliers
       - z-score based (zscore > 1.5)
       - Percentage based (pct_change > 0.30)
       - First appearance (is_first_appearance = TRUE)
       - Minimum threshold (current >= 10)
     - **Stage 2 (Distributed)**: Equalize remaining across all pairs
   - Returns DataFrame with plan schema

**Testing Approach**:
- Create `test_plan_db.py` to validate functions work
- Compare results against old CSV-based approach for 2-3 dates
- Validate that Stage 1/Stage 2 logic produces similar plans

### Phase 2: Update main.py UI
**Priority**: MEDIUM (depends on Phase 1)

**Changes Required**:
1. **Sidebar - Data Source**:
   ```python
   # REMOVE: store_glob input
   # REMOVE: cube path inputs
   # REMOVE: Build cube buttons
   
   # ADD:
   st.sidebar.header('Database')
   db_path = st.sidebar.text_input('Database', value=get_default_db_path(), disabled=True)
   # Validate database exists and has required tables
   ```

2. **Step 0: Preview Base Graph**:
   - Replace `base_national_series()` with `base_national_series_db()`
   - Should work immediately (uses `metrics.national_timeseries()`)

3. **Step 1: Scan Base Outliers**:
   ```python
   # REMOVE: use_cube checkbox (always use database now)
   # UPDATE: Call scan_base_outliers_db() instead
   ```

4. **Step 2: Build Plan**:
   - Replace CSV cube reading with rolling view query
   - Use `build_plan_from_rolling_view()` function
   - Keep same display logic (works with DataFrame)

5. **Step 3: Save Plan**:
   - Keep CSV export (backward compatible)
   - ADD: Optional database save
   ```python
   col1, col2, col3 = st.columns(3)
   with col1:
       if st.button('Save as CSV'):
           # existing logic
   with col2:
       if st.button('Save to Database'):
           # new logic: insert into suppressions.plans table
   with col3:
       if st.button('Save Both'):
           # do both
   ```

6. **Step 4: Build Suppressed Dataset**:
   - This needs investigation (likely uses CSV files)
   - May need to update to read from database tables

### Phase 3: Create Suppression Schema in Database
**Priority**: MEDIUM (for proper persistence)

**Implementation**:
1. Create `scripts/create_suppression_schema.py`:
   ```python
   import duckdb
   from tools.db import get_default_db_path
   
   con = duckdb.connect(get_default_db_path(), read_only=False)
   con.execute("CREATE SCHEMA IF NOT EXISTS suppressions")
   con.execute("""
       CREATE TABLE IF NOT EXISTS suppressions.plans (
           -- schema from above
       )
   """)
   con.execute("""
       CREATE SEQUENCE IF NOT EXISTS suppressions.plan_id_seq
   """)
   ```

2. Add helper functions to `tools.db`:
   - `save_suppression_plan(plan_df, plan_name, ds, mover_ind)`
   - `load_suppression_plans(plan_names=None, ds=None, date_range=None)`
   - `list_suppression_plans()`

3. Update main.py to use these functions

### Phase 4: Update Documentation
**Priority**: LOW (but important)

**Files to Update**:
1. `README.md` - Update workflow description
2. `AGENTS.md` - Document new database-first approach
3. `.agent_context.json` - Update with new structure
4. Create `docs/SUPPRESSION_WORKFLOW.md` - Detailed guide

---

## Detailed Implementation Plan

### Step-by-Step Execution Order

#### Task 1: Validate Current Database State
**Estimated Time**: 15 minutes

```bash
# Check tables exist
uv run python3 -c "from tools.db import *; print(table_exists('gamoshi_win_mover_cube'))"
uv run python3 -c "from tools.db import *; print(table_exists('gamoshi_win_mover_rolling'))"

# Check rolling view has correct schema
uv run python3 -c "import duckdb; con = duckdb.connect('data/databases/duck_suppression.db'); print(con.execute('DESCRIBE gamoshi_win_mover_rolling').df())"

# Validate rolling view has outlier flags
uv run python3 -c "import duckdb; con = duckdb.connect('data/databases/duck_suppression.db'); print(con.execute('SELECT COUNT(*) FROM gamoshi_win_mover_rolling WHERE is_outlier = TRUE').df())"
```

**Success Criteria**:
- All cube tables exist
- All rolling views exist
- Rolling views have outlier columns populated

#### Task 2: Create test_plan_db.py
**Estimated Time**: 30 minutes

Create `analysis/test_plan_db.py` to validate new functions work:

```python
"""
Test script to validate database-backed planning functions.
Compares against known dates with outliers.
"""
import pandas as pd
from tools.db import query, get_default_db_path
from tools.src.metrics import national_timeseries

# Test 1: National timeseries
print("Test 1: National timeseries")
df = national_timeseries(
    ds='gamoshi',
    mover_ind=True,
    start_date='2025-06-15',
    end_date='2025-06-25'
)
print(df.head())
print(f"Shape: {df.shape}")

# Test 2: Get outliers from rolling view
print("\nTest 2: Outliers from rolling view")
sql = """
SELECT the_date, winner, zscore, pct_change, is_first_appearance
FROM gamoshi_win_mover_rolling
WHERE is_outlier = TRUE
    AND the_date BETWEEN '2025-06-15' AND '2025-06-25'
ORDER BY the_date, winner
"""
outliers = query(sql)
print(outliers)
print(f"Found {len(outliers)} outlier date-winner pairs")

# Test 3: Get plan data for one outlier
if not outliers.empty:
    test_date = outliers.iloc[0]['the_date']
    test_winner = outliers.iloc[0]['winner']
    print(f"\nTest 3: Plan data for {test_winner} on {test_date}")
    
    sql = f"""
    SELECT 
        the_date, winner, loser, dma_name, state,
        total_wins as pair_wins_current,
        avg_wins_28d as pair_mu_wins,
        stddev_wins_28d as pair_sigma_wins,
        n_periods_28d as pair_mu_window,
        zscore as pair_z,
        pct_change,
        is_first_appearance,
        is_outlier
    FROM gamoshi_win_mover_rolling
    WHERE the_date = '{test_date}'
        AND winner = '{test_winner}'
        AND (is_outlier = TRUE OR is_first_appearance = TRUE)
    ORDER BY zscore DESC NULLS LAST
    LIMIT 20
    """
    plan_data = query(sql)
    print(plan_data)
    print(f"\nFound {len(plan_data)} pairs to potentially suppress")
```

**Success Criteria**:
- Script runs without errors
- Returns data for all 3 tests
- Outliers found for known problematic dates (2025-06-19, 2025-08-15-18)

#### Task 3: Implement base_national_series_db()
**Estimated Time**: 15 minutes

Add to `tools/src/plan.py`:

```python
def base_national_series_db(
    ds: str, 
    mover_ind: str | bool, 
    winners: List[str], 
    start_date: str, 
    end_date: str,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Get national timeseries from database for preview graph.
    
    Args:
        ds: Dataset name (e.g., 'gamoshi')
        mover_ind: Mover indicator (True/False or 'True'/'False')
        winners: List of winner carriers to include
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        db_path: Optional database path
        
    Returns:
        DataFrame with columns: the_date, winner, win_share
    """
    from tools.src import metrics
    
    # Get full timeseries
    df = metrics.national_timeseries(
        ds=ds,
        mover_ind=mover_ind,
        start_date=start_date,
        end_date=end_date,
        db_path=db_path
    )
    
    # Filter to selected winners
    if winners:
        df = df[df['winner'].isin(winners)]
    
    # Return just what we need for the graph
    return df[['the_date', 'winner', 'win_share']].copy()
```

#### Task 4: Implement scan_base_outliers_db()
**Estimated Time**: 20 minutes

Add to `tools/src/plan.py`:

```python
def scan_base_outliers_db(
    ds: str,
    mover_ind: str | bool,
    start_date: str,
    end_date: str,
    window: int = 14,  # Not used, kept for compatibility
    z_thresh: float = 1.5,  # Can override rolling view default
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Scan for national-level outliers using rolling view.
    
    Args:
        ds: Dataset name
        mover_ind: Mover indicator
        start_date: Start of view window
        end_date: End of view window
        window: Not used (rolling view pre-calculated)
        z_thresh: Z-score threshold (default: 1.5)
        db_path: Optional database path
        
    Returns:
        DataFrame with columns: the_date, winner
    """
    from tools import db as db_module
    
    # Normalize mover_ind
    if isinstance(mover_ind, str):
        mover_ind = (mover_ind == 'True')
    
    mover_str = "mover" if mover_ind else "non_mover"
    view_name = f"{ds}_win_{mover_str}_rolling"
    
    sql = f"""
    SELECT DISTINCT
        the_date,
        winner
    FROM {view_name}
    WHERE is_outlier = TRUE
        AND the_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        AND total_wins >= 10  -- Minimum threshold
        AND zscore >= {z_thresh}  -- Override default if needed
    ORDER BY the_date, winner
    """
    
    return db_module.query(sql, db_path)
```

#### Task 5: Implement build_plan_from_rolling_view()
**Estimated Time**: 60-90 minutes (MOST COMPLEX)

Add to `tools/src/plan.py`:

```python
def build_plan_from_rolling_view(
    ds: str,
    mover_ind: str | bool,
    outlier_dates_winners: pd.DataFrame,  # (the_date, winner) pairs from scan
    z_thresh: float = 1.5,
    pct_thresh: float = 0.30,
    min_current: int = 10,
    db_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Build suppression plan using rolling view data.
    
    Implements 2-stage approach:
    1. Auto: Remove outlier pairs (z-score, pct_change, first_appearance)
    2. Distributed: Equalize remaining removal across all pairs
    
    Args:
        ds: Dataset name
        mover_ind: Mover indicator
        outlier_dates_winners: DataFrame with (the_date, winner) to target
        z_thresh: Z-score threshold for outliers
        pct_thresh: Percentage change threshold (e.g., 0.30 for 30%)
        min_current: Minimum current value to consider
        db_path: Optional database path
        
    Returns:
        DataFrame with plan schema (see line 172-190 of main.py)
    """
    from tools import db as db_module
    import numpy as np
    
    if outlier_dates_winners.empty:
        return pd.DataFrame()
    
    # Normalize mover_ind
    if isinstance(mover_ind, str):
        mover_ind = (mover_ind == 'True')
    mover_str = "mover" if mover_ind else "non_mover"
    view_name = f"{ds}_win_{mover_str}_rolling"
    cube_name = f"{ds}_win_{mover_str}_cube"
    
    # TODO: Continue implementation...
    # This is the complex part - need to replicate logic from main.py lines 116-190
    # Key steps:
    # 1. For each (date, winner) pair:
    #    a. Get national stats (aggregate across all pairs)
    #    b. Calculate removal target (W - mu*T) / (1 - mu)
    #    c. Stage 1: Auto-remove outlier pairs
    #    d. Stage 2: Distribute remaining removal
    # 2. Build rows with all metrics
    # 3. Return DataFrame
```

**Note**: This function is complex and needs careful implementation. Should be done incrementally with testing.

#### Task 6: Update main.py Sidebar
**Estimated Time**: 20 minutes

```python
def ui():
    st.set_page_config(page_title='Suppression Tools', page_icon='🧰', layout='wide')
    st.title('🧰 Suppression Tools (Base → Outliers → Plan)')

    st.sidebar.header('Database')
    from tools.db import get_default_db_path, connect, table_exists
    
    db_path = get_default_db_path()
    st.sidebar.text_input('Database Path', value=db_path, disabled=True)
    
    # Validate database
    try:
        con = connect(db_path)
        con.close()
        st.sidebar.success('✅ Database connected')
    except Exception as e:
        st.sidebar.error(f'❌ Database error: {e}')
        st.stop()
    
    st.sidebar.header('Dataset Selection')
    ds = st.sidebar.text_input('ds', value='gamoshi')
    mover_ind = st.sidebar.selectbox('mover_ind', ['False','True'], index=0)
    
    # Validate required tables exist
    mover_str = "mover" if mover_ind == 'True' else "non_mover"
    required_tables = [
        f"{ds}_win_{mover_str}_cube",
        f"{ds}_win_{mover_str}_rolling"
    ]
    
    missing = [t for t in required_tables if not table_exists(t, db_path)]
    if missing:
        st.sidebar.error(f'❌ Missing tables: {", ".join(missing)}')
        st.sidebar.info('Run: uv run build_cubes_in_db.py --all')
        st.stop()
    else:
        st.sidebar.success(f'✅ All tables exist')
    
    # REST OF UI...
```

#### Task 7: Update main.py Steps 0-2
**Estimated Time**: 30 minutes

Update each step to use new database functions instead of parquet/CSV.

#### Task 8: Add Database Save Option
**Estimated Time**: 30 minutes

Implement suppression schema creation and save functions.

#### Task 9: Testing & Validation
**Estimated Time**: 60 minutes

- Test full workflow end-to-end
- Compare plans generated against old CSV method
- Validate graphs look correct
- Test edge cases (no outliers, missing data, etc.)

#### Task 10: Documentation
**Estimated Time**: 30 minutes

Update all relevant documentation files.

---

## Risk Assessment

### High Risk Items
1. **build_plan_from_rolling_view() complexity**: This replicates complex logic from main.py lines 116-190
   - **Mitigation**: Break into smaller functions, test incrementally
   
2. **Schema mismatch**: Rolling view might not have all metrics needed for plan
   - **Mitigation**: Add computed columns to rolling view if needed

3. **Performance regression**: Database queries might be slower than expected
   - **Mitigation**: Profile queries, add indexes if needed (already have some)

### Medium Risk Items
1. **Backward compatibility**: Existing CSV-based workflows might break
   - **Mitigation**: Keep CSV export, make database optional initially

2. **User confusion**: UI changes might confuse existing users
   - **Mitigation**: Add tooltips, help text, migration guide

### Low Risk Items
1. **Testing coverage**: Hard to test all edge cases
   - **Mitigation**: Focus on known problematic dates from analysis

---

## Success Criteria

### Must Have
- [ ] main.py runs without errors
- [ ] All 4 steps work (preview, scan, plan, save)
- [ ] Plans generated match old CSV method (within 10% for test dates)
- [ ] Performance is at least as fast as old method (preferably faster)
- [ ] Database contains all necessary tables and views

### Should Have
- [ ] Plans can be saved to database (not just CSV)
- [ ] UI is intuitive and provides good feedback
- [ ] Documentation is updated
- [ ] Tests validate key functionality

### Nice to Have
- [ ] Plans can be loaded from database for review
- [ ] UI shows plan history/versioning
- [ ] Performance is significantly faster (>2x)

---

## Timeline Estimate

### Optimistic: 4-5 hours
- Everything works first try
- No unexpected schema issues
- Testing is smooth

### Realistic: 6-8 hours
- Some debugging needed
- Minor schema adjustments
- Comprehensive testing

### Pessimistic: 10-12 hours
- Major issues with build_plan_from_rolling_view()
- Rolling view missing critical metrics
- Need to create additional database views
- Extensive refactoring needed

---

## Open Questions for User

1. **Suppression Storage**: Should we:
   - A. Keep CSV-only (backward compatible)
   - B. Database-only (modern, faster)
   - C. Both (most flexible but more complex)

2. **build_suppressed_dataset.py**: Does this script exist? What does it do?
   - How does it consume suppression plans?
   - Does it need to be updated too?

3. **Rolling View Metrics**: Does the current rolling view have everything needed?
   - Check: Do we need DMA-level aggregates for plan building?
   - Check: Do we need national-level stats per (date, winner)?

4. **Top 50 Carriers Filter**: Should main.py limit to top 50 carriers by default?
   - This was mentioned as important for performance
   - Should be optional or enforced?

5. **Outlier Threshold Defaults**: Current values in rolling view:
   - z_score: 1.5 (main.py default: 2.5)
   - pct_change: 0.30
   - Which should main.py use? Parameterize or hard-code?

6. **Census Block**: Should main.py support census block level analysis?
   - We have `gamoshi_win_mover_census_cube` table
   - Is this POC-only or production feature?

---

## Next Steps

Once you answer the open questions above, I will:

1. Create a feature branch for this work
2. Implement changes incrementally (one commit per task)
3. Test each component before moving to next
4. Provide progress updates
5. Request validation at key milestones

**Recommendation**: Start with Tasks 1-5 (validation + new functions), get those working and tested, then move to UI updates. This minimizes risk and allows for early validation.
