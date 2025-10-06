# Main.py Restoration Project Plan

**Date:** 2025-10-04  
**Goal:** Restore main.py to working state using database-backed cube tables instead of CSV files

---

## Executive Summary

Main.py currently references CSV cube files and parquet stores that no longer exist. We need to migrate it to use the DuckDB cube tables and rolling views that now power the project. The core outlier detection and distribution logic is sound - we just need to swap data sources.

---

## Current State Assessment

### What Works ✅
- **Database infrastructure:** `data/databases/duck_suppression.db` with all cube tables
- **Rolling views:** `gamoshi_win_mover_rolling` and `gamoshi_win_non_mover_rolling` with DOW-aware metrics
- **Outlier methods:** Z-score (1.5/2.0), percentage thresholds (30%), first appearance detection
- **Distribution algorithm:** Two-stage approach (auto outliers + distributed remainder)
- **UI structure:** Streamlit layout with 5-step workflow is intuitive

### What's Broken ❌
- **Data source:** References CSV cubes that don't exist (`win_cube_mover.csv`, `win_cube_non_mover.csv`)
- **Parquet glob:** References `duckdb_partitioned_store/**/*.parquet` (deleted)
- **Module imports:** Uses `tools.src.plan` functions that query parquet directly
- **Cube building:** Calls `build_win_cube.py` script that doesn't exist
- **Suppression storage:** Saves to CSV but no mechanism to load into DB

---

## Architecture Changes

### Data Flow (Old → New)

**OLD:**
```
Parquet files → DuckDB temp query → CSV cubes → pandas → UI
```

**NEW:**
```
DuckDB cube tables → Rolling views → pandas → UI
                   ↓
         Suppression tables (per round)
```

### Database Schema for Suppressions

Create `suppressions` schema in database:

```sql
-- Suppression round metadata
CREATE TABLE IF NOT EXISTS suppressions.rounds (
    round_id INTEGER PRIMARY KEY,
    round_name VARCHAR,
    ds VARCHAR,
    mover_ind BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

-- Suppression plan records
CREATE TABLE IF NOT EXISTS suppressions.plans (
    plan_id INTEGER PRIMARY KEY,
    round_id INTEGER REFERENCES suppressions.rounds(round_id),
    the_date DATE,
    winner VARCHAR,
    loser VARCHAR,
    dma_name VARCHAR,
    state VARCHAR,
    remove_units INTEGER,
    stage VARCHAR, -- 'auto' or 'distributed'
    impact INTEGER,
    -- National metrics
    nat_share_current DOUBLE,
    nat_mu_share DOUBLE,
    nat_sigma_share DOUBLE,
    nat_z_score DOUBLE,
    -- Pair metrics
    pair_wins_current INTEGER,
    pair_mu_wins DOUBLE,
    pair_sigma_wins DOUBLE,
    pair_z_score DOUBLE,
    pair_pct_change DOUBLE,
    -- Flags
    is_first_appearance BOOLEAN,
    is_rare_pair BOOLEAN,
    is_outlier BOOLEAN
);
```

---

## Implementation Plan

### Phase 1: Update Data Source Methods (tools/src/plan.py)

**Files to modify:** `tools/src/plan.py`

#### 1.1 Remove parquet-based `base_national_series()`
- **Current:** Queries parquet files directly
- **New:** Query database cube tables with national aggregation

```python
def base_national_series(
    ds: str, 
    mover_ind: bool, 
    winners: List[str], 
    start_date: str, 
    end_date: str,
    db_path: str = None
) -> pd.DataFrame:
    """Get national win share time series from database cubes."""
    if db_path is None:
        db_path = get_db_path()
    
    cube_table = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"
    
    # Query to get national shares
    sql = f"""
        SELECT 
            the_date,
            winner,
            SUM(total_wins) as winner_total,
            SUM(SUM(total_wins)) OVER (PARTITION BY the_date) as market_total,
            SUM(total_wins) / NULLIF(SUM(SUM(total_wins)) OVER (PARTITION BY the_date), 0) as win_share
        FROM {cube_table}
        WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
            AND winner IN ({','.join([f"'{w}'" for w in winners])})
        GROUP BY the_date, winner
        ORDER BY the_date, winner
    """
    return db.query(sql, db_path)
```

#### 1.2 Remove parquet-based `scan_base_outliers()`
- **Current:** Calculates DOW-partitioned z-scores on parquet
- **New:** Use pre-computed rolling views

```python
def scan_base_outliers(
    ds: str,
    mover_ind: bool,
    start_date: str,
    end_date: str,
    z_threshold: float = 2.5,
    db_path: str = None
) -> pd.DataFrame:
    """Scan for national-level outliers using rolling views."""
    if db_path is None:
        db_path = get_db_path()
    
    rolling_view = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_rolling"
    
    # National aggregation with outlier flag
    sql = f"""
        WITH national_daily AS (
            SELECT 
                the_date,
                winner,
                SUM(total_wins) as total_wins,
                SUM(avg_wins_28d) as avg_wins,
                -- National z-score calculation
                CASE 
                    WHEN STDDEV(total_wins) OVER (
                        PARTITION BY winner, day_of_week 
                        ORDER BY the_date 
                        ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                    ) > 0 THEN
                        (total_wins - AVG(total_wins) OVER (
                            PARTITION BY winner, day_of_week 
                            ORDER BY the_date 
                            ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                        )) / NULLIF(STDDEV(total_wins) OVER (
                            PARTITION BY winner, day_of_week 
                            ORDER BY the_date 
                            ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                        ), 0)
                    ELSE 0
                END as nat_z_score
            FROM {rolling_view}
            WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY the_date, winner, day_of_week, total_wins
        )
        SELECT DISTINCT the_date, winner
        FROM national_daily
        WHERE nat_z_score > {z_threshold}
        ORDER BY the_date, winner
    """
    return db.query(sql, db_path)
```

### Phase 2: Create Cube View Builder (tools/src/plan.py)

**New function:** Build enriched cube view with all metrics needed for UI

```python
def build_enriched_cube(
    ds: str,
    mover_ind: bool,
    db_path: str = None
) -> str:
    """Create/refresh materialized view with all metrics for UI.
    
    Returns: view name
    """
    if db_path is None:
        db_path = get_db_path()
    
    view_name = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_enriched"
    rolling_view = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_rolling"
    
    # Create view with all needed metrics
    sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH pair_level AS (
            SELECT 
                the_date,
                winner,
                loser,
                dma_name,
                state,
                total_wins as pair_wins_current,
                avg_wins_28d as pair_mu_wins,
                stddev_wins_28d as pair_sigma_wins,
                zscore as pair_z,
                pct_change,
                is_first_appearance as new_pair,
                is_outlier as pair_outlier_pos,
                CASE WHEN pct_change > 30 THEN true ELSE false END as pct_outlier_pos,
                CASE WHEN appearance_rank <= 5 THEN true ELSE false END as rare_pair,
                n_periods_28d as pair_mu_window
            FROM {rolling_view}
            WHERE total_wins >= 5  -- Minimum volume filter
        ),
        national_agg AS (
            SELECT
                the_date,
                winner,
                SUM(pair_wins_current) as nat_total_wins,
                SUM(SUM(pair_wins_current)) OVER (PARTITION BY the_date) as nat_market_wins,
                SUM(pair_wins_current) / NULLIF(
                    SUM(SUM(pair_wins_current)) OVER (PARTITION BY the_date), 0
                ) as nat_share_current,
                -- Rolling national metrics
                AVG(SUM(pair_wins_current)) OVER (
                    PARTITION BY winner, DAYOFWEEK(the_date)
                    ORDER BY the_date
                    ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                ) as nat_mu_share_abs,
                STDDEV(SUM(pair_wins_current)) OVER (
                    PARTITION BY winner, DAYOFWEEK(the_date)
                    ORDER BY the_date
                    ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                ) as nat_sigma_share,
                COUNT(*) OVER (
                    PARTITION BY winner, DAYOFWEEK(the_date)
                    ORDER BY the_date
                    ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING
                ) as nat_mu_window
            FROM pair_level
            GROUP BY the_date, winner
        )
        SELECT 
            p.*,
            n.nat_total_wins,
            n.nat_market_wins,
            n.nat_share_current,
            n.nat_mu_share_abs / NULLIF(n.nat_market_wins, 0) as nat_mu_share,
            n.nat_sigma_share,
            n.nat_mu_window
        FROM pair_level p
        JOIN national_agg n USING (the_date, winner)
    """
    
    db.execute(sql, db_path)
    return view_name
```

### Phase 3: Update main.py UI

**File to modify:** `main.py`

#### 3.1 Update imports and defaults

```python
import tools.db as db
from tools.src.plan import (
    build_enriched_cube,
    base_national_series, 
    scan_base_outliers
)

def default_db() -> str:
    return db.get_db_path()
```

#### 3.2 Update sidebar controls

```python
st.sidebar.header('Data Source')
db_path = st.sidebar.text_input('Database', value=default_db())
ds = st.sidebar.selectbox('Dataset', ['gamoshi'], index=0)  # Add more as available
mover_ind = st.sidebar.selectbox('mover_ind', [False, True], index=0, format_func=lambda x: 'True' if x else 'False')

# Remove parquet/CSV references
```

#### 3.3 Update Step 0: Base graph

```python
if st.button('Show base graph'):
    try:
        ts = base_national_series(
            ds=ds,
            mover_ind=mover_ind,
            winners=winners,
            start_date=str(view_start),
            end_date=str(view_end),
            db_path=db_path
        )
        # ... plotting code stays same ...
```

#### 3.4 Update Step 1: Scan outliers

```python
if st.button('Scan base outliers (view)'):
    try:
        # Ensure enriched cube exists
        build_enriched_cube(ds, mover_ind, db_path)
        
        # Scan for outliers
        out = scan_base_outliers(
            ds=ds,
            mover_ind=mover_ind,
            start_date=str(view_start),
            end_date=str(view_end),
            z_threshold=float(z),
            db_path=db_path
        )
        st.session_state['base_out'] = out
        # ... display code stays same ...
```

#### 3.5 Update Step 2: Build plan

```python
if st.button('Build plan preview'):
    out = st.session_state.get('base_out')
    if out is None or out.empty:
        st.error('No base outliers available. Run the scan first.')
    else:
        try:
            # Get enriched cube data
            view_name = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_enriched"
            
            # Filter to target dates/winners
            targets = out.copy()
            filter_clause = " OR ".join([
                f"(the_date = '{row.the_date}' AND winner = '{row.winner}')"
                for _, row in targets.iterrows()
            ])
            
            sql = f"SELECT * FROM {view_name} WHERE {filter_clause}"
            cube = db.query(sql, db_path)
            
            # Build plan per (date, winner) - SAME LOGIC AS BEFORE
            rows = []
            for (d, w), sub in cube.groupby(['the_date', 'winner']):
                # Calculate need
                nat = sub.iloc[0]
                W = float(nat['nat_total_wins'])
                T = float(nat['nat_market_wins'])
                mu = float(nat['nat_mu_share'])
                need = int(np.ceil(max((W - mu * T) / max(1e-12, (1 - mu)), 0)))
                
                # Stage 1: Auto from outliers (z-score, %, first appearance, rare)
                auto = sub[
                    (sub['pair_outlier_pos'] == True) |
                    (sub['pct_outlier_pos'] == True) |
                    (sub['rare_pair'] == True) |
                    (sub['new_pair'] == True)
                ].copy()
                
                # ... rest of distribution logic STAYS THE SAME ...
```

#### 3.6 Update Step 3: Save plan

Instead of CSV only, save to both CSV (for compatibility) and database:

```python
if st.button('Save plan CSV and DB'):
    if plan_prev is None or plan_prev.empty:
        st.error('No plan preview to save.')
    else:
        # Save CSV for backwards compatibility
        out_dir = os.path.join(os.getcwd(), 'suppressions', 'rounds')
        os.makedirs(out_dir, exist_ok=True)
        csv_path = os.path.join(out_dir, f'{round_name}.csv')
        plan_prev.to_csv(csv_path, index=False)
        
        # Save to database
        try:
            con = db.get_connection(db_path)
            
            # Create suppressions schema if not exists
            con.execute("CREATE SCHEMA IF NOT EXISTS suppressions")
            
            # Create rounds table
            con.execute("""
                CREATE TABLE IF NOT EXISTS suppressions.rounds (
                    round_name VARCHAR PRIMARY KEY,
                    ds VARCHAR,
                    mover_ind BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    num_records INTEGER
                )
            """)
            
            # Insert round metadata
            con.execute("""
                INSERT OR REPLACE INTO suppressions.rounds 
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)
            """, [round_name, ds, mover_ind, len(plan_prev)])
            
            # Create plan table
            table_name = f"suppressions.{round_name}"
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM plan_prev")
            con.register('plan_prev', plan_prev)
            con.execute(f"INSERT INTO {table_name} SELECT * FROM plan_prev")
            
            st.success(f'✅ Saved plan to CSV: {csv_path}')
            st.success(f'✅ Saved plan to DB: {table_name}')
            st.info('Reload suppression dashboard to apply.')
        except Exception as e:
            st.error(f'Database save failed: {e}')
```

#### 3.7 Update Step 5: Preview graph

Update to use database for suppression preview:

```python
if st.button('Preview graph with plan'):
    try:
        plan_prev = st.session_state.get('plan_prev')
        if plan_prev is None or plan_prev.empty:
            st.error('No plan available.')
        else:
            winners = sorted(plan_prev['winner'].unique().tolist())
            
            # Get base national series
            base_ts = base_national_series(
                ds=ds,
                mover_ind=mover_ind,
                winners=winners,
                start_date=str(view_start),
                end_date=str(view_end),
                db_path=db_path
            )
            
            # Apply suppressions in-memory
            cube_table = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"
            
            # ... suppression logic using db.query() ...
```

### Phase 4: Top 50 Carriers Filter

**New feature:** Focus on top 50 carriers by total wins, but flag egregious outliers outside top 50

#### 4.1 Add to enriched cube view

```python
def get_top_50_carriers(ds: str, mover_ind: bool, db_path: str = None) -> List[str]:
    """Get top 50 carriers by total wins over entire time series."""
    if db_path is None:
        db_path = get_db_path()
    
    cube_table = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"
    
    sql = f"""
        SELECT winner, SUM(total_wins) as total
        FROM {cube_table}
        GROUP BY winner
        ORDER BY total DESC
        LIMIT 50
    """
    df = db.query(sql, db_path)
    return df['winner'].tolist()
```

#### 4.2 Update scan_base_outliers

```python
def scan_base_outliers(
    ds: str,
    mover_ind: bool,
    start_date: str,
    end_date: str,
    z_threshold: float = 2.5,
    top_n: int = 50,
    egregious_threshold: int = 40,  # Impact threshold for non-top-50
    db_path: str = None
) -> pd.DataFrame:
    """Scan outliers, focusing on top N carriers but flagging egregious outliers."""
    
    top_carriers = get_top_50_carriers(ds, mover_ind, db_path)
    
    # ... outlier detection query ...
    
    # Filter: top 50 OR impact > threshold
    sql += f"""
        WHERE (winner IN ({','.join([f"'{w}'" for w in top_carriers])})
               OR impact > {egregious_threshold})
          AND nat_z_score > {z_threshold}
    """
```

### Phase 5: Configuration & Validation

#### 5.1 Update AGENTS.md

Add section about main.py workflow:

```markdown
### Main.py Suppression Workflow

The main.py dashboard provides a 5-step outlier suppression workflow:

1. **Preview base graph** - View unsuppressed national win shares
2. **Scan outliers** - Detect national-level anomalies using rolling views
3. **Build plan** - Generate suppression plan with auto + distributed stages
4. **Save plan** - Store to database and CSV
5. **Preview suppressed** - See before/after comparison

**Data Flow:**
- Cube tables → Rolling views (pre-computed metrics)
- Rolling views → Enriched view (UI-ready data)
- Enriched view → Suppression plans (saved to database)
- Plans applied → Visualizations

**Key Parameters:**
- Z-score threshold: 2.5 (national), 1.5 (DMA pairs)
- DOW windows: 14 days (min 4 for weekends)
- Top N carriers: 50 (default)
- Egregious threshold: 40+ impact (catches outliers outside top 50)
- Minimum volume: 5 wins per day for pair-level analysis
```

#### 5.2 Add validation script

Create `scripts/analysis/validate_main_py.py`:

```python
#!/usr/bin/env python3
"""Validate main.py restoration - ensure all functions work."""

import tools.db as db
from tools.src.plan import (
    get_top_50_carriers,
    build_enriched_cube,
    base_national_series,
    scan_base_outliers
)

def test_top_50():
    """Test top 50 carriers query."""
    carriers = get_top_50_carriers('gamoshi', True)
    assert len(carriers) == 50
    assert 'Spectrum' in carriers
    print(f"✅ Top 50 carriers: {carriers[:5]}...")

def test_enriched_cube():
    """Test enriched cube view creation."""
    view_name = build_enriched_cube('gamoshi', True)
    assert 'enriched' in view_name
    print(f"✅ Enriched cube created: {view_name}")

def test_base_series():
    """Test national time series."""
    ts = base_national_series(
        ds='gamoshi',
        mover_ind=True,
        winners=['Spectrum', 'Comcast'],
        start_date='2025-06-01',
        end_date='2025-06-30'
    )
    assert not ts.empty
    assert 'win_share' in ts.columns
    print(f"✅ Base series: {len(ts)} rows")

def test_scan_outliers():
    """Test outlier scanning."""
    outliers = scan_base_outliers(
        ds='gamoshi',
        mover_ind=True,
        start_date='2025-06-01',
        end_date='2025-06-30',
        z_threshold=2.5
    )
    print(f"✅ Scan outliers: {len(outliers)} found")

if __name__ == '__main__':
    test_top_50()
    test_enriched_cube()
    test_base_series()
    test_scan_outliers()
    print("\n✅ All validation tests passed!")
```

---

## Testing Strategy

### Phase 1: Unit Tests
- Test each new function in isolation
- Verify database queries return expected schema
- Check rolling view calculations

### Phase 2: Integration Tests
- Run validation script
- Test main.py step-by-step with known good data
- Compare results with old CSV approach

### Phase 3: End-to-End Test
- Full workflow: scan → plan → save → preview
- Verify suppression plans match expectations
- Check database tables are created correctly

---

## Migration Checklist

- [ ] **Phase 1:** Update `tools/src/plan.py` functions
  - [ ] `base_national_series()` - use cube tables
  - [ ] `scan_base_outliers()` - use rolling views
  - [ ] `build_enriched_cube()` - create materialized view
  - [ ] `get_top_50_carriers()` - filter function
  
- [ ] **Phase 2:** Update `main.py` UI
  - [ ] Remove CSV/parquet references
  - [ ] Update Step 0 (base graph)
  - [ ] Update Step 1 (scan outliers)
  - [ ] Update Step 2 (build plan)
  - [ ] Update Step 3 (save plan to DB)
  - [ ] Update Step 5 (preview with suppressions)
  
- [ ] **Phase 3:** Database schema
  - [ ] Create `suppressions` schema
  - [ ] Create `suppressions.rounds` metadata table
  - [ ] Create per-round suppression tables
  
- [ ] **Phase 4:** Configuration
  - [ ] Update AGENTS.md with workflow docs
  - [ ] Update .agent_memory.json
  - [ ] Add TODO for census block drill-down
  
- [ ] **Phase 5:** Testing
  - [ ] Create validation script
  - [ ] Run unit tests
  - [ ] End-to-end workflow test
  - [ ] Compare with historical results

---

## Open Questions for User

1. **Top 50 Carriers:**
   - Default to top 50 by wins? ✓
   - Egregious threshold for non-top-50: 40 impact? ✓
   - Should this be configurable in UI?

2. **Rolling Windows:**
   - Confirmed 28d for weekdays, 14d minimum ✓
   - Weekend minimum: 4 preceding? ✓
   - Should we show window size in UI?

3. **Suppression Rounds:**
   - Store in database with round names? ✓
   - Keep CSV for backwards compatibility? ✓
   - How to handle round conflicts/overwriting?

4. **Census Block Drill-Down:**
   - Add as TODO for later? ✓
   - Keep at DMA level for now? ✓
   - What's the priority timeline?

5. **View Parameterization:**
   - Can we dynamically adjust z-score threshold? ✓ (via views)
   - Should enriched view be cached or regenerated each time?
   - Performance tradeoff: views vs materialized tables?

---

## Timeline Estimate

- **Phase 1 (plan.py updates):** 2-3 hours
- **Phase 2 (main.py UI):** 3-4 hours
- **Phase 3 (DB schema):** 1 hour
- **Phase 4 (Config/docs):** 1 hour
- **Phase 5 (Testing):** 2 hours

**Total:** ~10-12 hours of development time

**Validation:** 2 hours for user testing and feedback

---

## Success Criteria

- [ ] Main.py loads without errors
- [ ] All 5 steps work end-to-end
- [ ] Suppression plans saved to database
- [ ] Preview graphs show before/after correctly
- [ ] Performance: < 5 seconds for outlier scan
- [ ] Top 50 carriers filter works
- [ ] Egregious outliers (>40 impact) flagged outside top 50

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Rolling views missing columns | Add computed columns to view definition |
| Performance degradation | Use indexed views, limit to top 50 |
| Breaking existing dashboards | Keep CSV export for backwards compatibility |
| Schema conflicts | Use suppressions namespace/schema |
| Data inconsistency | Add validation queries before plan generation |

---

## Notes

- **No breaking changes** to database structure
- **Additive only:** New views and suppression tables
- **CSV export maintained** for legacy tools
- **Top 50 filter** significantly improves performance
- **Rolling views** eliminate need for runtime calculations
- **Views are parameterizable** - can adjust thresholds without rebuilding cubes

