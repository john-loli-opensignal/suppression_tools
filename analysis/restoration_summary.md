# Main.py Restoration - Summary & Questions

## TLDR

Main.py is a **5-step suppression workflow dashboard** that's broken because it references CSV cubes and parquet files that no longer exist. The core logic is solid - we just need to:

1. **Swap data sources:** CSV cubes → DuckDB rolling views
2. **Add database persistence:** Save suppression plans to DB (not just CSV)
3. **Focus on top 50:** Filter to top 50 carriers, flag egregious outliers (40+ impact) outside
4. **Keep distribution logic:** Two-stage approach (auto outliers + distributed) works well

**Estimated effort:** 10-12 hours development + 2 hours validation

---

## What I Found in Commit History

### The Distribution Approach (circa commit 5e4d75c)

The old system had a **brilliant two-stage suppression algorithm:**

**Stage 1: Auto (Targeted)**
- Removes from pairs that are **actual outliers**:
  - Z-score > threshold (pair-level)
  - Percentage spike > 30%
  - Rare pairs (< 5 appearances)
  - First appearances
- Only removes **excess over baseline** (surgical)
- Minimum 5 wins/day to avoid noise

**Stage 2: Distributed (Spread)**
- If Stage 1 doesn't hit target, distribute remainder
- Spreads evenly across all pairs
- Respects capacity constraints
- Ensures no single DMA bears full burden

This is **exactly what you described wanting** - so we should keep this logic intact, just change the data source.

### Rolling Metrics Discovery

Your old CSV cubes had these columns:
```
- pair_wins_current, pair_mu_wins, pair_sigma_wins, pair_z
- nat_share_current, nat_mu_share, nat_sigma_share
- pair_outlier_pos, pct_outlier_pos, rare_pair, new_pair
- DOW-aware windows (14d weekday, fewer for weekends)
```

**Good news:** The database rolling views (`gamoshi_win_mover_rolling`) already have most of these! We just need to create a view that combines pair-level + national-level metrics.

### DOW Handling

You were doing **day-of-week partitioned rolling windows:**
- Weekdays: 14 day window (14 preceding same-DOW)
- Weekends: 4-14 day window (fewer Saturday/Sunday samples)

This **prevents false positives** from weekend volume spikes. The rolling views already do this.

---

## Key Architectural Decisions

### 1. Views vs Materialized Tables

**Recommendation: Use VIEWS (not materialized)**

**Why:**
- You want to tweak z-score thresholds (1.5, 2.0, 2.5, etc.)
- You want to adjust % thresholds (30%, 40%, etc.)
- Views let you parameterize these in queries
- Performance is fine since cubes are pre-aggregated
- No need to rebuild when changing thresholds

**Example:**
```sql
-- In your query, not the view:
SELECT * FROM gamoshi_win_mover_rolling
WHERE zscore > ? AND pct_change > ?
```

### 2. Suppression Storage

**Recommendation: Database tables + CSV backup**

**Database schema:**
```
suppressions.rounds - Metadata (round name, ds, mover_ind, created_at)
suppressions.{round_name} - Plan records (date, winner, loser, dma, remove_units, etc.)
```

**CSV backup:** Keep for backwards compatibility with other tools

**Why:**
- Database enables querying across rounds
- Can compare round effectiveness
- Easy to reload/revert
- CSV maintains compatibility

### 3. Top 50 Filter

**Recommendation: Filter to top 50, but flag egregious outliers outside**

**Logic:**
```python
top_50 = get_top_50_carriers(ds, mover_ind)  # By total wins

# Focus on top 50
outliers = scan_outliers(..., carriers=top_50)

# But also flag egregious outside top 50
egregious = scan_outliers(..., min_impact=40, exclude_top_50=True)

# Combine
final = outliers + egregious
```

**Why:**
- 90% of volume is in top 50
- Prevents wasting time on tail
- Still catches genuine issues in long tail (40+ impact is suspicious)

### 4. Census Block Drill-Down

**Recommendation: TODO for later (not in scope)**

**Why:**
- DMA-level suppression works well
- Census block adds complexity
- You want main.py working first
- Can add as enhancement later

**Add to README:**
```
## TODO
- [ ] Census block drill-down for surgical suppression (see census_block_outlier_dashboard.py)
- [ ] Integrate CB-level outliers into main.py workflow
```

---

## Clarifying Questions

### 1. Rolling Window Behavior ✓ (Answered by your requirements)

**Weekday:** 14 days (28d calc in rolling view = ~14 same-DOW)
**Weekend:** Minimum 4 preceding same-DOW
**Implementation:** Already in rolling views via DOW partitioning

### 2. Z-Score Thresholds

You mentioned:
- **National level:** 2.5 (Step 1: scan base outliers)
- **DMA pairs:** 1.5 (Step 2: auto stage)

**Should these be:**
- Hard-coded defaults?
- Configurable in UI sidebar?
- Stored in database config table?

**My recommendation:** UI sidebar sliders with sensible defaults

### 3. Egregious Threshold

**You said:** "I'd like to see anything aggregious (impact of 40 or more) outside top 50"

**Questions:**
- Impact = remove_units for that (date, winner, loser, DMA)?
- Or impact = total national impact for (date, winner)?
- Should this be configurable (40 is default)?

**My understanding:** National impact (sum of remove_units for winner on that date)

### 4. Enriched View Scope

Should the enriched view:
- **Option A:** Cover entire time series (expensive, one-time build)
- **Option B:** Filter to graph window (fast, rebuild on window change)
- **Option C:** Cover last N months, refresh nightly (hybrid)

**My recommendation:** Option B (filter to window) - keeps it fast and responsive

### 5. Round Overwrites

When saving a suppression round with existing name:
- **Option A:** Error and require new name
- **Option B:** Overwrite with confirmation
- **Option C:** Auto-version (round_name_v2, round_name_v3)

**My recommendation:** Option A (cleaner, prevents accidents)

### 6. Distribution Caps

In Stage 2 (distributed), you were capping by:
```python
caps = sub[['loser','dma_name','pair_wins_current']].copy()
base = need_after // len(caps)
caps['rm_base'] = np.minimum(caps['pair_wins_current'], base)
```

This **prevents removing more wins than exist**. Should we also:
- Cap at X% of pair_wins_current (e.g., max 50% removal)?
- Respect minimum post-suppression volume?

**Current:** No % cap, but respects capacity
**Recommendation:** Keep as-is unless you've seen issues

---

## What I Still Need From You

### Critical Confirmations

1. **Database path assertion:** I'll add this to every db.get_connection():
   ```python
   assert db_path == 'data/databases/duck_suppression.db', \
       "CRITICAL: Wrong database path! Must be 'data/databases/duck_suppression.db'"
   ```
   Is this acceptable? Will it break anything?

2. **Suppression schema:** Should I create `suppressions` schema or just prefix tables?
   - `suppressions.rounds` vs `suppression_rounds`
   - Schema is cleaner but requires DETACH/ATTACH for cross-db queries

3. **Top 50 calculation:** Total wins over entire time series, or within graph window?
   - **Entire series:** More stable, doesn't change with window
   - **Graph window:** More relevant to current analysis
   
   **My assumption:** Entire time series (more stable)

4. **First appearance at DMA level:** You said:
   > "honestly i'm now thinking that first appearance should probably be at the dma level since we get new blocks everyday"
   
   Should I change the rolling view to calculate first appearance at (winner, loser, DMA) level instead of (winner, loser, DMA, census_block)?
   
   **Current:** Census block level
   **Proposed:** DMA level

### Nice-to-Haves

5. **Performance SLA:** What's acceptable for "Scan base outliers" button?
   - Current cube queries: < 1 second
   - With enriched view + filtering: ~2-5 seconds?
   - Is 5 seconds too slow?

6. **Visualization preferences:** For Step 5 (preview graph):
   - Solid lines for base (original)
   - Dashed lines for suppressed
   - Both on same plot?
   - Or side-by-side subplots?

7. **Error handling:** If enriched view creation fails:
   - Show error and stop?
   - Fall back to cube query?
   - Retry once?

---

## Implementation Approach

Based on your guidance to **validate with you at each step**, here's my proposed flow:

### Step 1: Create helper functions (no UI changes)
- `get_top_50_carriers()`
- `build_enriched_cube()`
- `get_db_path()` with assertion

**Validate:** Run unit tests, show sample queries

### Step 2: Update plan.py data sources
- `base_national_series()` - use cubes
- `scan_base_outliers()` - use rolling views

**Validate:** Show before/after results for June 19

### Step 3: Update main.py Step 0-1 (preview + scan)
- Remove parquet references
- Wire up new functions

**Validate:** Show that steps 0-1 work in UI

### Step 4: Update main.py Step 2 (build plan)
- Use enriched view
- Keep distribution logic

**Validate:** Show sample plan for June 19 outliers

### Step 5: Update main.py Step 3 (save to DB)
- Create suppressions schema
- Save to both DB and CSV

**Validate:** Show database tables created

### Step 6: Update main.py Step 5 (preview)
- Apply suppressions in-memory
- Show before/after graph

**Validate:** Show visual comparison

**At each validation step:** You review, approve, or request changes before I proceed.

---

## Final Recommendations

### DO ✅
1. Keep the two-stage distribution algorithm (it's excellent)
2. Use views (not materialized tables) for flexibility
3. Save to both database and CSV
4. Filter to top 50 with egregious threshold
5. Add database path assertion everywhere
6. Update AGENTS.md with workflow docs
7. Create validation script
8. Test with June 19 data (known outliers)

### DON'T ❌
1. Don't change distribution logic
2. Don't add census block drill-down yet (TODO)
3. Don't break backwards compatibility (keep CSV)
4. Don't remove old main.py (version it first)
5. Don't create multiple database files
6. Don't hardcode thresholds (make configurable)

### WAIT FOR CLARIFICATION ⏸️
1. Z-score threshold UI controls
2. Egregious impact definition
3. First appearance at DMA vs CB level
4. Enriched view scope (window vs full series)
5. Round name conflict handling

---

## Next Steps

Once you've reviewed this plan and answered the clarifying questions, I'll:

1. **Create a feature branch:** `feature/restore-main-py-db`
2. **Implement in phases** with validation at each step
3. **Commit frequently** with clear messages
4. **Test against June 19** outliers (your benchmark)
5. **Document everything** in AGENTS.md and .agent_memory.json
6. **Merge to codex-agent** when validated

**Estimated timeline:**
- Phase 1-2: 4 hours (helper functions + data sources)
- Phase 3-4: 4 hours (UI steps 0-2)
- Phase 5-6: 3 hours (save to DB + preview)
- Testing: 2 hours
- **Total: ~13 hours** (spread over 2-3 sessions)

---

## Questions for You

**Please answer these so I can proceed correctly:**

1. **Z-score thresholds:** Hard-coded or UI configurable?
2. **Egregious threshold:** National impact > 40, or pair impact > 40?
3. **Top 50 scope:** Entire time series or graph window?
4. **First appearance:** DMA level or census block level?
5. **Round conflicts:** Error, overwrite, or version?
6. **View scope:** Filter to window or full time series?

**Also:**
- Any other concerns about the approach?
- Timeline pressure? (Can prioritize core functionality first)
- Specific test cases you want me to validate against?

