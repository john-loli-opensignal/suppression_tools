# Competitor View Bug Fix - October 6, 2025

## Problem Summary

The competitor analysis dashboard was showing incorrect data:
1. **Wins per Loss chart showed 1.0 across the board** (should vary by date)
2. **AT&T losses to Cox showed 36 instead of 17** on 2025-07-21
3. **All losses were inverted** - showing competitor's losses to primary instead of primary's losses to competitor

## Root Cause

**Fundamental misunderstanding of the `carrier_data` schema structure.**

### How `carrier_data` Actually Works

Each matchup is stored in **TWO separate rows** with inverted winner/loser:

```sql
-- For AT&T vs Cox on 2025-07-21:

-- Row 1: AT&T's perspective
winner='AT&T', loser='Cox', adjusted_wins=36, adjusted_losses=17
  ↳ AT&T beat Cox 36 times
  ↳ AT&T lost to Cox 17 times

-- Row 2: Cox's perspective  
winner='Cox', loser='AT&T', adjusted_wins=17, adjusted_losses=36
  ↳ Cox beat AT&T 17 times
  ↳ Cox lost to AT&T 36 times
```

**Key insight**: The `adjusted_losses` column in a row with `winner=X, loser=Y` represents **X's losses to Y**, NOT Y's losses to X.

### How Cubes Are Built

The loss cube aggregates:
```sql
CREATE TABLE loss_cube AS
SELECT winner, loser, SUM(adjusted_losses) as total_losses
FROM carrier_data
GROUP BY winner, loser
```

So in the loss cube:
- Row `winner=AT&T, loser=Cox, total_losses=17` ← AT&T's losses to Cox ✅
- Row `winner=Cox, loser=AT&T, total_losses=36` ← Cox's losses to AT&T ✅

## The Bug

The `competitor_view()` function was querying:

```sql
-- INCORRECT ❌
SELECT loser as competitor, SUM(total_losses)
FROM loss_cube
WHERE loser = 'AT&T'  -- Looking for AT&T as the loser
  AND winner IN ('Cox')  -- and Cox as the winner
```

This query returned the row where **Cox is the winner and AT&T is the loser**, which contains **Cox's losses to AT&T (36)**, not AT&T's losses to Cox.

## The Fix

Changed the query to:

```sql
-- CORRECT ✅
SELECT loser as competitor, SUM(total_losses)
FROM loss_cube
WHERE winner = 'AT&T'  -- Looking for AT&T as the winner
  AND loser IN ('Cox')  -- and Cox as the loser
```

Now it correctly returns AT&T's losses to Cox (17).

## Verification

Testing AT&T vs Cox on 2025-07-21 (non-mover):

| Metric | Before Fix | After Fix | Verified From carrier_data |
|--------|------------|-----------|----------------------------|
| AT&T wins over Cox | 36 ✅ | 36 ✅ | ✅ Correct |
| AT&T losses to Cox | **36** ❌ | **17** ✅ | ✅ Correct |
| Wins per Loss | **1.0** ❌ | **2.12** ✅ | ✅ Correct |

## Impact

This bug affected:
- **Competitor analysis** dashboard (all 3 views: wins, losses, wins per loss)
- Any analysis comparing head-to-head performance
- **Did NOT affect national views** or aggregated metrics (those don't use the loss cube join)

## Files Changed

1. `tools/src/metrics.py` - Fixed `competitor_view()` h2h_losses CTE
2. `.agent_memory.json` - Documented the schema structure to prevent future mistakes
3. This document - Detailed explanation for reference

## Key Lesson

**Always verify schema assumptions with sample queries before building aggregations.**

The carrier_data table's design (both sides of every matchup in separate rows) is elegant but non-obvious. When querying for "X's losses to Y", you must query the row where X is the **winner** (not the loser), because that row contains X's adjusted_losses.

## Related Commits

- `0d61ae0` - Bug fix
- `4b478fe` - Documentation update
