#!/usr/bin/env python3
"""
Performance testing script for census block cubes.
Validates query performance at different hierarchy levels.
"""
import duckdb
import time
import sys

DB_PATH = "duck_suppression.db"

def test_query_performance():
    """Test query performance at different hierarchy levels."""
    
    print("=" * 80)
    print("CENSUS BLOCK CUBE - PERFORMANCE VALIDATION")
    print("=" * 80)
    
    con = duckdb.connect(DB_PATH, read_only=True)
    
    tests = []
    
    # Test 1: National aggregation
    print("\n[Test 1/5] National aggregation (all H2H pairs)...")
    start = time.time()
    result1 = con.execute("""
        SELECT winner, loser, 
               SUM(total_wins) as total, 
               COUNT(DISTINCT census_blockid) as unique_blocks
        FROM gamoshi_win_mover_census_cube
        GROUP BY winner, loser
        ORDER BY total DESC
        LIMIT 10
    """).fetchdf()
    elapsed1 = time.time() - start
    tests.append(("National aggregation", elapsed1, len(result1)))
    print(f"   ✓ Completed in {elapsed1:.3f}s ({len(result1)} results)")
    print(f"   Top result: {result1.iloc[0]['winner']} vs {result1.iloc[0]['loser']}: {result1.iloc[0]['total']:,.0f} wins")
    
    # Get top H2H for subsequent tests
    winner = result1.iloc[0]['winner']
    loser = result1.iloc[0]['loser']
    
    # Test 2: H2H time series
    print(f"\n[Test 2/5] H2H time series ({winner} vs {loser})...")
    start = time.time()
    result2 = con.execute("""
        SELECT the_date, 
               SUM(total_wins) as daily_wins,
               COUNT(DISTINCT census_blockid) as blocks
        FROM gamoshi_win_mover_census_cube
        WHERE winner = ? AND loser = ?
        GROUP BY the_date
        ORDER BY the_date
    """, [winner, loser]).fetchdf()
    elapsed2 = time.time() - start
    tests.append(("H2H time series", elapsed2, len(result2)))
    print(f"   ✓ Completed in {elapsed2:.3f}s ({len(result2)} days)")
    print(f"   Total wins: {result2['daily_wins'].sum():,.0f}, Avg daily: {result2['daily_wins'].mean():.1f}")
    
    # Test 3: State breakdown
    print(f"\n[Test 3/5] State breakdown ({winner} vs {loser})...")
    start = time.time()
    result3 = con.execute("""
        SELECT state, 
               SUM(total_wins) as total,
               COUNT(DISTINCT census_blockid) as unique_blocks,
               COUNT(DISTINCT dma_name) as unique_dmas
        FROM gamoshi_win_mover_census_cube
        WHERE winner = ? AND loser = ?
        GROUP BY state
        ORDER BY total DESC
    """, [winner, loser]).fetchdf()
    elapsed3 = time.time() - start
    tests.append(("State breakdown", elapsed3, len(result3)))
    print(f"   ✓ Completed in {elapsed3:.3f}s ({len(result3)} states)")
    if len(result3) > 0:
        print(f"   Top state: {result3.iloc[0]['state']}: {result3.iloc[0]['total']:,.0f} wins, {result3.iloc[0]['unique_blocks']} blocks")
    
    # Test 4: DMA breakdown
    print(f"\n[Test 4/5] DMA breakdown ({winner} vs {loser})...")
    start = time.time()
    result4 = con.execute("""
        SELECT dma_name, state,
               SUM(total_wins) as total,
               COUNT(DISTINCT census_blockid) as unique_blocks
        FROM gamoshi_win_mover_census_cube
        WHERE winner = ? AND loser = ?
        GROUP BY dma_name, state
        ORDER BY total DESC
        LIMIT 20
    """, [winner, loser]).fetchdf()
    elapsed4 = time.time() - start
    tests.append(("DMA breakdown", elapsed4, len(result4)))
    print(f"   ✓ Completed in {elapsed4:.3f}s ({len(result4)} DMAs)")
    if len(result4) > 0:
        print(f"   Top DMA: {result4.iloc[0]['dma_name']}: {result4.iloc[0]['total']:,.0f} wins, {result4.iloc[0]['unique_blocks']} blocks")
    
    # Test 5: Census block outlier detection
    print(f"\n[Test 5/5] Census block outlier detection ({winner} vs {loser})...")
    start = time.time()
    result5 = con.execute("""
        WITH block_stats AS (
            SELECT census_blockid, state, dma_name,
                   SUM(total_wins) as total,
                   COUNT(*) as days_active
            FROM gamoshi_win_mover_census_cube
            WHERE winner = ? AND loser = ?
            GROUP BY census_blockid, state, dma_name
        ),
        stats AS (
            SELECT AVG(total) as mean, STDDEV(total) as std
            FROM block_stats
        )
        SELECT b.census_blockid, b.state, b.dma_name, b.total, b.days_active,
               (b.total - s.mean) / NULLIF(s.std, 0) as z_score
        FROM block_stats b, stats s
        WHERE ABS((b.total - s.mean) / NULLIF(s.std, 0)) > 3
        ORDER BY z_score DESC
    """, [winner, loser]).fetchdf()
    elapsed5 = time.time() - start
    tests.append(("Outlier detection", elapsed5, len(result5)))
    print(f"   ✓ Completed in {elapsed5:.3f}s ({len(result5)} outliers found)")
    if len(result5) > 0:
        print(f"   Top outlier: Block {result5.iloc[0]['census_blockid'][:15]}... in {result5.iloc[0]['state']}")
        print(f"                Z-score: {result5.iloc[0]['z_score']:.2f}, Total: {result5.iloc[0]['total']:,.0f}")
    
    # Test 6: Complex multi-level aggregation
    print(f"\n[Test 6/6] Complex multi-level aggregation (all data)...")
    start = time.time()
    result6 = con.execute("""
        SELECT 
            winner,
            COUNT(DISTINCT loser) as unique_losers,
            COUNT(DISTINCT state) as unique_states,
            COUNT(DISTINCT dma_name) as unique_dmas,
            COUNT(DISTINCT census_blockid) as unique_blocks,
            SUM(total_wins) as total_wins,
            AVG(total_wins) as avg_wins
        FROM gamoshi_win_mover_census_cube
        GROUP BY winner
        ORDER BY total_wins DESC
        LIMIT 10
    """).fetchdf()
    elapsed6 = time.time() - start
    tests.append(("Multi-level aggregation", elapsed6, len(result6)))
    print(f"   ✓ Completed in {elapsed6:.3f}s ({len(result6)} carriers)")
    if len(result6) > 0:
        print(f"   Top carrier: {result6.iloc[0]['winner']}")
        print(f"                {result6.iloc[0]['total_wins']:,.0f} wins across {result6.iloc[0]['unique_blocks']:,} blocks")
    
    con.close()
    
    # Summary
    print("\n" + "=" * 80)
    print("PERFORMANCE SUMMARY")
    print("=" * 80)
    print(f"{'Test':<30} {'Time (s)':<12} {'Results':<10} {'Status'}")
    print("-" * 80)
    
    all_fast = True
    for test_name, elapsed, count in tests:
        status = "✓ FAST" if elapsed < 2.0 else "⚠ SLOW"
        if elapsed >= 2.0:
            all_fast = False
        print(f"{test_name:<30} {elapsed:<12.3f} {count:<10} {status}")
    
    total_time = sum(t[1] for t in tests)
    print("-" * 80)
    print(f"{'TOTAL':<30} {total_time:<12.3f}")
    print("=" * 80)
    
    if all_fast:
        print("\n✓ All tests completed in <2s - Performance validated!")
        return True
    else:
        print("\n⚠ Some tests were slow - May need optimization")
        return False


def test_cube_statistics():
    """Display cube statistics."""
    print("\n" + "=" * 80)
    print("CUBE STATISTICS")
    print("=" * 80)
    
    con = duckdb.connect(DB_PATH, read_only=True)
    
    cubes = [
        'gamoshi_win_mover_census_cube',
        'gamoshi_win_non_mover_census_cube',
        'gamoshi_loss_mover_census_cube',
        'gamoshi_loss_non_mover_census_cube'
    ]
    
    print(f"\n{'Cube':<40} {'Rows':<12} {'Blocks':<10}")
    print("-" * 80)
    
    total_rows = 0
    for cube in cubes:
        try:
            stats = con.execute(f"""
                SELECT COUNT(*) as rows,
                       COUNT(DISTINCT census_blockid) as blocks
                FROM {cube}
            """).fetchone()
            rows, blocks = stats
            total_rows += rows
            print(f"{cube:<40} {rows:<12,} {blocks:<10,}")
        except Exception as e:
            print(f"{cube:<40} ERROR: {e}")
    
    print("-" * 80)
    print(f"{'TOTAL':<40} {total_rows:<12,}")
    print("=" * 80)
    
    con.close()


if __name__ == '__main__':
    try:
        test_cube_statistics()
        success = test_query_performance()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
