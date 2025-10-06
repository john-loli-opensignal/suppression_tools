"""Check what data is available for distribution."""
import sys
sys.path.insert(0, '/home/jloli/codebase-comparison/suppression_tools')
from tools import db
import pandas as pd

db_path = db.get_default_db_path()

# Test Windstream on 2025-07-25 (biggest outlier: 148 impact)
ds = "gamoshi"
mover_ind = False
test_date = "2025-07-25"
test_winner = "Windstream"

cube_name = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"

print("="*80)
print(f"Distribution Data for {test_winner} on {test_date}")
print("="*80)

# Query pair-level data (what the current code uses)
pair_query = f"""
SELECT 
    loser,
    dma_name,
    SUM(total_wins) as pair_wins
FROM {cube_name}
WHERE the_date = '{test_date}'
    AND winner = '{test_winner}'
GROUP BY loser, dma_name
ORDER BY pair_wins DESC
"""

pair_data = db.query(pair_query, db_path)
print(f"\nTotal pair-level records: {len(pair_data)}")
print(f"Total wins: {pair_data['pair_wins'].sum()}")

# Check by threshold
for threshold in [5, 10, 15, 20, 25]:
    eligible = pair_data[pair_data['pair_wins'] >= threshold]
    print(f"\nPairs with >= {threshold} wins: {len(eligible)} ({eligible['pair_wins'].sum()} wins)")

print("\n\nTop 20 pairs:")
print(pair_data.head(20))

# Query DMA-level data (what it SHOULD use)
dma_query = f"""
SELECT 
    dma_name,
    SUM(total_wins) as dma_wins,
    COUNT(DISTINCT loser) as num_losers
FROM {cube_name}
WHERE the_date = '{test_date}'
    AND winner = '{test_winner}'
GROUP BY dma_name
ORDER BY dma_wins DESC
"""

dma_data = db.query(dma_query, db_path)
print(f"\n\n{'='*80}")
print("DMA-Level Data (Better for distribution)")
print(f"{'='*80}")
print(f"\nTotal DMAs: {len(dma_data)}")
print(f"Total wins: {dma_data['dma_wins'].sum()}")

# Check by threshold
for threshold in [5, 10, 15, 20, 25]:
    eligible = dma_data[dma_data['dma_wins'] >= threshold]
    print(f"\nDMAs with >= {threshold} wins: {len(eligible)} ({eligible['dma_wins'].sum()} wins)")

print("\n\nTop 20 DMAs:")
print(dma_data.head(20))

# Show the problem
print(f"\n\n{'='*80}")
print("THE PROBLEM")
print(f"{'='*80}")
print(f"\nImpact to remove: 148 wins")
print(f"\nUsing PAIR-level (current approach) with min_wins=5:")
eligible_pairs = pair_data[pair_data['pair_wins'] >= 5]
print(f"  - Eligible pairs: {len(eligible_pairs)}")
print(f"  - Eligible wins: {eligible_pairs['pair_wins'].sum()}")
print(f"  - Coverage: {(eligible_pairs['pair_wins'].sum() / 148 * 100):.1f}%")

print(f"\nUsing DMA-level (better approach) with min_wins=5:")
eligible_dmas = dma_data[dma_data['dma_wins'] >= 5]
print(f"  - Eligible DMAs: {len(eligible_dmas)}")
print(f"  - Eligible wins: {eligible_dmas['dma_wins'].sum()}")
print(f"  - Coverage: {(eligible_dmas['dma_wins'].sum() / 148 * 100):.1f}%")

print(f"\n\n⚠️  By filtering at PAIR level, we're excluding many DMAs!")
print(f"⚠️  We should distribute across DMAs, not pairs!")
