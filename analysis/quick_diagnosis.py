"""Quick diagnosis of suppression issue."""
import sys
sys.path.insert(0, '/home/jloli/codebase-comparison/suppression_tools')
from tools import db
from tools.src import plan
import pandas as pd

db_path = db.get_default_db_path()

# Test parameters
ds = "gamoshi"
mover_ind = False
start_date = "2025-06-01"
end_date = "2025-09-04"

print("="*80)
print("DIAGNOSIS: Why Windstream isn't being suppressed")
print("="*80)

# Step 1: Get outliers from scan_base_outliers
print("\n1. Checking outliers detected by scan_base_outliers...")
outliers = plan.scan_base_outliers(
    ds=ds,
    mover_ind=mover_ind,
    start_date=start_date,
    end_date=end_date,
    z_threshold=2.5,
    top_n=25,
    min_share_pct=0.3,
    egregious_threshold=40,
    db_path=db_path
)

print(f"Total outlier records: {len(outliers)}")
print(f"\nUnique carriers with outliers: {outliers['winner'].nunique()}")
print(f"\nTop 10 by total impact:")
impact_summary = outliers.groupby('winner')['impact'].sum().sort_values(ascending=False).head(10)
print(impact_summary)

# Check Windstream
windstream = outliers[outliers['winner'].str.contains('Windstream', case=False, na=False)]
print(f"\n\nWindstream outliers: {len(windstream)} records")
if len(windstream) > 0:
    print(windstream[['the_date', 'winner', 'nat_total_wins', 'nat_mu_wins', 'impact', 'nat_z_score']])
else:
    print("⚠️  NO WINDSTREAM OUTLIERS DETECTED!")
    print("\nChecking if Windstream is in top 25...")
    top_25 = plan.get_top_n_carriers(ds, mover_ind, 25, 0.3, db_path)
    print(f"Top 25 carriers: {top_25}")
    print(f"Windstream in top 25? {any('Windstream' in c for c in top_25)}")

# Step 2: Check DMA distribution eligibility
print(f"\n{'='*80}")
print("2. Checking DMA distribution eligibility")
print(f"{'='*80}")

cube_name = f"{ds}_win_{'mover' if mover_ind else 'non_mover'}_cube"

# Check a specific outlier date
if len(outliers) > 0:
    test_row = outliers.iloc[0]
    test_date = str(test_row['the_date'])
    test_winner = test_row['winner']
    test_impact = test_row['impact']
    
    print(f"\nTest case: {test_winner} on {test_date}")
    print(f"Impact to remove: {test_impact}")
    
    # Check DMA breakdown
    dma_query = f"""
    SELECT 
        dma_name,
        SUM(total_wins) as wins
    FROM {cube_name}
    WHERE the_date = '{test_date}'
        AND winner = '{test_winner}'
    GROUP BY dma_name
    ORDER BY wins DESC
    """
    
    dma_data = db.query(dma_query, db_path)
    print(f"\nDMAs for this carrier/date: {len(dma_data)}")
    print(f"Total wins across DMAs: {dma_data['wins'].sum()}")
    
    # Check how many meet the 5 win threshold
    eligible = dma_data[dma_data['wins'] >= 5]
    print(f"\nDMAs with >= 5 wins: {len(eligible)}")
    if len(eligible) > 0:
        print(f"Eligible wins: {eligible['wins'].sum()}")
        print(f"Can cover: {(eligible['wins'].sum() / test_impact * 100):.1f}% of impact")
        print("\nTop eligible DMAs:")
        print(eligible.head(10))
    else:
        print("⚠️  NO DMAs MEET THRESHOLD! Can't distribute!")

print("\n" + "="*80)
print("DIAGNOSIS COMPLETE")
print("="*80)
