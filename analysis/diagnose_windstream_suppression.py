"""
Diagnose why Windstream and other carriers aren't being fully suppressed.
"""

import sys
sys.path.insert(0, '/home/jloli/codebase-comparison/suppression_tools')

import duckdb
from tools import db
import pandas as pd

# Get database path
db_path = db.get_default_db_path()
print(f"Using database: {db_path}")

# Parameters from user's test
ds = "gamoshi"
mover_ind = False  # non_mover based on context
start_date = "2025-06-01"
end_date = "2025-09-04"
min_share = 0.3  # 0.3%
min_wins = 5

print(f"\n{'='*80}")
print(f"DIAGNOSIS: Why aren't all outliers being suppressed?")
print(f"{'='*80}")
print(f"Dataset: {ds}")
print(f"Mover: {mover_ind}")
print(f"Window: {start_date} to {end_date}")
print(f"Min Share: {min_share}%")
print(f"Min Wins for Distribution: {min_wins}")

con = duckdb.connect(db_path)

# Step 1: Get all national outliers from the rolling view
print(f"\n{'='*80}")
print("STEP 1: National Outliers Detected")
print(f"{'='*80}")

mover_suffix = "mover" if mover_ind else "non_mover"
view_name = f"{ds}_win_{mover_suffix}_rolling"

national_outliers_query = f"""
SELECT 
    the_date,
    winner,
    current_wins,
    avg_wins,
    stddev_wins,
    zscore,
    pct_change,
    is_outlier,
    is_first_appearance,
    (current_wins - avg_wins) as excess_wins
FROM {view_name}
WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
    AND (is_outlier = true OR is_first_appearance = true)
ORDER BY the_date, excess_wins DESC
"""

national_outliers = con.execute(national_outliers_query).df()
print(f"\nTotal national outlier records: {len(national_outliers)}")
print(f"\nNational outliers by carrier:")
summary = national_outliers.groupby('winner').agg({
    'excess_wins': 'sum',
    'the_date': 'count'
}).rename(columns={'the_date': 'outlier_days'}).sort_values('excess_wins', ascending=False)
print(summary.head(20))

# Check Windstream specifically
windstream_outliers = national_outliers[national_outliers['winner'].str.contains('Windstream', case=False, na=False)]
print(f"\n\nWindstream outliers ({len(windstream_outliers)} records):")
if len(windstream_outliers) > 0:
    print(windstream_outliers[['the_date', 'current_wins', 'avg_wins', 'excess_wins', 'zscore', 'is_outlier', 'is_first_appearance']].head(10))

# Step 2: Check what plan.scan_base_outliers actually returns
print(f"\n{'='*80}")
print("STEP 2: What scan_base_outliers Returns")
print(f"{'='*80}")

from tools.src import plan

try:
    base_outliers_df = plan.scan_base_outliers(
        ds=ds,
        mover_ind=mover_ind,
        start_date=start_date,
        end_date=end_date,
        top_n=25,
        db_path=db_path
    )
    
    print(f"\nTotal outlier records from scan_base_outliers: {len(base_outliers_df)}")
    print(f"\nOutliers by carrier:")
    scan_summary = base_outliers_df.groupby('winner').agg({
        'impact': 'sum',
        'the_date': 'count'
    }).rename(columns={'the_date': 'outlier_days'}).sort_values('impact', ascending=False)
    print(scan_summary.head(20))
    
    # Check Windstream
    windstream_scan = base_outliers_df[base_outliers_df['winner'].str.contains('Windstream', case=False, na=False)]
    print(f"\n\nWindstream in scan results ({len(windstream_scan)} records):")
    if len(windstream_scan) > 0:
        print(windstream_scan[['the_date', 'winner', 'current', 'avg', 'impact', 'zscore']].head(10))
    
except Exception as e:
    print(f"Error calling scan_base_outliers: {e}")
    import traceback
    traceback.print_exc()

# Step 3: Check DMA-level data
print(f"\n{'='*80}")
print("STEP 3: DMA-Level Data for Distribution")
print(f"{'='*80}")

cube_name = f"{ds}_win_{mover_suffix}_cube"

dma_query = f"""
SELECT 
    the_date,
    winner,
    dma_name,
    SUM(total_metric_sum) as wins
FROM {cube_name}
WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
    AND winner IN (SELECT DISTINCT winner FROM {view_name} 
                   WHERE the_date BETWEEN '{start_date}' AND '{end_date}'
                   AND (is_outlier = true OR is_first_appearance = true))
GROUP BY the_date, winner, dma_name
HAVING SUM(total_metric_sum) >= {min_wins}
ORDER BY the_date, winner, wins DESC
"""

dma_data = con.execute(dma_query).df()
print(f"\nDMA records meeting min_wins threshold ({min_wins}): {len(dma_data)}")

# Group by carrier
dma_by_carrier = dma_data.groupby('winner').agg({
    'wins': 'sum',
    'dma_name': 'nunique'
}).rename(columns={'dma_name': 'num_dmas'})
print(f"\nDMAs available for distribution by carrier:")
print(dma_by_carrier.sort_values('wins', ascending=False).head(20))

# Check Windstream DMAs
windstream_dma = dma_data[dma_data['winner'].str.contains('Windstream', case=False, na=False)]
print(f"\n\nWindstream DMA records: {len(windstream_dma)}")
if len(windstream_dma) > 0:
    print(f"Unique dates: {windstream_dma['the_date'].nunique()}")
    print(f"Unique DMAs: {windstream_dma['dma_name'].nunique()}")
    print(f"Total wins: {windstream_dma['wins'].sum()}")
    print("\nSample:")
    print(windstream_dma.head(10))

# Step 4: Check if the issue is in the distribution algorithm
print(f"\n{'='*80}")
print("STEP 4: Distribution Algorithm Test")
print(f"{'='*80}")

# Pick one Windstream outlier date
if len(windstream_outliers) > 0:
    test_date = str(windstream_outliers.iloc[0]['the_date'])
    test_winner = windstream_outliers.iloc[0]['winner']
    test_excess = float(windstream_outliers.iloc[0]['excess_wins'])
    
    print(f"\nTest case: {test_winner} on {test_date}")
    print(f"Excess wins to remove: {test_excess}")
    
    # Get DMAs for this carrier on this date
    test_dma_query = f"""
    SELECT 
        dma_name,
        SUM(total_metric_sum) as wins
    FROM {cube_name}
    WHERE the_date = '{test_date}'
        AND winner = '{test_winner}'
    GROUP BY dma_name
    HAVING SUM(total_metric_sum) >= {min_wins}
    ORDER BY wins DESC
    """
    
    test_dmas = con.execute(test_dma_query).df()
    print(f"\nAvailable DMAs for distribution: {len(test_dmas)}")
    if len(test_dmas) > 0:
        print(test_dmas.head(10))
        
        # Calculate distribution
        total_eligible = test_dmas['wins'].sum()
        print(f"\nTotal eligible wins: {total_eligible}")
        print(f"Excess to remove: {test_excess}")
        
        if total_eligible > 0:
            test_dmas['proportion'] = test_dmas['wins'] / total_eligible
            test_dmas['to_remove'] = (test_dmas['proportion'] * test_excess).round().astype(int)
            test_dmas['after_suppression'] = test_dmas['wins'] - test_dmas['to_remove']
            
            print(f"\nDistribution calculation (top 10):")
            print(test_dmas.head(10))
            
            print(f"\nTotal to remove: {test_dmas['to_remove'].sum()}")
            print(f"Excess to remove: {test_excess}")
            print(f"Distribution covers: {(test_dmas['to_remove'].sum() / test_excess * 100):.1f}%")
        else:
            print(f"\n⚠️  NO ELIGIBLE WINS FOR DISTRIBUTION!")
    else:
        print(f"\n⚠️  NO DMAs MEET THE {min_wins} WIN THRESHOLD!")
        print(f"This is why {test_winner} can't be suppressed!")

con.close()

print(f"\n{'='*80}")
print("DIAGNOSIS COMPLETE")
print(f"{'='*80}")
