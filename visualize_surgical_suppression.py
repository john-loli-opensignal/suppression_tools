#!/usr/bin/env python3
"""
Quick visualization to show the power of surgical suppression vs broad suppression.
"""

import pandas as pd
import duckdb

DB_PATH = "duck_suppression.db"

def compare_suppression_approaches(ds="gamoshi", target_date="2025-08-16"):
    """
    Compare old vs new suppression approach for a specific date.
    """
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # Read the suppression list
    suppressions = pd.read_csv("census_block_analysis_results/gamoshi_deterministic_suppressions.csv")
    suppressions_on_date = suppressions[suppressions['the_date'] == target_date]
    
    print(f"\n{'='*80}")
    print(f"SURGICAL SUPPRESSION ANALYSIS: {target_date}")
    print(f"{'='*80}\n")
    
    # For each DMA affected, calculate:
    # 1. Total census blocks in the DMA on that date
    # 2. Census blocks flagged for suppression
    # 3. Data retention rate
    
    results = []
    
    for (dma, state), group in suppressions_on_date.groupby(['dma_name', 'state']):
        # Count UNIQUE census blocks to suppress (not combinations)
        unique_blocks_to_suppress = group['census_blockid'].nunique()
        
        # Get total census blocks in this DMA on this date
        total_query = f"""
        SELECT COUNT(DISTINCT census_blockid) as total_blocks
        FROM (
            SELECT census_blockid FROM {ds}_win_mover_census_cube WHERE the_date = '{target_date}' AND dma_name = '{dma}' AND state = '{state}'
            UNION
            SELECT census_blockid FROM {ds}_loss_mover_census_cube WHERE the_date = '{target_date}' AND dma_name = '{dma}' AND state = '{state}'
            UNION
            SELECT census_blockid FROM {ds}_win_non_mover_census_cube WHERE the_date = '{target_date}' AND dma_name = '{dma}' AND state = '{state}'
            UNION
            SELECT census_blockid FROM {ds}_loss_non_mover_census_cube WHERE the_date = '{target_date}' AND dma_name = '{dma}' AND state = '{state}'
        ) all_blocks
        """
        
        total_blocks = con.execute(total_query).fetchone()[0]
        
        # Also count total combinations for context
        total_combinations = len(group)
        
        retention_rate = (total_blocks - unique_blocks_to_suppress) / total_blocks * 100 if total_blocks > 0 else 0
        
        results.append({
            'dma': dma,
            'state': state,
            'total_blocks': total_blocks,
            'unique_blocks_to_suppress': unique_blocks_to_suppress,
            'total_suppression_records': total_combinations,
            'retention_rate_pct': retention_rate
        })
    
    con.close()
    
    results_df = pd.DataFrame(results).sort_values('unique_blocks_to_suppress', ascending=False)
    
    # Overall stats
    print(f"Total DMAs affected: {len(results_df)}")
    print(f"Total census blocks across all DMAs: {results_df['total_blocks'].sum():,}")
    print(f"Unique census blocks flagged for suppression: {results_df['unique_blocks_to_suppress'].sum():,}")
    print(f"Total suppression records (combinations): {results_df['total_suppression_records'].sum():,}")
    retained = results_df['total_blocks'].sum() - results_df['unique_blocks_to_suppress'].sum()
    retention_pct = retained / results_df['total_blocks'].sum() * 100
    print(f"Overall data retention rate: {retention_pct:.2f}%")
    print(f"\nAverage retention rate per DMA: {results_df['retention_rate_pct'].mean():.2f}%")
    print(f"Median retention rate per DMA: {results_df['retention_rate_pct'].median():.2f}%")
    
    print(f"\n{'='*80}")
    print("TOP 20 DMAs BY DATA RETENTION (Most Preserved)")
    print(f"{'='*80}\n")
    
    print("| Rank | DMA | State | Total Blocks | Unique Suppress | Retention % |")
    print("|------|-----|-------|--------------|-----------------|-------------|")
    
    for i, (_, row) in enumerate(results_df.nlargest(20, 'retention_rate_pct').iterrows(), 1):
        print(f"| {i:2d} | {row['dma'][:40]:40s} | {row['state'][:2]:2s} | {row['total_blocks']:6d} | {row['unique_blocks_to_suppress']:6d} | {row['retention_rate_pct']:6.2f}% |")
    
    print(f"\n{'='*80}")
    print("TOP 20 DMAs BY SUPPRESSION COUNT (Most Affected)")
    print(f"{'='*80}\n")
    
    print("| Rank | DMA | State | Total Blocks | Unique Suppress | Retention % |")
    print("|------|-----|-------|--------------|-----------------|-------------|")
    
    for i, (_, row) in enumerate(results_df.nlargest(20, 'unique_blocks_to_suppress').iterrows(), 1):
        print(f"| {i:2d} | {row['dma'][:40]:40s} | {row['state'][:2]:2s} | {row['total_blocks']:6d} | {row['unique_blocks_to_suppress']:6d} | {row['retention_rate_pct']:6.2f}% |")
    
    print(f"\n{'='*80}")
    print("KEY INSIGHT: The Old vs New Approach")
    print(f"{'='*80}\n")
    
    print("OLD APPROACH (DMA-level suppression):")
    print("  If we suppress at (date, ds, mover_ind, dma, winner, loser) level,")
    print("  we would remove ALL records for each affected combination.")
    print("  This could mean suppressing ENTIRE DMAs for certain carrier matchups.\n")
    
    print("NEW APPROACH (Census block-level suppression):")
    print("  We identify and suppress ONLY the problematic census blocks.")
    print(f"  On {target_date}, we preserve {retention_pct:.2f}% of census blocks!")
    print(f"  That's a {results_df['retention_rate_pct'].mean():.2f}% average retention rate per DMA.\n")
    
    print("EXAMPLE: Los Angeles DMA")
    la = results_df[results_df['dma'].str.contains('Los Angeles')].iloc[0] if len(results_df[results_df['dma'].str.contains('Los Angeles')]) > 0 else None
    if la is not None:
        print(f"  Total census blocks: {la['total_blocks']:,}")
        print(f"  Unique blocks to suppress: {la['unique_blocks_to_suppress']:,}")
        print(f"  Total suppression records: {la['total_suppression_records']:,}")
        print(f"  Data preserved: {la['retention_rate_pct']:.2f}%")
        saved = int(la['total_blocks'] - la['unique_blocks_to_suppress'])
        print(f"  --> We save {saved:,} legitimate census blocks from deletion!")
    
    # Save results
    results_df.to_csv(f"census_block_analysis_results/{ds}_surgical_suppression_comparison_{target_date}.csv", index=False)
    print(f"\n✓ Saved comparison to: census_block_analysis_results/{ds}_surgical_suppression_comparison_{target_date}.csv")


if __name__ == "__main__":
    compare_suppression_approaches(ds="gamoshi", target_date="2025-08-16")
