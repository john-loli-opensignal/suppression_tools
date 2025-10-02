-- cube_outliers.sql
-- Inputs: {store_glob}, {ds}, {mover_ind}, {start_date}, {end_date}, {window}, {z_nat}, {z_pair}, {only_outliers}
WITH ds AS (
  SELECT * FROM parquet_scan('{store_glob}')
), filt AS (
  SELECT * FROM ds
  WHERE ds = '{ds}'
    AND mover_ind = {mover_ind}
    AND CAST(the_date AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    {extra_filters}
    AND dma_name IS NOT NULL
    AND adjusted_wins > 0
), market AS (
  SELECT the_date, SUM(adjusted_wins) AS market_total_wins
  FROM filt GROUP BY 1
), nat AS (
  SELECT the_date, winner, SUM(adjusted_wins) AS nat_total_wins
  FROM filt GROUP BY 1,2
), nat_metrics AS (
  SELECT n.the_date, n.winner,
         n.nat_total_wins, m.market_total_wins,
         n.nat_total_wins / NULLIF(m.market_total_wins, 0) AS nat_share_current
  FROM nat n JOIN market m USING (the_date)
), nat_typed AS (
  SELECT *, CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                 WHEN strftime('%w', the_date)='0' THEN 'Sun'
                 ELSE 'Weekday' END AS day_type
  FROM nat_metrics
), nat_hist AS (
  SELECT r.the_date, r.winner, r.day_type,
         avg(h.nat_share_current) AS mu,
         stddev_samp(h.nat_share_current) AS sigma,
         COUNT(*) AS cnt
  FROM nat_typed r
  JOIN nat_typed h
    ON h.winner=r.winner AND h.day_type=r.day_type
   AND CAST(h.the_date AS DATE) < CAST(r.the_date AS DATE)
   AND CAST(h.the_date AS DATE) >= CAST(r.the_date AS DATE) - INTERVAL {window} DAY
  GROUP BY 1,2,3
), nat_scored AS (
  SELECT t.the_date, t.winner,
         t.nat_total_wins, t.market_total_wins, t.nat_share_current,
         h.mu AS nat_mu_share, h.sigma AS nat_sigma_share, {window} AS nat_mu_window,
         CASE WHEN h.cnt>1 AND h.sigma>0 THEN (t.nat_share_current - h.mu)/NULLIF(h.sigma,0) ELSE 0 END AS nat_zscore,
         CASE WHEN (CASE WHEN h.cnt>1 AND h.sigma>0 THEN (t.nat_share_current - h.mu)/NULLIF(h.sigma,0) ELSE 0 END) > {z_nat} THEN TRUE ELSE FALSE END AS nat_outlier_pos
  FROM nat_typed t
  LEFT JOIN nat_hist h ON h.the_date=t.the_date AND h.winner=t.winner AND h.day_type=t.day_type
), pair AS (
  SELECT CAST(the_date AS DATE) AS the_date,
         winner, loser, dma_name,
         SUM(adjusted_wins) AS pair_wins_current
  FROM filt
  GROUP BY 1,2,3,4
), pair_typed AS (
  SELECT *, CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                 WHEN strftime('%w', the_date)='0' THEN 'Sun'
                 ELSE 'Weekday' END AS day_type
  FROM pair
), pair_hist AS (
  SELECT r.the_date, r.winner, r.loser, r.dma_name, r.day_type,
         avg(h.pair_wins_current) AS mu,
         stddev_samp(h.pair_wins_current) AS sigma,
         COUNT(*) AS cnt
  FROM pair_typed r
  JOIN pair_typed h
    ON h.winner=r.winner AND h.loser=r.loser AND h.dma_name=r.dma_name AND h.day_type=r.day_type
   AND CAST(h.the_date AS DATE) < CAST(r.the_date AS DATE)
   AND CAST(h.the_date AS DATE) >= CAST(r.the_date AS DATE) - INTERVAL {window} DAY
  GROUP BY 1,2,3,4,5
)
SELECT ps.the_date, ps.winner, ps.loser, ps.dma_name,
       ps.pair_wins_current,
       ph.mu AS pair_mu_wins, ph.sigma AS pair_sigma_wins, {window} AS pair_mu_window,
       CASE WHEN ph.cnt>1 AND ph.sigma>0 THEN (ps.pair_wins_current - ph.mu)/NULLIF(ph.sigma,0) ELSE 0 END AS pair_z,
       (ph.mu IS NOT NULL AND ps.pair_wins_current > 1.3*ph.mu) AS pct_outlier_pos,
       (ph.mu IS NULL OR ph.mu = 0) AS new_pair,
       COALESCE(ph.mu,0) < 2.0 AS rare_pair,
       nm.nat_total_wins, nm.market_total_wins AS nat_market_wins, nm.nat_share_current, ns.nat_mu_share, ns.nat_sigma_share, ns.nat_mu_window,
       ns.nat_zscore, ns.nat_outlier_pos
FROM pair ps
JOIN nat_metrics nm ON nm.the_date = ps.the_date AND nm.winner = ps.winner
JOIN nat_scored  ns ON ns.the_date = ps.the_date AND ns.winner = ps.winner
LEFT JOIN pair_hist ph ON ph.the_date=ps.the_date AND ph.winner=ps.winner AND ph.loser=ps.loser AND ph.dma_name=ps.dma_name
{where_outlier}
ORDER BY 1,2,3,4;

