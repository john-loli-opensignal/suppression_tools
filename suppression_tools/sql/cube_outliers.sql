WITH ds AS (
  SELECT * FROM parquet_scan('{store_glob}')
), filt_all AS (
  SELECT * FROM ds
  WHERE ds = '{ds}'
    AND mover_ind = {mover_ind}
    {extra_filters}
    AND dma_name IS NOT NULL
    AND adjusted_wins > 0
), filt_r AS (
  SELECT * FROM filt_all
  WHERE CAST(the_date AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
), market_all AS (
  SELECT the_date, SUM(adjusted_wins) AS market_total_wins
  FROM filt_all GROUP BY 1
), nat_all AS (
  SELECT the_date, winner, SUM(adjusted_wins) AS nat_total_wins
  FROM filt_all GROUP BY 1,2
), nat_metrics_all AS (
  SELECT n.the_date, n.winner,
         n.nat_total_wins, m.market_total_wins,
         n.nat_total_wins / NULLIF(m.market_total_wins, 0) AS nat_share_current
  FROM nat_all n JOIN market_all m USING (the_date)
), nat_typed_all AS (
  SELECT *, CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                 WHEN strftime('%w', the_date)='0' THEN 'Sun'
                 ELSE 'Weekday' END AS day_type
  FROM nat_metrics_all
), nat_typed_r AS (
  SELECT * FROM nat_typed_all
  WHERE CAST(the_date AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
), nat_scored AS (
  SELECT t.the_date, t.winner,
         t.nat_total_wins, t.market_total_wins, t.nat_share_current,
         hist.mu AS nat_mu_share, hist.sigma AS nat_sigma_share, {window} AS nat_mu_window,
         CASE WHEN hist.cnt>1 AND hist.sigma>0 THEN ABS(t.nat_share_current - hist.mu)/NULLIF(hist.sigma,0) ELSE 0 END AS nat_zscore,
         CASE WHEN (CASE WHEN hist.cnt>1 AND hist.sigma>0 THEN ABS(t.nat_share_current - hist.mu)/NULLIF(hist.sigma,0) ELSE 0 END) > {z_nat} THEN TRUE ELSE FALSE END AS nat_outlier_pos
  FROM nat_typed_r t
  LEFT JOIN LATERAL (
    SELECT avg(s.nat_share_current) AS mu,
           stddev_samp(s.nat_share_current) AS sigma,
           COUNT(*) AS cnt
    FROM (
      SELECT h.nat_share_current
      FROM nat_typed_all h
      WHERE h.winner = t.winner
        AND h.day_type = t.day_type
        AND CAST(h.the_date AS DATE) < CAST(t.the_date AS DATE)
      ORDER BY h.the_date DESC
      LIMIT {window}
    ) s
  ) AS hist ON TRUE
), pair_all AS (
  SELECT CAST(the_date AS DATE) AS the_date,
         winner, loser, dma_name,
         SUM(adjusted_wins) AS pair_wins_current
  FROM filt_all
  GROUP BY 1,2,3,4
), pair_r AS (
  SELECT CAST(the_date AS DATE) AS the_date,
         winner, loser, dma_name,
         SUM(adjusted_wins) AS pair_wins_current
  FROM filt_r
  GROUP BY 1,2,3,4
), pair_typed_all AS (
  SELECT *, CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                 WHEN strftime('%w', the_date)='0' THEN 'Sun'
                 ELSE 'Weekday' END AS day_type
  FROM pair_all
)
SELECT ps.the_date, ps.winner, ps.loser, ps.dma_name,
       ps.pair_wins_current,
       hist.mu AS pair_mu_wins, hist.sigma AS pair_sigma_wins, {window} AS pair_mu_window,
       CASE WHEN hist.cnt>1 AND hist.sigma>0 THEN (ps.pair_wins_current - hist.mu)/NULLIF(hist.sigma,0) ELSE 0 END AS pair_z,
       (hist.mu IS NOT NULL AND ps.pair_wins_current > 1.3*hist.mu) AS pct_outlier_pos,
       (hist.mu IS NULL OR hist.mu = 0) AS new_pair,
       COALESCE(hist.mu,0) < 2.0 AS rare_pair,
       nm.nat_total_wins, nm.market_total_wins AS nat_market_wins, nm.nat_share_current, ns.nat_mu_share, ns.nat_sigma_share, ns.nat_mu_window,
       ns.nat_zscore, ns.nat_outlier_pos
FROM pair_r ps
JOIN nat_metrics_all nm ON nm.the_date = ps.the_date AND nm.winner = ps.winner
JOIN nat_scored  ns ON ns.the_date = ps.the_date AND ns.winner = ps.winner
LEFT JOIN LATERAL (
  SELECT avg(s.pair_wins_current) AS mu,
         stddev_samp(s.pair_wins_current) AS sigma,
         COUNT(*) AS cnt
  FROM (
    SELECT h.pair_wins_current
    FROM pair_typed_all h
    WHERE h.winner=ps.winner AND h.loser=ps.loser AND h.dma_name=ps.dma_name
      AND h.day_type = CASE WHEN strftime('%w', ps.the_date)='6' THEN 'Sat'
                            WHEN strftime('%w', ps.the_date)='0' THEN 'Sun'
                            ELSE 'Weekday' END
      AND CAST(h.the_date AS DATE) < CAST(ps.the_date AS DATE)
    ORDER BY h.the_date DESC
    LIMIT {window}
  ) s
) AS hist ON TRUE
{where_outlier}
ORDER BY 1,2,3,4;
