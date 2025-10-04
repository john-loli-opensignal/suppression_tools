WITH ds AS (
  SELECT * FROM parquet_scan('{store_glob}')
), filt_all AS (
  SELECT * FROM ds
  WHERE ds = '{ds}'
    AND mover_ind = {mover_ind}
    {extra_filters}
), filt_r AS (
  SELECT * FROM filt_all
  WHERE CAST(the_date AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
), market_all AS (
  SELECT the_date,
         SUM(adjusted_wins) AS market_total_wins,
         SUM(adjusted_losses) AS market_total_losses
  FROM filt_all GROUP BY 1
), nat_all AS (
  SELECT the_date, winner,
         SUM(adjusted_wins) AS nat_total_wins,
         SUM(adjusted_losses) AS nat_total_losses
  FROM filt_all GROUP BY 1,2
), m_all AS (
  SELECT n.the_date, n.winner,
         {metric_expr} AS metric_val
  FROM nat_all n JOIN market_all m USING (the_date)
), typed_all AS (
  SELECT *, CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                 WHEN strftime('%w', the_date)='0' THEN 'Sun'
                 ELSE 'Weekday' END AS day_type
  FROM m_all
), typed_r AS (
  SELECT * FROM typed_all
  WHERE CAST(the_date AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
)
SELECT t.the_date, t.winner,
       (CASE WHEN hist.cnt>1 AND hist.sigma>0 THEN ABS(t.metric_val - hist.mu)/NULLIF(hist.sigma,0) ELSE 0 END) AS z,
       CASE WHEN (CASE WHEN hist.cnt>1 AND hist.sigma>0 THEN ABS(t.metric_val - hist.mu)/NULLIF(hist.sigma,0) ELSE 0 END) > {z_thresh} THEN TRUE ELSE FALSE END AS nat_outlier_pos
FROM typed_r t
LEFT JOIN LATERAL (
  SELECT avg(s.metric_val) AS mu,
         stddev_samp(s.metric_val) AS sigma,
         COUNT(*) AS cnt
  FROM (
    SELECT h.metric_val
    FROM typed_all h
    WHERE h.winner = t.winner
      AND h.day_type = t.day_type
      AND CAST(h.the_date AS DATE) < CAST(t.the_date AS DATE)
    ORDER BY h.the_date DESC
    LIMIT {prev}
  ) s
) AS hist ON TRUE
ORDER BY 1,2;
