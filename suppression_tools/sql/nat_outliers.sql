-- nat_outliers.sql
-- Inputs: {store_glob}, {ds}, {mover_ind}, {start_date}, {end_date}, {window}, {z_thresh}
WITH ds AS (
  SELECT * FROM parquet_scan('{store_glob}')
), filt AS (
  SELECT * FROM ds
  WHERE ds = '{ds}'
    AND mover_ind = {mover_ind}
    AND CAST(the_date AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    {extra_filters}
), market AS (
  SELECT the_date, SUM(adjusted_wins) AS market_total_wins
  FROM filt GROUP BY 1
), nat AS (
  SELECT the_date, winner, SUM(adjusted_wins) AS total_wins
  FROM filt GROUP BY 1,2
), m AS (
  SELECT n.the_date, n.winner,
         n.total_wins / NULLIF(market_total_wins, 0) AS win_share
  FROM nat n JOIN market m USING (the_date)
), typed AS (
  SELECT *, CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                 WHEN strftime('%w', the_date)='0' THEN 'Sun'
                 ELSE 'Weekday' END AS day_type
  FROM m
), hist AS (
  SELECT r.the_date, r.winner, r.day_type,
         avg(h.win_share) AS mu,
         stddev_samp(h.win_share) AS sigma,
         COUNT(*) AS cnt
  FROM typed r
  JOIN typed h
    ON h.winner=r.winner AND h.day_type=r.day_type
   AND CAST(h.the_date AS DATE) < CAST(r.the_date AS DATE)
   AND CAST(h.the_date AS DATE) >= CAST(r.the_date AS DATE) - INTERVAL {window} DAY
  GROUP BY 1,2,3
)
SELECT t.the_date, t.winner,
       (CASE WHEN h.cnt>1 AND h.sigma>0 THEN (t.win_share - h.mu)/NULLIF(h.sigma,0) ELSE 0 END) AS z,
       CASE WHEN (CASE WHEN h.cnt>1 AND h.sigma>0 THEN (t.win_share - h.mu)/NULLIF(h.sigma,0) ELSE 0 END) > {z_thresh} THEN TRUE ELSE FALSE END AS nat_outlier_pos
FROM typed t
LEFT JOIN hist h ON h.the_date=t.the_date AND h.winner=t.winner AND h.day_type=t.day_type
ORDER BY 1,2;

