-- pair_metrics.sql
-- Inputs: {store_glob}, {ds}, {mover_ind}, {start_date}, {end_date}
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
), pair AS (
  SELECT CAST(the_date AS DATE) AS the_date,
         winner, loser, dma_name,
         SUM(adjusted_wins) AS pair_wins_current
  FROM filt
  GROUP BY 1,2,3,4
), dow AS (
  SELECT *, CASE WHEN strftime('%w', the_date)='6' THEN 'Sat'
                 WHEN strftime('%w', the_date)='0' THEN 'Sun'
                 ELSE 'Weekday' END AS day_type
  FROM pair
)
SELECT * FROM dow
ORDER BY 1,2,3,4;

