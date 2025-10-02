-- national_timeseries.sql
-- Inputs: {store_glob}, {ds}, {mover_ind}, {start_date}, {end_date}
WITH ds AS (
  SELECT * FROM parquet_scan('{store_glob}')
), filt AS (
  SELECT * FROM ds
  WHERE ds = '{ds}'
    AND mover_ind = {mover_ind}
    AND CAST(the_date AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    {extra_filters}
), market AS (
  SELECT the_date,
         SUM(adjusted_wins) AS market_total_wins,
         SUM(adjusted_losses) AS market_total_losses
  FROM filt
  GROUP BY 1
), nat AS (
  SELECT the_date, winner,
         SUM(adjusted_wins) AS total_wins,
         SUM(adjusted_losses) AS total_losses
  FROM filt
  GROUP BY 1,2
)
SELECT n.the_date,
       n.winner,
       n.total_wins / NULLIF(m.market_total_wins, 0) AS win_share,
       n.total_losses / NULLIF(m.market_total_losses, 0) AS loss_share,
       n.total_wins / NULLIF(n.total_losses, 0) AS wins_per_loss
FROM nat n
JOIN market m USING (the_date)
ORDER BY 1,2;

