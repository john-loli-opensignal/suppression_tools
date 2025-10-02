-- competitor_view.sql
-- Inputs: {store_glob}, {ds}, {mover_ind}, {start_date}, {end_date}, {primary}, {competitors}
WITH ds AS (
  SELECT * FROM parquet_scan('{store_glob}')
), filt AS (
  SELECT * FROM ds
  WHERE ds = '{ds}'
    AND mover_ind = {mover_ind}
    AND CAST(the_date AS DATE) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    {extra_filters}
), h2h AS (
  SELECT CAST(the_date AS DATE) AS the_date,
         loser AS competitor,
         SUM(adjusted_wins) AS h2h_wins,
         SUM(adjusted_losses) AS h2h_losses
  FROM filt
  WHERE winner = '{primary}' AND loser IN ({competitors})
  GROUP BY 1,2
), prim AS (
  SELECT CAST(the_date AS DATE) AS the_date,
         SUM(adjusted_wins) AS primary_total_wins,
         SUM(adjusted_losses) AS primary_total_losses
  FROM filt WHERE winner = '{primary}' GROUP BY 1
)
SELECT h.the_date, h.competitor,
       h.h2h_wins, h.h2h_losses,
       prim.primary_total_wins, prim.primary_total_losses
FROM h2h h
JOIN prim USING (the_date)
ORDER BY 1,2;

