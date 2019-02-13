DROP TABLE IF EXISTS finished_arbitrage_orders;
CREATE TABLE finished_arbitrage_orders AS
SELECT
  T.transaction_id
  , O.type as order_type
  , O.status
  , O.filled_qty
  , O.price_avg
  , O.fee
FROM runtime_orders O
JOIN runtime_transactions T
ON
  O.transaction_id = T.transaction_id
WHERE
  T.status = 'ended_normally'
  AND O.fee < 0
ORDER BY
  T.transaction_id
  , O.type
;

DROP TABLE IF EXISTS arbitrage_gains;
CREATE TABLE arbitrage_gains AS
SELECT
  transaction_id
  , sum(filled_qty / price_avg * 
      (CASE order_type
        WHEN 1 THEN -1
        WHEN 4 THEN -1
        ELSE 1
       END)
    ) AS pre_fee_gain
  , sum(filled_qty / price_avg * 
      (CASE order_type
        WHEN 1 THEN -1
        WHEN 4 THEN -1
        ELSE 1
       END)
       + fee
    ) AS gain
  , sum(fee) AS fee
FROM
  finished_arbitrage_orders
GROUP BY
  transaction_id
LIMIT 10
;
