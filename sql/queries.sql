-- Q1 Ingest summary + pipeline lag per symbol
-- pipeline lag = ingested_at - time: from the moment the pipeline received a tick to the
-- commit of its batch in Tiger Cloud, including the batching wait (at most --flush-seconds).
SELECT symbol,
       count(*)                                   AS ticks,
       min("time")                                AS first_tick,
       max("time")                                AS last_tick,
       round(count(*)::numeric / count(DISTINCT time_bucket('1 minute', "time")), 1)
                                                  AS ticks_per_min,   -- per minute with data
       round((percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - "time")))::numeric, 2)
                                                  AS median_pipeline_lag_s,
       round((percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - "time")))::numeric, 2)
                                                  AS p95_pipeline_lag_s
FROM crypto_ticks
GROUP BY symbol
ORDER BY symbol;

-- Q2 Latest 1-minute candles
SELECT bucket, symbol, "open", high, low, "close", ticks
FROM one_minute_candle
WHERE bucket >= now() - INTERVAL '15 minutes'
ORDER BY bucket DESC, symbol
LIMIT 30;

-- Q3 Daily candle (guide query)
SELECT * FROM one_day_candle
WHERE bucket >= now() - INTERVAL '14 days'
ORDER BY symbol, bucket;

-- Q4 Market summary, joined to crypto_assets
SELECT t.symbol,
       a."name",
       first(t.price, t."time")  AS first_price,
       last(t.price, t."time")   AS last_price,
       round(((last(t.price, t."time") / first(t.price, t."time") - 1) * 100)::numeric, 3) AS pct_change,
       min(t.price)              AS low,
       max(t.price)              AS high,
       count(*)                  AS ticks
FROM crypto_ticks t
LEFT JOIN crypto_assets a USING (symbol)
GROUP BY t.symbol, a."name"
ORDER BY t.symbol;

-- Q5 What was built
SELECT hypertable_name, num_chunks, compression_enabled
FROM timescaledb_information.hypertables
WHERE hypertable_name = 'crypto_ticks';

SELECT view_name, materialized_only
FROM timescaledb_information.continuous_aggregates
WHERE view_name IN ('one_minute_candle', 'one_day_candle');

SELECT j.job_id, j.proc_name, j.schedule_interval, coalesce(c.view_name, j.hypertable_name) AS target
FROM timescaledb_information.jobs j
LEFT JOIN timescaledb_information.continuous_aggregates c
       ON c.materialization_hypertable_name = j.hypertable_name
WHERE j.hypertable_name IN ('crypto_ticks', 'one_minute_candle', 'one_day_candle')
   OR c.view_name IN ('one_minute_candle', 'one_day_candle')
ORDER BY j.job_id;
