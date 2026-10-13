-- Schema for the real-time market data pipeline (Tiger Cloud / TimescaleDB 2.13+).
-- Based on https://www.tigerdata.com/docs/learn/tutorials/ingest-real-time-financial-data
-- Safe to re-run: every statement skips objects that already exist.

-- Raw ticks: hypertable partitioned on "time", columnstore segmented by symbol.
-- "time" is when the pipeline received the tick (microseconds, corrected to the
-- database clock). The Twelve Data trial feed rounds its own timestamps to the
-- minute, so they are kept separately in exchange_time and are not used for ordering.
CREATE TABLE IF NOT EXISTS crypto_ticks (
    "time"         TIMESTAMPTZ      NOT NULL,              -- received by the pipeline
    exchange_time  TIMESTAMPTZ      NOT NULL,              -- timestamp sent by Twelve Data
    symbol         TEXT             NOT NULL,
    price          DOUBLE PRECISION NOT NULL,
    day_volume     NUMERIC,                                -- NULL for instruments without volume
    ingested_at    TIMESTAMPTZ      NOT NULL DEFAULT now() -- set by Tiger Cloud when the batch commits
) WITH (
    tsdb.hypertable,
    tsdb.segmentby = 'symbol',
    tsdb.orderby   = 'time DESC'
);

-- Relational lookup table for symbol names.
CREATE TABLE IF NOT EXISTS crypto_assets (
    symbol  TEXT PRIMARY KEY,
    "name"  TEXT NOT NULL
);

-- 1-minute OHLCV candle. materialized_only = false makes it real-time:
-- ticks that are not materialized yet are aggregated on the fly.
CREATE MATERIALIZED VIEW IF NOT EXISTS one_minute_candle
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('1 minute', "time") AS bucket,
       symbol,
       first(price, "time")      AS "open",
       max(price)                AS high,
       min(price)                AS low,
       last(price, "time")       AS "close",
       last(day_volume, "time")  AS day_volume,
       count(*)                  AS ticks
FROM crypto_ticks
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('one_minute_candle',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => true);

-- Daily OHLCV candle (the guide's one_day_candle), real-time so today's candle shows immediately.
CREATE MATERIALIZED VIEW IF NOT EXISTS one_day_candle
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('1 day', "time") AS bucket,
       symbol,
       first(price, "time")      AS "open",
       max(price)                AS high,
       min(price)                AS low,
       last(price, "time")       AS "close",
       last(day_volume, "time")  AS day_volume
FROM crypto_ticks
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('one_day_candle',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists     => true);
