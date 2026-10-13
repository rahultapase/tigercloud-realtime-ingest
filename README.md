# tigercloud-realtime-ingest

A small real-time market data pipeline: live prices from the **Twelve Data websocket** are
batched in Python and bulk-inserted into a **hypertable on Tiger Cloud** (TimescaleDB), where
**continuous aggregates** turn the ticks into OHLCV candles.

Built for the MLH Global Hack Week challenge *Ingest real-time financial data into Tiger Cloud*,
following Tiger Data's tutorial
[Ingest real-time financial data](https://www.tigerdata.com/docs/learn/tutorials/ingest-real-time-financial-data),
updated for current Python and TimescaleDB.

## How it works

```
Twelve Data websocket ──► on_event (library thread)
   price events             stamps receive time, puts on a queue
                                   │
                                   ▼
                         main thread: batch of 100 ticks or every 5 s
                                   │  psycopg 3 executemany, one transaction
                                   ▼
                    crypto_ticks hypertable (Tiger Cloud)
                                   │
                 ┌─────────────────┴──────────────────┐
                 ▼                                    ▼
      one_minute_candle (CAGG)              one_day_candle (CAGG)
```

- **`crypto_ticks`** hypertable, columnstore segmented by `symbol`, ordered by `time DESC`.
- **`one_minute_candle`** and **`one_day_candle`** real-time continuous aggregates
  (`materialized_only = false`), each with a refresh policy.
- **`crypto_assets`** lookup table, joined into a market summary.

## Differences from the tutorial

| Tutorial | Here | Why |
|---|---|---|
| `datetime.utcfromtimestamp` | `datetime.fromtimestamp(ts, tz=timezone.utc)` | deprecated since Python 3.12 |
| psycopg2 + `execute_values` | psycopg 3 + `executemany` | psycopg 3 is the maintained driver |
| insert only at exactly 100 ticks | 100 ticks **or** every 5 s; last batch saved on exit | low tick rates and Ctrl+C don't lose data |
| DB writes on the websocket thread | queue → main thread | insert errors stop the run instead of being swallowed |
| exchange timestamp as `time` | receive time as `time`, exchange timestamp in `exchange_time` | the free plan's feed rounds timestamps to the minute, which would scramble candle open/close |
| daily candle, default settings | real-time candles (`materialized_only = false`) | since TimescaleDB 2.13 the default hides data that hasn't been refreshed yet |

The receive time is corrected to the database clock: at startup the script measures the
offset between the local clock and `clock_timestamp()` on Tiger Cloud.

## Run it

Requirements: Python 3.10+, the [Tiger CLI](https://www.tigerdata.com/docs/) (logged in with
`tiger auth login`), a Tiger Cloud service, and a free [Twelve Data](https://twelvedata.com) API key.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# create the tables and candles (psql, Tiger Console SQL editor, or Tiger MCP)
#   sql/schema.sql, then sql/seed.sql

# API key: environment variable, or a gitignored .env next to the script
#   TWELVEDATA_API_KEY=your_key
.\.venv\Scripts\python.exe pipeline.py --minutes 60 --symbols "BTC/USD,EUR/USD,AAPL"
```

The database connection comes from `DATABASE_URL` if set; otherwise the script runs
`tiger db uri <service> --with-password` (default service `db-ghw`, change with `--service`) and keeps
the URI in memory only. The API key and the connection string are never printed: the Twelve Data
library's log output goes through a filter that masks the key.

On the free Basic plan the websocket streams only **trial symbols** (at most 8, one connection).
`BTC/USD`, `EUR/USD` and `AAPL` were accepted; `ETH/USD` and `ETH/BTC` were not. `AAPL` only ticks
while US markets are open.

## Queries

`sql/queries.sql` has the queries used to check the pipeline:

1. ingest summary per symbol: ticks, time range, ticks per minute, pipeline lag (receive → commit)
2. latest 1-minute candles
3. daily candle (the tutorial's query)
4. market summary joined to `crypto_assets`: first/last price and % change
5. what was created: hypertable, continuous aggregates, jobs

## Files

| File | Purpose |
|---|---|
| `pipeline.py` | websocket → batch → insert |
| `sql/schema.sql` | hypertable, lookup table, candles, refresh policies |
| `sql/seed.sql` | symbol names for `crypto_assets` |
| `sql/queries.sql` | verification queries |
| `requirements.txt` | pinned dependencies |

## License

MIT
