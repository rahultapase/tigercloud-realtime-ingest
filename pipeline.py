"""Real-time market data pipeline: Twelve Data websocket -> Tiger Cloud hypertable.

Streams live prices over the Twelve Data websocket, batches them in memory and
bulk-inserts each batch into the `crypto_ticks` hypertable on a Tiger Cloud
(TimescaleDB) service.

Secrets are never hard-coded or printed:
  * TWELVEDATA_API_KEY comes from the environment, or from a gitignored .env file.
  * DATABASE_URL comes from the environment, or is fetched at runtime with
    `tiger db uri <service> --with-password` and kept in memory only.

Each tick is stamped with the time the pipeline received it (corrected to the
database clock); Twelve Data's own timestamp is kept in `exchange_time`.

Usage:
    python pipeline.py --minutes 60 --symbols "BTC/USD,EUR/USD,AAPL"
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import shutil
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from twelvedata import TDClient

INSERT_SQL = (
    "INSERT INTO crypto_ticks (time, exchange_time, symbol, price, day_volume) "
    "VALUES (%s, %s, %s, %s, %s)"
)
HEARTBEAT_EVERY_S = 10          # Twelve Data recommends a heartbeat every 10 s
SUBSCRIBE_TIMEOUT_S = 30        # give up if the server never answers the subscribe
NO_DATA_WARNING_S = 60          # warn if subscribed but no prices arrive


class SecretFilter(logging.Filter):
    """Replace secret values in log records (the library puts the key in its URL)."""

    def __init__(self, secrets: list[str]):
        super().__init__()
        self.secrets = [s for s in secrets if s]

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for secret in self.secrets:
            message = message.replace(secret, "***")
        record.msg, record.args = message, ()
        return True


class FatalError(Exception):
    """An error with a message that is safe to print."""


def now_local() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log(message: str) -> None:
    print(f"{now_local()}  {message}", flush=True)


def read_env_file(path: Path, name: str) -> str | None:
    """Return one variable from a simple KEY=VALUE file, without printing anything."""
    if not path.is_file():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == name:
            return value.strip().strip('"').strip("'") or None
    return None


def get_api_key(env_file: Path) -> str:
    key = os.environ.get("TWELVEDATA_API_KEY") or read_env_file(env_file, "TWELVEDATA_API_KEY")
    if not key:
        raise FatalError(f"TWELVEDATA_API_KEY is not set (environment or {env_file.name}).")
    return key


def get_database_url(service: str) -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    tiger = shutil.which("tiger") or str(
        Path(os.environ.get("LOCALAPPDATA", "")) / "Programs" / "TigerCLI" / "tiger.exe"
    )
    try:
        result = subprocess.run(
            [tiger, "db", "uri", service, "--with-password", "--version-check=false"],
            capture_output=True, text=True, timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise FatalError(f"Could not run the Tiger CLI ({type(exc).__name__}).") from None
    lines = [l.strip() for l in result.stdout.splitlines() if l.strip().startswith("postgres")]
    if result.returncode != 0 or not lines:
        raise FatalError(
            f"`tiger db uri {service}` did not return a connection URI "
            f"(exit code {result.returncode}). Run `tiger auth login` and try again."
        )
    return lines[-1]


def connect_db(url: str) -> psycopg.Connection:
    try:
        return psycopg.connect(url, autocommit=True, application_name="tiger-realtime-ingest",
                               connect_timeout=15)
    except psycopg.Error as exc:
        # psycopg messages can include the host name, so only the type is shown.
        raise FatalError(f"Database connection failed ({type(exc).__name__}; details hidden).") from None


def measure_clock_offset(conn: psycopg.Connection, samples: int = 5) -> timedelta:
    """Database clock minus local clock, from the round trip with the smallest delay."""
    best = None
    for _ in range(samples):
        before = datetime.now(timezone.utc)
        db_now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
        after = datetime.now(timezone.utc)
        rtt = after - before
        offset = db_now - (before + rtt / 2)
        if best is None or rtt < best[0]:
            best = (rtt, offset)
    return best[1]


class Pipeline:
    def __init__(self, args: argparse.Namespace, api_key: str, db_url: str):
        self.args = args
        self.api_key = api_key
        self.db_url = db_url
        self.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        self.events: queue.Queue = queue.Queue()
        self.batch: list[tuple] = []
        self.batches = 0
        self.totals: Counter = Counter()
        self.heartbeats = 0
        self.subscribed = False
        self.accepted: list[str] = []
        self.conn = connect_db(db_url)
        self.clock_offset = measure_clock_offset(self.conn)

    # Runs on the library's event thread: stamp the receive time, then hand the
    # event to the main thread (all database work happens there).
    def on_event(self, event: dict) -> None:
        self.events.put((datetime.now(timezone.utc) + self.clock_offset, event))

    def handle(self, item: tuple) -> None:
        received, event = item
        kind = event.get("event")
        if kind == "price":
            exchange_time = datetime.fromtimestamp(event["timestamp"], tz=timezone.utc)
            self.batch.append((received, exchange_time, event["symbol"], float(event["price"]),
                               event.get("day_volume")))
        elif kind == "subscribe-status":
            self.subscribed = True
            ok = [s.get("symbol") for s in (event.get("success") or [])]
            failed = [s.get("symbol") for s in (event.get("fails") or [])]
            self.accepted += ok
            log(f"subscribe status: {event.get('status')}  accepted={ok or '-'}  rejected={failed or '-'}")
        elif kind == "heartbeat":
            self.heartbeats += 1

    def flush(self) -> None:
        if not self.batch:
            return
        rows = self.batch
        for attempt in (1, 2):
            try:
                with self.conn.transaction(), self.conn.cursor() as cur:
                    cur.executemany(INSERT_SQL, rows)
                break
            except psycopg.Error as exc:
                if attempt == 2:
                    raise FatalError(
                        f"Insert failed twice ({type(exc).__name__}); {len(rows)} ticks not saved."
                    ) from None
                log(f"insert failed ({type(exc).__name__}); reconnecting and retrying once")
                try:
                    self.conn.close()
                except psycopg.Error:
                    pass
                self.conn = connect_db(self.db_url)
        self.batches += 1
        per_symbol = Counter(r[2] for r in rows)
        self.totals.update(per_symbol)
        detail = ", ".join(f"{s} {n}" for s, n in sorted(per_symbol.items()))
        log(f"batch #{self.batches:<4} +{len(rows):>3} rows  ({detail})  "
            f"total {sum(self.totals.values()):,}")
        self.batch = []

    def run(self) -> None:
        ws_logger = logging.getLogger("pipeline.twelvedata")
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s  twelvedata %(levelname)s: %(message)s",
                                               "%H:%M:%S"))
        ws_logger.addHandler(handler)
        ws_logger.setLevel(logging.INFO)
        ws_logger.propagate = False
        secret_filter = SecretFilter([self.api_key])
        ws_logger.addFilter(secret_filter)
        logging.getLogger("websocket").addFilter(secret_filter)

        td = TDClient(apikey=self.api_key)
        ws = td.websocket(symbols=self.symbols, on_event=self.on_event, logger=ws_logger)
        log(f"clock offset vs Tiger Cloud: {self.clock_offset.total_seconds():+.3f} s "
            "(applied to receive timestamps)")
        log(f"connecting to Twelve Data; symbols={self.symbols}; run {self.args.minutes} min; "
            f"batch {self.args.batch_size} or {self.args.flush_seconds}s")
        ws.connect()

        start = time.monotonic()
        deadline = start + self.args.minutes * 60
        last_flush = last_heartbeat = start
        warned_no_data = False
        try:
            while time.monotonic() < deadline:
                try:
                    self.handle(self.events.get(timeout=0.5))
                except queue.Empty:
                    pass
                now = time.monotonic()
                if len(self.batch) >= self.args.batch_size or (
                    self.batch and now - last_flush >= self.args.flush_seconds
                ):
                    self.flush()
                    last_flush = now
                if now - last_heartbeat >= HEARTBEAT_EVERY_S:
                    ws.heartbeat()
                    last_heartbeat = now
                if not self.subscribed and now - start > SUBSCRIBE_TIMEOUT_S:
                    raise FatalError("No response from Twelve Data in 30 s: check the API key, "
                                     "and close the dashboard WebSocket Playground (Basic allows one connection).")
                if self.subscribed and not self.accepted:
                    raise FatalError("Twelve Data rejected every symbol.")
                if (not warned_no_data and self.subscribed and not self.totals and not self.batch
                        and now - start > NO_DATA_WARNING_S):
                    log("warning: subscribed, but no prices in the first minute (market closed?)")
                    warned_no_data = True
        except KeyboardInterrupt:
            log("stopping (Ctrl+C)")
        finally:
            # Drain whatever already arrived, save it, then close everything.
            while True:
                try:
                    self.handle(self.events.get_nowait())
                except queue.Empty:
                    break
            try:
                self.flush()
            finally:
                ws.disconnect()
                self.conn.close()

        minutes = (time.monotonic() - start) / 60
        log(f"done: {sum(self.totals.values()):,} ticks in {self.batches} batches over "
            f"{minutes:.1f} min; heartbeats {self.heartbeats}")
        for symbol, n in sorted(self.totals.items()):
            log(f"  {symbol:<10} {n:>6,} ticks")


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--symbols", default="BTC/USD,EUR/USD,AAPL",
                        help="comma-separated Twelve Data symbols (Basic plan: trial symbols, max 8)")
    parser.add_argument("--minutes", type=float, default=60, help="how long to stream (default 60)")
    parser.add_argument("--batch-size", type=int, default=100, help="insert when this many ticks are queued")
    parser.add_argument("--flush-seconds", type=float, default=5, help="insert at least this often")
    parser.add_argument("--env-file", type=Path, default=Path(__file__).with_name(".env"),
                        help="file holding TWELVEDATA_API_KEY=... (default: .env next to this script)")
    parser.add_argument("--service", default="db-ghw",
                        help="Tiger Cloud service name or ID for `tiger db uri` (if DATABASE_URL is unset)")
    args = parser.parse_args()

    try:
        api_key = get_api_key(args.env_file)
        db_url = get_database_url(args.service)
        Pipeline(args, api_key, db_url).run()
    except FatalError as exc:
        log(f"error: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
