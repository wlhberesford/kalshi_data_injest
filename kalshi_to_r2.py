"""
kalshi_to_r2.py
══════════════════════════════════════════════════════════════════════════════
Fetches ALL current + historical market data for the Kalshi series KXBTCD
and writes it into Cloudflare R2 as Parquet files.

Storage layout in R2
─────────────────────
  kalshi/
    series/
      KXBTCD.parquet              ← one row, overwritten each run
    events/
      KXBTCD.parquet              ← all events, overwritten each run
    markets/
      KXBTCD.parquet              ← all markets, overwritten each run
    candlesticks/
      interval=1440/
        KXBTCD-26FEB2300.parquet  ← one file per market ticker
        KXBTCD-26FEB2301.parquet
        ...
    checkpoint/
      KXBTCD_1440.json            ← progress state, persists across runs

Flow per run
────────────
  Phase A (always, ~2 min): series → events → markets → write 3 Parquet files
  Phase B (resumes):        per market → fetch candles → write parquet → checkpoint

Setup
─────
  pip install requests boto3 pyarrow python-dotenv

  Required GitHub Actions secrets:
    R2_ENDPOINT_URL      https://<account-id>.r2.cloudflarestorage.com
    R2_ACCESS_KEY_ID     R2 API token Access Key ID
    R2_SECRET_ACCESS_KEY R2 API token Secret Access Key
    R2_BUCKET_NAME       your bucket name (e.g. kalshi-data)

  Optional:
    KALSHI_API_KEY       not required for public KXBTCD data
    PERIOD_INTERVAL      1440 (daily) | 60 (hourly) | 1 (minute)
    RUN_BUDGET_MINUTES   default 320
    LAST_N_EVENTS        fetch only the N most recently closed events (0 = all)
    LOOKBACK_DAYS        clamp candle history to last N days (0 = no limit)
══════════════════════════════════════════════════════════════════════════════
"""

import io
import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.config import Config
from dotenv import load_dotenv

# ─────────────────────────────── Config ──────────────────────────────────────

load_dotenv()

R2_ENDPOINT_URL      = os.environ["R2_ENDPOINT_URL"]
R2_ACCESS_KEY_ID     = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET_NAME       = os.environ["R2_BUCKET_NAME"]

KALSHI_API_KEY     = os.getenv("KALSHI_API_KEY", "")
PERIOD_INTERVAL    = int(os.getenv("PERIOD_INTERVAL", "1440"))
RUN_BUDGET_MINUTES = int(os.getenv("RUN_BUDGET_MINUTES", "320"))
LOOKBACK_DAYS      = int(os.getenv("LOOKBACK_DAYS", "0"))   # 0 = no limit
LAST_N_EVENTS      = int(os.getenv("LAST_N_EVENTS",  "0"))  # 0 = all events

KALSHI_BASE   = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKER = "KXBTCD"
PAGE_LIMIT    = 200
REQUEST_DELAY = 0.25     # seconds between Kalshi API calls
SAFETY_SECS   = 10 * 60  # stop 10 min before budget expires

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

RUN_START = time.monotonic()


def budget_exceeded() -> bool:
    return time.monotonic() - RUN_START >= (RUN_BUDGET_MINUTES * 60 - SAFETY_SECS)


# ─────────────────────────────── R2 client ───────────────────────────────────

_s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    config=Config(signature_version="s3v4"),
    region_name="auto",
)


def r2_put(key: str, data: bytes, content_type: str = "application/octet-stream"):
    _s3.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=data, ContentType=content_type)


def r2_get(key: str) -> bytes | None:
    try:
        resp = _s3.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        return resp["Body"].read()
    except _s3.exceptions.NoSuchKey:
        return None
    except Exception as exc:
        if "NoSuchKey" in str(exc) or "404" in str(exc):
            return None
        raise


def write_parquet(key: str, rows: list[dict]):
    """Convert list-of-dicts to Parquet and upload to R2."""
    if not rows:
        log.info("  (no rows — skipping %s)", key)
        return
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    r2_put(key, buf.getvalue())
    log.info("  ✓ %d rows → r2://%s/%s (%.1f KB)",
             len(rows), R2_BUCKET_NAME, key, len(buf.getvalue()) / 1024)


def read_parquet(key: str) -> list[dict]:
    """Download a Parquet file from R2 and return as list-of-dicts."""
    data = r2_get(key)
    if not data:
        return []
    table = pq.read_table(io.BytesIO(data))
    return table.to_pylist()


# ─────────────────────────────── Checkpoint ──────────────────────────────────

CHECKPOINT_KEY = f"kalshi/checkpoint/{SERIES_TICKER}_{PERIOD_INTERVAL}.json"


def load_checkpoint() -> dict:
    data = r2_get(CHECKPOINT_KEY)
    if data:
        cp = json.loads(data)
        cp["completed_tickers"] = set(cp.get("completed_tickers", []))
        return cp
    return {
        "series_ticker":       SERIES_TICKER,
        "period_interval_min": PERIOD_INTERVAL,
        "completed_tickers":   set(),
        "total_markets":       None,
        "last_ticker":         None,
        "run_count":           0,
        "finished":            False,
    }


def save_checkpoint(cp: dict, finished: bool = False):
    cp["finished"]   = finished
    cp["run_count"]  = cp.get("run_count", 0) + (1 if finished else 0)
    cp["last_run_at"] = now_iso()
    payload = {**cp, "completed_tickers": list(cp["completed_tickers"])}
    r2_put(CHECKPOINT_KEY, json.dumps(payload).encode(), "application/json")
    log.info("  💾 Checkpoint saved: %d done, finished=%s",
             len(cp["completed_tickers"]), finished)


# ─────────────────────────────── Kalshi HTTP ─────────────────────────────────

_ks = requests.Session()
if KALSHI_API_KEY:
    _ks.headers["Authorization"] = f"Bearer {KALSHI_API_KEY}"


def kalshi_get(path: str, params: dict = None) -> dict:
    url = f"{KALSHI_BASE}{path}"
    for attempt in range(3):
        try:
            r = _ks.get(url, params=params, timeout=15)
            r.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return r.json()
        except requests.HTTPError as exc:
            code = exc.response.status_code
            log.warning("HTTP %s → %s : %s", code, url, exc.response.text[:120])
            if code == 429:
                time.sleep(5 * (attempt + 1))
            elif code >= 500:
                time.sleep(2)
            else:
                raise
    raise RuntimeError(f"Kalshi request failed after 3 attempts: {url}")


def kalshi_paginate(path: str, list_key: str, params: dict = None) -> list:
    params = dict(params or {})
    params.setdefault("limit", PAGE_LIMIT)
    results, page = [], 1
    while True:
        data  = kalshi_get(path, params)
        batch = data.get(list_key, [])
        results.extend(batch)
        cursor = data.get("cursor", "")
        log.info("  %s p%d → +%d (total %d)", path, page, len(batch), len(results))
        if not cursor or not batch:
            break
        params["cursor"] = cursor
        page += 1
    return results


# ─────────────────────────────── Helpers ─────────────────────────────────────

def to_num(v):
    try:    return float(v) if v else None
    except: return None

def unix_to_ts(u):
    if u is None: return None
    if isinstance(u, str):
        return datetime.fromisoformat(u.replace("Z", "+00:00")).isoformat()
    return datetime.fromtimestamp(int(u), tz=timezone.utc).isoformat()

def to_unix_int(v) -> int:
    if v is None: return 0
    if isinstance(v, (int, float)): return int(v)
    try:
        return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def to_str(v):
    if v is None: return None
    if isinstance(v, (list, dict)): return json.dumps(v)
    return str(v)


# ─────────────────────────────── Row builders ────────────────────────────────

def build_series_row(s: dict) -> dict:
    return {
        "series_ticker":      s.get("ticker", SERIES_TICKER),
        "title":              s.get("title"),
        "category":           s.get("category"),
        "tags":               to_str(s.get("tags")),
        "frequency":          s.get("frequency"),
        "settlement_sources": to_str(s.get("settlement_sources")),
        "updated_at":         now_iso(),
    }


def build_event_row(ev: dict) -> dict:
    return {
        "event_ticker":          ev.get("event_ticker", ""),
        "series_ticker":         ev.get("series_ticker", SERIES_TICKER),
        "title":                 ev.get("title"),
        "sub_title":             ev.get("sub_title"),
        "category":              ev.get("category"),
        "status":                ev.get("status"),
        "strike_date":           unix_to_ts(ev.get("strike_date")),
        "strike_period":         ev.get("strike_period"),
        "mutually_exclusive":    ev.get("mutually_exclusive"),
        "collateral_return_type":ev.get("collateral_return_type"),
        "product_metadata":      to_str(ev.get("product_metadata")),
        "updated_at":            now_iso(),
    }


def build_market_row(m: dict) -> dict:
    def _s(k): return m.get(k)
    def _n(k): return to_num(m.get(k))
    return {
        "ticker":                     _s("ticker"),
        "event_ticker":               _s("event_ticker"),
        "series_ticker":              _s("series_ticker") or SERIES_TICKER,
        "title":                      _s("title"),
        "subtitle":                   _s("subtitle"),
        "yes_sub_title":              _s("yes_sub_title"),
        "no_sub_title":               _s("no_sub_title"),
        "status":                     _s("status"),
        "result":                     _s("result"),
        "open_time":                  unix_to_ts(_s("open_time")),
        "close_time":                 unix_to_ts(_s("close_time")),
        "expected_expiration_time":   unix_to_ts(_s("expected_expiration_time")),
        "settlement_ts":              unix_to_ts(_s("settlement_ts")),
        "expiration_value":           _s("expiration_value"),
        "last_price":                 _n("last_price"),
        "yes_bid":                    _n("yes_bid"),
        "yes_ask":                    _n("yes_ask"),
        "no_bid":                     _n("no_bid"),
        "no_ask":                     _n("no_ask"),
        "volume":                     _n("volume"),
        "volume_24h":                 _n("volume_24h"),
        "open_interest":              _n("open_interest"),
        "notional_value":             _n("notional_value"),
        "strike_type":                _s("strike_type"),
        "cap_strike":                 _n("cap_strike"),
        "custom_strike":              to_str(_s("custom_strike")),
        "rules_secondary":            _s("rules_secondary"),
        "can_close_early":            _s("can_close_early"),
        "price_ranges":               to_str(_s("price_ranges")),
        "settlement_value":           _n("settlement_value"),
        "updated_at":                 now_iso(),
    }


def build_candle_rows(ticker: str, candles: list) -> list[dict]:
    rows = []
    for c in candles:
        def _i(blk, f):
            v = c.get(blk, {}).get(f)
            if v is None: return None
            try: return int(float(v))
            except: return None
        def _d(blk, f): return to_num(c.get(blk, {}).get(f))

        rows.append({
            "market_ticker":       ticker,
            "period_interval_min": PERIOD_INTERVAL,
            "end_period_ts":       unix_to_ts(c.get("end_period_ts")),
            "yes_bid_open":        _i("yes_bid","open"),
            "yes_bid_low":         _i("yes_bid","low"),
            "yes_bid_high":        _i("yes_bid","high"),
            "yes_bid_close":       _i("yes_bid","close"),
            "yes_bid_open_d":      _d("yes_bid","open_dollars"),
            "yes_bid_low_d":       _d("yes_bid","low_dollars"),
            "yes_bid_high_d":      _d("yes_bid","high_dollars"),
            "yes_bid_close_d":     _d("yes_bid","close_dollars"),
            "yes_ask_open":        _i("yes_ask","open"),
            "yes_ask_low":         _i("yes_ask","low"),
            "yes_ask_high":        _i("yes_ask","high"),
            "yes_ask_close":       _i("yes_ask","close"),
            "yes_ask_open_d":      _d("yes_ask","open_dollars"),
            "yes_ask_low_d":       _d("yes_ask","low_dollars"),
            "yes_ask_high_d":      _d("yes_ask","high_dollars"),
            "yes_ask_close_d":     _d("yes_ask","close_dollars"),
            "volume":        to_num(c.get("volume_fp"))        or c.get("volume"),
            "open_interest": to_num(c.get("open_interest_fp")) or c.get("open_interest"),
        })
    return rows


# Max candles the API will return in one request at 1-min resolution
MAX_CANDLES_PER_REQUEST = 9000
CHUNK_SECS = MAX_CANDLES_PER_REQUEST * 60  # ~6.25 days of minutes


def fetch_candles(ticker: str, market: dict, cutoff_unix: int) -> list:
    open_ts      = market.get("open_time")
    close_ts     = market.get("close_time") or market.get("expiration_time")
    settled_str  = market.get("settlement_ts") or market.get("expiration_time")

    now_unix     = int(time.time())
    lookback_ts  = (now_unix - LOOKBACK_DAYS * 86400) if LOOKBACK_DAYS else 0

    start_unix   = to_unix_int(open_ts) or 0
    # Use the latest of close_time, expected_expiration_time, settlement_ts as end
    expiry_ts    = (market.get("expected_expiration_time")
                   or market.get("expiration_time")
                   or close_ts)
    end_unix     = max(
        to_unix_int(close_ts)   or 0,
        to_unix_int(expiry_ts)  or 0,
        to_unix_int(settled_str) or 0,
    ) or now_unix
    settled_unix = to_unix_int(settled_str)

    log.info("    time range: %s → %s (%d min)", unix_to_ts(start_unix), unix_to_ts(end_unix), (end_unix - start_unix) // 60)

    # Skip markets that closed entirely before the lookback window
    if LOOKBACK_DAYS and end_unix < lookback_ts:
        return []

    # Clamp start to lookback window
    if LOOKBACK_DAYS:
        start_unix = max(start_unix, lookback_ts)

    use_hist = settled_unix and settled_unix < cutoff_unix
    path = (f"/historical/markets/{ticker}/candlesticks" if use_hist
            else f"/series/{SERIES_TICKER}/markets/{ticker}/candlesticks")

    # Chunk requests so no single call exceeds the API's candle limit
    chunk_secs = CHUNK_SECS if PERIOD_INTERVAL == 1 else (end_unix - start_unix + 1)
    all_candles = []
    chunk_start = start_unix
    while chunk_start < end_unix:
        chunk_end = min(chunk_start + chunk_secs, end_unix)
        try:
            data = kalshi_get(path, {"start_ts": chunk_start, "end_ts": chunk_end,
                                      "period_interval": PERIOD_INTERVAL})
            all_candles.extend(data.get("candlesticks", []))
        except Exception as exc:
            log.warning("  ⚠ candle fetch failed for %s [%s-%s]: %s",
                        ticker, chunk_start, chunk_end, exc)
        chunk_start = chunk_end + 1

    return all_candles


def candle_key(ticker: str) -> str:
    return f"kalshi/candlesticks/interval={PERIOD_INTERVAL}/{ticker}.parquet"


def _write_github_output(key: str, value: str):
    gho = os.getenv("GITHUB_OUTPUT")
    if gho:
        with open(gho, "a") as f:
            f.write(f"{key}={value}\n")
    log.info("  → GitHub output: %s=%s", key, value)


# ─────────────────────────────── Main ────────────────────────────────────────

def main():
    log.info("══════════════════════════════════════════════════")
    log.info("  Kalshi → R2 Parquet  |  Series : %s", SERIES_TICKER)
    log.info("  Candle interval      : %d min",        PERIOD_INTERVAL)
    log.info("  Run budget           : %d min",        RUN_BUDGET_MINUTES)
    log.info("  Lookback             : %s",            f"{LOOKBACK_DAYS}d" if LOOKBACK_DAYS else "unlimited")
    log.info("  Last N events        : %s",            LAST_N_EVENTS if LAST_N_EVENTS else "all")
    log.info("  Bucket               : %s",            R2_BUCKET_NAME)
    log.info("══════════════════════════════════════════════════")

    # ── Load checkpoint ──────────────────────────────────────────────────────
    cp = load_checkpoint()
    log.info("Checkpoint: %d markets done, finished=%s",
             len(cp["completed_tickers"]), cp["finished"])

    if cp["finished"]:
        log.info("✅  Already complete — nothing to do.")
        _write_github_output("needs_rerun", "false")
        return

    # ── Phase A: metadata ────────────────────────────────────────────────────
    log.info("\n── Phase A: series / events / markets ─────────────────────")

    try:
        series_raw = kalshi_get(f"/series/{SERIES_TICKER}").get("series", {})
    except Exception:
        series_raw = {"ticker": SERIES_TICKER}
    write_parquet(f"kalshi/series/{SERIES_TICKER}.parquet", [build_series_row(series_raw)])

    cutoff_unix = to_unix_int(kalshi_get("/historical/cutoff").get("market_settled_ts"))
    log.info("  historical cutoff : %s", unix_to_ts(cutoff_unix))

    events = kalshi_paginate("/events", "events",
                             params={"series_ticker": SERIES_TICKER,
                                     "with_nested_markets": "true"})
    event_rows = [build_event_row(ev) for ev in events
                  if ev.get("series_ticker", SERIES_TICKER) == SERIES_TICKER]
    write_parquet(f"kalshi/events/{SERIES_TICKER}.parquet", event_rows)

    hist_markets = kalshi_paginate("/historical/markets", "markets",
                                   params={"series_ticker": SERIES_TICKER})

    market_index: dict[str, dict] = {}
    known_events = {r["event_ticker"] for r in event_rows}

    for ev in events:
        for m in (ev.get("markets") or []):
            m.setdefault("event_ticker",  ev["event_ticker"])
            m.setdefault("series_ticker", ev.get("series_ticker", SERIES_TICKER))
            # Skip any market whose ticker doesn't start with the series prefix
            if not m["ticker"].startswith(SERIES_TICKER):
                continue
            market_index[m["ticker"]] = m

    stub_events = []
    for m in hist_markets:
        # Filter by both series_ticker field AND ticker prefix to catch API leakage
        m_series = m.get("series_ticker") or ""
        if m_series != SERIES_TICKER:
            continue
        if not m.get("ticker", "").startswith(SERIES_TICKER):
            continue
        m.setdefault("series_ticker", SERIES_TICKER)
        ev_t = m.get("event_ticker", "")
        if ev_t and ev_t not in known_events:
            stub_events.append(build_event_row({"event_ticker": ev_t,
                                                "series_ticker": SERIES_TICKER}))
            known_events.add(ev_t)
        market_index.setdefault(m["ticker"], m)

    # Merge stub events into event file if any new ones found
    if stub_events:
        all_events = event_rows + stub_events
        write_parquet(f"kalshi/events/{SERIES_TICKER}.parquet", all_events)

    write_parquet(f"kalshi/markets/{SERIES_TICKER}.parquet",
                  [build_market_row(m) for m in market_index.values()])

    # ── Phase B: candlesticks ────────────────────────────────────────────────
    now_iso_str = now_iso()

    # Build event → [markets] map from market_index
    event_to_markets: dict[str, list[str]] = {}
    for ticker, m in market_index.items():
        ev_t = m.get("event_ticker", "")
        if ev_t:
            event_to_markets.setdefault(ev_t, []).append(ticker)

    # Find events that have fully closed, keyed by their close time
    # Use the latest close_time among their markets as the event close time
    def event_close_ts(ev_ticker: str) -> str:
        tickers = event_to_markets.get(ev_ticker, [])
        times = [market_index[t].get("close_time") or "" for t in tickers]
        return max(times) if times else ""

    closed_events = sorted(
        [ev for ev in event_to_markets.keys()
         if event_close_ts(ev) and event_close_ts(ev) <= now_iso_str],
        key=event_close_ts,
    )
    log.info("  %d events have fully closed", len(closed_events))

    if LAST_N_EVENTS:
        closed_events = closed_events[-LAST_N_EVENTS:]
        log.info("  Trimmed to last %d events", LAST_N_EVENTS)

    # Flatten to ordered market tickers (sorted by open_time within each event)
    all_tickers = []
    for ev_t in closed_events:
        ev_markets = sorted(
            event_to_markets[ev_t],
            key=lambda t: market_index[t].get("open_time") or ""
        )
        all_tickers.extend(ev_markets)

    remaining   = [t for t in all_tickers if t not in cp["completed_tickers"]]
    cp["total_markets"] = len(all_tickers)

    log.info("\n── Phase B: candlesticks ──────────────────────────────────")
    log.info("  %d markets across %d events | %d remaining | %d done",
             len(all_tickers), len(closed_events), len(remaining), len(cp["completed_tickers"]))

    candles_this_run = 0

    for i, ticker in enumerate(remaining, 1):
        if budget_exceeded():
            log.info("⏱  Budget reached after %d markets this run.", i - 1)
            save_checkpoint(cp, finished=False)
            _write_github_output("needs_rerun", "true")
            return

        total_done = len(cp["completed_tickers"])
        log.info("  [%d/%d] %s", total_done + 1, len(all_tickers), ticker)

        candles = fetch_candles(ticker, market_index[ticker], cutoff_unix)
        if candles:
            rows = build_candle_rows(ticker, candles)
            write_parquet(candle_key(ticker), rows)
            candles_this_run += len(candles)
        else:
            log.info("    (no candles)")

        cp["completed_tickers"].add(ticker)
        cp["last_ticker"] = ticker

        if len(cp["completed_tickers"]) % 25 == 0:
            save_checkpoint(cp, finished=False)

    # ── Done ─────────────────────────────────────────────────────────────────
    save_checkpoint(cp, finished=True)
    _write_github_output("needs_rerun", "false")

    log.info("\n══════════════════════════════════════════════════")
    log.info("  ✅  Complete!")
    log.info("  Events    : %d (total) / %d (fetched)", len(event_rows), len(closed_events))
    log.info("  Markets   : %d (fetched)", len(all_tickers))
    log.info("  Candles   : %d (this run)", candles_this_run)
    log.info("══════════════════════════════════════════════════")


if __name__ == "__main__":
    main()
