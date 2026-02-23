"""
kalshi_to_supabase.py
══════════════════════════════════════════════════════════════════════════════
Fetches ALL current + historical market data for the Kalshi series KXBTCD
and writes it into Supabase.  Designed to run inside GitHub Actions with a
self-retriggering workflow — it checkpoints progress to `kalshi_ingest_progress`
so each 6-hour run picks up exactly where the last one left off.

Flow per run
────────────
  Phase A (runs every time, very fast — just metadata):
    1. Upsert series info
    2. Fetch historical cutoff timestamp
    3. Upsert all events
    4. Upsert all markets (live + historical)

  Phase B (resumes across runs, one market at a time):
    5. For each market NOT yet in completed_tickers:
         fetch candlesticks → upsert → mark done
         stop gracefully if RUN_BUDGET_MINUTES is almost up

  On the next GitHub Actions run the workflow reads the `needs_rerun` output
  and triggers itself again automatically until finished=true.

Setup
─────
  pip install requests supabase python-dotenv

  Required GitHub Actions secrets:
    SUPABASE_URL          https://xxxx.supabase.co
    SUPABASE_KEY          service_role key (Project Settings → API)

  Optional secrets:
    KALSHI_API_KEY        not required for public KXBTCD data
    PERIOD_INTERVAL       1440 (daily) | 60 (hourly) | 1 (minute)
    RUN_BUDGET_MINUTES    default 320 (safely under the 360-min GHA limit)
══════════════════════════════════════════════════════════════════════════════
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

# ─────────────────────────────── Config ──────────────────────────────────────

load_dotenv()

SUPABASE_URL       = os.environ["SUPABASE_URL"]
SUPABASE_KEY       = os.environ["SUPABASE_KEY"]
KALSHI_API_KEY     = os.getenv("KALSHI_API_KEY", "")
PERIOD_INTERVAL    = int(os.getenv("PERIOD_INTERVAL", "1440"))
RUN_BUDGET_MINUTES = int(os.getenv("RUN_BUDGET_MINUTES", "320"))

KALSHI_BASE    = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKER  = "KXBTCD"
PAGE_LIMIT     = 200
REQUEST_DELAY  = 0.25    # seconds between Kalshi API calls
UPSERT_BATCH   = 500     # rows per Supabase upsert
SAFETY_SECS    = 10 * 60 # stop 10 min before budget expires

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

RUN_START = time.monotonic()


def budget_exceeded() -> bool:
    return time.monotonic() - RUN_START >= (RUN_BUDGET_MINUTES * 60 - SAFETY_SECS)


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
        data   = kalshi_get(path, params)
        batch  = data.get(list_key, [])
        results.extend(batch)
        cursor = data.get("cursor", "")
        log.info("  %s p%d → +%d (total %d)", path, page, len(batch), len(results))
        if not cursor or not batch:
            break
        params["cursor"] = cursor
        page += 1
    return results


# ─────────────────────────────── Tiny helpers ────────────────────────────────

def to_ts(v):
    return v if v else None

def to_num(v):
    try:    return float(v) if v else None
    except: return None

def unix_to_ts(u):
    """Accept a Unix int OR an ISO-8601 string; always return ISO-8601."""
    if u is None:
        return None
    if isinstance(u, str):
        return datetime.fromisoformat(u.replace("Z", "+00:00")).isoformat()
    return datetime.fromtimestamp(int(u), tz=timezone.utc).isoformat()


def to_unix_int(v) -> int:
    """Convert a Unix int or ISO-8601 string to a plain Unix integer."""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    try:
        return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0

def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────── Supabase helpers ────────────────────────────

def upsert(sb: Client, table: str, rows: list[dict], conflict_col: str):
    if not rows:
        return
    for i in range(0, len(rows), UPSERT_BATCH):
        sb.table(table).upsert(rows[i:i+UPSERT_BATCH], on_conflict=conflict_col).execute()
    log.info("  ✓ %d rows → %s", len(rows), table)


def upsert_candles(sb: Client, rows: list[dict]):
    if not rows:
        return
    for i in range(0, len(rows), UPSERT_BATCH):
        sb.table("kalshi_candlesticks").upsert(
            rows[i:i+UPSERT_BATCH],
            on_conflict="market_ticker,period_interval_min,end_period_ts",
        ).execute()
    log.info("    ✓ %d candle rows", len(rows))


# ─────────────────────────────── Checkpoint ──────────────────────────────────

PROGRESS_CONFLICT = "series_ticker,period_interval_min,phase"


def load_checkpoint(sb: Client) -> dict:
    res = (
        sb.table("kalshi_ingest_progress")
          .select("*")
          .eq("series_ticker",       SERIES_TICKER)
          .eq("period_interval_min", PERIOD_INTERVAL)
          .eq("phase",               "candlesticks")
          .limit(1)
          .execute()
    )
    if res.data:
        row = res.data[0]
        ct  = row.get("completed_tickers", [])
        row["completed_tickers"] = set(ct if isinstance(ct, list) else json.loads(ct))
        return row
    return {
        "series_ticker":       SERIES_TICKER,
        "period_interval_min": PERIOD_INTERVAL,
        "phase":               "candlesticks",
        "completed_tickers":   set(),
        "total_markets":       None,
        "last_ticker":         None,
        "run_count":           0,
        "finished":            False,
    }


def save_checkpoint(sb: Client, cp: dict, finished: bool = False):
    sb.table("kalshi_ingest_progress").upsert(
        {
            "series_ticker":       cp["series_ticker"],
            "period_interval_min": cp["period_interval_min"],
            "phase":               cp["phase"],
            "completed_tickers":   list(cp["completed_tickers"]),
            "total_markets":       cp.get("total_markets"),
            "last_ticker":         cp.get("last_ticker"),
            "last_run_at":         now_iso(),
            "run_count":           int(cp.get("run_count", 0)) + 1,
            "finished":            finished,
            "updated_at":          now_iso(),
        },
        on_conflict=PROGRESS_CONFLICT,
    ).execute()


# ─────────────────────────────── Row builders ────────────────────────────────

def build_series_row(s: dict) -> dict:
    return {
        "series_ticker":      s.get("ticker") or SERIES_TICKER,
        "title":              s.get("title"),
        "category":           s.get("category"),
        "tags":               s.get("tags") or [],
        "frequency":          s.get("frequency"),
        "settlement_sources": s.get("settlement_sources"),
        "updated_at":         now_iso(),
    }


def build_event_row(ev: dict) -> dict:
    markets  = ev.get("markets") or []
    statuses = {m.get("status") for m in markets if m.get("status")}
    status   = ("open"   if "open"   in statuses else
                "closed" if "closed" in statuses else
                "settled" if statuses else None)
    return {
        "event_ticker":           ev["event_ticker"],
        "series_ticker":          ev.get("series_ticker", SERIES_TICKER),
        "title":                  ev.get("title"),
        "sub_title":              ev.get("sub_title"),
        "category":               ev.get("category"),
        "strike_date":            to_ts(ev.get("strike_date")),
        "strike_period":          ev.get("strike_period"),
        "mutually_exclusive":     ev.get("mutually_exclusive"),
        "collateral_return_type": ev.get("collateral_return_type"),
        "available_on_brokers":   ev.get("available_on_brokers"),
        "product_metadata":       ev.get("product_metadata"),
        "status":                 status,
        "updated_at":             now_iso(),
    }


def build_market_row(m: dict) -> dict:
    return {
        "ticker":                     m["ticker"],
        "event_ticker":               m.get("event_ticker"),
        "series_ticker":              m.get("series_ticker", SERIES_TICKER),
        "market_type":                m.get("market_type"),
        "title":                      m.get("title"),
        "subtitle":                   m.get("subtitle"),
        "yes_sub_title":              m.get("yes_sub_title"),
        "no_sub_title":               m.get("no_sub_title"),
        "created_time":               to_ts(m.get("created_time")),
        "updated_time":               to_ts(m.get("updated_time")),
        "open_time":                  to_ts(m.get("open_time")),
        "close_time":                 to_ts(m.get("close_time")),
        "expiration_time":            to_ts(m.get("expiration_time")),
        "latest_expiration_time":     to_ts(m.get("latest_expiration_time")),
        "expected_expiration_time":   to_ts(m.get("expected_expiration_time")),
        "settlement_ts":              to_ts(m.get("settlement_ts")),
        "fee_waiver_expiration_time": to_ts(m.get("fee_waiver_expiration_time")),
        "settlement_timer_seconds":   m.get("settlement_timer_seconds"),
        "status":                     m.get("status"),
        "result":                     m.get("result"),
        "expiration_value":           m.get("expiration_value"),
        "settlement_value":           m.get("settlement_value"),
        "settlement_value_dollars":   to_num(m.get("settlement_value_dollars")),
        "is_provisional":             m.get("is_provisional"),
        "response_price_units":       m.get("response_price_units"),
        "yes_bid":                    m.get("yes_bid"),
        "yes_bid_dollars":            to_num(m.get("yes_bid_dollars")),
        "yes_ask":                    m.get("yes_ask"),
        "yes_ask_dollars":            to_num(m.get("yes_ask_dollars")),
        "no_bid":                     m.get("no_bid"),
        "no_bid_dollars":             to_num(m.get("no_bid_dollars")),
        "no_ask":                     m.get("no_ask"),
        "no_ask_dollars":             to_num(m.get("no_ask_dollars")),
        "last_price":                 m.get("last_price"),
        "last_price_dollars":         to_num(m.get("last_price_dollars")),
        "previous_yes_bid":           m.get("previous_yes_bid"),
        "previous_yes_bid_dollars":   to_num(m.get("previous_yes_bid_dollars")),
        "previous_yes_ask":           m.get("previous_yes_ask"),
        "previous_yes_ask_dollars":   to_num(m.get("previous_yes_ask_dollars")),
        "previous_price":             m.get("previous_price"),
        "previous_price_dollars":     to_num(m.get("previous_price_dollars")),
        "volume":          to_num(m.get("volume_fp"))        or m.get("volume"),
        "volume_24h":      to_num(m.get("volume_24h_fp"))   or m.get("volume_24h"),
        "open_interest":   to_num(m.get("open_interest_fp"))or m.get("open_interest"),
        "notional_value":             m.get("notional_value"),
        "notional_value_dollars":     to_num(m.get("notional_value_dollars")),
        "tick_size":                  m.get("tick_size"),
        "floor_strike":               m.get("floor_strike"),
        "cap_strike":                 m.get("cap_strike"),
        "strike_type":                m.get("strike_type"),
        "functional_strike":          m.get("functional_strike"),
        "custom_strike":              m.get("custom_strike"),
        "can_close_early":            m.get("can_close_early"),
        "early_close_condition":      m.get("early_close_condition"),
        "fractional_trading_enabled": m.get("fractional_trading_enabled"),
        "price_level_structure":      m.get("price_level_structure"),
        "price_ranges":               m.get("price_ranges"),
        "rules_primary":              m.get("rules_primary"),
        "rules_secondary":            m.get("rules_secondary"),
        "mve_collection_ticker":      m.get("mve_collection_ticker"),
        "primary_participant_key":    m.get("primary_participant_key"),
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
        def _d(blk, f):  return to_num(c.get(blk, {}).get(f))
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
            "price_open":          _i("price","open"),
            "price_low":           _i("price","low"),
            "price_high":          _i("price","high"),
            "price_close":         _i("price","close"),
            "price_mean":          _i("price","mean"),
            "price_previous":      _i("price","previous"),
            "price_min":           _i("price","min"),
            "price_max":           _i("price","max"),
            "price_open_d":        _d("price","open_dollars"),
            "price_low_d":         _d("price","low_dollars"),
            "price_high_d":        _d("price","high_dollars"),
            "price_close_d":       _d("price","close_dollars"),
            "price_mean_d":        _d("price","mean_dollars"),
            "price_previous_d":    _d("price","previous_dollars"),
            "price_min_d":         _d("price","min_dollars"),
            "price_max_d":         _d("price","max_dollars"),
            "volume":        to_num(c.get("volume_fp"))        or c.get("volume"),
            "open_interest": to_num(c.get("open_interest_fp")) or c.get("open_interest"),
        })
    return rows


def fetch_candles(ticker: str, market: dict, cutoff_unix: int) -> list:
    open_ts     = market.get("open_time")
    close_ts    = market.get("close_time") or market.get("expiration_time")
    settled_str = market.get("settlement_ts") or market.get("expiration_time")

    def _unix(ts_str):
        return to_unix_int(ts_str) or None

    start_unix   = _unix(open_ts)  or 0
    end_unix     = _unix(close_ts) or int(time.time())
    settled_unix = _unix(settled_str)

    use_hist = settled_unix is not None and settled_unix < cutoff_unix
    path = (f"/historical/markets/{ticker}/candlesticks" if use_hist
            else f"/series/{SERIES_TICKER}/markets/{ticker}/candlesticks")

    try:
        data = kalshi_get(path, {"start_ts": start_unix, "end_ts": end_unix,
                                  "period_interval": PERIOD_INTERVAL})
        return data.get("candlesticks", [])
    except Exception as exc:
        log.warning("  ⚠ candle fetch failed for %s: %s", ticker, exc)
        return []


def _write_github_output(key: str, value: str):
    gho = os.getenv("GITHUB_OUTPUT")
    if gho:
        with open(gho, "a") as f:
            f.write(f"{key}={value}\n")
    log.info("  → GitHub output: %s=%s", key, value)


# ─────────────────────────────── Main ────────────────────────────────────────

def main():
    log.info("══════════════════════════════════════════════════")
    log.info("  Kalshi → Supabase  |  Series : %s", SERIES_TICKER)
    log.info("  Candle interval   : %d min",         PERIOD_INTERVAL)
    log.info("  Run budget        : %d min",         RUN_BUDGET_MINUTES)
    log.info("══════════════════════════════════════════════════")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── Load checkpoint ─────────────────────────────────────────────────────
    cp = load_checkpoint(sb)
    log.info("Checkpoint: %d markets done, finished=%s",
             len(cp["completed_tickers"]), cp["finished"])

    if cp["finished"]:
        log.info("✅  Already complete — nothing to do.")
        _write_github_output("needs_rerun", "false")
        return

    # ── Phase A: metadata (fast, always runs) ───────────────────────────────
    log.info("\n── Phase A: series / events / markets ─────────────────────")

    try:
        series_raw = kalshi_get(f"/series/{SERIES_TICKER}").get("series", {})
    except Exception:
        series_raw = {"ticker": SERIES_TICKER}
    upsert(sb, "kalshi_series", [build_series_row(series_raw)], "series_ticker")

    cutoff_unix = to_unix_int(kalshi_get("/historical/cutoff").get("market_settled_ts"))
    log.info("  historical cutoff : %s", unix_to_ts(cutoff_unix))

    events = kalshi_paginate("/events", "events",
                             params={"series_ticker": SERIES_TICKER,
                                     "with_nested_markets": "true"})
    event_rows = [build_event_row(ev) for ev in events
                  if ev.get("series_ticker", SERIES_TICKER) == SERIES_TICKER]
    upsert(sb, "kalshi_events", event_rows, "event_ticker")

    hist_markets = kalshi_paginate("/historical/markets", "markets",
                                   params={"series_ticker": SERIES_TICKER})

    # Build unified market index
    market_index: dict[str, dict] = {}
    known_events = {r["event_ticker"] for r in event_rows}

    for ev in events:
        for m in (ev.get("markets") or []):
            m.setdefault("event_ticker",  ev["event_ticker"])
            m.setdefault("series_ticker", ev.get("series_ticker", SERIES_TICKER))
            market_index[m["ticker"]] = m

    stub_rows = []
    for m in hist_markets:
        # Only process markets that actually belong to this series.
        # The API filter should handle this, but defensively skip any market
        # whose series_ticker doesn't match (can leak in via nested data).
        m_series = m.get("series_ticker") or SERIES_TICKER
        if m_series != SERIES_TICKER:
            continue
        m.setdefault("series_ticker", SERIES_TICKER)
        ev_t = m.get("event_ticker", "")
        if ev_t and ev_t not in known_events:
            # Only create stub events for this series - never for foreign tickers
            stub_rows.append({"event_ticker": ev_t, "series_ticker": SERIES_TICKER,
                              "updated_at": now_iso()})
            known_events.add(ev_t)
        market_index.setdefault(m["ticker"], m)

    if stub_rows:
        upsert(sb, "kalshi_events", stub_rows, "event_ticker")
    upsert(sb, "kalshi_markets",
           [build_market_row(m) for m in market_index.values()], "ticker")

    # ── Phase B: candlesticks (resumes via checkpoint) ──────────────────────
    all_tickers = sorted(market_index.keys())
    remaining   = [t for t in all_tickers if t not in cp["completed_tickers"]]
    cp["total_markets"] = len(all_tickers)

    log.info("\n── Phase B: candlesticks ──────────────────────────────────")
    log.info("  %d total | %d remaining | %d done",
             len(all_tickers), len(remaining), len(cp["completed_tickers"]))

    candles_this_run = 0

    for i, ticker in enumerate(remaining, 1):
        if budget_exceeded():
            log.info("⏱  Budget reached after processing %d markets this run.", i - 1)
            save_checkpoint(sb, cp, finished=False)
            _write_github_output("needs_rerun", "true")
            return

        total_done = len(cp["completed_tickers"])
        log.info("  [%d/%d] %s", total_done + 1, len(all_tickers), ticker)

        candles = fetch_candles(ticker, market_index[ticker], cutoff_unix)
        if candles:
            upsert_candles(sb, build_candle_rows(ticker, candles))
            candles_this_run += len(candles)

        cp["completed_tickers"].add(ticker)
        cp["last_ticker"] = ticker

        # Persist checkpoint every 25 markets
        if len(cp["completed_tickers"]) % 25 == 0:
            save_checkpoint(sb, cp, finished=False)

    # ── All done ─────────────────────────────────────────────────────────────
    save_checkpoint(sb, cp, finished=True)
    _write_github_output("needs_rerun", "false")

    log.info("\n══════════════════════════════════════════════════")
    log.info("  ✅  Complete!")
    log.info("  Events          : %d", len(event_rows))
    log.info("  Markets         : %d", len(market_index))
    log.info("  Candles (run)   : %d", candles_this_run)
    log.info("══════════════════════════════════════════════════")


if __name__ == "__main__":
    main()
