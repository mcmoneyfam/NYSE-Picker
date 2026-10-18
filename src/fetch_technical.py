"""
Phase 1 — Technical scoring orchestrator.

Pulls OHLCV via yfinance per ticker, computes indicators (indicators.py)
and direction-aware scores (scoring.py), writes raw indicators to
`raw_metrics` and full results to `technical_scores`.

Speed optimizations (kept — these were working correctly):
1. Concurrent fetching (ThreadPoolExecutor, --workers, default 12).
   This is I/O-bound work (waiting on network responses), so threads
   overlap wait time instead of processing one ticker fully before
   starting the next. Indicator computation and scoring happen inside
   the same worker function, right after the fetch.
2. Batched DB writes (--commit-batch-size, default 50) instead of one
   commit per ticker.

Sector/market-cap filtering has been removed (previously in
fetch_sector_marketcap.py + filter flags here) — that enrichment step
repeatedly hit real, hard-to-resolve problems against yfinance's `.info`
endpoint (a Yahoo auth/crumb cascade failure, then a thread-safety freeze
in the fix for it) and was dropped rather than continuing to chase it.
This script now scores the full active, non-ETF universe with no sector/
market-cap filter — same as before that feature was added.

Per spec section 10: only the selected direction is computed — no
inverse/simultaneous scoring. This script accepts --direction and
--timeframe and computes exactly that combination.

Config (periods, thresholds, weights) is entirely in config.yaml —
nothing here is hard-coded, per spec section 10/11.

Requires a running MySQL instance with the schema already applied
(via fetch_universe.py) and a populated `universe` table.

Run this on a machine with normal internet access and a reachable
MySQL instance. The indicator math and scoring engine (indicators.py,
scoring.py) were live-tested with synthetic OHLCV data. The concurrency
and batching logic were live-tested with a mocked fetch function
(simulated network latency) against real MySQL — 87.4% wall-clock
reduction on 30 tickers with 12 workers. The actual yfinance fetch has
not been run end-to-end from this sandboxed dev environment.

Usage:
    python fetch_technical.py --direction LONG --timeframe DAILY --limit 20
    python fetch_technical.py --direction SHORT --timeframe WEEKLY --workers 15
"""

import argparse
import math
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import mysql.connector
import pandas as pd
import yaml
import yfinance as yf

from indicators import compute_all_raw_indicators
from scoring_technical import compute_final_score
from client_yfinance import (
    DEFAULT_WORKERS,
    COMMIT_BATCH_SIZE,
    get_shared_session,
    warm_session,
    throttle,
    call_with_retry,
    PhaseResult,
)

CONFIG_PATH = "config_technical.yaml"
LOOKBACK_MAP = {
    "DAILY": "2y",     # enough for 52-week (bar) lookback + warmup
    "WEEKLY": "5y",
    "MONTHLY": "10y",
}
TODAY = date.today().isoformat()
BATCH_DOWNLOAD_SIZE = 50  # reduced from 200 to avoid Yahoo rate limits; workers × 50 concurrent attempts per cycle is safer
RAW_METRIC_NAMES = ("rvol", "vol_ratio", "mfi", "macd_hist", "atr", "adx", "rsi", "pct_b", "high_52", "low_52", "range_pos")
# DEFAULT_WORKERS, COMMIT_BATCH_SIZE, and the fetch throttle now live in
# client_yfinance.py, shared with Phase 2 — see that module's docstring
# for why (running both phases with independent throttles would silently
# double the real request rate to Yahoo).


def normalize_yahoo_ticker(ticker):
    """Canonicalize Nasdaq/NYSE variants to Yahoo's accepted stock symbols.

    Rules are explicit and systematic:
    - common class-share variants become '-' separated: BRK.B -> BRK-B, C$R -> C-R
    - non-equity special securities like .U/.W/.R are rejected rather than treated as stocks
    - already-canonical Yahoo symbols are left alone
    """
    if ticker is None:
        return ticker
    value = str(ticker).strip().upper()
    if not value:
        return value
    if '-' in value:
        return value

    if re.search(r"(?:[$.][A-Z])$", value):
        sep = value[-2]
        suffix = value[-1]
        base = value[:-2]
        if sep == '$':
            return f"{base}-{suffix}"
        if sep == '.':
            allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ") - {"R", "S", "T", "U", "V", "W", "X", "Y", "Z"}
            if suffix in allowed:
                return f"{base}-{suffix}"
            return None

    return value


def is_probably_non_equity_ticker(ticker):
    """Return True for Nasdaq/NYSE securities that aren't common stocks.

    Common stock class-share variants are still valid and map to a Yahoo symbol.
    Unit/warrant/preferred-style securities are not.
    """
    if ticker is None:
        return False
    value = str(ticker).strip().upper()
    if not value:
        return False
    if normalize_yahoo_ticker(value) is None:
        return True
    return False


def canonicalize_yahoo_fetch_candidates(ticker, yahoo_symbol=None):
    """Return unique ticker strings to fetch, canonicalized to Yahoo form.

    This prevents accidental duplicate scans such as both JPM.L and JPM-L being
    requested in the same cycle, while still allowing the original ticker to be
    used as provenance when it is not already in canonical Yahoo form.
    """
    seen = set()
    candidates = []
    for raw_value in (yahoo_symbol, ticker):
        if raw_value is None:
            continue
        value = str(raw_value).strip().upper()
        if not value:
            continue
        normalized = normalize_yahoo_ticker(value)
        if normalized is None:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)
    return candidates


def load_config(path=CONFIG_PATH):
    with open(path) as f:
        return yaml.safe_load(f)


def get_connection(args):
    return mysql.connector.connect(
        host=args.host or os.environ.get("MYSQL_HOST", "localhost"),
        database=args.db or os.environ.get("MYSQL_DB", "screener"),
        user=args.user or os.environ.get("MYSQL_USER", "root"),
        password=args.password or os.environ.get("MYSQL_PASSWORD", ""),
        port=args.port or int(os.environ.get("MYSQL_PORT", "3306")),
    )


def get_ticker_alias_map(conn):
    cur = conn.cursor()
    cur.execute("SELECT nasdaq_ticker, yahoo_ticker FROM ticker_aliases WHERE is_active = TRUE")
    alias_map = {r[0]: r[1] for r in cur.fetchall()}
    cur.close()
    return alias_map


def get_tier1_tickers(conn, limit=None):
    alias_map = get_ticker_alias_map(conn)
    cur = conn.cursor()
    query = "SELECT ticker FROM universe WHERE is_active = TRUE AND is_etf = FALSE"
    if limit:
        query += f" LIMIT {int(limit)}"
    cur.execute(query)
    tickers = [r[0] for r in cur.fetchall()]
    cur.close()

    deduped = []
    seen = set()
    for ticker in tickers:
        if is_probably_non_equity_ticker(ticker):
            continue
        yahoo_symbol = alias_map.get(ticker, normalize_yahoo_ticker(ticker))
        if yahoo_symbol in seen:
            continue
        seen.add(yahoo_symbol)
        deduped.append((ticker, yahoo_symbol))
    return deduped


def fetch_yahoo_history(symbol, period):
    """Fetches one symbol's OHLCV history via the shared session, shared
    throttle, and shared auth-error retry (client_yfinance) — same
    module Phase 2 uses, so the two phases can't accidentally double
    Yahoo's effective request rate between them."""
    session = get_shared_session()

    def _do_fetch():
        t = yf.Ticker(symbol, session=session) if session is not None else yf.Ticker(symbol)
        return t.history(period=period)

    return call_with_retry(_do_fetch)


def fetch_yahoo_histories(symbols, period):
    cleaned = []
    seen = set()
    for symbol in symbols:
        if not symbol:
            continue
        symbol = str(symbol).strip()
        if symbol and symbol not in seen:
            seen.add(symbol)
            cleaned.append(symbol)
    if not cleaned:
        return {}

    # Try batch download once; if it fails for any reason, fall back to individual fetches
    data = None
    session = get_shared_session()
    throttle()
    try:
        data = yf.download(
            " ".join(cleaned),  # space-separated string, not list
            period=period,
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
            timeout=30,
            retries=2,
            session=session,
        )
    except Exception as e:
        # Fall back to individual fetches if batch fails
        pass

    results = {}
    if isinstance(data, pd.DataFrame) and not data.empty:
        if not isinstance(data.columns, pd.MultiIndex):
            for symbol in cleaned:
                results[symbol] = data
            return results
        for symbol in cleaned:
            try:
                hist = data[symbol]
            except Exception:
                hist = None
            if hist is not None and not hist.empty:
                results[symbol] = hist
            else:
                results[symbol] = None
        return results

    # Batch download failed or returned empty; fetch individually
    for symbol in cleaned:
        try:
            results[symbol] = fetch_yahoo_history(symbol, period)
        except Exception:
            results[symbol] = None
    return results


def score_symbol_from_history(ticker, yahoo_symbol, hist, direction, timeframe, cfg):
    tf_key = timeframe.lower()
    params = cfg["indicator_params"][tf_key]

    if hist is None or hist.empty:
        detail = "No OHLCV history returned for normalized Yahoo ticker"
        if is_probably_non_equity_ticker(ticker):
            detail = f"symbol is a Nasdaq unit/warrant/preferred issue, not a standard equity ({ticker})"
        return {"status": "flag", "flag_type": "missing", "detail": detail}

    if len(hist) < 60:
        return {"status": "flag", "flag_type": "missing", "detail": f"insufficient history ({len(hist)} rows)"}

    hist = hist.copy()
    hist.columns = [str(c).lower() for c in hist.columns]

    try:
        raw = compute_all_raw_indicators(hist, timeframe, params)
    except Exception as e:
        return {"status": "flag", "flag_type": "source_error", "detail": f"indicator computation failed: {e}"}

    if raw.empty or raw.iloc[-1].isna().all():
        return {"status": "flag", "flag_type": "missing", "detail": "insufficient bars after resampling/warmup"}

    last_row = raw.iloc[-1].to_dict()
    result = compute_final_score(
        last_row, direction, params, cfg["category_weights"],
        cfg["sub_weights"], cfg["scaling"], cfg["atr_multipliers"],
    )

    if result["final_score"] is None:
        return {"status": "flag", "flag_type": "partial", "detail": "score could not be computed (all category inputs NaN)"}

    raw_metrics = {name: last_row.get(name) for name in RAW_METRIC_NAMES}
    return {"status": "ok", "raw_metrics": raw_metrics, "score_result": result}


def fetch_and_score(ticker, yahoo_symbol, direction, timeframe, cfg):
    """Worker function: fetch OHLCV for a single ticker and score it."""
    yahoo_candidates = canonicalize_yahoo_fetch_candidates(ticker, yahoo_symbol)

    hist = None
    last_error = None
    for candidate in yahoo_candidates:
        try:
            hist = fetch_yahoo_history(candidate, LOOKBACK_MAP[timeframe])
        except Exception as e:
            last_error = e
            continue
        if hist is not None and not hist.empty:
            break

    return ticker, score_symbol_from_history(ticker, yahoo_symbol, hist, direction, timeframe, cfg)


def fetch_batch_scores(batch, direction, timeframe, cfg):
    symbols = [yahoo_symbol for _, yahoo_symbol in batch]
    histories = fetch_yahoo_histories(symbols, LOOKBACK_MAP[timeframe])
    results = []
    for ticker, yahoo_symbol in batch:
        hist = histories.get(yahoo_symbol)
        results.append((ticker, score_symbol_from_history(ticker, yahoo_symbol, hist, direction, timeframe, cfg)))
    return results


def write_results_batch(conn, batch, direction, timeframe, run_id):
    """Writes a batch of (ticker, result_dict) to raw_metrics,
    technical_scores, and data_flags in one commit."""
    cur = conn.cursor()
    for ticker, r in batch:
        if r["status"] == "flag":
            cur.execute(
                "INSERT INTO data_flags (ticker, domain, flag_type, detail) VALUES (%s, 'technical', %s, %s)",
                (ticker, r["flag_type"], r["detail"]),
            )
            continue

        for name, value in r["raw_metrics"].items():
            if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
                # NaN AND inf both need skipping here — not just NaN. A
                # raw indicator like RVOL/vol_ratio divides by an average
                # that can legitimately be 0 for an illiquid ticker,
                # producing inf. mysql-connector renders Python's
                # float('inf') as the bare, UNQUOTED token `inf` in the
                # SQL text, which MySQL then parses as a column
                # identifier rather than a value — "Unknown column
                # 'inf'". Treating it as missing (skip, same as NaN) is
                # correct anyway: an infinite ratio isn't a real score.
                continue
            cur.execute(
                """
                INSERT INTO raw_metrics (ticker, domain, metric_name, metric_value, source, as_of_date)
                VALUES (%s, 'technical', %s, %s, 'yfinance', %s)
                ON DUPLICATE KEY UPDATE metric_value = VALUES(metric_value), fetched_at = CURRENT_TIMESTAMP
                """,
                (ticker, f"{name}_{timeframe}", float(value), TODAY),
            )

        result = r["score_result"]
        cs = result["category_scores"]
        cur.execute(
            """
            INSERT INTO technical_scores
                (ticker, run_id, direction, timeframe,
                 final_score, overall_score_pre_atr,
                 volume_score, momentum_score, support_resistance_score, ob_os_score,
                 atr_multiplier_applied)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                final_score = VALUES(final_score),
                overall_score_pre_atr = VALUES(overall_score_pre_atr),
                volume_score = VALUES(volume_score),
                momentum_score = VALUES(momentum_score),
                support_resistance_score = VALUES(support_resistance_score),
                ob_os_score = VALUES(ob_os_score),
                atr_multiplier_applied = VALUES(atr_multiplier_applied),
                computed_at = CURRENT_TIMESTAMP
            """,
            (
                ticker, run_id, direction, timeframe,
                result["final_score"], result["overall_score_pre_atr"],
                cs["volume_score"], cs["momentum_score"],
                cs["support_resistance_score"], cs["ob_os_score"],
                result["atr_multiplier_applied"],
            ),
        )
    conn.commit()
    cur.close()


def build_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--direction", required=True, choices=["LONG", "SHORT"])
    parser.add_argument("--timeframe", required=True, choices=["DAILY", "WEEKLY", "MONTHLY"])
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--limit", type=int, default=None, help="Smoke-test against N tickers only")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Concurrent threads for Yahoo download batches")
    parser.add_argument("--commit-batch-size", type=int, default=COMMIT_BATCH_SIZE)
    parser.add_argument("--batch-download-size", type=int, default=BATCH_DOWNLOAD_SIZE, help="Ticker count per Yahoo batch download")
    parser.add_argument("--config", default=CONFIG_PATH)
    return parser


def run(args, warm=True):
    """Runs Phase 1 for an already-parsed args namespace and returns the
    run_id. Callable directly (e.g. by run_pipeline.py, which passes
    `warm=False` since it warms the shared session itself, once, before
    calling either phase) as well as from this script's own CLI main()."""
    if warm:
        print("Warming shared yfinance session (negotiating crumb once, before threads start)...")
        warm_session()

    cfg = load_config(args.config)
    run_id = args.run_id or f"technical-{args.direction}-{args.timeframe}-{date.today().isoformat()}-{uuid.uuid4().hex[:6]}"

    conn = get_connection(args)

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pipeline_runs (run_id, status, notes, started_at, completed_at)
        VALUES (%s, 'running', %s, CURRENT_TIMESTAMP, NULL)
        ON DUPLICATE KEY UPDATE
            status = 'running',
            notes = VALUES(notes),
            started_at = CURRENT_TIMESTAMP,
            completed_at = NULL
        """,
        (run_id, f"Phase 1 technical layer: {args.direction}/{args.timeframe}"),
    )
    conn.commit()
    cur.close()

    tickers = get_tier1_tickers(conn, limit=args.limit)
    print(f"Processing {len(tickers)} tickers with {args.workers} workers, direction={args.direction}, timeframe={args.timeframe} (run_id={run_id})...")

    start = time.time()
    batch = []
    succeeded, failed = 0, 0
    processed = 0

    def flush():
        nonlocal batch
        if batch:
            write_results_batch(conn, batch, args.direction, args.timeframe, run_id)
            batch = []

    chunk_size = max(1, args.batch_download_size)
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(fetch_batch_scores, tickers[i:i + chunk_size], args.direction, args.timeframe, cfg): i
            for i in range(0, len(tickers), chunk_size)
        }
        for future in as_completed(futures):
            for ticker, result in future.result():
                batch.append((ticker, result))
                processed += 1
                if result["status"] == "ok":
                    succeeded += 1
                else:
                    failed += 1
                if len(batch) >= args.commit_batch_size:
                    flush()
                if processed % 100 == 0:
                    elapsed = time.time() - start
                    rate = processed / elapsed
                    print(f"  {processed}/{len(tickers)} processed ({succeeded} ok, {failed} flagged) — {rate:.1f} tickers/sec")

    flush()

    cur = conn.cursor()
    cur.execute(
        "UPDATE pipeline_runs SET status='completed', completed_at=CURRENT_TIMESTAMP, universe_size=%s WHERE run_id=%s",
        (len(tickers), run_id),
    )
    conn.commit()
    cur.close()

    elapsed = time.time() - start
    print(f"Done in {elapsed:.1f}s. {succeeded} scored, {failed} flagged (see data_flags table). run_id={run_id}")
    conn.close()
    return PhaseResult(run_id=run_id, succeeded=succeeded, failed=failed, total=len(tickers))


def main():
    args = build_arg_parser().parse_args()
    run(args, warm=True)


if __name__ == "__main__":
    main()
