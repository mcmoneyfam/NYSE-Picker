"""
Phase 2 — Fundamental scoring orchestrator.

Data source order (direct instruction, overriding the spec draft's
yfinance-first ordering): SEC EDGAR primary, yfinance fallback. See
client_edgar.py's docstring for the honest caveat on which fields EDGAR
structurally cannot supply (price-dependent ratios, short interest) —
those always come from yfinance, not as a "fallback" but as a fixed
source boundary.

Direction linkage (direct instruction): this script does NOT take a
free-standing --direction flag for normal use. It takes
--technical-run-id, looks up the direction that Phase 1 run already used
(from `technical_scores.direction` for that run_id), and reuses it — so
if the user already ran Phase 1 as LONG, Phase 2 automatically scores
LONG too, without asking again. A direct --override-direction flag
exists ONLY for testing (deliberately scoring the opposite direction to
compare, or running Phase 2 standalone before any Phase 1 run exists);
using it always prints a loud warning so a real run can't drift out of
sync with its technical counterpart by accident.

Requires a running MySQL instance with schema.sql applied (includes the
Phase 2 addition: `fundamental_scores` table).

Caching: per-ticker fundamental fields are cached locally as JSON
(fundamental_cache/<ticker>.json) with a configurable TTL (default 24h,
config_fundamental.yaml: cache_ttl_hours) to avoid redundant EDGAR/
yfinance calls across repeated runs, per spec section 3/9.

Usage:
    # normal use — direction comes from the named Phase 1 run
    python fetch_fundamental.py --technical-run-id technical-LONG-DAILY-2026-08-17-a1b2c3

    # testing only — scores SHORT regardless of what that run used
    python fetch_fundamental.py --technical-run-id technical-LONG-DAILY-2026-08-17-a1b2c3 \\
        --override-direction SHORT --i-understand-this-is-for-testing-only
"""

import argparse
import json
import math
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import mysql.connector
import yaml

from client_edgar import fetch_ticker_cik_map, fetch_company_facts, extract_edgar_fields
from data_fundamental import assemble_from_facts, CACHE_DIR, TICKER_MAP_CACHE
from scoring_fundamental import compute_fundamental_score
from client_yfinance import DEFAULT_WORKERS, COMMIT_BATCH_SIZE, warm_session, PhaseResult

CONFIG_PATH = "config_fundamental.yaml"
TODAY = date.today().isoformat()
# DEFAULT_WORKERS/COMMIT_BATCH_SIZE now come from client_yfinance, shared
# with Phase 1, per direct instruction to keep both phases uniform. (EDGAR
# calls have their own separate, unaffected rate limit in client_edgar.py
# — a different host, so it doesn't need to share Yahoo's throttle.)


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


def resolve_direction(conn, technical_run_id):
    """Looks up the direction a given Phase 1 run already used. Raises
    if the run_id has no technical_scores rows (typo, or Phase 1 hasn't
    been run yet) rather than silently defaulting — a wrong silent
    default here would desync Phase 1/Phase 2 without anyone noticing."""
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT direction FROM technical_scores WHERE run_id = %s", (technical_run_id,)
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        raise ValueError(
            f"No technical_scores rows found for run_id={technical_run_id!r}. "
            "Run fetch_technical.py first, or check the run_id."
        )
    directions = {r[0] for r in rows}
    if len(directions) > 1:
        raise ValueError(
            f"run_id={technical_run_id!r} has mixed directions {directions} — "
            "expected exactly one. This shouldn't happen for a normal Phase 1 run."
        )
    return directions.pop()


def get_ticker_cik_map(refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    if not refresh and os.path.exists(TICKER_MAP_CACHE):
        with open(TICKER_MAP_CACHE) as f:
            return json.load(f)
    mapping = fetch_ticker_cik_map()
    with open(TICKER_MAP_CACHE, "w") as f:
        json.dump(mapping, f)
    return mapping


def get_scored_tickers(conn, technical_run_id, limit=None):
    """Scores the same ticker set Phase 1 already scored for this
    run_id, rather than the full universe — Phase 2 rides on Phase 1's
    already-established scope for a given run."""
    cur = conn.cursor()
    query = "SELECT ticker FROM technical_scores WHERE run_id = %s"
    if limit:
        query += f" LIMIT {int(limit)}"
    cur.execute(query, (technical_run_id,))
    tickers = [r[0] for r in cur.fetchall()]
    cur.close()
    return tickers


def score_one_ticker(ticker, cik_map, direction, cfg):
    cik = cik_map.get(ticker)
    edgar_raw = None
    if cik is not None:
        try:
            facts = fetch_company_facts(cik)
            if facts is not None:
                edgar_raw = extract_edgar_fields(facts)
        except Exception as e:
            edgar_raw = None  # falls through to all-yfinance combine; logged via field_sources

    try:
        fields, sources, conflicts = assemble_from_facts(ticker, edgar_raw, ttl_hours=cfg.get("cache_ttl_hours", 24))
    except Exception as e:
        return {"status": "flag", "flag_type": "source_error", "detail": f"data assembly failed: {e}"}

    try:
        result = compute_fundamental_score(fields, direction, cfg, cfg.get("missing_data_policy", "zero"))
    except Exception as e:
        return {"status": "flag", "flag_type": "source_error", "detail": f"scoring failed: {e}"}

    if result["fundamental_score"] is None:
        return {"status": "flag", "flag_type": "partial", "detail": "score could not be computed"}

    return {"status": "ok", "fields": fields, "field_sources": sources, "conflicts": conflicts, "result": result}


def write_results_batch(conn, batch, direction, run_id, technical_run_id):
    cur = conn.cursor()
    for ticker, r in batch:
        if r["status"] == "flag":
            cur.execute(
                "INSERT INTO data_flags (ticker, domain, flag_type, detail) VALUES (%s, 'fundamental', %s, %s)",
                (ticker, r["flag_type"], r["detail"]),
            )
            continue

        for name, value in r["fields"].items():
            if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
                # See fetch_technical.py's write_results_batch for why
                # inf must be skipped alongside NaN, not just NaN alone —
                # mysql-connector renders float('inf') as an unquoted
                # bare `inf` token, which MySQL misparses as a column
                # identifier ("Unknown column 'inf'") rather than a value.
                continue
            cur.execute(
                """
                INSERT INTO raw_metrics (ticker, domain, metric_name, metric_value, source, as_of_date)
                VALUES (%s, 'fundamental', %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE metric_value = VALUES(metric_value), fetched_at = CURRENT_TIMESTAMP
                """,
                (ticker, name, float(value), r["field_sources"].get(name, "unknown"), TODAY),
            )

        res = r["result"]
        s = res["indicator_scores"]
        field_sources_with_conflicts = dict(r["field_sources"])
        if r.get("conflicts"):
            field_sources_with_conflicts["_conflicts"] = r["conflicts"]
            for c in r["conflicts"]:
                cur.execute(
                    "INSERT INTO data_flags (ticker, domain, flag_type, detail) VALUES (%s, 'fundamental', 'partial', %s)",
                    (ticker, f"EDGAR/yfinance conflict on {c['field']}: edgar={c['edgar']}, yfinance={c['yfinance']} ({c['pct_diff']}% diff)"),
                )
        cur.execute(
            """
            INSERT INTO fundamental_scores
                (ticker, run_id, technical_run_id, direction, fundamental_score,
                 pe_ratio_score, ev_ebitda_score, fcf_yield_score, roe_score,
                 gross_margin_score, ocf_ni_score, revenue_growth_score,
                 net_debt_ebitda_score, short_percent_float_score, buyback_yield_score,
                 roa_score, quality_gate_applied, field_sources)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                fundamental_score = VALUES(fundamental_score),
                pe_ratio_score = VALUES(pe_ratio_score),
                ev_ebitda_score = VALUES(ev_ebitda_score),
                fcf_yield_score = VALUES(fcf_yield_score),
                roe_score = VALUES(roe_score),
                gross_margin_score = VALUES(gross_margin_score),
                ocf_ni_score = VALUES(ocf_ni_score),
                revenue_growth_score = VALUES(revenue_growth_score),
                net_debt_ebitda_score = VALUES(net_debt_ebitda_score),
                short_percent_float_score = VALUES(short_percent_float_score),
                buyback_yield_score = VALUES(buyback_yield_score),
                roa_score = VALUES(roa_score),
                quality_gate_applied = VALUES(quality_gate_applied),
                field_sources = VALUES(field_sources),
                computed_at = CURRENT_TIMESTAMP
            """,
            (
                ticker, run_id, technical_run_id, direction, res["fundamental_score"],
                s["pe_ratio"], s["ev_ebitda"], s["fcf_yield"], s["roe"],
                s["gross_margin"], s["ocf_ni"], s["revenue_growth_yoy"],
                s["net_debt_ebitda"], s["short_percent_float"], s["buyback_yield"],
                s["roa"], res["quality_gate_applied"], json.dumps(field_sources_with_conflicts),
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
    parser.add_argument("--technical-run-id", required=True, help="Phase 1 run_id to link direction + ticker scope from")
    parser.add_argument(
        "--override-direction", choices=["LONG", "SHORT"], default=None,
        help="TESTING ONLY — score a direction other than the linked technical run's. Requires --i-understand-this-is-for-testing-only.",
    )
    parser.add_argument("--i-understand-this-is-for-testing-only", action="store_true")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--commit-batch-size", type=int, default=COMMIT_BATCH_SIZE)
    parser.add_argument("--config", default=CONFIG_PATH)
    parser.add_argument("--refresh-cik-map", action="store_true")
    return parser


def run(args, warm=True):
    """Runs Phase 2 for an already-parsed args namespace and returns the
    run_id. Callable directly (e.g. by run_pipeline.py, which passes
    `warm=False` since it warms the shared session itself, once, before
    calling either phase) as well as from this script's own CLI main()."""
    if args.override_direction and not args.i_understand_this_is_for_testing_only:
        raise SystemExit(
            "--override-direction requires --i-understand-this-is-for-testing-only. "
            "Normal runs should not override the direction linked from Phase 1 — "
            "see this script's module docstring."
        )

    cfg = load_config(args.config)
    conn = get_connection(args)

    linked_direction = resolve_direction(conn, args.technical_run_id)
    if args.override_direction:
        direction = args.override_direction
        print(
            f"*** WARNING: --override-direction is active. Phase 1 run "
            f"{args.technical_run_id!r} used {linked_direction}, but this Phase 2 "
            f"run is scoring {direction} instead. This is a testing-only path — "
            f"the resulting fundamental_scores row will NOT match its linked "
            f"technical run's direction. ***"
        )
    else:
        direction = linked_direction
        print(f"Direction auto-linked from technical run {args.technical_run_id!r}: {direction}")

    run_id = args.run_id or f"fundamental-{direction}-{date.today().isoformat()}-{uuid.uuid4().hex[:6]}"

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pipeline_runs (run_id, status, notes, started_at, completed_at)
        VALUES (%s, 'running', %s, CURRENT_TIMESTAMP, NULL)
        ON DUPLICATE KEY UPDATE status='running', notes=VALUES(notes), started_at=CURRENT_TIMESTAMP, completed_at=NULL
        """,
        (run_id, f"Phase 2 fundamental layer: {direction}, linked to {args.technical_run_id}"),
    )
    conn.commit()
    cur.close()

    print("Loading ticker->CIK map...")
    cik_map = get_ticker_cik_map(refresh=args.refresh_cik_map)

    tickers = get_scored_tickers(conn, args.technical_run_id, limit=args.limit)
    print(f"Scoring {len(tickers)} tickers with {args.workers} workers, direction={direction} (run_id={run_id})...")

    start = time.time()
    batch = []
    succeeded, failed = 0, 0
    processed = 0

    def flush():
        nonlocal batch
        if batch:
            write_results_batch(conn, batch, direction, run_id, args.technical_run_id)
            batch = []

    if warm:
        print("Warming shared yfinance session (negotiating crumb once, before threads start)...")
        warm_session()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(score_one_ticker, t, cik_map, direction, cfg): t for t in tickers}
        for future in as_completed(futures):
            ticker = futures[future]
            result = future.result()
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
                print(f"  {processed}/{len(tickers)} processed ({succeeded} ok, {failed} flagged) — {processed/elapsed:.1f} tickers/sec")

    flush()

    cur = conn.cursor()
    cur.execute(
        "UPDATE pipeline_runs SET status='completed', completed_at=CURRENT_TIMESTAMP, universe_size=%s WHERE run_id=%s",
        (len(tickers), run_id),
    )
    conn.commit()
    cur.close()

    elapsed = time.time() - start
    print(f"Done in {elapsed:.1f}s. {succeeded} scored, {failed} flagged. run_id={run_id}, direction={direction}")
    conn.close()
    return PhaseResult(run_id=run_id, succeeded=succeeded, failed=failed, total=len(tickers))


def main():
    args = build_arg_parser().parse_args()
    run(args, warm=True)


if __name__ == "__main__":
    main()
