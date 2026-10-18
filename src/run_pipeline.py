"""
Pipeline orchestrator — runs Phase 1 (technical) then Phase 2
(fundamental) sequentially, in one process, sharing one warmed session
and one rate limiter (see client_yfinance.py), with uniform
--workers/--commit-batch-size applied to both phases.

--- CHANGE LOG (fixing the 2026-08-22 pipeline run) ---

The production run's Phase 2 hit a sustained ~5000-ticker 401 storm
that turned out to be a wall-clock-timed Yahoo throttle, not a
worker-count-driven race (see client_yfinance.py's docstring for the
full diagnosis). The auto-retry here made it worse in two ways that
are now fixed:

1. It reused the existing session (`warm=False`, "session already
   warmed; retry doesn't need to re-negotiate the crumb") — but the
   whole reason the retry was needed is that the crumb got poisoned.
   Reusing it just walked into the same block again. The retry now
   calls client_yfinance.reset_session() and re-warms with a genuinely
   fresh crumb.

2. It retried IMMEDIATELY. Since the underlying block is time-based,
   retrying instantly just repeats the same ~13-minute failure window.
   The retry now sleeps for RETRY_COOLDOWN_SECONDS first, so the retry
   attempt has a real chance of landing after Yahoo's own throttle has
   cleared, on top of client_yfinance.py's own in-run circuit breaker
   (which now also pauses+refreshes the crumb mid-phase instead of
   waiting for the whole phase to finish before anyone reacts).
"""

import argparse
import time
import uuid
from datetime import date

import fetch_technical
import fetch_fundamental
from client_yfinance import DEFAULT_WORKERS, COMMIT_BATCH_SIZE, warm_session, reset_session

# --- Auto-retry tuning — change these to adjust behavior everywhere ---
FAILURE_RATE_THRESHOLD = 0.15   # >15% of tickers flagged/failed triggers a retry
WORKER_REDUCTION_STEP = 2       # e.g. 10 workers -> 8 on retry
MIN_WORKERS = 3                 # never reduce below this
MAX_RETRIES_PER_PHASE = 1       # per direct instruction: exactly one auto-retry, then stop
RETRY_COOLDOWN_SECONDS = 180    # wait this long before retrying a failed phase — a time-based
                                 # Yahoo throttle won't be fixed by retrying instantly; see above


def build_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--user", default=None)
    parser.add_argument("--password", default=None)
    parser.add_argument("--port", type=int, default=None)

    parser.add_argument("--direction", required=True, choices=["LONG", "SHORT"])
    parser.add_argument("--timeframe", required=True, choices=["DAILY", "WEEKLY", "MONTHLY"])
    parser.add_argument("--limit", type=int, default=None, help="Smoke-test against N tickers only (applies to both phases)")

    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--commit-batch-size", type=int, default=COMMIT_BATCH_SIZE)

    parser.add_argument("--technical-run-id", default=None, help="Override the auto-generated Phase 1 run_id")
    parser.add_argument("--fundamental-run-id", default=None, help="Override the auto-generated Phase 2 run_id")
    parser.add_argument("--technical-config", default="config_technical.yaml")
    parser.add_argument("--fundamental-config", default="config_fundamental.yaml")
    parser.add_argument("--batch-download-size", type=int, default=fetch_technical.BATCH_DOWNLOAD_SIZE)
    parser.add_argument("--refresh-cik-map", action="store_true")

    parser.add_argument(
        "--override-fundamental-direction", choices=["LONG", "SHORT"], default=None,
        help="TESTING ONLY — see fetch_fundamental.py's docstring.",
    )
    parser.add_argument("--i-understand-this-is-for-testing-only", action="store_true")

    parser.add_argument("--failure-threshold", type=float, default=FAILURE_RATE_THRESHOLD,
                         help="Fraction of flagged tickers that triggers an auto-retry with reduced workers")
    parser.add_argument("--worker-reduction-step", type=int, default=WORKER_REDUCTION_STEP)
    parser.add_argument("--min-workers", type=int, default=MIN_WORKERS)
    parser.add_argument("--retry-cooldown-seconds", type=int, default=RETRY_COOLDOWN_SECONDS,
                         help="Seconds to wait, and re-negotiate a fresh session, before an auto-retry")

    return parser


def _technical_args_namespace(args, run_id, workers):
    return argparse.Namespace(
        host=args.host, db=args.db, user=args.user, password=args.password, port=args.port,
        direction=args.direction, timeframe=args.timeframe,
        run_id=run_id, limit=args.limit,
        workers=workers, commit_batch_size=args.commit_batch_size,
        batch_download_size=args.batch_download_size, config=args.technical_config,
    )


def _fundamental_args_namespace(args, technical_run_id, run_id, workers):
    return argparse.Namespace(
        host=args.host, db=args.db, user=args.user, password=args.password, port=args.port,
        technical_run_id=technical_run_id,
        override_direction=args.override_fundamental_direction,
        i_understand_this_is_for_testing_only=args.i_understand_this_is_for_testing_only,
        run_id=run_id, limit=args.limit,
        workers=workers, commit_batch_size=args.commit_batch_size,
        config=args.fundamental_config, refresh_cik_map=args.refresh_cik_map,
    )


def run_phase_with_retry(phase_label, run_callable, build_args, workers, args, warm_first=False):
    """Runs one phase; if it crashes OR its failure rate exceeds
    args.failure_threshold, retries it exactly once with
    args.worker_reduction_step fewer workers (floored at
    args.min_workers), reusing the same run_id. Before that retry, it
    now waits args.retry_cooldown_seconds AND forces a fresh
    session/crumb (reset_session() + warm_session()) rather than
    reusing the one that just got throttled — see this file's docstring
    for why the old "retry immediately, reuse the session" behavior
    made a time-based Yahoo throttle worse instead of better."""
    current_workers = workers
    attempt = 0
    warm = warm_first

    while True:
        phase_args = build_args(current_workers)
        run_id = phase_args.run_id
        print(f"\n--- {phase_label} (attempt {attempt + 1}/{MAX_RETRIES_PER_PHASE + 1}, "
              f"workers={current_workers}, run_id={run_id}) ---")

        if warm:
            warm_session()

        result = None
        error = None
        try:
            result = run_callable(phase_args, warm=False)  # warm-up (if any) already done above
        except Exception as e:
            error = e
            print(f"!!! {phase_label} crashed on attempt {attempt + 1}: {error}")

        if result is not None:
            failure_rate = (result.failed / result.total) if result.total else 0.0
            print(f"{phase_label} result: {result.succeeded} succeeded, {result.failed} flagged, "
                  f"{result.total} total ({failure_rate * 100:.1f}% failure rate)")
            if failure_rate <= args.failure_threshold:
                return result
            print(f"!!! {phase_label} failure rate {failure_rate * 100:.1f}% exceeds "
                  f"threshold {args.failure_threshold * 100:.0f}%")

        if attempt >= MAX_RETRIES_PER_PHASE:
            reason = f"crashed: {error}" if error is not None else "failure rate exceeded threshold"
            raise RuntimeError(
                f"{phase_label} failed after {attempt + 1} attempt(s) ({reason}). "
                f"Auto-retry budget (1) exhausted — stopping for manual investigation. "
                f"Check data_flags for run_id={run_id} to see what's failing, and consider "
                f"whether this is a genuine data issue vs. still a rate-limit/session problem."
            )

        attempt += 1
        current_workers = max(args.min_workers, current_workers - args.worker_reduction_step)
        print(f"Auto-retrying {phase_label} once with reduced workers: {current_workers} "
              f"(reused run_id={run_id}). Cooling down {args.retry_cooldown_seconds}s and "
              f"re-negotiating a fresh session/crumb first — a high failure rate here is "
              f"usually a Yahoo-side throttle, not something an instant retry fixes...")
        reset_session()
        time.sleep(args.retry_cooldown_seconds)
        warm = True  # force a genuinely fresh crumb, not the one that just got throttled


def main():
    args = build_arg_parser().parse_args()

    technical_run_id = args.technical_run_id or (
        f"technical-{args.direction}-{args.timeframe}-{date.today().isoformat()}-{uuid.uuid4().hex[:6]}"
    )
    fundamental_run_id = args.fundamental_run_id or (
        f"fundamental-{args.direction}-{date.today().isoformat()}-{uuid.uuid4().hex[:6]}"
    )

    print(f"=== Pipeline start: direction={args.direction}, timeframe={args.timeframe}, "
          f"workers={args.workers} (uniform across both phases), "
          f"commit_batch_size={args.commit_batch_size}, "
          f"failure_threshold={args.failure_threshold * 100:.0f}% ===")
    start = time.time()

    print("Warming shared yfinance session (negotiating crumb once, before either phase starts)...")
    warm_session()

    technical_result = run_phase_with_retry(
        "Phase 1: technical",
        fetch_technical.run,
        lambda workers: _technical_args_namespace(args, technical_run_id, workers),
        args.workers,
        args,
        warm_first=False,  # already warmed above
    )

    fundamental_result = run_phase_with_retry(
        "Phase 2: fundamental",
        fetch_fundamental.run,
        lambda workers: _fundamental_args_namespace(args, technical_result.run_id, fundamental_run_id, workers),
        args.workers,
        args,
        # Phase 2 leans hard on the crumb-sensitive .info endpoint right after
        # Phase 1 has already been making yfinance calls for a while — give it
        # its own fresh crumb rather than trusting Phase 1's still-warm one.
        warm_first=True,
    )

    elapsed = time.time() - start
    print(f"\n=== Pipeline done in {elapsed:.1f}s. "
          f"technical_run_id={technical_result.run_id} ({technical_result.succeeded}/{technical_result.total} ok), "
          f"fundamental_run_id={fundamental_result.run_id} ({fundamental_result.succeeded}/{fundamental_result.total} ok) ===")
    print(f"\nView results with:")
    print(f"  python show_results.py --technical-run-id {technical_result.run_id}")
    print(f"  python data_quality_report.py --run-id {fundamental_result.run_id}")


if __name__ == "__main__":
    main()
