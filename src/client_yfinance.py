"""
Shared yfinance access layer — session, rate limiter, retry logic, and
now a circuit breaker, used by BOTH fetch_technical.py (Phase 1) and
data_fundamental.py (Phase 2).

--- CHANGE LOG (fixing the 2026-08-22 pipeline run) ---

The production run showed a ~5000-ticker-long storm of HTTP 401
"Invalid Crumb" errors in Phase 2, on BOTH the first attempt (8
workers) and the auto-retried attempt (6 workers) — and in both cases
the storm cleared at almost the same *wall-clock time* (~750-800s in),
not the same ticker count. Worker count barely moved the clearing
point. That is the signature of a Yahoo-side temporary IP-level
throttle that has to cool down on its own — NOT the threaded
crumb-negotiation race this module was originally built to fix. Three
things follow from that:

1. `call_with_retry`'s per-call backoff (max 4 attempts, ~0.4-3.6s)
   is much too short to ride out a cooldown that lasts several
   hundred seconds. Every one of those retries just adds to the 401
   count and gets flagged as a failure.

2. The single shared session/crumb, once Yahoo starts blocking it,
   never recovers on its own — call_with_retry keeps reusing the same
   poisoned crumb. There was no mechanism to detect "this isn't a
   one-off, we're in a storm" and actually get a NEW crumb.

3. run_pipeline.py's phase-level auto-retry reused the same session
   (`warm=False`) and retried immediately, so it walked into the exact
   same wall-clock-timed block again.

Fix: a shared circuit breaker. Every auth-style failure increments a
counter; every success resets it. If CONSECUTIVE_AUTH_FAILURE_LIMIT
consecutive auth failures happen across ALL callers (any phase, any
worker), every thread blocks on a cooldown (CIRCUIT_COOLDOWN_SECONDS),
then ONE thread negotiates a brand new session (new curl_cffi session
-> new cookies -> new crumb) before anyone proceeds. This turns a
~13-minute 90%-failure storm into a handful of flagged tickers plus one
paid cooldown pause, instead of silently flagging thousands.

Also: .info (quoteSummary) is the most crumb-sensitive yfinance
endpoint and the one Phase 2 leans on hardest — it gets its own,
slightly slower throttle interval than lighter calls, since it appears
to be what actually trips Yahoo's throttle. See INFO_FETCH_INTERVAL_SECONDS.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass

import yfinance as yf

DEFAULT_WORKERS = 8
COMMIT_BATCH_SIZE = 50


@dataclass(frozen=True)
class PhaseResult:
    run_id: str
    succeeded: int
    failed: int
    total: int


# --- Throttling ---
#
# Two separate intervals: a faster one for lighter calls (history/chart,
# used by Phase 1), and a slower one specifically for `.info`
# (quoteSummary, used by Phase 2), which is the endpoint that actually
# seems to trip Yahoo's throttle. Both still share ONE lock/timestamp so
# the true combined request rate to Yahoo is still bounded regardless of
# which phase(s) are calling.
FETCH_INTERVAL_SECONDS = 0.15          # general / Phase 1 chart calls
INFO_FETCH_INTERVAL_SECONDS = 0.35     # .info / quoteSummary calls (Phase 2)

_throttle_lock = threading.Lock()
_last_fetch_ts = {"ts": 0.0}


def throttle(interval: float = FETCH_INTERVAL_SECONDS):
    """Blocks the calling thread until it's safe to make the next
    yfinance request, across ALL callers sharing this module."""
    while True:
        with _throttle_lock:
            now = time.monotonic()
            wait_until = _last_fetch_ts["ts"] + interval
            if now >= wait_until:
                _last_fetch_ts["ts"] = now
                return
            sleep_for = wait_until - now
        time.sleep(sleep_for)


# --- Circuit breaker for auth-error storms ---
#
# A handful of transient 401s is normal and handled by call_with_retry's
# per-call backoff. A SUSTAINED run of them (hundreds in a row) means
# Yahoo has throttled/blocked the current session/IP and no amount of
# per-call retrying will fix it until either (a) time passes, or (b) a
# fresh crumb is negotiated. This breaker detects that pattern and pays
# the cooldown cost ONCE, centrally, instead of every worker thread
# independently retrying into the wall.
CONSECUTIVE_AUTH_FAILURE_LIMIT = 25   # trip the breaker after this many auth fails in a row (any thread)
CIRCUIT_COOLDOWN_SECONDS = 90         # wait this long before trying to re-negotiate
CIRCUIT_MAX_TRIPS = 6                 # give up asking for cooldowns after this many trips in one run (~9-13 min of pausing) so a genuinely broken run still stops instead of hanging forever

_circuit_lock = threading.Lock()
_circuit_state = {"consecutive_auth_failures": 0, "trips": 0, "tripped_until": 0.0}


def _circuit_wait_if_tripped():
    """If another thread already tripped the breaker, everyone waits
    for the cooldown instead of hammering more requests during it."""
    while True:
        with _circuit_lock:
            wait_left = _circuit_state["tripped_until"] - time.monotonic()
            if wait_left <= 0:
                return
        time.sleep(min(wait_left, 5))


def _record_auth_failure():
    """Called by call_with_retry on every exhausted auth-style failure.
    Returns True if this call just tripped the breaker (so the caller
    can log it once instead of every thread logging it)."""
    with _circuit_lock:
        _circuit_state["consecutive_auth_failures"] += 1
        if _circuit_state["consecutive_auth_failures"] < CONSECUTIVE_AUTH_FAILURE_LIMIT:
            return False
        if _circuit_state["trips"] >= CIRCUIT_MAX_TRIPS:
            # Stop paying cooldowns indefinitely — something deeper is
            # wrong (e.g. genuine IP ban); let failures surface normally
            # so the phase's own failure-rate check can stop the run.
            return False
        _circuit_state["trips"] += 1
        _circuit_state["consecutive_auth_failures"] = 0
        _circuit_state["tripped_until"] = time.monotonic() + CIRCUIT_COOLDOWN_SECONDS
        return True


def _record_auth_success():
    with _circuit_lock:
        _circuit_state["consecutive_auth_failures"] = 0


def circuit_status() -> dict:
    with _circuit_lock:
        return dict(_circuit_state)


# --- Shared session (fixes "Invalid Crumb" storms under threading) ---
_session_lock = threading.Lock()
_shared_session = None


def get_shared_session():
    global _shared_session
    if _shared_session is not None:
        return _shared_session
    with _session_lock:
        if _shared_session is None:
            try:
                from curl_cffi import requests as curl_requests
                _shared_session = curl_requests.Session(impersonate="chrome124")
            except ImportError:
                _shared_session = None
    return _shared_session


def reset_session():
    """Forces a brand new session on the next get_shared_session() call
    — used when the circuit breaker trips, since the whole point of a
    trip is that the CURRENT session's crumb is poisoned and reusing it
    (the old warm_session()-once-and-never-again behavior) just repeats
    the failure. Also usable standalone before a phase-level auto-retry
    in run_pipeline.py, instead of trusting the old session is still
    good."""
    global _shared_session
    with _session_lock:
        _shared_session = None


def warm_session():
    """Negotiates the crumb/cookie once, synchronously. Safe to call
    again after reset_session() — it will renegotiate from scratch."""
    session = get_shared_session()
    throttle()
    try:
        yf.Ticker("AAPL", session=session).info  # noqa: B018 — deliberate warm-up call
        _record_auth_success()
        return True
    except Exception as e:
        print(f"  warning: session warm-up call failed ({e}); proceeding anyway, "
              f"first few requests may retry")
        return False


def yf_ticker(symbol):
    session = get_shared_session()
    return yf.Ticker(symbol, session=session) if session is not None else yf.Ticker(symbol)


def call_with_retry(fn, *, max_attempts=4, is_auth_error=None, interval=FETCH_INTERVAL_SECONDS):
    """Runs fn() with throttling + per-call retry-with-backoff for
    auth-style failures, PLUS the shared circuit breaker: if this call
    (after its own retries are exhausted) is part of a sustained auth
    failure storm, the breaker trips and every thread cools down and
    gets a fresh crumb before anyone continues.
    """
    if is_auth_error is None:
        is_auth_error = lambda e: any(s in str(e) for s in ("401", "Crumb", "Unauthorized"))

    _circuit_wait_if_tripped()

    last_exc = None
    for attempt in range(max_attempts):
        throttle(interval)
        try:
            result = fn()
            _record_auth_success()
            return result
        except Exception as e:
            last_exc = e
            if not is_auth_error(e):
                raise
            if attempt == max_attempts - 1:
                tripped = _record_auth_failure()
                if tripped:
                    print(f"\n!!! Circuit breaker tripped: {CONSECUTIVE_AUTH_FAILURE_LIMIT}+ "
                          f"consecutive auth failures across workers. Cooling down "
                          f"{CIRCUIT_COOLDOWN_SECONDS}s and re-negotiating a fresh crumb "
                          f"before continuing...\n")
                    reset_session()
                    time.sleep(CIRCUIT_COOLDOWN_SECONDS)
                    warm_session()
                raise
            time.sleep((0.4 * (2 ** attempt)) + random.uniform(0, 0.3))
    raise last_exc
