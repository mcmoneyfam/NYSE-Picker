"""
Phase 2 — SEC EDGAR client (primary fundamental data source).

--- CHANGE LOG (fixing the 2026-08-22 pipeline run) ---

The production log showed a DIFFERENT failure mode than the Yahoo
crumb storm: starting around ticker ~1500-1600, the "ok" count froze
completely and every subsequent ticker got flagged, with NO error text
printed at all, and it never recovered for the rest of the run. That
combination — permanent, silent, 100%-failure-from-here-on — doesn't
match Yahoo's throttle (which was noisy about every 401, and
self-cleared after ~13 minutes). It matches SEC EDGAR blocking the
requesting IP: SEC's fair-access policy wants a genuine, identifying
User-Agent (this file had a placeholder), and once SEC decides to
throttle a source it tends to hold that block far longer than Yahoo's
does — this file had NO handling at all for 403/429, so
`resp.raise_for_status()` just threw and whatever caught it upstream
(in fetch_fundamental.py) silently counted it as a flag, forever,
for every remaining ticker.

Fixes:
1. SET SEC_HEADERS["User-Agent"] to a REAL app name + contact email
   before running this again — see the TODO below. A generic
   placeholder is itself a plausible reason SEC started blocking you.
2. fetch_company_facts (and the ticker-map fetch) now distinguish
   403/429 (blocked/rate-limited — worth backing off and retrying)
   from 404 (genuinely no filer for this CIK — expected, not an error)
   from other HTTP errors (raised as before).
3. A shared circuit breaker: after a run of consecutive 403/429s
   across calls, everything pauses for a real cooldown and prints what
   happened, instead of every subsequent ticker silently failing with
   no visible reason.
"""

from __future__ import annotations

import json
import threading
import time

import requests

# TODO: replace with a REAL app name + your real contact email before
# running against SEC again. SEC's fair-access guidance
# (https://www.sec.gov/os/webmaster-faq#developers) explicitly wants an
# identifying User-Agent for automated access; a generic placeholder is
# itself a plausible reason for getting throttled/blocked.
SEC_HEADERS = {
    "User-Agent": "stockwiz@gmail.com",  # <-- CHANGE THIS
    "Accept-Encoding": "gzip, deflate",
}

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"

# SEC asks for <=10 requests/sec; stay well under that across threads.
_MIN_INTERVAL = 0.15
_last_request_ts = {"ts": 0.0}
_throttle_lock = threading.Lock()


def _throttle():
    with _throttle_lock:
        now = time.time()
        wait = _MIN_INTERVAL - (now - _last_request_ts["ts"])
        if wait > 0:
            time.sleep(wait)
        _last_request_ts["ts"] = time.time()


class EdgarBlockedError(Exception):
    """Raised when SEC returns 403/429 even after backoff+retry — i.e.
    a real, sustained block, not a one-off transient hiccup. Distinct
    from a plain requests.HTTPError so callers (fetch_fundamental.py)
    can tell "SEC is blocking us, stop and investigate" apart from
    "this one ticker/CIK genuinely has no data" (404) or an unrelated
    error."""


# --- Circuit breaker for SEC throttling/blocking ---
CONSECUTIVE_BLOCK_LIMIT = 10      # trip after this many 403/429s in a row (any thread)
CIRCUIT_COOLDOWN_SECONDS = 120    # SEC blocks tend to last longer than Yahoo's; cool down more
CIRCUIT_MAX_TRIPS = 4             # stop paying cooldowns after this many trips (~8 min of pausing)
                                   # so a genuine sustained ban surfaces as a real error instead of
                                   # hanging the run indefinitely

_circuit_lock = threading.Lock()
_circuit_state = {"consecutive_blocks": 0, "trips": 0, "tripped_until": 0.0}


def _circuit_wait_if_tripped():
    while True:
        with _circuit_lock:
            wait_left = _circuit_state["tripped_until"] - time.monotonic()
            if wait_left <= 0:
                return
        time.sleep(min(wait_left, 5))


def _record_block():
    """Returns True if this call just tripped the breaker."""
    with _circuit_lock:
        _circuit_state["consecutive_blocks"] += 1
        if _circuit_state["consecutive_blocks"] < CONSECUTIVE_BLOCK_LIMIT:
            return False
        if _circuit_state["trips"] >= CIRCUIT_MAX_TRIPS:
            return False
        _circuit_state["trips"] += 1
        _circuit_state["consecutive_blocks"] = 0
        _circuit_state["tripped_until"] = time.monotonic() + CIRCUIT_COOLDOWN_SECONDS
        return True


def _record_success():
    with _circuit_lock:
        _circuit_state["consecutive_blocks"] = 0


def _get_with_backoff(url: str, *, max_attempts: int = 3):
    """GET with SEC-aware error handling:
      - 404  -> return None immediately (no filer/CIK — genuinely expected, not an error)
      - 403/429 -> retry with backoff; if it keeps happening, trip the
        shared circuit breaker (cooldown + visible log line) instead of
        silently failing every subsequent ticker forever
      - anything else -> raise_for_status() as before
    """
    _circuit_wait_if_tripped()

    last_exc = None
    for attempt in range(max_attempts):
        _throttle()
        resp = requests.get(url, headers=SEC_HEADERS, timeout=30)

        if resp.status_code == 404:
            _record_success()
            return None

        if resp.status_code in (403, 429):
            last_exc = requests.HTTPError(f"{resp.status_code} for {url}", response=resp)
            if attempt < max_attempts - 1:
                time.sleep((1.0 * (2 ** attempt)))
                continue
            tripped = _record_block()
            if tripped:
                print(f"\n!!! SEC EDGAR circuit breaker tripped: {CONSECUTIVE_BLOCK_LIMIT}+ "
                      f"consecutive 403/429 responses. This usually means SEC is throttling or "
                      f"blocking this IP/User-Agent — check SEC_HEADERS['User-Agent'] in "
                      f"client_edgar.py is a real, identifying contact, not a placeholder. "
                      f"Cooling down {CIRCUIT_COOLDOWN_SECONDS}s before continuing...\n")
                time.sleep(CIRCUIT_COOLDOWN_SECONDS)
            raise EdgarBlockedError(
                f"SEC EDGAR returned {resp.status_code} for {url} after {max_attempts} attempts "
                f"— likely throttled/blocked. Check User-Agent and request rate."
            )

        resp.raise_for_status()
        _record_success()
        return resp.json()

    raise last_exc


def fetch_ticker_cik_map() -> dict:
    """Returns {ticker: cik_int}. Fetch once, cache to disk by caller."""
    data = _get_with_backoff(TICKER_MAP_URL)
    if data is None:
        raise EdgarBlockedError("Ticker->CIK map fetch returned 404 — unexpected, check TICKER_MAP_URL")
    return {row["ticker"].upper(): row["cik_str"] for row in data.values()}


def fetch_company_facts(cik: int) -> dict | None:
    url = COMPANYFACTS_URL.format(cik=cik)
    return _get_with_backoff(url)


# us-gaap tag candidates per field, tried in order — companies vary in
# which tag they use for the same concept (e.g. some tag operating cash
# flow as NetCashProvidedByUsedInOperatingActivities, others append
# "Continuing").
TAG_CANDIDATES = {
    "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"],
    "net_income": ["NetIncomeLoss"],
    "gross_profit": ["GrossProfit"],
    "operating_cash_flow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ],
    "capital_expenditures": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
    ],
    "total_debt": ["LongTermDebtNoncurrent", "LongTermDebt"],
    "current_debt": ["LongTermDebtCurrent", "DebtCurrent"],
    "cash_and_equivalents": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ],
    "stockholders_equity": ["StockholdersEquity"],
    "total_assets": ["Assets"],
    "depreciation_amortization": ["DepreciationDepletionAndAmortization", "DepreciationAndAmortization"],
    "operating_income": ["OperatingIncomeLoss"],
    "shares_outstanding": ["CommonStockSharesOutstanding"],
    "treasury_stock_purchased": ["PaymentsForRepurchaseOfCommonStock"],
}


def _latest_annual_value(facts: dict, tag: str):
    try:
        units = facts["facts"]["us-gaap"][tag]["units"]
    except KeyError:
        return None
    for unit_key, entries in units.items():
        annual = [e for e in entries if e.get("form") == "10-K" and e.get("fp") == "FY"]
        candidates = annual if annual else entries
        if not candidates:
            continue
        best = max(candidates, key=lambda e: e.get("end", ""))
        return best.get("val")
    return None


def _two_latest_annual_values(facts: dict, tag: str):
    try:
        units = facts["facts"]["us-gaap"][tag]["units"]
    except KeyError:
        return None, None
    for unit_key, entries in units.items():
        annual = [e for e in entries if e.get("form") == "10-K" and e.get("fp") == "FY"]
        candidates = annual if annual else entries
        if not candidates:
            continue
        by_end = {}
        for e in candidates:
            by_end[e.get("end", "")] = e.get("val")
        sorted_ends = sorted(by_end.keys(), reverse=True)
        if len(sorted_ends) < 2:
            return by_end.get(sorted_ends[0]) if sorted_ends else None, None
        return by_end[sorted_ends[0]], by_end[sorted_ends[1]]
    return None, None


def extract_edgar_fields(facts: dict) -> dict:
    out = {}
    for field, tags in TAG_CANDIDATES.items():
        value = None
        for tag in tags:
            value = _latest_annual_value(facts, tag)
            if value is not None:
                break
        out[field] = value

    out["revenue_prior_year"] = None
    for tag in TAG_CANDIDATES["revenue"]:
        latest, prior = _two_latest_annual_values(facts, tag)
        if prior is not None:
            out["revenue_prior_year"] = prior
            break

    return out
