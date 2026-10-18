"""
Phase 2 — Fundamental data assembly: EDGAR (primary) + yfinance
(fallback for missing EDGAR tags, and structural source for
price-dependent / short-interest fields — see client_edgar.py's
docstring for why those can never come from EDGAR).

--- CHANGE LOG (fixing the 2026-08-22 pipeline run) ---

`.info` (quoteSummary) is the single most crumb-sensitive yfinance
endpoint, and this file was calling it for EVERY field, including
market_cap and current_price — both of which are also available from
`fast_info`, a much lighter endpoint that doesn't drive the same
quoteSummary/crumb machinery as hard. Splitting the fetch into a cheap
`fast_info` call (market cap, price) plus the heavier `.info` call
(everything else: margins, ROE/ROA, short interest, cashflow) cuts the
volume of the specific calls that were tripping Yahoo's throttle in
Phase 2's production run. See client_yfinance.py's change log for the
matching throttle/circuit-breaker changes.

Produces the flat field dict scoring_fundamental.compute_fundamental_score
expects: pe_ttm, ev_ebitda, fcf_yield, roe, gross_margin, ocf_ni,
revenue_growth_yoy, net_debt_ebitda, short_percent_float, buyback_yield, roa.
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone

from client_edgar import extract_edgar_fields
from client_yfinance import (  # noqa: F401 — re-exported for callers that used to import these from here
    yf_ticker,
    call_with_retry,
    warm_session,
    get_shared_session,
    INFO_FETCH_INTERVAL_SECONDS,
    FETCH_INTERVAL_SECONDS,
)

CACHE_DIR = "fundamental_cache"
TICKER_MAP_CACHE = os.path.join(CACHE_DIR, "_ticker_cik_map.json")

CONFLICT_THRESHOLD = 0.20


def _check_conflict(field, edgar_val, yf_val, threshold=CONFLICT_THRESHOLD):
    if edgar_val is None or yf_val is None:
        return None
    if edgar_val == 0 and yf_val == 0:
        return None
    denom = max(abs(edgar_val), abs(yf_val), 1e-9)
    pct_diff = abs(edgar_val - yf_val) / denom
    if pct_diff <= threshold:
        return None
    return {
        "field": field,
        "edgar": edgar_val,
        "yfinance": yf_val,
        "pct_diff": round(pct_diff * 100, 1),
    }


def _yfinance_fast_info_with_retry(ticker):
    """Cheap, less crumb-sensitive endpoint for market cap / price.
    Tried first so the heavier `.info` call below doesn't have to carry
    fields this can already answer."""
    def _fetch():
        fi = yf_ticker(ticker).fast_info
        # fast_info behaves like a mapping but isn't always a plain dict
        # across yfinance versions — normalize via .get with getattr fallback.
        def g(key, *alt_keys):
            for k in (key, *alt_keys):
                try:
                    v = fi[k]
                    if v is not None:
                        return v
                except Exception:
                    pass
                v = getattr(fi, k, None)
                if v is not None:
                    return v
            return None
        return {
            "market_cap": g("market_cap", "marketCap"),
            "current_price": g("last_price", "lastPrice", "regular_market_previous_close"),
        }
    try:
        return call_with_retry(_fetch, interval=FETCH_INTERVAL_SECONDS)
    except Exception:
        # fast_info is a nice-to-have optimization — if it fails for any
        # reason, fall through silently and let the full .info call below
        # supply these fields instead (it always requests them anyway).
        return {"market_cap": None, "current_price": None}


def _yfinance_info_with_retry(ticker):
    """Fetches .info for one ticker via the shared session, shared
    throttle, and shared auth-error retry + circuit breaker in
    client_yfinance. Uses the slower INFO_FETCH_INTERVAL_SECONDS since
    this is the endpoint that was tripping Yahoo's throttle."""
    return call_with_retry(
        lambda: yf_ticker(ticker).info or {},
        interval=INFO_FETCH_INTERVAL_SECONDS,
    )


def _cache_path(ticker):
    return os.path.join(CACHE_DIR, f"{ticker}.json")


def load_cached(ticker, ttl_hours):
    path = _cache_path(ticker)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        entry = json.load(f)
    fetched_at = datetime.fromisoformat(entry["fetched_at"])
    age_hours = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600
    if age_hours > ttl_hours:
        return None
    return entry["fields"], entry.get("field_sources", {}), entry.get("conflicts", [])


def save_cache(ticker, fields, field_sources, conflicts):
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(_cache_path(ticker), "w") as f:
        json.dump(
            {
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "fields": fields,
                "field_sources": field_sources,
                "conflicts": conflicts,
            },
            f,
        )


def _safe_div(a, b):
    if a is None or b in (None, 0):
        return None
    try:
        if isinstance(b, float) and math.isnan(b):
            return None
        return a / b
    except ZeroDivisionError:
        return None


def _yfinance_supplement(ticker):
    """Fields yfinance must supply regardless of EDGAR (price-dependent,
    short interest), plus fallback values for anything EDGAR missed.
    market_cap/current_price come from the lighter fast_info first;
    everything else still needs the full .info payload."""
    fast = _yfinance_fast_info_with_retry(ticker)
    info = _yfinance_info_with_retry(ticker)

    market_cap = fast.get("market_cap") or info.get("marketCap")
    current_price = (
        fast.get("current_price")
        or info.get("currentPrice")
        or info.get("regularMarketPrice")
    )

    return {
        "market_cap": market_cap,
        "current_price": current_price,
        "trailing_pe": info.get("trailingPE"),
        "ev_to_ebitda_yf": info.get("enterpriseToEbitda"),
        "short_percent_float": (
            info.get("shortPercentOfFloat") * 100 if info.get("shortPercentOfFloat") is not None else None
        ),
        "ebitda_yf": info.get("ebitda"),
        "total_debt_yf": info.get("totalDebt"),
        "cash_yf": info.get("totalCash"),
        "net_income_yf": info.get("netIncomeToCommon"),
        "revenue_yf": info.get("totalRevenue"),
        "revenue_growth_yf": (
            info.get("revenueGrowth") * 100 if info.get("revenueGrowth") is not None else None
        ),
        "gross_margin_yf": info.get("grossMargins") * 100 if info.get("grossMargins") is not None else None,
        "roe_yf": info.get("returnOnEquity") * 100 if info.get("returnOnEquity") is not None else None,
        "roa_yf": info.get("returnOnAssets") * 100 if info.get("returnOnAssets") is not None else None,
        "operating_cash_flow_yf": info.get("operatingCashflow"),
        "free_cash_flow_yf": info.get("freeCashflow"),
        "shares_outstanding_yf": info.get("sharesOutstanding"),
    }


def assemble_from_facts(ticker: str, edgar_raw: dict | None, ttl_hours: float = 24) -> tuple[dict, dict, list]:
    cached = load_cached(ticker, ttl_hours)
    if cached is not None:
        return cached

    yf_data = _yfinance_supplement(ticker)
    fields, sources, conflicts = _combine(edgar_raw or {}, yf_data)
    save_cache(ticker, fields, sources, conflicts)
    return fields, sources, conflicts


def _combine(edgar: dict, yf_data: dict) -> tuple[dict, dict, list]:
    fields = {}
    sources = {}
    conflicts = []

    def check(field, edgar_val, yf_val):
        c = _check_conflict(field, edgar_val, yf_val)
        if c is not None:
            conflicts.append(c)

    market_cap = yf_data.get("market_cap")
    net_income = edgar.get("net_income") if edgar.get("net_income") is not None else yf_data.get("net_income_yf")
    sources["net_income"] = "edgar" if edgar.get("net_income") is not None else "yfinance"
    check("net_income", edgar.get("net_income"), yf_data.get("net_income_yf"))

    revenue = edgar.get("revenue") if edgar.get("revenue") is not None else yf_data.get("revenue_yf")
    sources["revenue"] = "edgar" if edgar.get("revenue") is not None else "yfinance"
    check("revenue", edgar.get("revenue"), yf_data.get("revenue_yf"))

    gross_profit = edgar.get("gross_profit")
    ocf = edgar.get("operating_cash_flow") if edgar.get("operating_cash_flow") is not None else yf_data.get("operating_cash_flow_yf")
    sources["operating_cash_flow"] = "edgar" if edgar.get("operating_cash_flow") is not None else "yfinance"
    check("operating_cash_flow", edgar.get("operating_cash_flow"), yf_data.get("operating_cash_flow_yf"))

    total_debt = edgar.get("total_debt")
    edgar_total_debt_for_check = None
    if total_debt is None:
        total_debt = yf_data.get("total_debt_yf")
        sources["total_debt"] = "yfinance"
    else:
        current_debt = edgar.get("current_debt") or 0
        total_debt = total_debt + current_debt
        edgar_total_debt_for_check = total_debt
        sources["total_debt"] = "edgar"
    check("total_debt", edgar_total_debt_for_check, yf_data.get("total_debt_yf"))

    cash = edgar.get("cash_and_equivalents") if edgar.get("cash_and_equivalents") is not None else yf_data.get("cash_yf")
    sources["cash"] = "edgar" if edgar.get("cash_and_equivalents") is not None else "yfinance"
    check("cash", edgar.get("cash_and_equivalents"), yf_data.get("cash_yf"))

    equity = edgar.get("stockholders_equity")
    assets = edgar.get("total_assets")
    op_income = edgar.get("operating_income")
    d_and_a = edgar.get("depreciation_amortization")
    shares_out = edgar.get("shares_outstanding") or yf_data.get("shares_outstanding_yf")
    treasury_purchased = edgar.get("treasury_stock_purchased")

    pe_ttm = yf_data.get("trailing_pe")
    if pe_ttm is None and market_cap and net_income:
        pe_ttm = _safe_div(market_cap, net_income)
        sources["pe_ttm"] = "derived"
    else:
        sources["pe_ttm"] = "yfinance"
    fields["pe_ttm"] = pe_ttm

    ebitda = None
    if op_income is not None and d_and_a is not None:
        ebitda = op_income + d_and_a
        sources["ebitda"] = "edgar-derived"
    elif yf_data.get("ebitda_yf") is not None:
        ebitda = yf_data["ebitda_yf"]
        sources["ebitda"] = "yfinance"

    ev_ebitda = yf_data.get("ev_to_ebitda_yf")
    if ev_ebitda is None and market_cap is not None and ebitda:
        net_debt_for_ev = (total_debt or 0) - (cash or 0)
        ev = market_cap + net_debt_for_ev
        ev_ebitda = _safe_div(ev, ebitda)
        sources["ev_ebitda"] = "derived"
    else:
        sources["ev_ebitda"] = "yfinance" if ev_ebitda is not None else "missing"
    fields["ev_ebitda"] = ev_ebitda

    capex = edgar.get("capital_expenditures")
    if ocf is not None and capex is not None:
        fcf = ocf - capex
        sources["fcf"] = "edgar-derived"
    elif yf_data.get("free_cash_flow_yf") is not None:
        fcf = yf_data.get("free_cash_flow_yf")
        sources["fcf"] = "yfinance"
    elif ocf is not None:
        fcf = ocf
        sources["fcf"] = "ocf-proxy"
    else:
        fcf = None
        sources["fcf"] = "missing"
    check("fcf", (ocf - capex) if (ocf is not None and capex is not None) else None, yf_data.get("free_cash_flow_yf"))
    fcf_yield = _safe_div(fcf, market_cap)
    fields["fcf_yield"] = fcf_yield * 100 if fcf_yield is not None else None

    if net_income is not None and equity:
        fields["roe"] = _safe_div(net_income, equity) * 100
        sources["roe"] = "edgar-derived"
        check("roe", fields["roe"], yf_data.get("roe_yf"))
    else:
        fields["roe"] = yf_data.get("roe_yf")
        sources["roe"] = "yfinance"

    if gross_profit is not None and revenue:
        fields["gross_margin"] = _safe_div(gross_profit, revenue) * 100
        sources["gross_margin"] = "edgar-derived"
        check("gross_margin", fields["gross_margin"], yf_data.get("gross_margin_yf"))
    else:
        fields["gross_margin"] = yf_data.get("gross_margin_yf")
        sources["gross_margin"] = "yfinance"

    fields["ocf_ni"] = _safe_div(ocf, net_income) if (ocf is not None and net_income) else None
    sources["ocf_ni"] = "derived" if fields["ocf_ni"] is not None else "missing"

    revenue_prior_year = edgar.get("revenue_prior_year")
    if revenue is not None and revenue_prior_year:
        fields["revenue_growth_yoy"] = _safe_div(revenue - revenue_prior_year, revenue_prior_year) * 100
        sources["revenue_growth_yoy"] = "edgar-derived"
        check("revenue_growth_yoy", fields["revenue_growth_yoy"], yf_data.get("revenue_growth_yf"))
    else:
        fields["revenue_growth_yoy"] = yf_data.get("revenue_growth_yf")
        sources["revenue_growth_yoy"] = "yfinance" if fields["revenue_growth_yoy"] is not None else "missing"

    if total_debt is not None and cash is not None and ebitda:
        net_debt = total_debt - cash
        fields["net_debt_ebitda"] = _safe_div(net_debt, ebitda)
        sources["net_debt_ebitda"] = "derived"
    else:
        fields["net_debt_ebitda"] = None
        sources["net_debt_ebitda"] = "missing"

    fields["short_percent_float"] = yf_data.get("short_percent_float")
    sources["short_percent_float"] = "yfinance"

    if treasury_purchased is not None and market_cap:
        fields["buyback_yield"] = _safe_div(treasury_purchased, market_cap) * 100
        sources["buyback_yield"] = "edgar-derived"
    else:
        fields["buyback_yield"] = None
        sources["buyback_yield"] = "missing"

    if net_income is not None and assets:
        fields["roa"] = _safe_div(net_income, assets) * 100
        sources["roa"] = "edgar-derived"
        check("roa", fields["roa"], yf_data.get("roa_yf"))
    else:
        fields["roa"] = yf_data.get("roa_yf")
        sources["roa"] = "yfinance"

    return fields, sources, conflicts
