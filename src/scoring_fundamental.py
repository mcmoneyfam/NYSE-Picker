"""
Phase 2 — Direction-aware fundamental scoring engine (spec sections 6-7).

Pure functions: given one ticker's raw fundamental fields (a dict) and a
direction (LONG/SHORT), compute the 11 per-indicator 0-100 scores and the
weighted-average overall fundamental score. No I/O, no DB, no network —
mirrors the indicators.py/scoring.py split from Phase 1.

Direction is resolved by the *caller* (fetch_fundamental.py), which reads
it from the linked Phase 1 technical run rather than asking the user
again — see that module's docstring.
"""

from __future__ import annotations

import numpy as np

# Note on spec inconsistency: section 4's summary table gives ROE > 18%
# and Gross Margin > 45% as headline "what a good LONG looks like"
# figures, but section 6's actual scoring formulas gate on ROE > 5% and
# Gross Margin > 15% (the numbers a score is >0 above). Implemented
# section 6 literally, since that's the only version with an executable
# formula; the config file's `long_threshold` values for roe/gross_margin
# reflect the sections 6 gate, not the section 4 table. Flagged here per
# spec section 10 ("document deviations").


def _clamp(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return float("nan")
    return max(0.0, min(100.0, x))


def _missing(value):
    return value is None or (isinstance(value, float) and np.isnan(value))


# ---------------------------------------------------------------------
# 6.1 P/E Ratio (TTM)
# ---------------------------------------------------------------------

def score_pe_ratio(pe_ttm, direction, cfg):
    if _missing(pe_ttm):
        return float("nan")
    if direction == "LONG":
        if pe_ttm < cfg["long_threshold"]:
            return _clamp(100 - (pe_ttm - 5) * cfg["score_long_slope"])
        return 0.0
    else:
        if pe_ttm > cfg["short_threshold"]:
            return _clamp((pe_ttm - cfg["short_threshold"]) * cfg["score_short_slope"])
        return 0.0


# ---------------------------------------------------------------------
# 6.2 EV/EBITDA
# ---------------------------------------------------------------------

def score_ev_ebitda(ev_ebitda, direction, cfg):
    if _missing(ev_ebitda):
        return float("nan")
    if direction == "LONG":
        if ev_ebitda < cfg["long_threshold"]:
            return _clamp(100 - (ev_ebitda - 3) * cfg["score_long_slope"])
        return 0.0
    else:
        if ev_ebitda > cfg["short_threshold"]:
            return _clamp((ev_ebitda - cfg["short_threshold"]) * cfg["score_short_slope"])
        return 0.0


# ---------------------------------------------------------------------
# 6.3 FCF Yield (percent, e.g. 6.0 = 6%)
# ---------------------------------------------------------------------

def score_fcf_yield(fcf_yield, direction, cfg):
    if _missing(fcf_yield):
        return float("nan")
    if direction == "LONG":
        if fcf_yield > 0:
            return _clamp(fcf_yield * cfg["score_long_slope"])
        return 0.0
    else:
        if fcf_yield < 0:
            return 100.0
        elif fcf_yield < cfg["short_threshold"]:
            return _clamp(80 - fcf_yield * cfg["score_short_slope"])
        return 0.0


# ---------------------------------------------------------------------
# 6.4 ROE (percent)
# ---------------------------------------------------------------------

def score_roe(roe, direction, cfg):
    if _missing(roe):
        return float("nan")
    if direction == "LONG":
        if roe > cfg["long_threshold"]:
            return _clamp((roe - cfg["long_threshold"]) * cfg["score_long_slope"])
        return 0.0
    else:
        if roe < cfg["short_threshold"]:
            return _clamp((cfg["short_threshold"] - roe) * cfg["score_short_slope"])
        return 0.0


# ---------------------------------------------------------------------
# 6.5 Gross Margin (percent)
# ---------------------------------------------------------------------

def score_gross_margin(gross_margin, direction, cfg):
    if _missing(gross_margin):
        return float("nan")
    if direction == "LONG":
        if gross_margin > cfg["long_threshold"]:
            return _clamp((gross_margin - cfg["long_threshold"]) * cfg["score_long_slope"])
        return 0.0
    else:
        if gross_margin < cfg["short_threshold"]:
            return _clamp((cfg["short_threshold"] - gross_margin) * cfg["score_short_slope"])
        return 0.0


# ---------------------------------------------------------------------
# 6.6 OCF vs Net Income (ratio)
# ---------------------------------------------------------------------

def score_ocf_ni(ocf_ni, direction, cfg):
    if _missing(ocf_ni):
        return float("nan")
    if direction == "LONG":
        if ocf_ni > cfg["long_threshold"]:
            return _clamp((ocf_ni - cfg["long_threshold"]) * cfg["score_long_slope"])
        return 0.0
    else:
        if ocf_ni < cfg["short_threshold"]:
            return _clamp((cfg["short_threshold"] - ocf_ni) * cfg["score_short_slope"])
        return 0.0


# ---------------------------------------------------------------------
# 6.7 Revenue Growth YoY (percent)
# ---------------------------------------------------------------------

def score_revenue_growth(revenue_growth_yoy, direction, cfg):
    if _missing(revenue_growth_yoy):
        return float("nan")
    if direction == "LONG":
        if revenue_growth_yoy > 0:
            return _clamp(revenue_growth_yoy * cfg["score_long_slope"])
        return 0.0
    else:
        if revenue_growth_yoy < 0:
            return _clamp(-revenue_growth_yoy * cfg["score_short_slope"])
        return 0.0


# ---------------------------------------------------------------------
# 6.8 Net Debt / EBITDA
# ---------------------------------------------------------------------

def score_net_debt_ebitda(net_debt_ebitda, direction, cfg):
    if _missing(net_debt_ebitda):
        return float("nan")
    if direction == "LONG":
        if net_debt_ebitda < cfg["long_threshold"]:
            return _clamp(100 - net_debt_ebitda * cfg["score_long_slope"])
        return 0.0
    else:
        if net_debt_ebitda > cfg["short_threshold"]:
            return _clamp(net_debt_ebitda * cfg["score_short_slope"])
        return 0.0


# ---------------------------------------------------------------------
# 6.9 Short % of Float (percent)
# ---------------------------------------------------------------------

def score_short_percent_float(short_float, direction, cfg):
    if _missing(short_float):
        return float("nan")
    if direction == "LONG":
        if short_float > cfg["long_threshold"]:
            return _clamp((short_float - cfg["long_threshold"]) * cfg["score_long_slope"])
        return 0.0
    else:
        if short_float < cfg["short_threshold"]:
            return _clamp((cfg["short_threshold"] - short_float) * cfg["score_short_slope"])
        return 0.0


# ---------------------------------------------------------------------
# 6.10 Buyback Yield (percent)
# ---------------------------------------------------------------------

def score_buyback_yield(buyback_yield, direction, cfg):
    if _missing(buyback_yield):
        return float("nan")
    if direction == "LONG":
        if buyback_yield > 0:
            return _clamp(buyback_yield * cfg["score_long_slope"])
        return 0.0
    else:
        if buyback_yield < 0:
            return _clamp(-buyback_yield * cfg["score_short_slope"])
        return 0.0


# ---------------------------------------------------------------------
# 6.11 ROA (percent)
# ---------------------------------------------------------------------

def score_roa(roa, direction, cfg):
    if _missing(roa):
        return float("nan")
    if direction == "LONG":
        if roa > cfg["long_threshold"]:
            return _clamp((roa - cfg["long_threshold"]) * cfg["score_long_slope"])
        return 0.0
    else:
        if roa < cfg["short_threshold"]:
            return _clamp((cfg["short_threshold"] - roa) * cfg["score_short_slope"])
        return 0.0


INDICATOR_FIELDS = {
    "pe_ratio": "pe_ttm",
    "ev_ebitda": "ev_ebitda",
    "fcf_yield": "fcf_yield",
    "roe": "roe",
    "gross_margin": "gross_margin",
    "ocf_ni": "ocf_ni",
    "revenue_growth_yoy": "revenue_growth_yoy",
    "net_debt_ebitda": "net_debt_ebitda",
    "short_percent_float": "short_percent_float",
    "buyback_yield": "buyback_yield",
    "roa": "roa",
}

_SCORERS = {
    "pe_ratio": score_pe_ratio,
    "ev_ebitda": score_ev_ebitda,
    "fcf_yield": score_fcf_yield,
    "roe": score_roe,
    "gross_margin": score_gross_margin,
    "ocf_ni": score_ocf_ni,
    "revenue_growth_yoy": score_revenue_growth,
    "net_debt_ebitda": score_net_debt_ebitda,
    "short_percent_float": score_short_percent_float,
    "buyback_yield": score_buyback_yield,
    "roa": score_roa,
}


def compute_fundamental_score(fields: dict, direction: str, cfg: dict, missing_data_policy: str = "zero"):
    """Full pipeline for one ticker: per-indicator scores -> weighted
    aggregate -> optional quality gate.

    `fields` is a dict keyed by INDICATOR_FIELDS' values (pe_ttm,
    ev_ebitda, fcf_yield, roe, gross_margin, ocf_ni, revenue_growth_yoy,
    net_debt_ebitda, short_percent_float, buyback_yield, roa). Missing
    keys / None / NaN are treated as missing data.

    `missing_data_policy`: 'zero' scores a missing indicator as 0 (spec
    default); 'neutral' scores it 50; either way the indicator's weight
    still counts toward the denominator (spec section 7 doesn't call for
    renormalization the way Phase 1 does for missing technical
    sub-indicators — a fully missing fundamental field is worse than a
    bad-but-present one, so it should drag the score down, not be
    shrugged off).
    """
    direction = direction.upper()
    if direction not in ("LONG", "SHORT"):
        raise ValueError("direction must be LONG or SHORT")

    ind_cfg = cfg["fundamental_indicators"]
    indicator_scores = {}
    for name, field in INDICATOR_FIELDS.items():
        raw_score = _SCORERS[name](fields.get(field), direction, ind_cfg[name])
        if _missing(raw_score):
            raw_score = 50.0 if missing_data_policy == "neutral" else 0.0
        indicator_scores[name] = round(raw_score, 2)

    total = 0.0
    for name, score in indicator_scores.items():
        total += score * ind_cfg[name]["weight"]

    fundamental_score = _clamp(total)

    quality_gate_applied = False
    scoring_cfg = cfg.get("scoring", {})
    if scoring_cfg.get("quality_gate_enabled"):
        gate_metric = scoring_cfg["quality_gate_metric"]
        gate_field = INDICATOR_FIELDS[gate_metric]
        gate_value = fields.get(gate_field)
        if not _missing(gate_value) and gate_value < scoring_cfg["quality_gate_threshold"]:
            fundamental_score = _clamp(fundamental_score * scoring_cfg["quality_gate_multiplier"])
            quality_gate_applied = True

    return {
        "fundamental_score": round(fundamental_score, 2) if not np.isnan(fundamental_score) else None,
        "indicator_scores": indicator_scores,
        "quality_gate_applied": quality_gate_applied,
    }
