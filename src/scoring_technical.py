"""
Phase 1 — Direction-aware scoring engine (spec sections 6-8).

Takes one row of raw indicators (a dict/Series with rvol, vol_ratio, mfi,
macd_hist, atr, adx, rsi, pct_b, high_52, low_52, range_pos, open, close)
plus a direction (LONG/SHORT) and the config for the active timeframe,
and returns category scores + a final risk-adjusted score.

All formulas are direction-specific per spec section 6 — no inversion of
one direction's formula to get the other.
"""

from __future__ import annotations

import numpy as np


def _clamp(x: float) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return float("nan")
    return max(0.0, min(100.0, x))


# ---------------------------------------------------------------------
# 6.1 Volume category
# ---------------------------------------------------------------------

def score_rvol(rvol, close, open_, direction, cfg):
    denom = cfg["rvol_threshold_strong"] - 1
    base = 50 + 50 * (rvol - 1) / denom if denom != 0 else 50
    if direction == "LONG":
        favorable = close > open_
    else:
        favorable = close < open_
    score = base if favorable else base * 0.5
    return _clamp(score)


def score_vol_vs_ma(vol_ratio, close, open_, direction, cfg):
    denom = cfg["volume_multiplier"] - 1
    base = 50 + 50 * (vol_ratio - 1) / denom if denom != 0 else 50
    if direction == "LONG":
        favorable = close > open_
    else:
        favorable = close < open_
    score = base if favorable else base * 0.5
    return _clamp(score)


def score_mfi_volume(mfi, direction, cfg):
    ob, os_ = cfg["mfi_ob"], cfg["mfi_os"]
    if direction == "LONG":
        if mfi < 50:
            score = 50 - (50 - mfi) * 2
        elif mfi < ob:
            score = 50 + (mfi - 50) * 2
        else:
            score = 100 - (mfi - ob) * 4
    else:  # SHORT
        if mfi > 50:
            score = 50 - (mfi - 50) * 2
        elif mfi > os_:
            score = 50 + (50 - mfi) * 2
        else:
            score = 100 - (os_ - mfi) * 4
    return _clamp(score)


def score_volume_category(row, direction, cfg, sub_weights):
    rvol_s = score_rvol(row["rvol"], row["close"], row["open"], direction, cfg)
    vma_s = score_vol_vs_ma(row["vol_ratio"], row["close"], row["open"], direction, cfg)
    mfi_s = score_mfi_volume(row["mfi"], direction, cfg)
    parts = {"rvol": rvol_s, "vol_vs_ma": vma_s, "mfi": mfi_s}
    return _weighted(parts, sub_weights), parts


# ---------------------------------------------------------------------
# 6.2 Momentum category
# ---------------------------------------------------------------------

def score_macd(macd_hist, atr, direction, cfg, k):
    if atr is None or atr == 0 or (isinstance(atr, float) and np.isnan(atr)):
        return float("nan")
    norm = macd_hist / atr
    if direction == "LONG":
        score = 50 + 50 * np.tanh(norm * k)
    else:
        score = 50 - 50 * np.tanh(norm * k)
    return _clamp(score)


def score_adx(adx, macd_hist, direction, cfg):
    strong = cfg["adx_strong"]
    if direction == "LONG":
        trending_favorably = adx > strong and macd_hist > 0
    else:
        trending_favorably = adx > strong and macd_hist < 0
    if trending_favorably:
        score = 50 + (adx - 20) * 1.5
    else:
        score = 50 - (strong - adx) * 1.5
    return _clamp(score)


def score_range_pos_momentum(range_pos, direction):
    score = range_pos if direction == "LONG" else 100 - range_pos
    return _clamp(score)


def score_momentum_category(row, direction, cfg, sub_weights, macd_k):
    macd_s = score_macd(row["macd_hist"], row["atr"], direction, cfg, macd_k)
    adx_s = score_adx(row["adx"], row["macd_hist"], direction, cfg)
    range_s = score_range_pos_momentum(row["range_pos"], direction)
    parts = {"macd": macd_s, "adx": adx_s, "range_pos": range_s}
    return _weighted(parts, sub_weights), parts


# ---------------------------------------------------------------------
# 6.3 Support/Resistance category
# ---------------------------------------------------------------------

def score_bb_pctb(pct_b, direction):
    if direction == "LONG":
        if pct_b < 0.2:
            score = 80.0
        elif pct_b <= 0.8:
            score = 50 + (pct_b - 0.5) * 80
        else:
            score = 100 - (pct_b - 0.8) * 200
    else:  # SHORT
        if pct_b > 0.8:
            score = 80.0
        elif pct_b >= 0.2:
            score = 50 + (0.5 - pct_b) * 80
        else:
            score = 100 - (0.2 - pct_b) * 200
    return _clamp(score)


def score_sr_52w(close, high_52, low_52, range_pos, direction):
    if direction == "LONG":
        if close <= low_52 * 1.05:
            score = 80.0
        elif close > high_52:
            score = 90.0
        else:
            score = 50 + (range_pos - 50) * 0.6
    else:  # SHORT
        if close >= high_52 * 0.95:
            score = 80.0
        elif close < low_52:
            score = 90.0
        else:
            score = 50 + (50 - range_pos) * 0.6
    return _clamp(score)


def score_support_resistance_category(row, direction, sub_weights):
    bb_s = score_bb_pctb(row["pct_b"], direction)
    sr_s = score_sr_52w(row["close"], row["high_52"], row["low_52"], row["range_pos"], direction)
    parts = {"bb_pctb": bb_s, "sr_52w": sr_s}
    return _weighted(parts, sub_weights), parts


# ---------------------------------------------------------------------
# 6.4 Overbought/Oversold category
# ---------------------------------------------------------------------

def score_rsi_obos(rsi, direction, cfg):
    ob, os_ = cfg["rsi_ob"], cfg["rsi_os"]
    if direction == "LONG":
        if rsi < 50:
            score = 50 - (50 - rsi) * 1.5
        elif rsi < ob:
            score = 50 + (rsi - 50) * 2
        else:
            score = 100 - (rsi - ob) * 4
    else:  # SHORT
        if rsi > 50:
            score = 50 - (rsi - 50) * 1.5
        elif rsi > os_:
            score = 50 + (50 - rsi) * 2
        else:
            score = 100 - (os_ - rsi) * 4
    return _clamp(score)


def score_mfi_obos(mfi, direction, cfg):
    ob, os_ = cfg["mfi_ob"], cfg["mfi_os"]
    if direction == "LONG":
        if mfi < 50:
            score = 50 - (50 - mfi) * 1.5
        elif mfi < ob:
            score = 50 + (mfi - 50) * 2
        else:
            score = 100 - (mfi - ob) * 4
    else:  # SHORT
        if mfi > 50:
            score = 50 - (mfi - 50) * 1.5
        elif mfi > os_:
            score = 50 + (50 - mfi) * 2
        else:
            score = 100 - (os_ - mfi) * 4
    return _clamp(score)


def score_ob_os_category(row, direction, cfg, sub_weights):
    rsi_s = score_rsi_obos(row["rsi"], direction, cfg)
    mfi_s = score_mfi_obos(row["mfi"], direction, cfg)
    parts = {"rsi": rsi_s, "mfi": mfi_s}
    return _weighted(parts, sub_weights), parts


# ---------------------------------------------------------------------
# Aggregation (section 7) + ATR risk adjustment (section 8)
# ---------------------------------------------------------------------

def _weighted(parts: dict, weights: dict) -> float:
    """Weighted average, skipping NaN components and renormalizing
    remaining weights (spec doesn't cover missing sub-indicators
    explicitly; renormalizing avoids silently zeroing out a category
    when one input is unavailable)."""
    total_w = 0.0
    total = 0.0
    for k, v in parts.items():
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        w = weights[k]
        total += v * w
        total_w += w
    if total_w == 0:
        return float("nan")
    return total / total_w


def apply_atr_risk_adjustment(overall_score, atr, atr_cfg, multiplier_cfg):
    floor = atr_cfg.get("atr_floor")
    ceiling = atr_cfg.get("atr_ceiling")
    if floor is not None and atr < floor:
        mult = multiplier_cfg["atr_illiquid_multiplier"]
    elif ceiling is not None and atr > ceiling:
        mult = multiplier_cfg["atr_excessive_volatility_multiplier"]
    else:
        mult = multiplier_cfg["atr_normal_multiplier"]
    return overall_score * mult, mult


def compute_final_score(row, direction, timeframe_cfg, category_weights, sub_weights_cfg, scaling_cfg, atr_multiplier_cfg):
    """Full pipeline: category scores -> overall -> ATR-adjusted final.
    `row` is a dict/Series of raw indicator values for one bar (see
    indicators.compute_all_raw_indicators, last row = most recent)."""
    direction = direction.upper()
    if direction not in ("LONG", "SHORT"):
        raise ValueError("direction must be LONG or SHORT")

    vol_score, vol_parts = score_volume_category(row, direction, timeframe_cfg, sub_weights_cfg["volume"])
    mom_score, mom_parts = score_momentum_category(
        row, direction, timeframe_cfg, sub_weights_cfg["momentum"], scaling_cfg["macd_normalization_k"]
    )
    sr_score, sr_parts = score_support_resistance_category(row, direction, sub_weights_cfg["support_resistance"])
    obos_score, obos_parts = score_ob_os_category(row, direction, timeframe_cfg, sub_weights_cfg["ob_os"])

    category_scores = {
        "volume_score": vol_score,
        "momentum_score": mom_score,
        "support_resistance_score": sr_score,
        "ob_os_score": obos_score,
    }

    overall = (
        vol_score * category_weights["weight_volume"]
        + mom_score * category_weights["weight_momentum"]
        + sr_score * category_weights["weight_support_resistance"]
        + obos_score * category_weights["weight_ob_os"]
    )

    final_score, atr_mult = apply_atr_risk_adjustment(overall, row["atr"], timeframe_cfg, atr_multiplier_cfg)

    return {
        "final_score": round(_clamp(final_score), 2) if not np.isnan(final_score) else None,
        "overall_score_pre_atr": round(overall, 2) if not np.isnan(overall) else None,
        "category_scores": {k: (round(v, 2) if not np.isnan(v) else None) for k, v in category_scores.items()},
        "atr_multiplier_applied": atr_mult,
        "sub_scores": {
            "volume": vol_parts,
            "momentum": mom_parts,
            "support_resistance": sr_parts,
            "ob_os": obos_parts,
        },
    }
