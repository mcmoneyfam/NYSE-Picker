"""
Phase 1 — Raw technical indicator computations (spec section 4).

Pure, vectorized functions over OHLCV data. No config/scoring logic here —
that lives in scoring.py. Kept separate so indicators can be unit-tested
independent of the direction-aware scoring engine.

Input: pandas DataFrame with columns [open, high, low, close, volume],
indexed by date ascending.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Resamples daily OHLCV to WEEKLY or MONTHLY per spec section 3.
    DAILY returns df unchanged."""
    timeframe = timeframe.upper()
    if timeframe == "DAILY":
        return df
    rule = {"WEEKLY": "W", "MONTHLY": "ME"}.get(timeframe)
    if rule is None:
        raise ValueError(f"Unknown timeframe: {timeframe}")
    agg = df.resample(rule).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    return agg.dropna(subset=["open", "high", "low", "close"])


def compute_rvol(volume: pd.Series, period: int) -> pd.Series:
    avg = volume.shift(1).rolling(window=period, min_periods=period).mean()
    return volume / avg


def compute_vol_ratio(volume: pd.Series, period: int) -> pd.Series:
    """Current volume vs. moving average of volume (inclusive MA, per spec
    'Volume vs MA' — distinct from RVOL's trailing-exclusive average)."""
    ma = volume.rolling(window=period, min_periods=period).mean()
    return volume / ma


def compute_mfi(
    high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int
) -> pd.Series:
    """Money Flow Index — volume-weighted RSI."""
    typical_price = (high + low + close) / 3
    raw_money_flow = typical_price * volume
    price_change = typical_price.diff()

    positive_flow = raw_money_flow.where(price_change > 0, 0.0)
    negative_flow = raw_money_flow.where(price_change < 0, 0.0)

    positive_sum = positive_flow.rolling(window=period, min_periods=period).sum()
    negative_sum = negative_flow.rolling(window=period, min_periods=period).sum()

    money_ratio = positive_sum / negative_sum.replace(0, np.nan)
    mfi = 100 - (100 / (1 + money_ratio))
    return mfi.fillna(100)  # no negative flow in window -> maximally bullish


def compute_macd_hist(close: pd.Series, fast: int, slow: int, signal: int) -> pd.Series:
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line - signal_line


def compute_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def compute_adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    """Wilder's ADX."""
    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=high.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=high.index)

    atr = compute_atr(high, low, close, period)
    atr_safe = atr.replace(0, np.nan)

    plus_di = 100 * plus_dm.ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / atr_safe
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / atr_safe

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    return adx.fillna(0)


def compute_rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    result = 100 - (100 / (1 + rs))
    return result.fillna(100)


def compute_bollinger_pct_b(close: pd.Series, period: int, num_std: float) -> pd.Series:
    mid = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std()
    upper = mid + num_std * std
    lower = mid - num_std * std
    band_range = (upper - lower).replace(0, np.nan)
    return (close - lower) / band_range


def compute_52w_range(high: pd.Series, low: pd.Series, close: pd.Series, bars: int = 52) -> pd.DataFrame:
    """'52-week' means the rolling window of `bars` bars of whatever
    timeframe is active (52 daily bars is NOT 52 weeks — spec section 10
    defines this per-timeframe: 52 weekly bars for WEEKLY, 52 monthly
    bars for MONTHLY. Caller passes the already-resampled series and the
    bar count appropriate to that timeframe.)"""
    high_52 = high.rolling(window=bars, min_periods=1).max()
    low_52 = low.rolling(window=bars, min_periods=1).min()
    rng = (high_52 - low_52).replace(0, np.nan)
    range_pos = ((close - low_52) / rng * 100).fillna(50)
    return pd.DataFrame({"high_52": high_52, "low_52": low_52, "range_pos": range_pos})


def compute_all_raw_indicators(df: pd.DataFrame, timeframe: str, params: dict) -> pd.DataFrame:
    """Computes every raw indicator from spec section 4 on a (possibly
    resampled) OHLCV frame, using the config params for that timeframe.
    Returns a DataFrame aligned to df's index with one column per indicator."""
    resampled = resample_ohlcv(df, timeframe)
    o, h, l, c, v = (
        resampled["open"],
        resampled["high"],
        resampled["low"],
        resampled["close"],
        resampled["volume"],
    )

    out = pd.DataFrame(index=resampled.index)
    out["open"] = o
    out["close"] = c
    out["rvol"] = compute_rvol(v, params["rvol_period"])
    out["vol_ratio"] = compute_vol_ratio(v, params["volume_ma_period"])
    out["mfi"] = compute_mfi(h, l, c, v, params["mfi_period"])
    out["macd_hist"] = compute_macd_hist(c, params["macd_fast"], params["macd_slow"], params["macd_signal"])
    out["atr"] = compute_atr(h, l, c, params["atr_period"])
    out["adx"] = compute_adx(h, l, c, params["adx_period"])
    out["rsi"] = compute_rsi(c, params["rsi_period"])
    out["pct_b"] = compute_bollinger_pct_b(c, params["bb_period"], params["bb_std"])

    range_df = compute_52w_range(h, l, c, bars=52)
    out["high_52"] = range_df["high_52"]
    out["low_52"] = range_df["low_52"]
    out["range_pos"] = range_df["range_pos"]

    return out
