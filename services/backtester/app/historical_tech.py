"""Reconstruct technical indicator scores from historical OHLCV data.

For a given instrument_id and cutoff_date, slices historical_prices from
the DB up to that date and runs all 18 indicators.
"""

import logging
import math
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Copy of INDICATOR_GROUPS + SIGNAL_SCORES + GROUP_WEIGHT_PROFILES from scorer.py
# (duplicated to keep backtester self-contained)
# ---------------------------------------------------------------------------

SIGNAL_SCORES = {
    "strong_buy":  3.0,
    "buy":         1.5,
    "neutral":     0.0,
    "sell":       -1.5,
    "strong_sell": -3.0,
}

INDICATOR_GROUPS = {
    "trend":      ["SMA_50", "SMA_200", "EMA_20", "EMA_CROSS", "MACD", "ICHIMOKU"],
    "momentum":   ["RSI", "STOCHASTIC", "WILLIAMS_R", "CCI"],
    "volume":     ["OBV", "VWAP", "MFI"],
    "levels":     ["SUPPORT_RESISTANCE", "FIBONACCI"],
    "volatility": ["BOLLINGER"],
}

GROUP_WEIGHT_PROFILES = {
    "stock": {
        "short": {"trend": 0.28, "momentum": 0.30, "volume": 0.20, "levels": 0.16, "volatility": 0.06},
        "long":  {"trend": 0.38, "momentum": 0.20, "volume": 0.18, "levels": 0.18, "volatility": 0.06},
    },
    "etf": {
        "short": {"trend": 0.30, "momentum": 0.25, "volume": 0.22, "levels": 0.17, "volatility": 0.06},
        "long":  {"trend": 0.42, "momentum": 0.18, "volume": 0.18, "levels": 0.16, "volatility": 0.06},
    },
    "commodity": {
        "short": {"trend": 0.25, "momentum": 0.28, "volume": 0.20, "levels": 0.20, "volatility": 0.07},
        "long":  {"trend": 0.35, "momentum": 0.20, "volume": 0.18, "levels": 0.20, "volatility": 0.07},
    },
}


def _clip(v: float, lo: float = -3.0, hi: float = 3.0) -> float:
    return max(lo, min(hi, v))


# ---------------------------------------------------------------------------
# Indicator calculation functions (same logic as technical-analysis service)
# ---------------------------------------------------------------------------

def _calc_sma(df: pd.DataFrame, period: int) -> dict:
    sma = df["close"].rolling(window=period).mean()
    current = sma.iloc[-1]
    price = df["close"].iloc[-1]
    if pd.isna(current):
        return {"indicator_name": f"SMA_{period}", "signal": "neutral"}
    deviation = ((price - current) / current) * 100
    strong_threshold = min(1.0 + (period // 50), 3.0)
    if deviation > strong_threshold:
        signal = "strong_buy"
    elif deviation > 0:
        signal = "buy"
    elif deviation < -strong_threshold:
        signal = "strong_sell"
    elif deviation < 0:
        signal = "sell"
    else:
        signal = "neutral"
    return {"indicator_name": f"SMA_{period}", "signal": signal}


def _calc_ema(df: pd.DataFrame, period: int) -> dict:
    ema = df["close"].ewm(span=period, adjust=False).mean()
    current = ema.iloc[-1]
    price = df["close"].iloc[-1]
    if pd.isna(current):
        return {"indicator_name": f"EMA_{period}", "signal": "neutral"}
    deviation = ((price - current) / current) * 100
    strong_threshold = min(1.0 + (period // 50), 3.0)
    if deviation > strong_threshold:
        signal = "strong_buy"
    elif deviation > 0:
        signal = "buy"
    elif deviation < -strong_threshold:
        signal = "strong_sell"
    elif deviation < 0:
        signal = "sell"
    else:
        signal = "neutral"
    return {"indicator_name": "EMA_20", "signal": signal}


def _calc_ema_cross(df: pd.DataFrame) -> dict:
    ema50 = df["close"].ewm(span=50, adjust=False).mean()
    ema200 = df["close"].ewm(span=200, adjust=False).mean()
    c50, c200 = ema50.iloc[-1], ema200.iloc[-1]
    p50 = ema50.iloc[-2] if len(ema50) > 1 else c50
    p200 = ema200.iloc[-2] if len(ema200) > 1 else c200
    if pd.isna(c50) or pd.isna(c200):
        return {"indicator_name": "EMA_CROSS", "signal": "neutral"}
    if c50 > c200 and p50 <= p200:
        signal = "strong_buy"
    elif c50 < c200 and p50 >= p200:
        signal = "strong_sell"
    elif c50 > c200:
        signal = "buy"
    elif c50 < c200:
        signal = "sell"
    else:
        signal = "neutral"
    return {"indicator_name": "EMA_CROSS", "signal": signal}


def _calc_macd(df: pd.DataFrame) -> dict:
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line
    curr_hist = histogram.iloc[-1]
    prev_hist = histogram.iloc[-2] if len(histogram) > 1 else 0
    if pd.isna(curr_hist):
        return {"indicator_name": "MACD", "signal": "neutral"}
    if curr_hist > 0 and prev_hist <= 0:
        signal = "strong_buy"
    elif curr_hist > 0:
        signal = "buy"
    elif curr_hist < 0 and prev_hist >= 0:
        signal = "strong_sell"
    elif curr_hist < 0:
        signal = "sell"
    else:
        signal = "neutral"
    return {"indicator_name": "MACD", "signal": signal}


def _calc_ichimoku(df: pd.DataFrame) -> dict:
    tenkan = (df["high"].rolling(9).max() + df["low"].rolling(9).min()) / 2
    kijun = (df["high"].rolling(26).max() + df["low"].rolling(26).min()) / 2
    senkou_a = (tenkan + kijun) / 2
    senkou_b = (df["high"].rolling(52).max() + df["low"].rolling(52).min()) / 2
    price = df["close"].iloc[-1]
    t, k, sa, sb = tenkan.iloc[-1], kijun.iloc[-1], senkou_a.iloc[-1], senkou_b.iloc[-1]
    if any(pd.isna(v) for v in [t, k, sa, sb]):
        return {"indicator_name": "ICHIMOKU", "signal": "neutral"}
    cloud_top = max(sa, sb)
    cloud_bottom = min(sa, sb)
    if price > cloud_top and t > k:
        signal = "strong_buy"
    elif price > cloud_top:
        signal = "buy"
    elif price < cloud_bottom and t < k:
        signal = "strong_sell"
    elif price < cloud_bottom:
        signal = "sell"
    else:
        signal = "neutral"
    return {"indicator_name": "ICHIMOKU", "signal": signal}


def _calc_rsi(df: pd.DataFrame) -> dict:
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    current = rsi.iloc[-1]
    if pd.isna(current):
        return {"indicator_name": "RSI", "signal": "neutral"}
    if current >= 70:
        signal = "strong_sell"
    elif current >= 60:
        signal = "sell"
    elif current <= 30:
        signal = "strong_buy"
    elif current <= 40:
        signal = "buy"
    else:
        signal = "neutral"
    return {"indicator_name": "RSI", "signal": signal}


def _calc_stochastic(df: pd.DataFrame) -> dict:
    low_min = df["low"].rolling(14).min()
    high_max = df["high"].rolling(14).max()
    denom = high_max - low_min
    k = 100 * (df["close"] - low_min) / denom.replace(0, np.nan)
    current_k = k.iloc[-1]
    if pd.isna(current_k):
        return {"indicator_name": "STOCHASTIC", "signal": "neutral"}
    if current_k >= 80:
        signal = "strong_sell"
    elif current_k >= 60:
        signal = "sell"
    elif current_k <= 20:
        signal = "strong_buy"
    elif current_k <= 40:
        signal = "buy"
    else:
        signal = "neutral"
    return {"indicator_name": "STOCHASTIC", "signal": signal}


def _calc_williams_r(df: pd.DataFrame) -> dict:
    high_max = df["high"].rolling(14).max()
    low_min = df["low"].rolling(14).min()
    denom = high_max - low_min
    wr = -100 * (high_max - df["close"]) / denom.replace(0, np.nan)
    current = wr.iloc[-1]
    if pd.isna(current):
        return {"indicator_name": "WILLIAMS_R", "signal": "neutral"}
    if current >= -20:
        signal = "strong_sell"
    elif current >= -40:
        signal = "sell"
    elif current <= -80:
        signal = "strong_buy"
    elif current <= -60:
        signal = "buy"
    else:
        signal = "neutral"
    return {"indicator_name": "WILLIAMS_R", "signal": signal}


def _calc_cci(df: pd.DataFrame) -> dict:
    tp = (df["high"] + df["low"] + df["close"]) / 3
    sma_tp = tp.rolling(20).mean()
    mad = tp.rolling(20).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
    cci = (tp - sma_tp) / (0.015 * mad.replace(0, np.nan))
    current = cci.iloc[-1]
    if pd.isna(current):
        return {"indicator_name": "CCI", "signal": "neutral"}
    if current > 200:
        signal = "strong_sell"
    elif current > 100:
        signal = "sell"
    elif current < -200:
        signal = "strong_buy"
    elif current < -100:
        signal = "buy"
    else:
        signal = "neutral"
    return {"indicator_name": "CCI", "signal": signal}


def _calc_bollinger(df: pd.DataFrame) -> dict:
    sma = df["close"].rolling(20).mean()
    std = df["close"].rolling(20).std()
    upper = sma + 2 * std
    lower = sma - 2 * std
    price = df["close"].iloc[-1]
    cu, cl = upper.iloc[-1], lower.iloc[-1]
    if pd.isna(cu):
        return {"indicator_name": "BOLLINGER", "signal": "neutral"}
    if price >= cu * 1.02:
        signal = "strong_sell"
    elif price >= cu:
        signal = "sell"
    elif price <= cl * 0.98:
        signal = "strong_buy"
    elif price <= cl:
        signal = "buy"
    else:
        signal = "neutral"
    return {"indicator_name": "BOLLINGER", "signal": signal}


def _calc_adx(df: pd.DataFrame) -> dict:
    high, low, close = df["high"], df["low"], df["close"]
    ph, pl = high.shift(1), low.shift(1)
    plus_dm = (high - ph).clip(lower=0)
    minus_dm = (pl - low).clip(lower=0)
    plus_dm = plus_dm.where(plus_dm > minus_dm, 0)
    minus_dm = minus_dm.where(minus_dm > plus_dm, 0)
    tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
    alpha = 1.0 / 14
    atr = tr.ewm(alpha=alpha, min_periods=14, adjust=False).mean()
    sp = plus_dm.ewm(alpha=alpha, min_periods=14, adjust=False).mean()
    sm = minus_dm.ewm(alpha=alpha, min_periods=14, adjust=False).mean()
    pdi = 100 * sp / atr.replace(0, np.nan)
    mdi = 100 * sm / atr.replace(0, np.nan)
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    adx = dx.ewm(alpha=alpha, min_periods=14, adjust=False).mean()
    cadx = adx.iloc[-1]
    if pd.isna(cadx):
        return {"indicator_name": "ADX", "signal": "neutral", "adx_value": 0.0}
    return {"indicator_name": "ADX", "signal": "neutral", "adx_value": float(cadx)}


def _calc_atr(df: pd.DataFrame) -> dict:
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    current_atr = atr.iloc[-1]
    price = close.iloc[-1]
    if pd.isna(current_atr) or price == 0:
        return {"indicator_name": "ATR", "signal": "neutral", "atr_pct": 2.0}
    atr_pct = (current_atr / price) * 100
    return {"indicator_name": "ATR", "signal": "neutral", "atr_pct": float(atr_pct)}


def _calc_obv(df: pd.DataFrame) -> dict:
    direction = np.sign(df["close"].diff())
    obv = (direction * df["volume"]).fillna(0).cumsum()
    obv_sma = obv.rolling(20).mean()
    obv_std = obv.rolling(20).std().iloc[-1]
    current_obv = obv.iloc[-1]
    current_sma = obv_sma.iloc[-1]
    if pd.isna(current_sma) or pd.isna(obv_std):
        return {"indicator_name": "OBV", "signal": "neutral"}
    if current_obv > current_sma + obv_std:
        signal = "strong_buy"
    elif current_obv > current_sma:
        signal = "buy"
    elif current_obv < current_sma - obv_std:
        signal = "strong_sell"
    elif current_obv < current_sma:
        signal = "sell"
    else:
        signal = "neutral"
    return {"indicator_name": "OBV", "signal": signal}


def _calc_vwap(df: pd.DataFrame) -> dict:
    tp = (df["high"] + df["low"] + df["close"]) / 3
    vol = df["volume"].replace(0, np.nan)
    vwap = (tp * vol).rolling(20).sum() / vol.rolling(20).sum().replace(0, np.nan)
    current_vwap = vwap.iloc[-1]
    price = df["close"].iloc[-1]
    if pd.isna(current_vwap):
        return {"indicator_name": "VWAP", "signal": "neutral"}
    deviation = ((price - current_vwap) / current_vwap) * 100
    if deviation > 2.0:
        signal = "strong_buy"
    elif deviation > 0:
        signal = "buy"
    elif deviation < -2.0:
        signal = "strong_sell"
    elif deviation < 0:
        signal = "sell"
    else:
        signal = "neutral"
    return {"indicator_name": "VWAP", "signal": signal}


def _calc_mfi(df: pd.DataFrame) -> dict:
    tp = (df["high"] + df["low"] + df["close"]) / 3
    mf = tp * df["volume"]
    tp_diff = tp.diff()
    pos_mf = mf.where(tp_diff > 0, 0.0)
    neg_mf = mf.where(tp_diff < 0, 0.0)
    pos_sum = pos_mf.rolling(14).sum()
    neg_sum = neg_mf.rolling(14).sum()
    mr = pos_sum / neg_sum.replace(0, np.nan)
    mfi = 100 - (100 / (1 + mr))
    current = mfi.iloc[-1]
    if pd.isna(current):
        return {"indicator_name": "MFI", "signal": "neutral"}
    if current >= 80:
        signal = "strong_sell"
    elif current >= 60:
        signal = "sell"
    elif current <= 20:
        signal = "strong_buy"
    elif current <= 40:
        signal = "buy"
    else:
        signal = "neutral"
    return {"indicator_name": "MFI", "signal": signal}


def _calc_support_resistance(df: pd.DataFrame) -> dict:
    resistance = df["high"].rolling(20).max().iloc[-1]
    support = df["low"].rolling(20).min().iloc[-1]
    price = df["close"].iloc[-1]
    if pd.isna(resistance) or pd.isna(support):
        return {"indicator_name": "SUPPORT_RESISTANCE", "signal": "neutral"}
    range_total = resistance - support
    position = (price - support) / range_total if range_total != 0 else 0.5
    if position >= 0.95:
        signal = "strong_sell"
    elif position >= 0.8:
        signal = "sell"
    elif position <= 0.05:
        signal = "strong_buy"
    elif position <= 0.2:
        signal = "buy"
    else:
        signal = "neutral"
    return {"indicator_name": "SUPPORT_RESISTANCE", "signal": signal}


def _calc_fibonacci(df: pd.DataFrame) -> dict:
    recent = df.tail(60)
    swing_high = recent["high"].max()
    swing_low = recent["low"].min()
    price = df["close"].iloc[-1]
    if pd.isna(swing_high) or swing_high == swing_low:
        return {"indicator_name": "FIBONACCI", "signal": "neutral"}
    position = (swing_high - price) / (swing_high - swing_low)
    if position >= 0.786:
        signal = "strong_buy"
    elif position >= 0.618:
        signal = "buy"
    elif position <= 0.236:
        signal = "sell"
    elif position <= 0.0:
        signal = "strong_sell"
    else:
        signal = "neutral"
    return {"indicator_name": "FIBONACCI", "signal": signal}


def _compute_indicators_from_df(df: pd.DataFrame) -> list[dict]:
    """Run all indicators on an OHLCV DataFrame. Returns list of {indicator_name, signal} dicts."""
    if len(df) < 26:
        return []
    fns = [
        lambda d: _calc_sma(d, 50),
        lambda d: _calc_sma(d, 200),
        lambda d: _calc_ema(d, 20),
        _calc_ema_cross,
        _calc_macd,
        _calc_ichimoku,
        _calc_rsi,
        _calc_stochastic,
        _calc_williams_r,
        _calc_cci,
        _calc_bollinger,
        _calc_adx,
        _calc_atr,
        _calc_obv,
        _calc_vwap,
        _calc_mfi,
        _calc_support_resistance,
        _calc_fibonacci,
    ]
    results = []
    for fn in fns:
        try:
            results.append(fn(df))
        except Exception:
            continue
    return results


async def get_historical_ohlcv(instrument_id: str) -> pd.DataFrame:
    """Fetch all historical OHLCV rows for an instrument, sorted ascending."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT date, open, high, low, close, volume
                FROM historical_prices
                WHERE instrument_id = :iid
                ORDER BY date ASC
            """),
            {"iid": instrument_id},
        )
        rows = result.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    df["volume"] = df["volume"].astype(float)
    df.set_index("date", inplace=True)
    return df


def calc_technical_score(
    df: pd.DataFrame,
    cutoff_date: date,
    category: str = "stock",
    term: str = "short",
) -> tuple[float, float]:
    """Compute technical score for a cutoff_date using data up to (and including) that date.

    Returns (score ∈ [-3, 3], data_completeness ∈ [0, 1]).
    No look-ahead bias: only uses rows with date <= cutoff_date.
    """
    cutoff = pd.Timestamp(cutoff_date)
    slice_df = df[df.index <= cutoff].copy()

    if len(slice_df) < 26:
        return 0.0, 0.0

    indicators = _compute_indicators_from_df(slice_df)
    if not indicators:
        return 0.0, 0.0

    # Build name -> signal map
    raw: dict[str, str] = {ind["indicator_name"]: ind["signal"] for ind in indicators}

    # Extract ADX and ATR modifiers
    adx_multiplier = 1.0
    atr_risk_factor = 1.0

    adx_info = next((i for i in indicators if i["indicator_name"] == "ADX"), None)
    if adx_info:
        adx_val = adx_info.get("adx_value", 0.0)
        if adx_val < 20:
            adx_multiplier = 0.70
        elif adx_val > 40:
            adx_multiplier = 1.25

    atr_info = next((i for i in indicators if i["indicator_name"] == "ATR"), None)
    if atr_info:
        atr_pct = atr_info.get("atr_pct", 2.0)
        if atr_pct > 5.0:
            atr_risk_factor = 0.65
        elif atr_pct > 3.5:
            atr_risk_factor = 0.80
        elif atr_pct > 2.5:
            atr_risk_factor = 0.92

    group_profile = GROUP_WEIGHT_PROFILES.get(category, GROUP_WEIGHT_PROFILES["stock"])
    group_weights = group_profile.get(term, group_profile["short"])

    group_scores: dict[str, tuple[float, int]] = {}
    for group_name, indicators_list in INDICATOR_GROUPS.items():
        present_scores = [SIGNAL_SCORES.get(raw[ind], 0.0) for ind in indicators_list if ind in raw]
        if not present_scores:
            group_scores[group_name] = (0.0, 0)
            continue
        avg = sum(present_scores) / len(present_scores)
        if group_name == "trend":
            avg *= adx_multiplier
        group_scores[group_name] = (_clip(avg), len(present_scores))

    total_weight = 0.0
    weighted_sum = 0.0
    for group_name, (score, count) in group_scores.items():
        if count == 0:
            continue
        w = group_weights.get(group_name, 0.0)
        completeness = count / len(INDICATOR_GROUPS[group_name])
        effective_w = w * completeness
        weighted_sum += score * effective_w
        total_weight += effective_w

    raw_tech = weighted_sum / total_weight if total_weight > 0 else 0.0

    # Trend-momentum divergence dampener
    t_score = group_scores.get("trend", (0.0, 0))[0]
    m_score = group_scores.get("momentum", (0.0, 0))[0]
    t_count = group_scores.get("trend", (0.0, 0))[1]
    m_count = group_scores.get("momentum", (0.0, 0))[1]
    if t_count >= 2 and m_count >= 2:
        divergence = abs(t_score - m_score)
        if divergence >= 1.5 and abs(t_score) > 0.5 and abs(m_score) > 0.5:
            raw_tech *= 0.80
        elif divergence >= 1.0 and abs(t_score) > 0.5 and abs(m_score) > 0.5:
            raw_tech *= 0.90

    final = _clip(raw_tech * atr_risk_factor)

    # Data completeness
    scored_names = {i["indicator_name"] for i in indicators if i["indicator_name"] not in ("ADX", "ATR")}
    total_slots = sum(len(v) for v in INDICATOR_GROUPS.values())
    completeness = len(scored_names) / total_slots if total_slots else 0.0

    return round(final, 4), round(completeness, 3)
