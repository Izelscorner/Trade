"""Technical analysis indicator calculations using pandas/numpy.

Institutional-grade indicator suite covering:
- Trend: SMA (50/200), EMA (20/50/200), MACD, Ichimoku Cloud, EMA Cross (Golden/Death)
- Momentum: RSI (Wilder's), Stochastic, Williams %R, CCI, MFI
- Volatility: Bollinger Bands, ATR
- Trend Strength: ADX
- Volume: OBV, VWAP
- Levels: Support/Resistance, Fibonacci Retracement
"""

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Trend Indicators
# ---------------------------------------------------------------------------

def calc_sma(df: pd.DataFrame, period: int = 50) -> dict:
    """Simple Moving Average."""
    sma = df["close"].rolling(window=period).mean()
    current = sma.iloc[-1]
    price = df["close"].iloc[-1]

    if pd.isna(current):
        return {"indicator_name": f"SMA_{period}", "value": {}, "signal": "neutral"}

    signal = "buy" if price > current else "sell"

    return {
        "indicator_name": f"SMA_{period}",
        "value": {"sma": round(float(current), 4), "price": round(float(price), 4), "period": period},
        "signal": signal,
    }


def calc_ema(df: pd.DataFrame, period: int = 20) -> dict:
    """Exponential Moving Average."""
    ema = df["close"].ewm(span=period, adjust=False).mean()
    current = ema.iloc[-1]
    price = df["close"].iloc[-1]

    if pd.isna(current):
        return {"indicator_name": f"EMA_{period}", "value": {}, "signal": "neutral"}

    signal = "buy" if price > current else "sell"

    return {
        "indicator_name": f"EMA_{period}",
        "value": {"ema": round(float(current), 4), "price": round(float(price), 4), "period": period},
        "signal": signal,
    }


def calc_ema_crossover(df: pd.DataFrame) -> dict:
    """EMA 50/200 Golden Cross / Death Cross detector.

    Golden Cross (EMA50 crosses above EMA200) = strong_buy
    Death Cross (EMA50 crosses below EMA200) = strong_sell
    """
    ema50 = df["close"].ewm(span=50, adjust=False).mean()
    ema200 = df["close"].ewm(span=200, adjust=False).mean()

    current_50 = ema50.iloc[-1]
    current_200 = ema200.iloc[-1]
    prev_50 = ema50.iloc[-2] if len(ema50) > 1 else current_50
    prev_200 = ema200.iloc[-2] if len(ema200) > 1 else current_200

    if pd.isna(current_50) or pd.isna(current_200):
        return {"indicator_name": "EMA_CROSS", "value": {}, "signal": "neutral"}

    # Detect crossovers
    if current_50 > current_200 and prev_50 <= prev_200:
        signal = "strong_buy"  # Golden Cross
    elif current_50 < current_200 and prev_50 >= prev_200:
        signal = "strong_sell"  # Death Cross
    elif current_50 > current_200:
        signal = "buy"  # Above — bullish alignment
    elif current_50 < current_200:
        signal = "sell"  # Below — bearish alignment
    else:
        signal = "neutral"

    return {
        "indicator_name": "EMA_CROSS",
        "value": {
            "ema50": round(float(current_50), 4),
            "ema200": round(float(current_200), 4),
            "spread": round(float(current_50 - current_200), 4),
        },
        "signal": signal,
    }


def calc_macd(df: pd.DataFrame) -> dict:
    """Moving Average Convergence Divergence."""
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line

    current_macd = macd_line.iloc[-1]
    current_signal = signal_line.iloc[-1]
    current_hist = histogram.iloc[-1]
    prev_hist = histogram.iloc[-2] if len(histogram) > 1 else 0

    if pd.isna(current_macd):
        return {"indicator_name": "MACD", "value": {}, "signal": "neutral"}

    if current_hist > 0 and prev_hist <= 0:
        signal = "strong_buy"
    elif current_hist > 0:
        signal = "buy"
    elif current_hist < 0 and prev_hist >= 0:
        signal = "strong_sell"
    elif current_hist < 0:
        signal = "sell"
    else:
        signal = "neutral"

    return {
        "indicator_name": "MACD",
        "value": {
            "macd": round(float(current_macd), 4),
            "signal_line": round(float(current_signal), 4),
            "histogram": round(float(current_hist), 4),
        },
        "signal": signal,
    }


def calc_ichimoku(df: pd.DataFrame) -> dict:
    """Ichimoku Cloud (Ichimoku Kinko Hyo).

    Tenkan-sen  (Conversion): 9-period midpoint
    Kijun-sen   (Base):       26-period midpoint
    Senkou A    (Leading A):  (Tenkan + Kijun) / 2
    Senkou B    (Leading B):  52-period midpoint
    """
    high9 = df["high"].rolling(9).max()
    low9 = df["low"].rolling(9).min()
    tenkan = (high9 + low9) / 2

    high26 = df["high"].rolling(26).max()
    low26 = df["low"].rolling(26).min()
    kijun = (high26 + low26) / 2

    senkou_a = (tenkan + kijun) / 2
    high52 = df["high"].rolling(52).max()
    low52 = df["low"].rolling(52).min()
    senkou_b = (high52 + low52) / 2

    price = df["close"].iloc[-1]
    t = tenkan.iloc[-1]
    k = kijun.iloc[-1]
    sa = senkou_a.iloc[-1]
    sb = senkou_b.iloc[-1]

    if any(pd.isna(v) for v in [t, k, sa, sb]):
        return {"indicator_name": "ICHIMOKU", "value": {}, "signal": "neutral"}

    cloud_top = max(sa, sb)
    cloud_bottom = min(sa, sb)

    # Signal: price above cloud + bullish TK cross
    if price > cloud_top and t > k:
        signal = "strong_buy"
    elif price > cloud_top:
        signal = "buy"
    elif price < cloud_bottom and t < k:
        signal = "strong_sell"
    elif price < cloud_bottom:
        signal = "sell"
    else:
        signal = "neutral"  # Inside the cloud — indecision

    return {
        "indicator_name": "ICHIMOKU",
        "value": {
            "tenkan": round(float(t), 4),
            "kijun": round(float(k), 4),
            "senkou_a": round(float(sa), 4),
            "senkou_b": round(float(sb), 4),
            "price": round(float(price), 4),
        },
        "signal": signal,
    }


# ---------------------------------------------------------------------------
# Momentum Indicators
# ---------------------------------------------------------------------------

def calc_rsi(df: pd.DataFrame, period: int = 14) -> dict:
    """Relative Strength Index using Wilder's smoothing (EMA).

    The standard RSI uses Wilder's exponential smoothing — NOT a simple
    rolling mean — for the average gain/loss.  This matches the
    industry-standard calculation used by Bloomberg, TradingView, etc.
    """
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    # Wilder's smoothing (equivalent to EMA with alpha = 1/period)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    current = rsi.iloc[-1]

    if pd.isna(current):
        return {"indicator_name": "RSI", "value": {}, "signal": "neutral"}

    if current >= 70:
        signal = "strong_sell"  # Overbought
    elif current >= 60:
        signal = "sell"
    elif current <= 30:
        signal = "strong_buy"  # Oversold
    elif current <= 40:
        signal = "buy"
    else:
        signal = "neutral"

    return {
        "indicator_name": "RSI",
        "value": {"rsi": round(float(current), 2), "period": period},
        "signal": signal,
    }


def calc_stochastic(df: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> dict:
    """Stochastic Oscillator (%K and %D)."""
    low_min = df["low"].rolling(window=k_period).min()
    high_max = df["high"].rolling(window=k_period).max()

    denom = high_max - low_min
    k = 100 * (df["close"] - low_min) / denom.replace(0, np.nan)
    d = k.rolling(window=d_period).mean()

    current_k = k.iloc[-1]
    current_d = d.iloc[-1]

    if pd.isna(current_k):
        return {"indicator_name": "STOCHASTIC", "value": {}, "signal": "neutral"}

    if current_k >= 80:
        signal = "sell"  # Overbought
    elif current_k <= 20:
        signal = "buy"  # Oversold
    elif current_k > current_d:
        signal = "buy"
    elif current_k < current_d:
        signal = "sell"
    else:
        signal = "neutral"

    return {
        "indicator_name": "STOCHASTIC",
        "value": {"k": round(float(current_k), 2), "d": round(float(current_d), 2)},
        "signal": signal,
    }


def calc_williams_r(df: pd.DataFrame, period: int = 14) -> dict:
    """Williams %R — momentum oscillator, inverse of Stochastic %K.

    Range: -100 (oversold) to 0 (overbought).
    """
    high_max = df["high"].rolling(window=period).max()
    low_min = df["low"].rolling(window=period).min()
    denom = high_max - low_min

    wr = -100 * (high_max - df["close"]) / denom.replace(0, np.nan)
    current = wr.iloc[-1]

    if pd.isna(current):
        return {"indicator_name": "WILLIAMS_R", "value": {}, "signal": "neutral"}

    if current >= -20:
        signal = "sell"  # Overbought
    elif current <= -80:
        signal = "buy"  # Oversold
    else:
        signal = "neutral"

    return {
        "indicator_name": "WILLIAMS_R",
        "value": {"williams_r": round(float(current), 2), "period": period},
        "signal": signal,
    }


def calc_cci(df: pd.DataFrame, period: int = 20) -> dict:
    """Commodity Channel Index — measures price deviation from mean.

    Important for commodities but useful for all asset classes.
    """
    tp = (df["high"] + df["low"] + df["close"]) / 3
    sma_tp = tp.rolling(window=period).mean()
    mad = tp.rolling(window=period).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)

    cci = (tp - sma_tp) / (0.015 * mad.replace(0, np.nan))
    current = cci.iloc[-1]

    if pd.isna(current):
        return {"indicator_name": "CCI", "value": {}, "signal": "neutral"}

    if current > 200:
        signal = "strong_sell"  # Extreme overbought
    elif current > 100:
        signal = "sell"  # Overbought
    elif current < -200:
        signal = "strong_buy"  # Extreme oversold
    elif current < -100:
        signal = "buy"  # Oversold
    else:
        signal = "neutral"

    return {
        "indicator_name": "CCI",
        "value": {"cci": round(float(current), 2), "period": period},
        "signal": signal,
    }


def calc_mfi(df: pd.DataFrame, period: int = 14) -> dict:
    """Money Flow Index — volume-weighted RSI.

    Combines price and volume to measure buying/selling pressure.
    """
    tp = (df["high"] + df["low"] + df["close"]) / 3
    mf = tp * df["volume"]

    pos_mf = pd.Series(0.0, index=df.index)
    neg_mf = pd.Series(0.0, index=df.index)

    tp_diff = tp.diff()
    pos_mf = mf.where(tp_diff > 0, 0.0)
    neg_mf = mf.where(tp_diff < 0, 0.0)

    pos_sum = pos_mf.rolling(window=period).sum()
    neg_sum = neg_mf.rolling(window=period).sum()

    mr = pos_sum / neg_sum.replace(0, np.nan)
    mfi = 100 - (100 / (1 + mr))
    current = mfi.iloc[-1]

    if pd.isna(current):
        return {"indicator_name": "MFI", "value": {}, "signal": "neutral"}

    if current >= 80:
        signal = "sell"  # Overbought
    elif current <= 20:
        signal = "buy"  # Oversold
    else:
        signal = "neutral"

    return {
        "indicator_name": "MFI",
        "value": {"mfi": round(float(current), 2), "period": period},
        "signal": signal,
    }


# ---------------------------------------------------------------------------
# Volatility Indicators
# ---------------------------------------------------------------------------

def calc_bollinger(df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> dict:
    """Bollinger Bands."""
    sma = df["close"].rolling(window=period).mean()
    std = df["close"].rolling(window=period).std()

    upper = sma + std_dev * std
    lower = sma - std_dev * std

    current_price = df["close"].iloc[-1]
    current_upper = upper.iloc[-1]
    current_lower = lower.iloc[-1]
    current_sma = sma.iloc[-1]

    if pd.isna(current_upper):
        return {"indicator_name": "BOLLINGER", "value": {}, "signal": "neutral"}

    band_width = (current_upper - current_lower) / current_sma if current_sma != 0 else 0

    if current_price >= current_upper:
        signal = "sell"  # At upper band — overbought
    elif current_price <= current_lower:
        signal = "buy"  # At lower band — oversold
    else:
        signal = "neutral"

    return {
        "indicator_name": "BOLLINGER",
        "value": {
            "upper": round(float(current_upper), 4),
            "middle": round(float(current_sma), 4),
            "lower": round(float(current_lower), 4),
            "price": round(float(current_price), 4),
            "bandwidth": round(float(band_width), 4),
        },
        "signal": signal,
    }


def calc_atr(df: pd.DataFrame, period: int = 14) -> dict:
    """Average True Range — measures market volatility.

    Used by institutions for position sizing, stop-loss placement,
    and risk management.
    """
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    # Wilder's smoothing for ATR
    atr = tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    current_atr = atr.iloc[-1]
    price = df["close"].iloc[-1]

    if pd.isna(current_atr) or price == 0:
        return {"indicator_name": "ATR", "value": {}, "signal": "neutral"}

    atr_pct = (current_atr / price) * 100

    # ATR itself doesn't give buy/sell — it measures volatility magnitude
    # High volatility (>3%) = uncertain, Low volatility (<1%) = consolidation
    if atr_pct > 3.0:
        signal = "neutral"  # High volatility — be cautious
    elif atr_pct < 1.0:
        signal = "neutral"  # Low volatility — potential breakout incoming
    else:
        signal = "neutral"

    return {
        "indicator_name": "ATR",
        "value": {
            "atr": round(float(current_atr), 4),
            "atr_percent": round(float(atr_pct), 2),
            "period": period,
        },
        "signal": signal,
    }


# ---------------------------------------------------------------------------
# Trend Strength
# ---------------------------------------------------------------------------

def calc_adx(df: pd.DataFrame, period: int = 14) -> dict:
    """Average Directional Index — measures trend strength regardless of direction.

    ADX > 25 = trending, ADX < 20 = no clear trend.
    +DI > -DI = bullish trend, -DI > +DI = bearish trend.
    """
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_high = high.shift(1)
    prev_low = low.shift(1)

    plus_dm = (high - prev_high).clip(lower=0)
    minus_dm = (prev_low - low).clip(lower=0)

    # When +DM > -DM, set -DM to 0 and vice versa
    plus_dm = plus_dm.where(plus_dm > minus_dm, 0)
    minus_dm = minus_dm.where(minus_dm > plus_dm, 0)

    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    # Wilder's smoothing
    alpha = 1.0 / period
    atr = tr.ewm(alpha=alpha, min_periods=period, adjust=False).mean()
    smooth_plus = plus_dm.ewm(alpha=alpha, min_periods=period, adjust=False).mean()
    smooth_minus = minus_dm.ewm(alpha=alpha, min_periods=period, adjust=False).mean()

    plus_di = 100 * smooth_plus / atr.replace(0, np.nan)
    minus_di = 100 * smooth_minus / atr.replace(0, np.nan)

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=alpha, min_periods=period, adjust=False).mean()

    current_adx = adx.iloc[-1]
    current_plus = plus_di.iloc[-1]
    current_minus = minus_di.iloc[-1]

    if pd.isna(current_adx):
        return {"indicator_name": "ADX", "value": {}, "signal": "neutral"}

    # ADX > 25 = strong trend; direction from DI comparison
    if current_adx > 25:
        if current_plus > current_minus:
            signal = "buy" if current_adx < 40 else "strong_buy"
        else:
            signal = "sell" if current_adx < 40 else "strong_sell"
    else:
        signal = "neutral"  # Weak trend — no directional conviction

    return {
        "indicator_name": "ADX",
        "value": {
            "adx": round(float(current_adx), 2),
            "plus_di": round(float(current_plus), 2),
            "minus_di": round(float(current_minus), 2),
        },
        "signal": signal,
    }


# ---------------------------------------------------------------------------
# Volume Indicators
# ---------------------------------------------------------------------------

def calc_obv(df: pd.DataFrame) -> dict:
    """On-Balance Volume."""
    direction = np.sign(df["close"].diff())
    obv = (direction * df["volume"]).fillna(0).cumsum()

    obv_sma = obv.rolling(window=20).mean()
    current_obv = obv.iloc[-1]
    current_sma = obv_sma.iloc[-1]

    if pd.isna(current_sma):
        return {"indicator_name": "OBV", "value": {}, "signal": "neutral"}

    if current_obv > current_sma:
        signal = "buy"
    elif current_obv < current_sma:
        signal = "sell"
    else:
        signal = "neutral"

    return {
        "indicator_name": "OBV",
        "value": {"obv": round(float(current_obv), 0), "obv_sma20": round(float(current_sma), 0)},
        "signal": signal,
    }


def calc_vwap(df: pd.DataFrame) -> dict:
    """Volume Weighted Average Price (rolling 20-day approximation).

    Institutional benchmark: price above VWAP = buyers in control,
    price below VWAP = sellers in control.
    """
    window = 20
    tp = (df["high"] + df["low"] + df["close"]) / 3
    vol = df["volume"].replace(0, np.nan)

    cum_tp_vol = (tp * vol).rolling(window=window).sum()
    cum_vol = vol.rolling(window=window).sum()
    vwap = cum_tp_vol / cum_vol.replace(0, np.nan)

    current_vwap = vwap.iloc[-1]
    price = df["close"].iloc[-1]

    if pd.isna(current_vwap):
        return {"indicator_name": "VWAP", "value": {}, "signal": "neutral"}

    deviation = ((price - current_vwap) / current_vwap) * 100

    if deviation > 2.0:
        signal = "sell"  # Far above VWAP — overextended
    elif deviation > 0:
        signal = "buy"  # Above VWAP — buyers in control
    elif deviation < -2.0:
        signal = "buy"  # Far below VWAP — potential bounce
    elif deviation < 0:
        signal = "sell"  # Below VWAP — sellers in control
    else:
        signal = "neutral"

    return {
        "indicator_name": "VWAP",
        "value": {
            "vwap": round(float(current_vwap), 4),
            "price": round(float(price), 4),
            "deviation_pct": round(float(deviation), 2),
        },
        "signal": signal,
    }


# ---------------------------------------------------------------------------
# Level Indicators
# ---------------------------------------------------------------------------

def calc_support_resistance(df: pd.DataFrame, window: int = 20) -> dict:
    """Support and resistance levels from rolling highs/lows."""
    resistance = df["high"].rolling(window=window).max().iloc[-1]
    support = df["low"].rolling(window=window).min().iloc[-1]
    price = df["close"].iloc[-1]

    if pd.isna(resistance) or pd.isna(support):
        return {"indicator_name": "SUPPORT_RESISTANCE", "value": {}, "signal": "neutral"}

    range_total = resistance - support
    if range_total == 0:
        position = 0.5
    else:
        position = (price - support) / range_total

    if position >= 0.9:
        signal = "sell"  # Near resistance
    elif position <= 0.1:
        signal = "buy"  # Near support
    else:
        signal = "neutral"

    return {
        "indicator_name": "SUPPORT_RESISTANCE",
        "value": {
            "resistance": round(float(resistance), 4),
            "support": round(float(support), 4),
            "price": round(float(price), 4),
            "position": round(float(position), 4),
        },
        "signal": signal,
    }


def calc_fibonacci(df: pd.DataFrame, lookback: int = 60) -> dict:
    """Fibonacci Retracement levels from recent swing high/low.

    Key institutional levels: 23.6%, 38.2%, 50%, 61.8%, 78.6%.
    """
    recent = df.tail(lookback)
    swing_high = recent["high"].max()
    swing_low = recent["low"].min()
    price = df["close"].iloc[-1]

    if pd.isna(swing_high) or pd.isna(swing_low) or swing_high == swing_low:
        return {"indicator_name": "FIBONACCI", "value": {}, "signal": "neutral"}

    diff = swing_high - swing_low
    levels = {
        "0.0": round(float(swing_high), 4),
        "23.6": round(float(swing_high - 0.236 * diff), 4),
        "38.2": round(float(swing_high - 0.382 * diff), 4),
        "50.0": round(float(swing_high - 0.500 * diff), 4),
        "61.8": round(float(swing_high - 0.618 * diff), 4),
        "78.6": round(float(swing_high - 0.786 * diff), 4),
        "100.0": round(float(swing_low), 4),
    }

    # Determine which zone price is in
    position = (swing_high - price) / diff

    # Near key support (61.8%, 78.6%) = buy zone
    # Near key resistance (23.6%, 0%) = sell zone
    if position >= 0.786:
        signal = "strong_buy"  # Deep retracement — strong support
    elif position >= 0.618:
        signal = "buy"  # Golden ratio level
    elif position <= 0.236:
        signal = "sell"  # Near swing high
    elif position <= 0.0:
        signal = "strong_sell"  # Above swing high — extended
    else:
        signal = "neutral"

    return {
        "indicator_name": "FIBONACCI",
        "value": {
            "swing_high": round(float(swing_high), 4),
            "swing_low": round(float(swing_low), 4),
            "price": round(float(price), 4),
            "retracement": round(float(position), 4),
            "levels": levels,
        },
        "signal": signal,
    }


# ---------------------------------------------------------------------------
# Master runner
# ---------------------------------------------------------------------------

def run_all_indicators(df: pd.DataFrame) -> list[dict]:
    """Run all technical indicators on a DataFrame with OHLCV columns.

    Institutional-grade suite: 18 indicators covering trend, momentum,
    volatility, volume, and key price levels.
    """
    if len(df) < 52:
        return []

    results = []
    indicator_fns = [
        # Trend (6)
        lambda d: calc_sma(d, 50),
        lambda d: calc_sma(d, 200),
        lambda d: calc_ema(d, 20),
        calc_ema_crossover,
        calc_macd,
        calc_ichimoku,
        # Momentum (5)
        calc_rsi,
        calc_stochastic,
        calc_williams_r,
        calc_cci,
        calc_mfi,
        # Volatility (2)
        calc_bollinger,
        calc_atr,
        # Trend Strength (1)
        calc_adx,
        # Volume (2)
        calc_obv,
        calc_vwap,
        # Levels (2)
        calc_support_resistance,
        calc_fibonacci,
    ]

    for calc_fn in indicator_fns:
        try:
            result = calc_fn(df)
            if result["value"]:
                results.append(result)
        except Exception:
            continue

    return results
