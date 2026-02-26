"""Technical analysis indicator calculations using pandas/numpy."""

import json
import numpy as np
import pandas as pd


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

    # Bullish: MACD crosses above signal; Bearish: crosses below
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


def calc_rsi(df: pd.DataFrame, period: int = 14) -> dict:
    """Relative Strength Index."""
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

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
        signal = "sell"  # At upper band - overbought
    elif current_price <= current_lower:
        signal = "buy"  # At lower band - oversold
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


def calc_obv(df: pd.DataFrame) -> dict:
    """On-Balance Volume."""
    obv = pd.Series(0, index=df.index, dtype=float)
    for i in range(1, len(df)):
        if df["close"].iloc[i] > df["close"].iloc[i - 1]:
            obv.iloc[i] = obv.iloc[i - 1] + df["volume"].iloc[i]
        elif df["close"].iloc[i] < df["close"].iloc[i - 1]:
            obv.iloc[i] = obv.iloc[i - 1] - df["volume"].iloc[i]
        else:
            obv.iloc[i] = obv.iloc[i - 1]

    # Trend: compare OBV SMA to detect divergence
    obv_sma = obv.rolling(window=20).mean()
    current_obv = obv.iloc[-1]
    current_sma = obv_sma.iloc[-1]

    if pd.isna(current_sma):
        return {"indicator_name": "OBV", "value": {}, "signal": "neutral"}

    if current_obv > current_sma:
        signal = "buy"  # Volume confirming uptrend
    elif current_obv < current_sma:
        signal = "sell"  # Volume confirming downtrend
    else:
        signal = "neutral"

    return {
        "indicator_name": "OBV",
        "value": {"obv": round(float(current_obv), 0), "obv_sma20": round(float(current_sma), 0)},
        "signal": signal,
    }


def calc_support_resistance(df: pd.DataFrame, window: int = 20) -> dict:
    """Basic support and resistance levels from rolling highs/lows."""
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


def run_all_indicators(df: pd.DataFrame) -> list[dict]:
    """Run all technical indicators on a DataFrame with OHLCV columns."""
    if len(df) < 50:
        return []

    results = []
    for calc_fn in [
        lambda d: calc_sma(d, 50),
        lambda d: calc_sma(d, 200),
        lambda d: calc_ema(d, 20),
        calc_macd,
        calc_rsi,
        calc_stochastic,
        calc_bollinger,
        calc_obv,
        calc_support_resistance,
    ]:
        try:
            result = calc_fn(df)
            if result["value"]:
                results.append(result)
        except Exception:
            continue

    return results
