"""Reconstruct technical indicator scores from historical OHLCV data.

Vectorized approach: pre-compute all 18 indicator series once on the full
DataFrame, then for any cutoff_date just index into the pre-computed series.
This avoids recomputing rolling windows for every backtest date (~1000x faster).
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
# Vectorized indicator series (computed once on full DataFrame)
# ---------------------------------------------------------------------------

class PrecomputedIndicators:
    """Pre-compute all 18 indicator signal series on the full OHLCV DataFrame.

    Each indicator becomes a pd.Series of signal strings indexed by date.
    ADX and ATR store their numeric values as separate Series.
    """

    def __init__(self, df: pd.DataFrame):
        self.valid_from_idx = 0  # index where we have enough data (>=26 rows)
        n = len(df)
        if n < 26:
            self.signals: dict[str, pd.Series] = {}
            self.adx_values = pd.Series(dtype=float)
            self.atr_pct_values = pd.Series(dtype=float)
            return

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]
        idx = df.index

        self.signals = {}

        # -- SMA_50 --
        sma50 = close.rolling(50).mean()
        dev50 = ((close - sma50) / sma50) * 100
        thresh50 = min(1.0 + (50 // 50), 3.0)  # = 2.0
        self.signals["SMA_50"] = _vectorized_deviation_signal(dev50, thresh50)

        # -- SMA_200 --
        sma200 = close.rolling(200).mean()
        dev200 = ((close - sma200) / sma200) * 100
        thresh200 = min(1.0 + (200 // 50), 3.0)  # = 3.0
        self.signals["SMA_200"] = _vectorized_deviation_signal(dev200, thresh200)

        # -- EMA_20 --
        ema20 = close.ewm(span=20, adjust=False).mean()
        dev20 = ((close - ema20) / ema20) * 100
        thresh20 = min(1.0 + (20 // 50), 3.0)  # = 1.0
        self.signals["EMA_20"] = _vectorized_deviation_signal(dev20, thresh20)

        # -- EMA_CROSS (EMA50 vs EMA200) --
        ema50 = close.ewm(span=50, adjust=False).mean()
        ema200_ = close.ewm(span=200, adjust=False).mean()
        prev_ema50 = ema50.shift(1)
        prev_ema200 = ema200_.shift(1)
        sig_cross = pd.Series("neutral", index=idx)
        golden = (ema50 > ema200_) & (prev_ema50 <= prev_ema200)
        death = (ema50 < ema200_) & (prev_ema50 >= prev_ema200)
        above = (ema50 > ema200_) & ~golden
        below = (ema50 < ema200_) & ~death
        # Assign weaker signals first, stronger override
        sig_cross[above] = "buy"
        sig_cross[below] = "sell"
        sig_cross[golden] = "strong_buy"
        sig_cross[death] = "strong_sell"
        sig_cross[ema50.isna() | ema200_.isna()] = "neutral"
        self.signals["EMA_CROSS"] = sig_cross

        # -- MACD --
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        histogram = macd_line - signal_line
        prev_hist = histogram.shift(1)
        sig_macd = pd.Series("neutral", index=idx)
        # Weaker signals first, stronger override
        sig_macd[histogram > 0] = "buy"
        sig_macd[histogram < 0] = "sell"
        sig_macd[(histogram > 0) & (prev_hist <= 0)] = "strong_buy"
        sig_macd[(histogram < 0) & (prev_hist >= 0)] = "strong_sell"
        sig_macd[histogram.isna()] = "neutral"
        self.signals["MACD"] = sig_macd

        # -- ICHIMOKU --
        tenkan = (high.rolling(9).max() + low.rolling(9).min()) / 2
        kijun = (high.rolling(26).max() + low.rolling(26).min()) / 2
        senkou_a = (tenkan + kijun) / 2
        senkou_b = (high.rolling(52).max() + low.rolling(52).min()) / 2
        cloud_top = pd.concat([senkou_a, senkou_b], axis=1).max(axis=1)
        cloud_bottom = pd.concat([senkou_a, senkou_b], axis=1).min(axis=1)
        sig_ichi = pd.Series("neutral", index=idx)
        # Weaker signals first, stronger override
        sig_ichi[close > cloud_top] = "buy"
        sig_ichi[close < cloud_bottom] = "sell"
        sig_ichi[(close > cloud_top) & (tenkan > kijun)] = "strong_buy"
        sig_ichi[(close < cloud_bottom) & (tenkan < kijun)] = "strong_sell"
        sig_ichi[tenkan.isna() | kijun.isna() | senkou_a.isna() | senkou_b.isna()] = "neutral"
        self.signals["ICHIMOKU"] = sig_ichi

        # -- RSI (Wilder's smoothing) --
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss_s = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        avg_loss = loss_s.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        sig_rsi = pd.Series("neutral", index=idx)
        sig_rsi[rsi >= 70] = "strong_sell"
        sig_rsi[(rsi >= 60) & (rsi < 70)] = "sell"
        sig_rsi[rsi <= 30] = "strong_buy"
        sig_rsi[(rsi > 30) & (rsi <= 40)] = "buy"
        sig_rsi[rsi.isna()] = "neutral"
        self.signals["RSI"] = sig_rsi

        # -- STOCHASTIC --
        low_min14 = low.rolling(14).min()
        high_max14 = high.rolling(14).max()
        denom_stoch = (high_max14 - low_min14).replace(0, np.nan)
        k_pct = 100 * (close - low_min14) / denom_stoch
        d_pct = k_pct.rolling(3).mean()
        sig_stoch = pd.Series("neutral", index=idx)
        sig_stoch[k_pct >= 80] = "strong_sell"
        sig_stoch[(k_pct >= 60) & (k_pct < 80)] = "sell"
        sig_stoch[k_pct <= 20] = "strong_buy"
        sig_stoch[(k_pct > 20) & (k_pct <= 40)] = "buy"
        # K vs D crossover for middle range
        mid_range = (k_pct > 40) & (k_pct < 60)
        sig_stoch[mid_range & (k_pct > d_pct) & ~d_pct.isna()] = "buy"
        sig_stoch[mid_range & (k_pct < d_pct) & ~d_pct.isna()] = "sell"
        sig_stoch[k_pct.isna()] = "neutral"
        self.signals["STOCHASTIC"] = sig_stoch

        # -- WILLIAMS_R --
        wr = -100 * (high_max14 - close) / denom_stoch
        sig_wr = pd.Series("neutral", index=idx)
        sig_wr[wr >= -20] = "strong_sell"
        sig_wr[(wr >= -40) & (wr < -20)] = "sell"
        sig_wr[wr <= -80] = "strong_buy"
        sig_wr[(wr > -80) & (wr <= -60)] = "buy"
        sig_wr[wr.isna()] = "neutral"
        self.signals["WILLIAMS_R"] = sig_wr

        # -- CCI --
        tp = (high + low + close) / 3
        sma_tp = tp.rolling(20).mean()
        mad = tp.rolling(20).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
        cci = (tp - sma_tp) / (0.015 * mad.replace(0, np.nan))
        sig_cci = pd.Series("neutral", index=idx)
        sig_cci[cci > 200] = "strong_sell"
        sig_cci[(cci > 100) & (cci <= 200)] = "sell"
        sig_cci[cci < -200] = "strong_buy"
        sig_cci[(cci < -100) & (cci >= -200)] = "buy"
        sig_cci[cci.isna()] = "neutral"
        self.signals["CCI"] = sig_cci

        # -- BOLLINGER --
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        upper_bb = sma20 + 2 * std20
        lower_bb = sma20 - 2 * std20
        sig_bb = pd.Series("neutral", index=idx)
        sig_bb[close >= upper_bb * 1.02] = "strong_sell"
        sig_bb[(close >= upper_bb) & (close < upper_bb * 1.02)] = "sell"
        sig_bb[close <= lower_bb * 0.98] = "strong_buy"
        sig_bb[(close <= lower_bb) & (close > lower_bb * 0.98)] = "buy"
        sig_bb[upper_bb.isna()] = "neutral"
        self.signals["BOLLINGER"] = sig_bb

        # -- ADX --
        ph = high.shift(1)
        pl = low.shift(1)
        plus_dm = (high - ph).clip(lower=0)
        minus_dm = (pl - low).clip(lower=0)
        plus_dm = plus_dm.where(plus_dm > minus_dm, 0)
        minus_dm = minus_dm.where(minus_dm > plus_dm, 0)
        tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
        alpha_adx = 1.0 / 14
        atr_raw = tr.ewm(alpha=alpha_adx, min_periods=14, adjust=False).mean()
        sp = plus_dm.ewm(alpha=alpha_adx, min_periods=14, adjust=False).mean()
        sm = minus_dm.ewm(alpha=alpha_adx, min_periods=14, adjust=False).mean()
        pdi = 100 * sp / atr_raw.replace(0, np.nan)
        mdi = 100 * sm / atr_raw.replace(0, np.nan)
        dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
        adx = dx.ewm(alpha=alpha_adx, min_periods=14, adjust=False).mean()
        sig_adx = pd.Series("neutral", index=idx)
        trending = adx > 25
        strong_trend = adx > 40
        sig_adx[trending & (pdi > mdi) & ~strong_trend] = "buy"
        sig_adx[trending & (pdi > mdi) & strong_trend] = "strong_buy"
        sig_adx[trending & (pdi <= mdi) & ~strong_trend] = "sell"
        sig_adx[trending & (pdi <= mdi) & strong_trend] = "strong_sell"
        sig_adx[adx.isna()] = "neutral"
        self.signals["ADX"] = sig_adx
        self.adx_values = adx

        # -- ATR --
        atr_series = tr.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        self.atr_pct_values = (atr_series / close.replace(0, np.nan)) * 100

        # -- OBV --
        direction = np.sign(close.diff())
        obv = (direction * volume).fillna(0).cumsum()
        obv_sma = obv.rolling(20).mean()
        obv_std = obv.rolling(20).std()
        sig_obv = pd.Series("neutral", index=idx)
        sig_obv[obv > obv_sma + obv_std] = "strong_buy"
        sig_obv[(obv > obv_sma) & (obv <= obv_sma + obv_std)] = "buy"
        sig_obv[obv < obv_sma - obv_std] = "strong_sell"
        sig_obv[(obv < obv_sma) & (obv >= obv_sma - obv_std)] = "sell"
        sig_obv[obv_sma.isna() | obv_std.isna()] = "neutral"
        self.signals["OBV"] = sig_obv

        # -- VWAP (20-day rolling) --
        tp_vwap = (high + low + close) / 3
        vol_clean = volume.replace(0, np.nan)
        vwap = (tp_vwap * vol_clean).rolling(20).sum() / vol_clean.rolling(20).sum().replace(0, np.nan)
        dev_vwap = ((close - vwap) / vwap) * 100
        sig_vwap = pd.Series("neutral", index=idx)
        sig_vwap[dev_vwap > 2.0] = "strong_buy"
        sig_vwap[(dev_vwap > 0) & (dev_vwap <= 2.0)] = "buy"
        sig_vwap[dev_vwap < -2.0] = "strong_sell"
        sig_vwap[(dev_vwap < 0) & (dev_vwap >= -2.0)] = "sell"
        sig_vwap[vwap.isna()] = "neutral"
        self.signals["VWAP"] = sig_vwap

        # -- MFI --
        tp_mfi = (high + low + close) / 3
        mf = tp_mfi * volume
        tp_diff = tp_mfi.diff()
        pos_mf = mf.where(tp_diff > 0, 0.0)
        neg_mf = mf.where(tp_diff < 0, 0.0)
        pos_sum = pos_mf.rolling(14).sum()
        neg_sum = neg_mf.rolling(14).sum()
        mr = pos_sum / neg_sum.replace(0, np.nan)
        mfi = 100 - (100 / (1 + mr))
        sig_mfi = pd.Series("neutral", index=idx)
        sig_mfi[mfi >= 80] = "strong_sell"
        sig_mfi[(mfi >= 60) & (mfi < 80)] = "sell"
        sig_mfi[mfi <= 20] = "strong_buy"
        sig_mfi[(mfi > 20) & (mfi <= 40)] = "buy"
        sig_mfi[mfi.isna()] = "neutral"
        self.signals["MFI"] = sig_mfi

        # -- SUPPORT_RESISTANCE (20-day range) --
        resistance = high.rolling(20).max()
        support = low.rolling(20).min()
        range_total = resistance - support
        position_sr = (close - support) / range_total.replace(0, np.nan)
        # Default to 0.5 where range is 0
        position_sr = position_sr.fillna(0.5)
        sig_sr = pd.Series("neutral", index=idx)
        sig_sr[position_sr >= 0.95] = "strong_sell"
        sig_sr[(position_sr >= 0.8) & (position_sr < 0.95)] = "sell"
        sig_sr[position_sr <= 0.05] = "strong_buy"
        sig_sr[(position_sr > 0.05) & (position_sr <= 0.2)] = "buy"
        sig_sr[resistance.isna()] = "neutral"
        self.signals["SUPPORT_RESISTANCE"] = sig_sr

        # -- FIBONACCI (60-day retracement) --
        swing_high_60 = high.rolling(60).max()
        swing_low_60 = low.rolling(60).min()
        fib_range = swing_high_60 - swing_low_60
        fib_position = (swing_high_60 - close) / fib_range.replace(0, np.nan)
        sig_fib = pd.Series("neutral", index=idx)
        sig_fib[fib_position >= 0.786] = "strong_buy"
        sig_fib[(fib_position >= 0.618) & (fib_position < 0.786)] = "buy"
        sig_fib[(fib_position <= 0.236) & (fib_position > 0.0)] = "sell"
        sig_fib[fib_position <= 0.0] = "strong_sell"
        sig_fib[swing_high_60.isna() | (fib_range == 0)] = "neutral"
        self.signals["FIBONACCI"] = sig_fib

        # Determine first valid index (need at least 26 rows of data)
        self.valid_from_idx = 25  # 0-indexed, so 26th row

    def _resolve_idx(self, cutoff: pd.Timestamp) -> int | None:
        """Find the positional index for the last date <= cutoff using searchsorted.

        Returns None if insufficient data (< 26 rows before cutoff).
        """
        if not self.signals:
            return None
        # Use the first signal's index (all share the same index)
        idx = next(iter(self.signals.values())).index
        pos = idx.searchsorted(cutoff, side="right") - 1
        if pos < 25:  # Need at least 26 rows
            return None
        return pos

    def get_signals_at(self, cutoff: pd.Timestamp) -> dict[str, str] | None:
        """Get all indicator signals at a specific date.

        Returns dict of {indicator_name: signal} or None if insufficient data.
        """
        pos = self._resolve_idx(cutoff)
        if pos is None:
            return None

        return {name: series.iloc[pos] for name, series in self.signals.items()}

    def get_adx_value_at(self, cutoff: pd.Timestamp) -> float:
        pos = self._resolve_idx(cutoff)
        if pos is None:
            return 0.0
        v = self.adx_values.iloc[pos]
        return float(v) if not pd.isna(v) else 0.0

    def get_atr_pct_at(self, cutoff: pd.Timestamp) -> float:
        pos = self._resolve_idx(cutoff)
        if pos is None:
            return 2.0
        v = self.atr_pct_values.iloc[pos]
        return float(v) if not pd.isna(v) else 2.0


def _vectorized_deviation_signal(deviation: pd.Series, strong_threshold: float) -> pd.Series:
    """Convert price-vs-indicator deviation to signal series."""
    sig = pd.Series("neutral", index=deviation.index)
    sig[deviation > strong_threshold] = "strong_buy"
    sig[(deviation > 0) & (deviation <= strong_threshold)] = "buy"
    sig[deviation < -strong_threshold] = "strong_sell"
    sig[(deviation < 0) & (deviation >= -strong_threshold)] = "sell"
    sig[deviation.isna()] = "neutral"
    return sig


def precompute_indicators(df: pd.DataFrame) -> PrecomputedIndicators:
    """Pre-compute all technical indicator series on the full OHLCV DataFrame.

    Call this once per instrument, then use calc_technical_score_fast() for each date.
    """
    return PrecomputedIndicators(df)


# ---------------------------------------------------------------------------
# OHLCV loading (unchanged)
# ---------------------------------------------------------------------------

_yf_ohlcv_cache: dict[str, pd.DataFrame] = {}


async def get_historical_ohlcv(
    instrument_id: str,
    yfinance_symbol: str | None = None,
    use_yfinance: bool = False,
) -> pd.DataFrame:
    """Fetch historical OHLCV data for an instrument.

    Tries DB first, falls back to yfinance if DB has no data or use_yfinance=True.
    """
    # Try DB first unless forced to yfinance
    if not use_yfinance:
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

        if rows:
            df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
            df["date"] = pd.to_datetime(df["date"])
            for col in ["open", "high", "low", "close"]:
                df[col] = df[col].astype(float)
            df["volume"] = df["volume"].astype(float)
            df.set_index("date", inplace=True)
            return df

    # Fallback / backtest-only: fetch from yfinance with cache
    if not yfinance_symbol:
        return pd.DataFrame()

    return _fetch_yfinance_ohlcv(yfinance_symbol)


def _fetch_yfinance_ohlcv(yf_symbol: str) -> pd.DataFrame:
    """Fetch max OHLCV history from yfinance with disk + memory caching."""
    import json
    import os

    if yf_symbol in _yf_ohlcv_cache:
        return _yf_ohlcv_cache[yf_symbol]

    cache_dir = "/cache/ohlcv"
    os.makedirs(cache_dir, exist_ok=True)
    safe_name = yf_symbol.replace("=", "_").replace("/", "_")
    cache_path = os.path.join(cache_dir, f"{safe_name}.parquet")

    if os.path.exists(cache_path):
        try:
            df = pd.read_parquet(cache_path)
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            _yf_ohlcv_cache[yf_symbol] = df
            logger.info("[%s] OHLCV loaded from cache (%d rows)", yf_symbol, len(df))
            return df
        except Exception as e:
            logger.warning("[%s] Cache read failed: %s", yf_symbol, e)

    # Fetch from yfinance
    try:
        import yfinance as yf

        logger.info("[%s] Fetching OHLCV from yfinance (max history)...", yf_symbol)
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(period="max", auto_adjust=True)

        if df.empty:
            logger.warning("[%s] No yfinance data returned", yf_symbol)
            return pd.DataFrame()

        # Normalize columns
        df.columns = [c.lower() for c in df.columns]
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = df[col].astype(float)
        if "volume" in df.columns:
            df["volume"] = df["volume"].astype(float)

        # Keep only needed columns
        keep_cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
        df = df[keep_cols]

        # Remove timezone info from index
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        # Save to cache
        df.to_parquet(cache_path)
        _yf_ohlcv_cache[yf_symbol] = df
        logger.info("[%s] OHLCV fetched: %d rows (%s to %s)",
                    yf_symbol, len(df), df.index[0].date(), df.index[-1].date())
        return df

    except Exception as e:
        logger.warning("[%s] yfinance fetch failed: %s", yf_symbol, e)
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Fast scoring (uses pre-computed indicators)
# ---------------------------------------------------------------------------

def calc_technical_score(
    df: pd.DataFrame,
    cutoff_date: date,
    category: str = "stock",
    term: str = "short",
    precomputed: PrecomputedIndicators | None = None,
) -> tuple[float, float]:
    """Compute technical score for a cutoff_date.

    If precomputed is provided, uses the pre-computed indicator series (fast path).
    Otherwise falls back to computing from scratch (slow path, for single-date use).

    Returns (score in [-3, 3], data_completeness in [0, 1]).
    """
    cutoff = pd.Timestamp(cutoff_date)

    if precomputed is not None:
        return _score_from_precomputed(precomputed, cutoff, category, term)

    # Slow path: compute from scratch (kept for backward compatibility)
    slice_df = df[df.index <= cutoff].copy()
    if len(slice_df) < 26:
        return 0.0, 0.0

    indicators = _compute_indicators_from_df(slice_df)
    if not indicators:
        return 0.0, 0.0

    return _score_from_indicators(indicators, category, term)


def _score_from_precomputed(
    pre: PrecomputedIndicators,
    cutoff: pd.Timestamp,
    category: str,
    term: str,
) -> tuple[float, float]:
    """Score using pre-computed indicator series (fast path)."""
    raw = pre.get_signals_at(cutoff)
    if raw is None:
        return 0.0, 0.0

    # ADX modifier
    adx_multiplier = 1.0
    adx_val = pre.get_adx_value_at(cutoff)
    adx_sig = raw.get("ADX", "neutral")
    if adx_sig == "neutral" or adx_val < 20:
        adx_multiplier = 0.70
    elif adx_sig in ("strong_buy", "strong_sell") or adx_val > 40:
        adx_multiplier = 1.25

    # ATR risk factor
    atr_risk_factor = 1.0
    atr_pct = pre.get_atr_pct_at(cutoff)
    if atr_pct > 5.0:
        atr_risk_factor = 0.65
    elif atr_pct > 3.5:
        atr_risk_factor = 0.80
    elif atr_pct > 2.5:
        atr_risk_factor = 0.92

    return _compute_weighted_score(raw, adx_multiplier, atr_risk_factor, category, term)


def _score_from_indicators(
    indicators: list[dict],
    category: str,
    term: str,
) -> tuple[float, float]:
    """Score from indicator dicts (slow path)."""
    raw = {ind["indicator_name"]: ind["signal"] for ind in indicators}

    adx_multiplier = 1.0
    atr_risk_factor = 1.0

    adx_info = next((i for i in indicators if i["indicator_name"] == "ADX"), None)
    if adx_info:
        adx_val = adx_info.get("adx_value", 0.0)
        adx_sig = adx_info.get("signal", "neutral")
        if adx_sig == "neutral" or adx_val < 20:
            adx_multiplier = 0.70
        elif adx_sig in ("strong_buy", "strong_sell") or adx_val > 40:
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

    return _compute_weighted_score(raw, adx_multiplier, atr_risk_factor, category, term)


def _compute_weighted_score(
    raw: dict[str, str],
    adx_multiplier: float,
    atr_risk_factor: float,
    category: str,
    term: str,
) -> tuple[float, float]:
    """Shared scoring logic: group-weighted average with modifiers."""
    cat_key = category.lower()
    group_profile = GROUP_WEIGHT_PROFILES.get(cat_key, GROUP_WEIGHT_PROFILES["stock"])
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
    scored_names = {ind for ind in raw if ind not in ("ADX", "ATR")}
    total_slots = sum(len(v) for v in INDICATOR_GROUPS.values())
    completeness = len(scored_names) / total_slots if total_slots else 0.0

    return round(final, 4), round(completeness, 3)


# ---------------------------------------------------------------------------
# Legacy per-date indicator computation (kept for backward compat / single use)
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
    d = k.rolling(3).mean()
    current_k = k.iloc[-1]
    current_d = d.iloc[-1]
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
    elif not pd.isna(current_d) and current_k > current_d:
        signal = "buy"
    elif not pd.isna(current_d) and current_k < current_d:
        signal = "sell"
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
    cpdi = pdi.iloc[-1]
    cmdi = mdi.iloc[-1]
    if pd.isna(cadx):
        return {"indicator_name": "ADX", "signal": "neutral", "adx_value": 0.0}
    if cadx > 25:
        if cpdi > cmdi:
            signal = "buy" if cadx < 40 else "strong_buy"
        else:
            signal = "sell" if cadx < 40 else "strong_sell"
    else:
        signal = "neutral"
    return {"indicator_name": "ADX", "signal": signal, "adx_value": float(cadx)}


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
