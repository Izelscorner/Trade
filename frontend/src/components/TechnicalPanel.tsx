/** Technical analysis indicators panel */

import type { TechnicalIndicator } from "../types";
import SignalBadge from "./SignalBadge";
import { Activity } from "lucide-react";

interface TechnicalPanelProps {
  indicators: TechnicalIndicator[];
}

const indicatorDescriptions: Record<string, string> = {
  SMA_50: "50-Day Simple Moving Average",
  SMA_200: "200-Day Simple Moving Average",
  EMA_20: "20-Day Exponential Moving Average",
  EMA_CROSS: "EMA 50/200 Golden Cross / Death Cross",
  MACD: "Moving Average Convergence Divergence",
  ICHIMOKU: "Ichimoku Cloud",
  RSI: "Relative Strength Index (Wilder's)",
  STOCHASTIC: "Stochastic Oscillator",
  WILLIAMS_R: "Williams %R",
  CCI: "Commodity Channel Index",
  MFI: "Money Flow Index",
  BOLLINGER: "Bollinger Bands",
  ATR: "Average True Range",
  ADX: "Average Directional Index",
  OBV: "On-Balance Volume",
  VWAP: "Volume Weighted Avg Price",
  SUPPORT_RESISTANCE: "Support & Resistance Levels",
  FIBONACCI: "Fibonacci Retracement",
};

function formatValue(name: string, value: Record<string, unknown>): string {
  const v = value as Record<string, number>;
  if (name === "RSI") return `RSI: ${v.rsi?.toFixed(1)}`;
  if (name === "MACD")
    return `MACD: ${v.macd?.toFixed(2)}, Signal: ${v.signal_line?.toFixed(2)}, Hist: ${v.histogram?.toFixed(2)}`;
  if (name === "STOCHASTIC")
    return `%K: ${v.k?.toFixed(1)}, %D: ${v.d?.toFixed(1)}`;
  if (name === "BOLLINGER")
    return `Upper: $${v.upper?.toFixed(2)}, Lower: $${v.lower?.toFixed(2)}, BW: ${v.bandwidth?.toFixed(3)}`;
  if (name === "OBV") return `OBV: ${(v.obv / 1_000_000).toFixed(1)}M`;
  if (name === "SUPPORT_RESISTANCE")
    return `S: $${v.support?.toFixed(2)}, R: $${v.resistance?.toFixed(2)}, Pos: ${(v.position * 100)?.toFixed(0)}%`;
  if (name === "EMA_CROSS")
    return `EMA50: $${v.ema50?.toFixed(2)}, EMA200: $${v.ema200?.toFixed(2)}, Spread: ${v.spread?.toFixed(2)}`;
  if (name === "ICHIMOKU")
    return `Tenkan: $${v.tenkan?.toFixed(2)}, Kijun: $${v.kijun?.toFixed(2)}`;
  if (name === "WILLIAMS_R") return `%R: ${v.williams_r?.toFixed(1)}`;
  if (name === "CCI") return `CCI: ${v.cci?.toFixed(1)}`;
  if (name === "MFI") return `MFI: ${v.mfi?.toFixed(1)}`;
  if (name === "ATR")
    return `ATR: $${v.atr?.toFixed(2)} (${v.atr_percent?.toFixed(1)}%)`;
  if (name === "ADX")
    return `ADX: ${v.adx?.toFixed(1)}, +DI: ${v.plus_di?.toFixed(1)}, -DI: ${v.minus_di?.toFixed(1)}`;
  if (name === "VWAP")
    return `VWAP: $${v.vwap?.toFixed(2)}, Dev: ${v.deviation_pct?.toFixed(1)}%`;
  if (name === "FIBONACCI")
    return `Retracement: ${(v.retracement * 100)?.toFixed(1)}%, High: $${v.swing_high?.toFixed(2)}, Low: $${v.swing_low?.toFixed(2)}`;
  if (name.startsWith("SMA") || name.startsWith("EMA")) {
    const key = name.startsWith("SMA") ? "sma" : "ema";
    return `${name}: $${v[key]?.toFixed(2)} (Price: $${v.price?.toFixed(2)})`;
  }
  return JSON.stringify(value);
}

export default function TechnicalPanel({ indicators }: TechnicalPanelProps) {
  const signalCounts = {
    buy: indicators.filter(
      (i) => i.signal === "buy" || i.signal === "strong_buy",
    ).length,
    neutral: indicators.filter((i) => i.signal === "neutral").length,
    sell: indicators.filter(
      (i) => i.signal === "sell" || i.signal === "strong_sell",
    ).length,
  };

  return (
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <Activity size={18} className="text-accent-cyan" />
          <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
            Technical Indicators
          </h2>
        </div>
        <div className="flex items-center gap-3 text-xs">
          <span className="text-signal-buy font-mono">
            {signalCounts.buy} Buy
          </span>
          <span className="text-signal-neutral font-mono">
            {signalCounts.neutral} Neutral
          </span>
          <span className="text-signal-sell font-mono">
            {signalCounts.sell} Sell
          </span>
        </div>
      </div>

      {/* Signal Summary Bar */}
      <div className="flex rounded-full h-2 overflow-hidden mb-5 bg-surface-3">
        {indicators.length > 0 && (
          <>
            <div
              className="bg-signal-strong-buy transition-all duration-500"
              style={{
                width: `${(signalCounts.buy / indicators.length) * 100}%`,
              }}
            />
            <div
              className="bg-signal-neutral transition-all duration-500"
              style={{
                width: `${(signalCounts.neutral / indicators.length) * 100}%`,
              }}
            />
            <div
              className="bg-signal-strong-sell transition-all duration-500"
              style={{
                width: `${(signalCounts.sell / indicators.length) * 100}%`,
              }}
            />
          </>
        )}
      </div>

      {/* Indicator List */}
      <div className="space-y-2">
        {indicators.length > 0 ? (
          indicators.map((ind) => (
            <div
              key={ind.indicator_name}
              className="flex items-center justify-between p-3 rounded-lg bg-surface-2/50 hover:bg-surface-3/50 transition-all"
            >
              <div>
                <p className="text-sm font-medium text-text-primary">
                  {ind.indicator_name}
                </p>
                <p className="text-[10px] text-text-muted">
                  {indicatorDescriptions[ind.indicator_name] ||
                    ind.indicator_name}
                </p>
                <p className="text-xs text-text-secondary font-mono mt-0.5">
                  {formatValue(ind.indicator_name, ind.value)}
                </p>
              </div>
              <SignalBadge signal={ind.signal} />
            </div>
          ))
        ) : (
          <p className="text-text-muted text-sm text-center py-4">
            No technical data available yet
          </p>
        )}
      </div>
    </div>
  );
}
