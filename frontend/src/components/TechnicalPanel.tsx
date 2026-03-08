/** Technical analysis indicators panel — grouped by category */

import { useMemo } from "react";
import type { TechnicalIndicator } from "../types";
import SignalBadge from "./SignalBadge";
import { Activity, TrendingUp, Zap, BarChart2, Target, Wind } from "lucide-react";

interface TechnicalPanelProps {
  indicators: TechnicalIndicator[];
}

// ---------------------------------------------------------------------------
// Indicator group definitions (mirrors scorer.py INDICATOR_GROUPS)
// ---------------------------------------------------------------------------
const GROUP_META: Record<string, { label: string; icon: React.ElementType; indicators: string[]; color: string }> = {
  trend:      { label: "Trend",      icon: TrendingUp, color: "text-blue-400",   indicators: ["SMA_50", "SMA_200", "EMA_20", "EMA_CROSS", "MACD", "ICHIMOKU"] },
  momentum:   { label: "Momentum",   icon: Zap,        color: "text-violet-400", indicators: ["RSI", "STOCHASTIC", "WILLIAMS_R", "CCI"] },
  volume:     { label: "Volume",     icon: BarChart2,   color: "text-cyan-400",   indicators: ["OBV", "VWAP", "MFI"] },
  levels:     { label: "Levels",     icon: Target,     color: "text-amber-400",  indicators: ["SUPPORT_RESISTANCE", "FIBONACCI"] },
  volatility: { label: "Volatility", icon: Wind,       color: "text-slate-400",  indicators: ["BOLLINGER"] },
  modifiers:  { label: "Modifiers",  icon: Activity,   color: "text-text-muted", indicators: ["ADX", "ATR"] },
};

// Reverse lookup
const INDICATOR_TO_GROUP: Record<string, string> = {};
for (const [grp, meta] of Object.entries(GROUP_META)) {
  for (const ind of meta.indicators) INDICATOR_TO_GROUP[ind] = grp;
}

const INDICATOR_DESCRIPTIONS: Record<string, string> = {
  SMA_50: "50-Day Simple Moving Average",
  SMA_200: "200-Day Simple Moving Average",
  EMA_20: "20-Day Exponential Moving Average",
  EMA_CROSS: "EMA 50/200 Golden/Death Cross",
  MACD: "Moving Average Convergence Divergence",
  ICHIMOKU: "Ichimoku Cloud",
  RSI: "Relative Strength Index (Wilder's)",
  STOCHASTIC: "Stochastic Oscillator",
  WILLIAMS_R: "Williams %R",
  CCI: "Commodity Channel Index",
  MFI: "Money Flow Index (Volume-Weighted RSI)",
  BOLLINGER: "Bollinger Bands",
  ATR: "Average True Range (risk modifier)",
  ADX: "Average Directional Index (trend strength multiplier)",
  OBV: "On-Balance Volume",
  VWAP: "Volume Weighted Average Price",
  SUPPORT_RESISTANCE: "Support & Resistance Levels",
  FIBONACCI: "Fibonacci Retracement",
};

// ---------------------------------------------------------------------------
// Value formatter
// ---------------------------------------------------------------------------
function formatValue(name: string, value: Record<string, unknown>): string {
  const v = value as Record<string, number>;
  if (name === "RSI")               return `RSI: ${v.rsi?.toFixed(1)}`;
  if (name === "MACD")              return `MACD: ${v.macd?.toFixed(2)}, Sig: ${v.signal_line?.toFixed(2)}, H: ${v.histogram?.toFixed(3)}`;
  if (name === "STOCHASTIC")        return `%K: ${v.k?.toFixed(1)}, %D: ${v.d?.toFixed(1)}`;
  if (name === "BOLLINGER")         return `U: $${v.upper?.toFixed(2)}, L: $${v.lower?.toFixed(2)}, BW: ${v.bandwidth?.toFixed(3)}`;
  if (name === "OBV")               return `OBV: ${(v.obv / 1_000_000).toFixed(2)}M`;
  if (name === "SUPPORT_RESISTANCE") return `S: $${v.support?.toFixed(2)}, R: $${v.resistance?.toFixed(2)}, Pos: ${(v.position * 100)?.toFixed(0)}%`;
  if (name === "EMA_CROSS")         return `EMA50: $${v.ema50?.toFixed(2)}, EMA200: $${v.ema200?.toFixed(2)}`;
  if (name === "ICHIMOKU")          return `Tenkan: $${v.tenkan?.toFixed(2)}, Kijun: $${v.kijun?.toFixed(2)}`;
  if (name === "WILLIAMS_R")        return `%R: ${v.williams_r?.toFixed(1)}`;
  if (name === "CCI")               return `CCI: ${v.cci?.toFixed(1)}`;
  if (name === "MFI")               return `MFI: ${v.mfi?.toFixed(1)}`;
  if (name === "ATR")               return `ATR: $${v.atr?.toFixed(2)} (${v.atr_percent?.toFixed(1)}% of price)`;
  if (name === "ADX")               return `ADX: ${v.adx?.toFixed(1)}, +DI: ${v.plus_di?.toFixed(1)}, −DI: ${v.minus_di?.toFixed(1)}`;
  if (name === "VWAP")              return `VWAP: $${v.vwap?.toFixed(2)}, Dev: ${v.deviation_pct?.toFixed(1)}%`;
  if (name === "FIBONACCI")         return `Ret: ${(v.retracement * 100)?.toFixed(1)}%, H: $${v.swing_high?.toFixed(2)}, L: $${v.swing_low?.toFixed(2)}`;
  if (name.startsWith("SMA")) return `SMA: $${v.sma?.toFixed(2)} (price: $${v.price?.toFixed(2)})`;
  if (name.startsWith("EMA")) return `EMA: $${v.ema?.toFixed(2)} (price: $${v.price?.toFixed(2)})`;
  return JSON.stringify(value);
}

// ---------------------------------------------------------------------------
// Signal numeric score (for group average display)
// ---------------------------------------------------------------------------
const SIGNAL_SCORE: Record<string, number> = {
  strong_buy: 3, buy: 1.5, neutral: 0, sell: -1.5, strong_sell: -3,
};

function groupScoreColor(score: number) {
  if (score > 0.5) return "text-emerald-400";
  if (score < -0.5) return "text-red-400";
  return "text-slate-400";
}

function groupScoreBg(score: number) {
  if (score > 0.5) return "bg-emerald-500/10 border-emerald-500/20";
  if (score < -0.5) return "bg-red-500/10 border-red-500/20";
  return "bg-surface-3 border-border-subtle";
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
export default function TechnicalPanel({ indicators }: TechnicalPanelProps) {
  // Group indicators
  const grouped = useMemo(() => {
    const map: Record<string, TechnicalIndicator[]> = {};
    for (const ind of indicators) {
      const grp = INDICATOR_TO_GROUP[ind.indicator_name] ?? "modifiers";
      if (!map[grp]) map[grp] = [];
      map[grp].push(ind);
    }
    return map;
  }, [indicators]);

  // Group scores (average of numeric signal scores)
  const groupScores = useMemo(() => {
    const out: Record<string, number> = {};
    for (const [grp, inds] of Object.entries(grouped)) {
      if (grp === "modifiers") continue;  // ADX/ATR are modifiers, not averaged
      const scored = inds.map((i) => SIGNAL_SCORE[i.signal] ?? 0);
      out[grp] = scored.length ? scored.reduce((a, b) => a + b, 0) / scored.length : 0;
    }
    return out;
  }, [grouped]);

  // Overall buy/sell counts
  const totals = useMemo(() => ({
    buy:     indicators.filter((i) => i.signal === "buy" || i.signal === "strong_buy").length,
    neutral: indicators.filter((i) => i.signal === "neutral").length,
    sell:    indicators.filter((i) => i.signal === "sell" || i.signal === "strong_sell").length,
  }), [indicators]);

  const total = indicators.length;

  return (
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Activity size={18} className="text-accent-cyan" />
          <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
            Technical Indicators
          </h2>
        </div>
        <div className="flex items-center gap-3 text-xs font-mono">
          <span className="text-emerald-400">{totals.buy} Buy</span>
          <span className="text-slate-400">{totals.neutral} Neutral</span>
          <span className="text-red-400">{totals.sell} Sell</span>
        </div>
      </div>

      {/* Stacked signal bar */}
      {total > 0 && (
        <div className="flex rounded-full h-2 overflow-hidden bg-surface-3">
          <div className="bg-emerald-500 transition-all duration-500" style={{ width: `${(totals.buy / total) * 100}%` }} />
          <div className="bg-slate-500 transition-all duration-500" style={{ width: `${(totals.neutral / total) * 100}%` }} />
          <div className="bg-red-500 transition-all duration-500" style={{ width: `${(totals.sell / total) * 100}%` }} />
        </div>
      )}

      {/* Group score summary row */}
      {Object.keys(groupScores).length > 0 && (
        <div className="grid grid-cols-5 gap-1.5">
          {(["trend", "momentum", "volume", "levels", "volatility"] as const).map((grp) => {
            const s = groupScores[grp];
            if (s === undefined) return null;
            const meta = GROUP_META[grp];
            const Icon = meta.icon;
            return (
              <div key={grp} className={`rounded-lg border px-2 py-1.5 text-center ${groupScoreBg(s)}`}>
                <Icon size={10} className={`mx-auto mb-0.5 ${meta.color}`} />
                <div className={`text-xs font-mono font-bold ${groupScoreColor(s)}`}>
                  {s > 0 ? "+" : ""}{s.toFixed(1)}
                </div>
                <div className="text-[9px] text-text-muted">{meta.label}</div>
              </div>
            );
          })}
        </div>
      )}

      {/* Grouped indicator list */}
      <div className="space-y-4">
        {Object.entries(GROUP_META).map(([grpKey, meta]) => {
          const inds = grouped[grpKey];
          if (!inds || inds.length === 0) return null;
          const Icon = meta.icon;
          const isModifier = grpKey === "modifiers";

          return (
            <div key={grpKey}>
              <div className={`flex items-center gap-1.5 mb-1.5 pb-1 border-b border-border-subtle`}>
                <Icon size={11} className={meta.color} />
                <span className={`text-[10px] font-semibold uppercase tracking-wider ${meta.color}`}>
                  {meta.label}
                </span>
                {isModifier && (
                  <span className="text-[9px] text-text-muted italic ml-1">
                    signal modifiers — not directly scored
                  </span>
                )}
                {!isModifier && groupScores[grpKey] !== undefined && (
                  <span className={`ml-auto text-[10px] font-mono font-bold ${groupScoreColor(groupScores[grpKey])}`}>
                    avg {groupScores[grpKey] > 0 ? "+" : ""}{groupScores[grpKey].toFixed(2)}
                  </span>
                )}
              </div>
              <div className="space-y-1.5">
                {inds.map((ind) => (
                  <div
                    key={ind.indicator_name}
                    className="flex items-center justify-between p-2.5 rounded-lg bg-surface-2/50 hover:bg-surface-3/50 transition-all"
                  >
                    <div className="min-w-0 flex-1">
                      <p className="text-xs font-medium text-text-primary">
                        {INDICATOR_DESCRIPTIONS[ind.indicator_name] || ind.indicator_name}
                      </p>
                      <p className="text-[10px] text-text-secondary font-mono mt-0.5">
                        {formatValue(ind.indicator_name, ind.value)}
                      </p>
                    </div>
                    <div className="ml-3 shrink-0">
                      <SignalBadge signal={ind.signal} />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          );
        })}

        {indicators.length === 0 && (
          <p className="text-text-muted text-sm text-center py-4">
            No technical data available yet
          </p>
        )}
      </div>
    </div>
  );
}
