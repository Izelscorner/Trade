/** Macro Economic Indicators — DXY, 10Y Treasury, GDP Growth, Brent Crude */

import type { MacroIndicator } from "../types";
import { Activity, TrendingDown, TrendingUp, ArrowLeftRight } from "lucide-react";

interface MacroIndicatorsCardProps {
  indicators: MacroIndicator[];
}

const INDICATOR_CONFIG: Record<
  string,
  {
    label: string;
    format: (v: number) => string;
    zones: { good: [number, number]; warn: [number, number] };
    expectedRange: string;
    directionHint: { icon: "up" | "down" | "range"; text: string };
    impact: string;
  }
> = {
  dxy: {
    label: "US Dollar Index",
    format: (v) => v.toFixed(2),
    zones: { good: [95, 108], warn: [90, 115] },
    expectedRange: "95 – 108",
    directionHint: { icon: "range", text: "Stable is ideal for equities" },
    impact: "Strong $ hurts exporters & EM; weak $ boosts commodities",
  },
  treasury_10y: {
    label: "10Y Treasury",
    format: (v) => `${v.toFixed(2)}%`,
    zones: { good: [3.5, 4.25], warn: [3.0, 5.0] },
    expectedRange: "3.5% – 4.25%",
    directionHint: { icon: "down", text: "Lower is better for stocks" },
    impact: "Higher yields compete with equities for capital",
  },
  gdp_growth: {
    label: "US GDP Growth",
    format: (v) => `${v.toFixed(1)}%`,
    zones: { good: [2.0, 3.5], warn: [1.0, 5.0] },
    expectedRange: "2.0% – 3.5%",
    directionHint: { icon: "up", text: "Higher is better (expansion)" },
    impact: "Strong growth supports earnings; too high may trigger rate hikes",
  },
  brent_crude: {
    label: "Brent Crude Oil",
    format: (v) => `$${v.toFixed(2)}`,
    zones: { good: [65, 85], warn: [50, 100] },
    expectedRange: "$65 – $85",
    directionHint: { icon: "range", text: "Moderate is best for growth" },
    impact: "High oil raises costs & inflation; low oil signals weak demand",
  },
};

function getZoneInfo(
  name: string,
  value: number,
): { color: string; bg: string; verdict: string } {
  const config = INDICATOR_CONFIG[name];
  if (!config) return { color: "text-slate-400", bg: "bg-surface-3 border-border-subtle", verdict: "—" };

  const { good, warn } = config.zones;
  if (value >= good[0] && value <= good[1]) {
    return { color: "text-emerald-400", bg: "bg-emerald-500/10 border-emerald-500/20", verdict: "Normal" };
  }
  if (value >= warn[0] && value <= warn[1]) {
    const isHigh = value > good[1];
    return {
      color: "text-amber-400",
      bg: "bg-amber-500/10 border-amber-500/20",
      verdict: isHigh ? "Elevated" : "Low",
    };
  }
  const isHigh = value > warn[1];
  return {
    color: "text-red-400",
    bg: "bg-red-500/10 border-red-500/20",
    verdict: isHigh ? "Very High" : "Very Low",
  };
}

export default function MacroIndicatorsCard({ indicators }: MacroIndicatorsCardProps) {
  if (!indicators || indicators.length === 0) return null;

  return (
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5">
      <div className="flex items-center gap-2 mb-4">
        <Activity size={18} className="text-accent-cyan" />
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
          Macro Economic Indicators
        </h2>
        {indicators[0]?.fetched_at && (
          <span className="text-[10px] text-text-muted ml-auto">
            Updated {new Date(indicators[0].fetched_at).toLocaleDateString()}
          </span>
        )}
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {indicators.map((ind) => {
          const config = INDICATOR_CONFIG[ind.name];
          if (!config) return null;
          const { color, bg, verdict } = getZoneInfo(ind.name, ind.value);

          const verdictColor =
            verdict === "Normal"
              ? "text-emerald-400"
              : verdict === "Elevated" || verdict === "Low"
                ? "text-amber-400"
                : "text-red-400";

          const DirectionIcon =
            config.directionHint.icon === "down"
              ? TrendingDown
              : config.directionHint.icon === "up"
                ? TrendingUp
                : ArrowLeftRight;

          return (
            <div key={ind.name} className={`rounded-lg border p-3 ${bg}`}>
              <div className="text-xs text-text-muted font-medium">{config.label}</div>
              <div className="flex items-baseline gap-2 mt-1">
                <span className={`text-lg font-mono font-bold ${color}`}>
                  {config.format(ind.value)}
                </span>
                <span className={`text-[10px] font-semibold uppercase tracking-wider ${verdictColor}`}>
                  {verdict}
                </span>
              </div>
              <div className="mt-1.5 pt-1.5 border-t border-border-subtle/50 space-y-0.5">
                <div className="text-[10px] text-text-muted/60">
                  Fair range: <span className="text-text-muted font-mono">{config.expectedRange}</span>
                </div>
                <div className="flex items-center gap-1 text-[10px] text-text-muted/50">
                  <DirectionIcon size={9} />
                  <span>{config.directionHint.text}</span>
                </div>
                <div className="text-[9px] text-text-muted/40 italic leading-tight">{config.impact}</div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
