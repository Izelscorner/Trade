/** Macro Economic Indicators — config-driven from backend */

import type { MacroIndicator } from "../types";
import { Activity, TrendingDown, TrendingUp, ArrowLeftRight } from "lucide-react";

interface MacroIndicatorsCardProps {
  indicators: MacroIndicator[];
}

const FORMAT: Record<string, (v: number) => string> = {
  dxy: (v) => v.toFixed(2),
  treasury_10y: (v) => `${v.toFixed(2)}%`,
  gdp_growth: (v) => `${v.toFixed(1)}%`,
  brent_crude: (v) => `$${v.toFixed(2)}`,
};

const DirectionIcons = {
  down: TrendingDown,
  up: TrendingUp,
  range: ArrowLeftRight,
};

function getZoneVerdict(
  value: number,
  goodZone: [number, number],
  warnZone: [number, number],
): { color: string; bg: string; verdict: string } {
  if (value >= goodZone[0] && value <= goodZone[1]) {
    return { color: "text-emerald-400", bg: "bg-emerald-500/10 border-emerald-500/20", verdict: "Normal" };
  }
  if (value >= warnZone[0] && value <= warnZone[1]) {
    const isHigh = value > goodZone[1];
    return {
      color: "text-amber-400",
      bg: "bg-amber-500/10 border-amber-500/20",
      verdict: isHigh ? "Elevated" : "Low",
    };
  }
  const isHigh = value > warnZone[1];
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
          const cfg = ind.config;
          if (!cfg) return null;

          const format = FORMAT[ind.name] ?? ((v: number) => v.toFixed(2));
          const { color, bg, verdict } = getZoneVerdict(
            ind.value,
            cfg.good_zone,
            cfg.warn_zone,
          );

          const verdictColor =
            verdict === "Normal"
              ? "text-emerald-400"
              : verdict === "Elevated" || verdict === "Low"
                ? "text-amber-400"
                : "text-red-400";

          const Icon = DirectionIcons[cfg.direction] ?? ArrowLeftRight;

          return (
            <div key={ind.name} className={`rounded-lg border p-3 ${bg}`}>
              <div className="text-xs text-text-muted font-medium">{ind.label}</div>
              <div className="flex items-baseline gap-2 mt-1">
                <span className={`text-lg font-mono font-bold ${color}`}>
                  {format(ind.value)}
                </span>
                <span className={`text-[10px] font-semibold uppercase tracking-wider ${verdictColor}`}>
                  {verdict}
                </span>
              </div>
              <div className="mt-1.5 pt-1.5 border-t border-border-subtle/50 space-y-0.5">
                <div className="text-[10px] text-text-muted/60">
                  Fair range: <span className="text-text-muted font-mono">{cfg.expected_range}</span>
                </div>
                <div className="flex items-center gap-1 text-[10px] text-text-muted/50">
                  <Icon size={9} />
                  <span>{cfg.direction_text}</span>
                </div>
                <div className="text-[9px] text-text-muted/40 italic leading-tight">{cfg.impact}</div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
