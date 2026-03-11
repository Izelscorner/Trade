/** Macro Economic Indicators — DXY, 10Y Treasury, GDP Growth, Brent Crude */

import type { MacroIndicator } from "../types";
import { Activity } from "lucide-react";

interface MacroIndicatorsCardProps {
  indicators: MacroIndicator[];
}

const INDICATOR_CONFIG: Record<
  string,
  { label: string; format: (v: number) => string; zones: { good: [number, number]; warn: [number, number] } }
> = {
  dxy: {
    label: "US Dollar Index",
    format: (v) => v.toFixed(2),
    zones: { good: [95, 108], warn: [90, 115] },
  },
  treasury_10y: {
    label: "10Y Treasury",
    format: (v) => `${v.toFixed(2)}%`,
    zones: { good: [3.5, 4.25], warn: [3.0, 5.0] },
  },
  gdp_growth: {
    label: "Global GDP Growth",
    format: (v) => `${v.toFixed(1)}%`,
    zones: { good: [2.8, 3.3], warn: [2.0, 4.0] },
  },
  brent_crude: {
    label: "Brent Crude Oil",
    format: (v) => `$${v.toFixed(2)}`,
    zones: { good: [70, 85], warn: [55, 100] },
  },
};

function getZoneColor(name: string, value: number): { color: string; bg: string } {
  const config = INDICATOR_CONFIG[name];
  if (!config) return { color: "text-slate-400", bg: "bg-surface-3 border-border-subtle" };

  const { good, warn } = config.zones;
  if (value >= good[0] && value <= good[1]) {
    return { color: "text-emerald-400", bg: "bg-emerald-500/10 border-emerald-500/20" };
  }
  if (value >= warn[0] && value <= warn[1]) {
    return { color: "text-amber-400", bg: "bg-amber-500/10 border-amber-500/20" };
  }
  return { color: "text-red-400", bg: "bg-red-500/10 border-red-500/20" };
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
          const { color, bg } = getZoneColor(ind.name, ind.value);
          return (
            <div key={ind.name} className={`rounded-lg border p-3 ${bg}`}>
              <div className="text-xs text-text-muted font-medium">{config.label}</div>
              <div className={`text-lg font-mono font-bold ${color} mt-1`}>
                {config.format(ind.value)}
              </div>
              <div className="text-[10px] text-text-muted/60 mt-0.5">{ind.unit}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
