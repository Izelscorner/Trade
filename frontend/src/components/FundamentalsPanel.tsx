/** Fundamentals Panel — displays P/E, ROE, D/E, PEG with sector-relative ranges from backend */

import type { FundamentalMetrics, MetricConfig } from "../types";
import { BarChart3, TrendingDown, TrendingUp, ArrowLeftRight } from "lucide-react";

interface FundamentalsPanelProps {
  metrics: FundamentalMetrics | null;
  category: string;
}

function getVerdict(value: number, cfg: MetricConfig): { verdict: string; color: string; bg: string } {
  if (value < 0) {
    return { verdict: "Negative", color: "text-red-400", bg: "bg-red-500/10 border-red-500/20" };
  }

  if (cfg.direction === "lower") {
    if (value <= (cfg.good ?? 22)) {
      return { verdict: "Good", color: "text-emerald-400", bg: "bg-emerald-500/10 border-emerald-500/20" };
    }
    if (value <= (cfg.fair ?? 35)) {
      return { verdict: "Fair", color: "text-amber-400", bg: "bg-amber-500/10 border-amber-500/20" };
    }
    return { verdict: "High", color: "text-red-400", bg: "bg-red-500/10 border-red-500/20" };
  }

  if (cfg.direction === "higher") {
    if (value >= (cfg.good ?? 0.15)) {
      return { verdict: "Good", color: "text-emerald-400", bg: "bg-emerald-500/10 border-emerald-500/20" };
    }
    if (value >= (cfg.fair ?? 0.08)) {
      return { verdict: "Fair", color: "text-amber-400", bg: "bg-amber-500/10 border-amber-500/20" };
    }
    return { verdict: "Poor", color: "text-red-400", bg: "bg-red-500/10 border-red-500/20" };
  }

  // range
  if (cfg.range_good) {
    const [lo, hi] = cfg.range_good;
    if (value >= lo && value <= hi) {
      return { verdict: "Ideal", color: "text-emerald-400", bg: "bg-emerald-500/10 border-emerald-500/20" };
    }
    return {
      verdict: value < lo ? "Below range" : "Above range",
      color: "text-amber-400",
      bg: "bg-amber-500/10 border-amber-500/20",
    };
  }

  return { verdict: "Neutral", color: "text-slate-400", bg: "bg-surface-3 border-border-subtle" };
}

const FORMAT: Record<string, (v: number) => string> = {
  pe_ratio: (v) => v.toFixed(1),
  roe: (v) => `${(v * 100).toFixed(1)}%`,
  de_ratio: (v) => v.toFixed(2),
  peg_ratio: (v) => v.toFixed(2),
};

const METRIC_KEYS = ["pe_ratio", "roe", "de_ratio", "peg_ratio"] as const;

const DirectionIcons = {
  lower: TrendingDown,
  higher: TrendingUp,
  range: ArrowLeftRight,
};

export default function FundamentalsPanel({ metrics, category }: FundamentalsPanelProps) {
  if (category === "commodity" || !metrics) return null;

  const hasAnyData = METRIC_KEYS.some((k) => metrics[k] !== null);
  if (!hasAnyData) return null;

  const config = metrics.config;

  return (
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5">
      <div className="flex items-center gap-2 mb-4">
        <BarChart3 size={18} className="text-accent-cyan" />
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
          Fundamentals
        </h2>
        {metrics.sector && (
          <span className="text-[10px] uppercase tracking-wider px-2 py-0.5 rounded-md bg-surface-3 text-text-muted border border-border-subtle font-medium">
            {metrics.sector.replace("_", " ")}
          </span>
        )}
        {metrics.fetched_at && (
          <span className="text-[10px] text-text-muted ml-auto">
            Updated {new Date(metrics.fetched_at).toLocaleDateString()}
          </span>
        )}
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {METRIC_KEYS.map((key) => {
          const cfg = config?.[key];
          if (!cfg) return null;
          const value = metrics[key];
          const format = FORMAT[key] ?? ((v: number) => v.toFixed(2));
          const Icon = DirectionIcons[cfg.direction] ?? ArrowLeftRight;

          if (value === null) {
            return (
              <div key={key} className="rounded-lg bg-surface-2/50 border border-border-subtle p-3">
                <div className="text-xs text-text-muted font-medium">{cfg.label}</div>
                <div className="text-[10px] text-text-muted/60 mb-1">{cfg.sublabel}</div>
                <div className="text-lg font-mono font-bold text-text-muted/40">N/A</div>
                <div className="mt-1.5 pt-1.5 border-t border-border-subtle/50">
                  <div className="text-[10px] text-text-muted/50">
                    Fair range: <span className="font-mono">{cfg.expected_range}</span>
                  </div>
                </div>
              </div>
            );
          }

          const { verdict, color, bg } = getVerdict(value, cfg);
          const verdictColor =
            verdict === "Good" || verdict === "Ideal"
              ? "text-emerald-400"
              : verdict === "Negative" || verdict === "High" || verdict === "Poor"
                ? "text-red-400"
                : verdict === "Neutral"
                  ? "text-slate-400"
                  : "text-amber-400";

          return (
            <div key={key} className={`rounded-lg border p-3 ${bg}`}>
              <div className="text-xs text-text-muted font-medium">{cfg.label}</div>
              <div className="text-[10px] text-text-muted/60 mb-1">{cfg.sublabel}</div>
              <div className="flex items-baseline gap-2">
                <span className={`text-lg font-mono font-bold ${color}`}>{format(value)}</span>
                <span className={`text-[10px] font-semibold uppercase tracking-wider ${verdictColor}`}>{verdict}</span>
              </div>
              <div className="mt-1.5 pt-1.5 border-t border-border-subtle/50 space-y-0.5">
                <div className="text-[10px] text-text-muted/60">
                  Fair range: <span className="text-text-muted font-mono">{cfg.expected_range}</span>
                </div>
                <div className="flex items-center gap-1 text-[10px] text-text-muted/50">
                  <Icon size={9} />
                  <span>{cfg.direction_text}</span>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
