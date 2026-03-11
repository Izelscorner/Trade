/** Fundamentals Panel — displays P/E, ROE, D/E, PEG metrics in a 2x2 grid */

import type { FundamentalMetrics } from "../types";
import { BarChart3 } from "lucide-react";

interface FundamentalsPanelProps {
  metrics: FundamentalMetrics | null;
  category: string;
}

function MetricCard({
  label,
  sublabel,
  value,
  format,
  thresholds,
}: {
  label: string;
  sublabel: string;
  value: number | null;
  format: (v: number) => string;
  thresholds: { good: number; fair: number; direction: "lower" | "higher" | "range"; rangeGood?: [number, number] };
}) {
  if (value === null) {
    return (
      <div className="rounded-lg bg-surface-2/50 border border-border-subtle p-3">
        <div className="text-xs text-text-muted font-medium">{label}</div>
        <div className="text-[10px] text-text-muted/60 mb-1">{sublabel}</div>
        <div className="text-lg font-mono font-bold text-text-muted/40">N/A</div>
      </div>
    );
  }

  let color = "text-slate-400";
  let bg = "bg-surface-3 border-border-subtle";

  if (thresholds.direction === "lower") {
    if (value <= thresholds.good) {
      color = "text-emerald-400";
      bg = "bg-emerald-500/10 border-emerald-500/20";
    } else if (value <= thresholds.fair) {
      color = "text-amber-400";
      bg = "bg-amber-500/10 border-amber-500/20";
    } else {
      color = "text-red-400";
      bg = "bg-red-500/10 border-red-500/20";
    }
  } else if (thresholds.direction === "higher") {
    if (value >= thresholds.good) {
      color = "text-emerald-400";
      bg = "bg-emerald-500/10 border-emerald-500/20";
    } else if (value >= thresholds.fair) {
      color = "text-amber-400";
      bg = "bg-amber-500/10 border-amber-500/20";
    } else {
      color = "text-red-400";
      bg = "bg-red-500/10 border-red-500/20";
    }
  } else if (thresholds.direction === "range" && thresholds.rangeGood) {
    const [lo, hi] = thresholds.rangeGood;
    if (value >= lo && value <= hi) {
      color = "text-emerald-400";
      bg = "bg-emerald-500/10 border-emerald-500/20";
    } else if (value >= 0) {
      color = "text-amber-400";
      bg = "bg-amber-500/10 border-amber-500/20";
    } else {
      color = "text-red-400";
      bg = "bg-red-500/10 border-red-500/20";
    }
  }

  if (value < 0) {
    color = "text-red-400";
    bg = "bg-red-500/10 border-red-500/20";
  }

  return (
    <div className={`rounded-lg border p-3 ${bg}`}>
      <div className="text-xs text-text-muted font-medium">{label}</div>
      <div className="text-[10px] text-text-muted/60 mb-1">{sublabel}</div>
      <div className={`text-lg font-mono font-bold ${color}`}>{format(value)}</div>
    </div>
  );
}

export default function FundamentalsPanel({ metrics, category }: FundamentalsPanelProps) {
  if (category === "commodity" || !metrics) return null;

  const hasAnyData =
    metrics.pe_ratio !== null ||
    metrics.roe !== null ||
    metrics.de_ratio !== null ||
    metrics.peg_ratio !== null;

  if (!hasAnyData) return null;

  return (
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5">
      <div className="flex items-center gap-2 mb-4">
        <BarChart3 size={18} className="text-accent-cyan" />
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
          Fundamentals
        </h2>
        {metrics.fetched_at && (
          <span className="text-[10px] text-text-muted ml-auto">
            Updated {new Date(metrics.fetched_at).toLocaleDateString()}
          </span>
        )}
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <MetricCard
          label="P/E Ratio"
          sublabel="Valuation"
          value={metrics.pe_ratio}
          format={(v) => v.toFixed(1)}
          thresholds={{ good: 22, fair: 35, direction: "lower" }}
        />
        <MetricCard
          label="ROE"
          sublabel="Profitability"
          value={metrics.roe}
          format={(v) => `${(v * 100).toFixed(1)}%`}
          thresholds={{ good: 0.15, fair: 0.08, direction: "higher" }}
        />
        <MetricCard
          label="D/E Ratio"
          sublabel="Financial Health"
          value={metrics.de_ratio}
          format={(v) => v.toFixed(2)}
          thresholds={{ good: 0.7, fair: 1.5, direction: "lower" }}
        />
        <MetricCard
          label="PEG Ratio"
          sublabel="Growth"
          value={metrics.peg_ratio}
          format={(v) => v.toFixed(2)}
          thresholds={{ good: 0, fair: 0, direction: "range", rangeGood: [0.5, 1.5] }}
        />
      </div>
    </div>
  );
}
