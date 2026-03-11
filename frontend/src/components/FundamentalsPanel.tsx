/** Fundamentals Panel — displays P/E, ROE, D/E, PEG metrics with expected ranges */

import type { FundamentalMetrics } from "../types";
import { BarChart3, TrendingDown, TrendingUp, ArrowLeftRight } from "lucide-react";

interface FundamentalsPanelProps {
  metrics: FundamentalMetrics | null;
  category: string;
}

interface MetricThresholds {
  good: number;
  fair: number;
  direction: "lower" | "higher" | "range";
  rangeGood?: [number, number];
}

function MetricCard({
  label,
  sublabel,
  value,
  format,
  thresholds,
  expectedRange,
  directionHint,
}: {
  label: string;
  sublabel: string;
  value: number | null;
  format: (v: number) => string;
  thresholds: MetricThresholds;
  expectedRange: string;
  directionHint: { icon: "up" | "down" | "range"; text: string };
}) {
  if (value === null) {
    return (
      <div className="rounded-lg bg-surface-2/50 border border-border-subtle p-3">
        <div className="text-xs text-text-muted font-medium">{label}</div>
        <div className="text-[10px] text-text-muted/60 mb-1">{sublabel}</div>
        <div className="text-lg font-mono font-bold text-text-muted/40">N/A</div>
        <div className="mt-1.5 pt-1.5 border-t border-border-subtle/50">
          <div className="text-[10px] text-text-muted/50">Fair range: {expectedRange}</div>
        </div>
      </div>
    );
  }

  let color = "text-slate-400";
  let bg = "bg-surface-3 border-border-subtle";
  let verdict = "Neutral";

  if (thresholds.direction === "lower") {
    if (value < 0) {
      color = "text-red-400";
      bg = "bg-red-500/10 border-red-500/20";
      verdict = "Negative";
    } else if (value <= thresholds.good) {
      color = "text-emerald-400";
      bg = "bg-emerald-500/10 border-emerald-500/20";
      verdict = "Good";
    } else if (value <= thresholds.fair) {
      color = "text-amber-400";
      bg = "bg-amber-500/10 border-amber-500/20";
      verdict = "Fair";
    } else {
      color = "text-red-400";
      bg = "bg-red-500/10 border-red-500/20";
      verdict = "High";
    }
  } else if (thresholds.direction === "higher") {
    if (value < 0) {
      color = "text-red-400";
      bg = "bg-red-500/10 border-red-500/20";
      verdict = "Negative";
    } else if (value >= thresholds.good) {
      color = "text-emerald-400";
      bg = "bg-emerald-500/10 border-emerald-500/20";
      verdict = "Good";
    } else if (value >= thresholds.fair) {
      color = "text-amber-400";
      bg = "bg-amber-500/10 border-amber-500/20";
      verdict = "Fair";
    } else {
      color = "text-red-400";
      bg = "bg-red-500/10 border-red-500/20";
      verdict = "Poor";
    }
  } else if (thresholds.direction === "range" && thresholds.rangeGood) {
    const [lo, hi] = thresholds.rangeGood;
    if (value < 0) {
      color = "text-red-400";
      bg = "bg-red-500/10 border-red-500/20";
      verdict = "Negative";
    } else if (value >= lo && value <= hi) {
      color = "text-emerald-400";
      bg = "bg-emerald-500/10 border-emerald-500/20";
      verdict = "Ideal";
    } else if (value < lo) {
      color = "text-amber-400";
      bg = "bg-amber-500/10 border-amber-500/20";
      verdict = "Below range";
    } else {
      color = "text-amber-400";
      bg = "bg-amber-500/10 border-amber-500/20";
      verdict = "Above range";
    }
  }

  const DirectionIcon =
    directionHint.icon === "down" ? TrendingDown : directionHint.icon === "up" ? TrendingUp : ArrowLeftRight;

  const verdictColor =
    verdict === "Good" || verdict === "Ideal"
      ? "text-emerald-400"
      : verdict === "Fair" || verdict === "Below range" || verdict === "Above range"
        ? "text-amber-400"
        : verdict === "Neutral"
          ? "text-slate-400"
          : "text-red-400";

  return (
    <div className={`rounded-lg border p-3 ${bg}`}>
      <div className="text-xs text-text-muted font-medium">{label}</div>
      <div className="text-[10px] text-text-muted/60 mb-1">{sublabel}</div>
      <div className="flex items-baseline gap-2">
        <span className={`text-lg font-mono font-bold ${color}`}>{format(value)}</span>
        <span className={`text-[10px] font-semibold uppercase tracking-wider ${verdictColor}`}>{verdict}</span>
      </div>
      <div className="mt-1.5 pt-1.5 border-t border-border-subtle/50 space-y-0.5">
        <div className="text-[10px] text-text-muted/60">Fair range: <span className="text-text-muted font-mono">{expectedRange}</span></div>
        <div className="flex items-center gap-1 text-[10px] text-text-muted/50">
          <DirectionIcon size={9} />
          <span>{directionHint.text}</span>
        </div>
      </div>
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
          expectedRange="15 – 25"
          directionHint={{ icon: "down", text: "Lower is better (cheaper)" }}
        />
        <MetricCard
          label="ROE"
          sublabel="Profitability"
          value={metrics.roe}
          format={(v) => `${(v * 100).toFixed(1)}%`}
          thresholds={{ good: 0.15, fair: 0.08, direction: "higher" }}
          expectedRange="10% – 25%"
          directionHint={{ icon: "up", text: "Higher is better (profitable)" }}
        />
        <MetricCard
          label="D/E Ratio"
          sublabel="Financial Health"
          value={metrics.de_ratio}
          format={(v) => v.toFixed(2)}
          thresholds={{ good: 0.7, fair: 1.5, direction: "lower" }}
          expectedRange="0.3 – 1.0"
          directionHint={{ icon: "down", text: "Lower is better (less debt)" }}
        />
        <MetricCard
          label="PEG Ratio"
          sublabel="Growth vs Price"
          value={metrics.peg_ratio}
          format={(v) => v.toFixed(2)}
          thresholds={{ good: 0, fair: 0, direction: "range", rangeGood: [0.5, 1.5] }}
          expectedRange="0.5 – 1.5"
          directionHint={{ icon: "range", text: "0.5–1.0 ideal (Peter Lynch)" }}
        />
      </div>
    </div>
  );
}
