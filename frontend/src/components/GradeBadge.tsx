/** Buy-confidence badge — shows percentage + action label instead of letter grade. */

import { scoreToBuyConfidence, buyConfidenceToAction } from "../types";

interface GradeBadgeProps {
  grade: string | null; // action label (or legacy letter grade)
  score?: number | null; // raw score [-3, 3] — used to derive confidence
  confidence?: number | null; // pre-computed confidence [0, 100]
  size?: "sm" | "md" | "lg";
  showLabel?: boolean;
  label?: string;
  gradedAt?: string | null;
}

/** Continuous hue: red (0 %) → amber (50 %) → emerald (100 %) */
function confidenceColor(pct: number): {
  bg: string;
  text: string;
  border: string;
  bar: string;
} {
  if (pct >= 78)
    return {
      bg: "bg-emerald-500/15",
      text: "text-emerald-400",
      border: "border-emerald-500/30",
      bar: "bg-emerald-500",
    };
  if (pct >= 63)
    return {
      bg: "bg-green-500/15",
      text: "text-green-400",
      border: "border-green-500/30",
      bar: "bg-green-500",
    };
  if (pct >= 54)
    return {
      bg: "bg-lime-500/15",
      text: "text-lime-400",
      border: "border-lime-500/30",
      bar: "bg-lime-500",
    };
  if (pct >= 46)
    return {
      bg: "bg-slate-500/15",
      text: "text-slate-400",
      border: "border-slate-500/30",
      bar: "bg-slate-500",
    };
  if (pct >= 37)
    return {
      bg: "bg-amber-500/15",
      text: "text-amber-400",
      border: "border-amber-500/30",
      bar: "bg-amber-500",
    };
  if (pct >= 22)
    return {
      bg: "bg-orange-500/15",
      text: "text-orange-400",
      border: "border-orange-500/30",
      bar: "bg-orange-500",
    };
  return {
    bg: "bg-red-500/15",
    text: "text-red-400",
    border: "border-red-500/30",
    bar: "bg-red-500",
  };
}

function formatGradedAt(dateStr: string) {
  try {
    const d = new Date(dateStr);
    if (isNaN(d.getTime())) return "";
    return new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "numeric",
    }).format(d);
  } catch {
    return "";
  }
}

const sizeClasses = {
  sm: { wrap: "min-w-[52px]", pct: "text-xs font-bold", lbl: "text-[9px]" },
  md: { wrap: "min-w-[64px]", pct: "text-sm font-bold", lbl: "text-[10px]" },
  lg: { wrap: "min-w-[80px]", pct: "text-xl font-bold", lbl: "text-xs" },
};

export default function GradeBadge({
  grade,
  score,
  confidence,
  size = "md",
  showLabel,
  label,
  gradedAt,
}: GradeBadgeProps) {
  // Resolve confidence: prefer pre-computed, else derive from score, else 50
  const pct: number =
    confidence ??
    (score !== undefined && score !== null ? scoreToBuyConfidence(score) : 50);
  const action =
    grade && grade.length > 2
      ? grade // already an action label from new backend
      : buyConfidenceToAction(pct);

  const colors = confidenceColor(pct);
  const sz = sizeClasses[size];

  if (grade === null && score === null && confidence === undefined) {
    return (
      <span className="inline-flex items-center justify-center rounded-lg border bg-surface-2/50 text-text-muted border-border-subtle px-2 py-1 text-xs">
        —
      </span>
    );
  }

  return (
    <div
      className={`inline-flex flex-col items-center gap-0.5 ${sz.wrap}`}
      title={gradedAt ? `Calculated: ${formatGradedAt(gradedAt)}` : undefined}
    >
      {/* Percentage pill */}
      <span
        className={`inline-flex items-center justify-center rounded-lg border font-mono transition-all duration-200 hover:scale-105 px-2 py-0.5 gap-1 ${colors.bg} ${colors.text} ${colors.border} ${sz.pct} w-full`}
      >
        {pct.toFixed(0)}%
      </span>

      {/* Action label */}
      <span
        className={`${colors.text} ${sz.lbl} font-semibold tracking-tight leading-none text-center w-full`}
      >
        {action}
      </span>

      {showLabel && label && (
        <span className="text-[9px] text-text-muted uppercase tracking-wider">
          {label}
        </span>
      )}
      {gradedAt && (
        <span className="text-[8px] text-text-muted/50 tracking-tighter mt-0.5">
          {formatGradedAt(gradedAt)}
        </span>
      )}
    </div>
  );
}
