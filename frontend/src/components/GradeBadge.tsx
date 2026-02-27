/** Grade badge component with color coding */

interface GradeBadgeProps {
  grade: string | null;
  size?: "sm" | "md" | "lg";
  showLabel?: boolean;
  label?: string;
  gradedAt?: string | null;
}

const gradeColors: Record<string, string> = {
  "A+": "bg-grade-a-plus/20 text-grade-a-plus border-grade-a-plus/30",
  A: "bg-grade-a/20 text-grade-a border-grade-a/30",
  "B+": "bg-grade-b-plus/20 text-grade-b-plus border-grade-b-plus/30",
  B: "bg-grade-b/20 text-grade-b border-grade-b/30",
  C: "bg-grade-c/20 text-grade-c border-grade-c/30",
  D: "bg-grade-d/20 text-grade-d border-grade-d/30",
  "D-": "bg-grade-d-minus/20 text-grade-d-minus border-grade-d-minus/30",
  F: "bg-grade-f/20 text-grade-f border-grade-f/30",
};

const sizeClasses = {
  sm: "text-xs px-2 py-0.5 min-w-[28px]",
  md: "text-sm px-3 py-1 min-w-[36px]",
  lg: "text-lg px-4 py-2 min-w-[48px] font-bold",
};

// Formatter for graded date
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
  } catch (e) {
    return "";
  }
}

export default function GradeBadge({
  grade,
  size = "md",
  showLabel,
  label,
  gradedAt,
}: GradeBadgeProps) {
  if (!grade) {
    return (
      <span
        className={`inline-flex items-center justify-center rounded-md border bg-surface-2/50 text-text-muted border-border-subtle ${sizeClasses[size]}`}
      >
        —
      </span>
    );
  }

  const colorClass =
    gradeColors[grade] ||
    "bg-surface-2/50 text-text-muted border-border-subtle";

  return (
    <div
      className="inline-flex flex-col items-center gap-0.5"
      title={gradedAt ? `Calculated: ${formatGradedAt(gradedAt)}` : undefined}
    >
      <span
        className={`inline-flex items-center justify-center rounded-md border font-semibold font-mono tracking-wide ${colorClass} ${sizeClasses[size]} transition-all duration-200 hover:scale-105`}
      >
        {grade}
      </span>
      {showLabel && label && (
        <span className="text-[10px] text-text-muted uppercase tracking-wider">
          {label}
        </span>
      )}
      {gradedAt && (
        <span className="text-[9px] text-text-muted/60 tracking-tighter mt-0.5">
          {formatGradedAt(gradedAt)}
        </span>
      )}
    </div>
  );
}
