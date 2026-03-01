/** Grade detail breakdown component */

import type { Grade } from "../types";
import GradeBadge from "./GradeBadge";
import { Award, TrendingUp, Newspaper, Globe } from "lucide-react";

interface GradeDetailProps {
  shortGrade: Grade | null;
  longGrade: Grade | null;
}

function ScoreBar({
  label,
  score,
  icon: Icon,
  confidence,
  articleCount,
}: {
  label: string;
  score: number;
  icon: React.ElementType;
  confidence?: number;
  articleCount?: number;
}) {
  // Score ranges from -1 to 1, normalize to 0-100 for the bar
  const pct = Math.max(0, Math.min(100, (score + 1) * 50));
  const isPositive = score > 0;

  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1.5">
          <Icon size={12} className="text-text-muted" />
          <span className="text-xs text-text-secondary font-medium">
            {label}
          </span>
          {confidence !== undefined && (
            <span className="text-[10px] text-text-muted font-mono">
              {(confidence * 100).toFixed(0)}% conf
              {articleCount !== undefined && ` (${articleCount} articles)`}
            </span>
          )}
        </div>
        <span
          className={`text-xs font-mono font-semibold ${isPositive ? "text-accent-emerald" : score < 0 ? "text-accent-rose" : "text-text-muted"}`}
        >
          {score > 0 ? "+" : ""}
          {score.toFixed(4)}
        </span>
      </div>
      <div className="h-1.5 rounded-full bg-surface-3 overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-700 ${isPositive ? "bg-accent-emerald" : score < 0 ? "bg-accent-rose" : "bg-text-muted"}`}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

function GradeSection({ grade, label }: { grade: Grade; label: string }) {
  return (
    <div className="rounded-xl bg-surface-2/50 border border-border-subtle p-5 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Award size={16} className="text-accent-amber" />
          <h3 className="text-sm font-semibold uppercase tracking-wider text-text-primary">
            {label}
          </h3>
        </div>
        <GradeBadge
          grade={grade.overall_grade}
          size="lg"
          gradedAt={grade.graded_at}
        />
      </div>

      <div className="text-center py-2">
        <span
          className={`text-3xl font-bold font-mono ${grade.overall_score > 0 ? "text-accent-emerald" : grade.overall_score < 0 ? "text-accent-rose" : "text-text-secondary"}`}
        >
          {grade.overall_score > 0 ? "+" : ""}
          {grade.overall_score.toFixed(4)}
        </span>
        <p className="text-[10px] text-text-muted uppercase tracking-wider mt-1">
          Overall Score
        </p>
      </div>

      <div className="space-y-3">
        <ScoreBar
          label="Technical"
          score={grade.technical_score}
          icon={TrendingUp}
        />
        <ScoreBar
          label="Sentiment"
          score={grade.sentiment_score}
          icon={Newspaper}
          confidence={grade.details?.sentiment?.confidence}
          articleCount={grade.details?.sentiment?.articles}
        />
        <ScoreBar
          label="Macro"
          score={grade.macro_score}
          icon={Globe}
          confidence={grade.details?.macro?.global?.confidence}
          articleCount={grade.details?.macro?.global?.articles}
        />
      </div>

      {grade.details?.weights && (
        <div className="flex items-center gap-3 pt-2 border-t border-border-subtle">
          <span className="text-[10px] text-text-muted">Weights:</span>
          <span className="text-[10px] text-text-secondary font-mono">
            Tech {(grade.details.weights.technical * 100).toFixed(0)}% • Sent{" "}
            {(grade.details.weights.sentiment * 100).toFixed(0)}% • Macro{" "}
            {(grade.details.weights.macro * 100).toFixed(0)}%
          </span>
        </div>
      )}
    </div>
  );
}

export default function GradeDetail({
  shortGrade,
  longGrade,
}: GradeDetailProps) {
  return (
    <div className="space-y-4">
      <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider flex items-center gap-2">
        <Award size={18} className="text-accent-amber" />
        Investment Grades
      </h2>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {shortGrade ? (
          <GradeSection grade={shortGrade} label="Short-Term" />
        ) : (
          <div className="rounded-xl bg-surface-2/50 border border-border-subtle p-5 flex items-center justify-center text-text-muted text-sm">
            No short-term grade available
          </div>
        )}
        {longGrade ? (
          <GradeSection grade={longGrade} label="Long-Term" />
        ) : (
          <div className="rounded-xl bg-surface-2/50 border border-border-subtle p-5 flex items-center justify-center text-text-muted text-sm">
            No long-term grade available
          </div>
        )}
      </div>
    </div>
  );
}
