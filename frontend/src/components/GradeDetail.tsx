/** Grade detail breakdown — percentage-first, math-transparent */

import type { Grade } from "../types";
import { scoreToBuyConfidence, buyConfidenceToAction } from "../types";
import { Award, TrendingUp, Newspaper, Globe, ShieldCheck, Zap } from "lucide-react";

interface GradeDetailProps {
  shortGrade: Grade | null;
  longGrade: Grade | null;
}

// ---------------------------------------------------------------------------
// Confidence arc (SVG radial gauge)
// ---------------------------------------------------------------------------
function ConfidenceGauge({ pct }: { pct: number }) {
  const radius = 42;
  const circumference = 2 * Math.PI * radius;
  // Half-arc: starts at 7 o'clock, sweeps 180° to 5 o'clock
  const arcLen = circumference * 0.75;
  const filled = (pct / 100) * arcLen;

  const color =
    pct >= 78 ? "#10b981" :
    pct >= 63 ? "#22c55e" :
    pct >= 54 ? "#84cc16" :
    pct >= 46 ? "#64748b" :
    pct >= 37 ? "#f59e0b" :
    pct >= 22 ? "#f97316" :
               "#ef4444";

  return (
    <svg width="104" height="104" viewBox="0 0 104 104" className="mx-auto">
      {/* Track */}
      <circle
        cx="52" cy="52" r={radius}
        fill="none"
        stroke="#1e2a3b"
        strokeWidth="8"
        strokeDasharray={`${arcLen} ${circumference - arcLen}`}
        strokeDashoffset={circumference * 0.125}
        strokeLinecap="round"
      />
      {/* Fill */}
      <circle
        cx="52" cy="52" r={radius}
        fill="none"
        stroke={color}
        strokeWidth="8"
        strokeDasharray={`${filled} ${circumference - filled}`}
        strokeDashoffset={circumference * 0.125}
        strokeLinecap="round"
        className="transition-all duration-700"
        style={{ filter: `drop-shadow(0 0 6px ${color}66)` }}
      />
      {/* Label */}
      <text x="52" y="52" textAnchor="middle" dominantBaseline="middle"
        fill={color} fontSize="18" fontWeight="bold" fontFamily="monospace">
        {pct.toFixed(0)}%
      </text>
      <text x="52" y="68" textAnchor="middle" dominantBaseline="middle"
        fill="#64748b" fontSize="8" fontFamily="sans-serif" letterSpacing="1">
        BUY CONFIDENCE
      </text>
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Confidence bar for sub-scores
// ---------------------------------------------------------------------------
function ScoreBar({
  label,
  score,
  icon: Icon,
  confidence,
  articles,
  extraInfo,
}: {
  label: string;
  score: number;
  icon: React.ElementType;
  confidence?: number;
  articles?: number;
  extraInfo?: string;
}) {
  // Score ∈ [-3, 3] → 0–100% for bar
  const barPct = Math.max(0, Math.min(100, ((score + 3) / 6) * 100));
  const isPositive = score > 0.05;
  const isNegative = score < -0.05;

  const barColor = isPositive ? "bg-emerald-500" : isNegative ? "bg-red-500" : "bg-slate-500";
  const textColor = isPositive ? "text-emerald-400" : isNegative ? "text-red-400" : "text-slate-400";

  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1.5">
          <Icon size={11} className="text-text-muted" />
          <span className="text-xs text-text-secondary font-medium">{label}</span>
          {confidence !== undefined && (
            <span className="text-[10px] text-text-muted font-mono bg-surface-3 px-1 rounded">
              {(confidence * 100).toFixed(0)}% conf
              {articles !== undefined ? ` · ${articles} art` : ""}
            </span>
          )}
          {extraInfo && (
            <span className="text-[10px] text-text-muted/70 italic">{extraInfo}</span>
          )}
        </div>
        <span className={`text-xs font-mono font-bold ${textColor}`}>
          {score > 0 ? "+" : ""}{score.toFixed(3)}
        </span>
      </div>
      {/* Center-origin bar */}
      <div className="h-1.5 rounded-full bg-surface-3 overflow-hidden relative">
        <div className="absolute left-1/2 top-0 w-px h-full bg-surface-2" />
        <div
          className={`h-full rounded-full transition-all duration-700 absolute top-0 ${barColor}`}
          style={
            isPositive
              ? { left: "50%", width: `${(barPct - 50)}%` }
              : isNegative
              ? { right: `${100 - barPct}%`, width: `${50 - barPct}%`, left: "auto" }
              : { left: "50%", width: "1px" }
          }
        />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Indicator group summary
// ---------------------------------------------------------------------------
function GroupSummary({ groups }: { groups: Record<string, { score: number; count: number; indicators: Record<string, string> }> }) {
  const groupNames: Record<string, string> = {
    trend: "Trend", momentum: "Momentum", volume: "Volume",
    levels: "Levels", volatility: "Volatility",
  };

  return (
    <div className="space-y-1.5 pt-2 border-t border-border-subtle">
      <p className="text-[10px] text-text-muted uppercase tracking-wider font-medium">Technical Groups</p>
      <div className="grid grid-cols-5 gap-1">
        {Object.entries(groups).map(([key, gd]) => {
          const s = gd.score;
          const color = s > 0.3 ? "text-emerald-400" : s < -0.3 ? "text-red-400" : "text-slate-400";
          const bg = s > 0.3 ? "bg-emerald-500/10 border-emerald-500/20" : s < -0.3 ? "bg-red-500/10 border-red-500/20" : "bg-surface-3 border-border-subtle";
          return (
            <div key={key} className={`rounded-lg border px-1.5 py-1.5 text-center ${bg}`}>
              <div className={`text-xs font-mono font-bold ${color}`}>
                {s > 0 ? "+" : ""}{s.toFixed(2)}
              </div>
              <div className="text-[9px] text-text-muted mt-0.5">{groupNames[key] ?? key}</div>
              <div className="text-[9px] text-text-muted/60">{gd.count} ind</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// One grade panel
// ---------------------------------------------------------------------------
function GradeSection({ grade, label }: { grade: Grade; label: string }) {
  const buyConf = grade.details?.buy_confidence ?? scoreToBuyConfidence(grade.overall_score);
  const action = grade.details?.action ?? buyConfidenceToAction(buyConf);
  const sentConf = grade.details?.sentiment?.confidence;
  const sentArticles = grade.details?.sentiment?.articles;
  const macroConf = grade.details?.macro?.confidence;
  const macroArticles = grade.details?.macro?.articles;
  const techCompleteness = grade.details?.technical?.data_completeness;
  const atrFactor = grade.details?.technical?.atr_risk_factor;
  const groups = grade.details?.technical?.group_scores as
    | Record<string, { score: number; count: number; indicators: Record<string, string> }>
    | undefined;
  const effectiveWeights = grade.details?.effective_weights ?? grade.details?.weights;

  const actionColor =
    buyConf >= 78 ? "text-emerald-400" :
    buyConf >= 63 ? "text-green-400" :
    buyConf >= 54 ? "text-lime-400" :
    buyConf >= 46 ? "text-slate-400" :
    buyConf >= 37 ? "text-amber-400" :
    buyConf >= 22 ? "text-orange-400" :
    "text-red-400";

  return (
    <div className="rounded-xl bg-surface-2/50 border border-border-subtle p-5 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Award size={15} className="text-accent-amber" />
          <h3 className="text-sm font-semibold uppercase tracking-wider text-text-primary">{label}</h3>
        </div>
        <span className="text-[10px] text-text-muted font-mono bg-surface-3 px-2 py-0.5 rounded">
          score {grade.overall_score > 0 ? "+" : ""}{grade.overall_score.toFixed(3)}
        </span>
      </div>

      {/* Gauge + action */}
      <div className="flex flex-col items-center gap-1">
        <ConfidenceGauge pct={buyConf} />
        <span className={`text-base font-bold tracking-wide ${actionColor}`}>{action}</span>
      </div>

      {/* Sub-score bars */}
      <div className="space-y-2.5">
        <ScoreBar
          label="Technical"
          score={grade.technical_score}
          icon={TrendingUp}
          confidence={techCompleteness}
          extraInfo={atrFactor !== undefined && atrFactor < 1 ? `ATR ×${atrFactor}` : undefined}
        />
        <ScoreBar
          label="Sentiment"
          score={grade.sentiment_score}
          icon={Newspaper}
          confidence={sentConf}
          articles={typeof sentArticles === "number" ? sentArticles : undefined}
          extraInfo="48h decay"
        />
        <ScoreBar
          label="Macro"
          score={grade.macro_score}
          icon={Globe}
          confidence={macroConf}
          articles={typeof macroArticles === "number" ? macroArticles : undefined}
          extraInfo="6h decay"
        />
      </div>

      {/* Effective weights row */}
      {effectiveWeights && (
        <div className="flex items-center gap-3 pt-2 border-t border-border-subtle flex-wrap">
          <span className="text-[10px] text-text-muted flex items-center gap-1">
            <Zap size={9} /> Effective weights:
          </span>
          {(["technical", "sentiment", "macro"] as const).map((k) => (
            <span key={k} className="text-[10px] font-mono text-text-secondary bg-surface-3 px-1.5 py-0.5 rounded">
              {k.slice(0, 4)} {((effectiveWeights[k] ?? 0) * 100).toFixed(0)}%
            </span>
          ))}
          {techCompleteness !== undefined && (
            <span className="text-[10px] text-text-muted flex items-center gap-1 ml-auto">
              <ShieldCheck size={9} />
              {(techCompleteness * 100).toFixed(0)}% data coverage
            </span>
          )}
        </div>
      )}

      {/* Technical group breakdown */}
      {groups && Object.keys(groups).length > 0 && (
        <GroupSummary groups={groups} />
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main export
// ---------------------------------------------------------------------------
export default function GradeDetail({ shortGrade, longGrade }: GradeDetailProps) {
  return (
    <div className="space-y-4">
      <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider flex items-center gap-2">
        <Award size={18} className="text-accent-amber" />
        Buy Confidence &amp; Trade Signal
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
