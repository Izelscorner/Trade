/** Macro sentiment display for dashboard — dual-horizon (short + long term) */

import { Link } from "react-router-dom";
import type { MacroSentiment } from "../types";
import { Globe, TrendingUp, TrendingDown, Minus } from "lucide-react";

interface MacroSentimentCardProps {
  sentiments: MacroSentiment[];
}

function SentimentRow({ sentiment, label }: { sentiment: MacroSentiment; label: string }) {
  const isPositive = sentiment.score > 0.15;
  const isNegative = sentiment.score < -0.15;

  const color = isPositive
    ? "text-accent-emerald"
    : isNegative
      ? "text-accent-rose"
      : "text-text-secondary";

  const bgColor = isPositive
    ? "bg-accent-emerald/10"
    : isNegative
      ? "bg-accent-rose/10"
      : "bg-surface-3";

  const Icon = isPositive ? TrendingUp : isNegative ? TrendingDown : Minus;

  return (
    <div className={`flex items-center justify-between p-2.5 rounded-lg ${bgColor}`}>
      <div className="flex items-center gap-2">
        <Globe size={14} className={color} />
        <span className="text-xs text-text-primary font-medium">{label}</span>
      </div>
      <div className="flex items-center gap-2.5">
        <span className="text-[10px] text-text-muted">
          {sentiment.article_count} art
        </span>
        <div className={`flex items-center gap-1 font-mono font-semibold text-xs ${color}`}>
          <Icon size={12} />
          {sentiment.score > 0 ? "+" : ""}
          {sentiment.score.toFixed(4)}
        </div>
        <span className={`text-[9px] uppercase tracking-wider px-1.5 py-0.5 rounded font-medium ${color} ${bgColor} border border-current/20`}>
          {sentiment.label}
        </span>
      </div>
    </div>
  );
}

export default function MacroSentimentCard({
  sentiments,
}: MacroSentimentCardProps) {
  if (!sentiments || sentiments.length === 0) {
    return (
      <div className="rounded-xl bg-surface-1 border border-border-subtle p-5 animate-slide-up">
        <div className="flex items-center gap-2 mb-4">
          <Globe size={18} className="text-accent-violet" />
          <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
            Global Macro Sentiment
          </h2>
        </div>
        <p className="text-text-muted text-sm text-center py-4">
          No macro sentiment data available yet
        </p>
      </div>
    );
  }

  const shortSentiment = sentiments.find(s => s.term === "short") ?? sentiments[0];
  const longSentiment = sentiments.find(s => s.term === "long");

  return (
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5 animate-slide-up">
      <div className="flex items-center gap-2 mb-4">
        <Globe size={18} className="text-accent-violet" />
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
          Global Macro Sentiment
        </h2>
      </div>
      <Link to="/news?type=macro" className="space-y-1.5 block transition-all hover:brightness-125 cursor-pointer">
        <SentimentRow sentiment={shortSentiment} label="Short-Term" />
        {longSentiment && (
          <SentimentRow sentiment={longSentiment} label="Long-Term" />
        )}
      </Link>
    </div>
  );
}
