/** Macro sentiment display for dashboard — single global macro view */

import { Link } from "react-router-dom";
import type { MacroSentiment } from "../types";
import { Globe, TrendingUp, TrendingDown, Minus } from "lucide-react";

interface MacroSentimentCardProps {
  sentiments: MacroSentiment[];
}

export default function MacroSentimentCard({
  sentiments,
}: MacroSentimentCardProps) {
  // Use the first (and only) global sentiment entry
  const sentiment = sentiments[0];

  if (!sentiment) {
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
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5 animate-slide-up">
      <div className="flex items-center gap-2 mb-4">
        <Globe size={18} className="text-accent-violet" />
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
          Global Macro Sentiment
        </h2>
      </div>
      <Link
        to="/news?type=macro"
        className={`flex items-center justify-between p-3 rounded-lg ${bgColor} transition-all hover:brightness-125 cursor-pointer`}
      >
        <div className="flex items-center gap-2">
          <Globe size={16} className={color} />
          <span className="text-sm text-text-primary">Global</span>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs text-text-muted">
            {sentiment.article_count} articles
          </span>
          <div
            className={`flex items-center gap-1 font-mono font-semibold text-sm ${color}`}
          >
            <Icon size={14} />
            {sentiment.score > 0 ? "+" : ""}
            {sentiment.score.toFixed(4)}
          </div>
          <span
            className={`text-[10px] uppercase tracking-wider px-2 py-0.5 rounded font-medium ${color} ${bgColor} border border-current/20`}
          >
            {sentiment.label}
          </span>
        </div>
      </Link>
    </div>
  );
}
