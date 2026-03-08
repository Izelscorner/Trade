/** News feed list component */

import type { ReactNode } from "react";
import type { NewsArticle } from "../types";
import { ExternalLink, Clock, Newspaper } from "lucide-react";

interface NewsFeedProps {
  articles: NewsArticle[];
  title?: string;
  icon?: ReactNode;
  compact?: boolean;
  className?: string;
}

const sentimentColors: Record<string, string> = {
  "very positive":
    "text-accent-emerald bg-accent-emerald/20 border border-accent-emerald/30",
  positive: "text-accent-emerald bg-accent-emerald/10",
  negative: "text-accent-rose bg-accent-rose/10",
  "very negative":
    "text-accent-rose bg-accent-rose/20 border border-accent-rose/30",
  neutral: "text-text-muted bg-surface-3",
};

function timeAgo(dateStr: string | null): string {
  if (!dateStr) return "";
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function categoryLabel(cat: string): string {
  return cat
    .replace("macro_", "")
    .replace("_", " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export default function NewsFeed({
  articles,
  title = "Latest News",
  icon,
  compact = false,
  className = "",
}: NewsFeedProps) {
  const isFullHeight = className.includes("h-full");

  return (
    <div
      className={`rounded-xl bg-surface-1 border border-border-subtle p-5 flex flex-col ${className}`}
    >
      <div className="flex items-center gap-2 mb-4 shrink-0">
        {icon || <Newspaper size={18} className="text-accent-amber" />}
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
          {title}
        </h2>
        <div className="ml-auto flex flex-col items-end">
          <span className="text-xs text-text-muted">
            {articles.length} articles
          </span>
          <span className="text-[10px] text-accent-cyan/60 font-mono">
            {new Date().toLocaleTimeString()}
          </span>
        </div>
      </div>
      <div
        className={`space-y-1 ${isFullHeight ? "" : compact ? "max-h-[800px]" : "max-h-[1200px]"} overflow-y-auto pr-1 flex-1`}
      >
        {articles.length > 0 ? (
          articles.map((article) => (
            <a
              key={article.id}
              href={article.link || "#"}
              target="_blank"
              rel="noopener noreferrer"
              className="group flex items-start gap-3 p-3 rounded-lg hover:bg-surface-2/50 transition-all duration-200"
            >
              <div className="flex-1 min-w-0">
                <p className="text-sm text-text-primary group-hover:text-accent-cyan transition-colors line-clamp-2 leading-snug">
                  {article.title}
                </p>
                <div className="flex items-center gap-2 mt-1.5 flex-wrap">
                  <span className="text-[10px] text-text-muted font-medium">
                    {article.source}
                  </span>
                  <span className="text-[10px] text-text-muted/50">•</span>
                  <span className="text-[10px] text-text-muted flex items-center gap-0.5">
                    <Clock size={8} />
                    {timeAgo(article.published_at)}
                  </span>
                  <span className="text-[10px] text-text-muted/50">•</span>
                  <span className="text-[10px] text-text-muted uppercase tracking-wider">
                    {categoryLabel(article.category)}
                  </span>
                </div>
              </div>
              <div className="flex flex-col items-end gap-1 shrink-0">
                {article.sentiment && (
                  <div className="flex flex-col items-end gap-0.5">
                    <div className="flex items-center gap-1">
                      <span className="text-[8px] text-text-muted/60 font-mono">ST</span>
                      <span
                        className={`text-[9px] uppercase tracking-wider px-1.5 py-0.5 rounded font-semibold ${sentimentColors[article.sentiment.label]}`}
                      >
                        {article.sentiment.label}
                      </span>
                    </div>
                    {article.sentiment.long_term_label && article.sentiment.long_term_label !== article.sentiment.label && (
                      <div className="flex items-center gap-1">
                        <span className="text-[8px] text-text-muted/60 font-mono">LT</span>
                        <span
                          className={`text-[9px] uppercase tracking-wider px-1.5 py-0.5 rounded font-semibold ${sentimentColors[article.sentiment.long_term_label]}`}
                        >
                          {article.sentiment.long_term_label}
                        </span>
                      </div>
                    )}
                  </div>
                )}
                <ExternalLink
                  size={12}
                  className="text-text-muted opacity-0 group-hover:opacity-100 transition-opacity"
                />
              </div>
            </a>
          ))
        ) : (
          <p className="text-text-muted text-sm text-center py-8">
            No news articles available yet
          </p>
        )}
      </div>
    </div>
  );
}
