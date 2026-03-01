/** Full news browser page with category filters + real-time WS updates */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  Newspaper,
  ExternalLink,
  Clock,
  Filter,
  Search,
  X,
} from "lucide-react";
import Fuse from "fuse.js";
import { fetchNews } from "../api/client";
import { wsSubscribe } from "../ws";
import type { NewsArticle } from "../types";

const sentimentColors: Record<string, string> = {
  "very positive":
    "text-accent-emerald bg-accent-emerald/20 border border-accent-emerald/30",
  positive: "text-accent-emerald bg-accent-emerald/10",
  negative: "text-accent-rose bg-accent-rose/10",
  "very negative":
    "text-accent-rose bg-accent-rose/20 border border-accent-rose/30",
  neutral: "text-text-muted bg-surface-3",
};

const categoryBadgeStyles: Record<
  string,
  { label: string; className: string }
> = {
  macro_markets: {
    label: "Markets",
    className: "text-accent-amber bg-accent-amber/10",
  },
  macro_politics: {
    label: "Politics",
    className: "text-accent-violet bg-accent-violet/10",
  },
  macro_conflict: {
    label: "Conflict",
    className: "text-accent-rose bg-accent-rose/10",
  },
  asset_specific: {
    label: "Asset",
    className: "text-accent-blue bg-accent-blue/10",
  },
};

const categoryOptions = [
  { value: "all", label: "All" },
  { value: "macro", label: "Macro" },
  { value: "macro_markets", label: "Markets" },
  { value: "macro_politics", label: "Politics" },
  { value: "macro_conflict", label: "Conflict" },
  { value: "asset_specific", label: "Asset" },
] as const;

const macroCategories = [
  "macro_markets",
  "macro_politics",
  "macro_conflict",
];

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

export default function News() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchQuery, setSearchQuery] = useState("");
  const [newIds, setNewIds] = useState<Set<string>>(new Set());
  const prevIdsRef = useRef<Set<string>>(new Set());

  const categoryType = searchParams.get("type") || "all";

  // Subscribe WS to news with current filters
  useEffect(() => {
    wsSubscribe({
      page: "news",
      category: categoryType !== "all" ? categoryType : undefined,
    });
  }, [categoryType]);

  const { data: rawArticles = [], isLoading } = useQuery<NewsArticle[]>({
    queryKey: ["news-page", categoryType],
    queryFn: () => {
      const opts: { category?: string; limit: number } = {
        limit: 200,
      };
      if (categoryType !== "all") {
        opts.category = categoryType;
      }
      return fetchNews(opts);
    },
    refetchInterval: 120_000,
  });

  // Client-side filtering for "macro" meta-category
  const articles = useMemo(() => {
    if (categoryType === "all") return rawArticles;
    if (categoryType === "macro") {
      return rawArticles.filter(
        (a) => macroCategories.includes(a.category) || a.is_macro,
      );
    }
    return rawArticles;
  }, [rawArticles, categoryType]);

  // Track new article IDs for animation
  useEffect(() => {
    const currentIds = new Set(articles.map((a) => a.id));
    if (prevIdsRef.current.size > 0) {
      const fresh = new Set<string>();
      currentIds.forEach((id) => {
        if (!prevIdsRef.current.has(id)) fresh.add(id);
      });
      if (fresh.size > 0) {
        setTimeout(() => setNewIds(fresh), 0);
        const timer = setTimeout(() => setNewIds(new Set()), 2000);
        return () => clearTimeout(timer);
      }
    }
    prevIdsRef.current = currentIds;
  }, [articles]);

  // Fuse.js fuzzy search
  const fuse = useMemo(
    () =>
      new Fuse(articles, {
        keys: ["title", "summary", "source"],
        threshold: 0.4,
        ignoreLocation: true,
      }),
    [articles],
  );

  const displayedArticles = useMemo(() => {
    if (!searchQuery.trim()) return articles;
    return fuse.search(searchQuery).map((r) => r.item);
  }, [articles, fuse, searchQuery]);

  const setFilter = useCallback(
    (key: string, value: string) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        if (value === "all") {
          next.delete(key);
        } else {
          next.set(key, value);
        }
        return next;
      });
    },
    [setSearchParams],
  );

  return (
    <div className="max-w-[1400px] mx-auto px-6 py-8">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <Newspaper size={24} className="text-accent-amber" />
        <h1 className="text-2xl font-bold text-text-primary">News</h1>
        <span className="text-sm text-text-muted ml-2">
          {displayedArticles.length} articles
        </span>
      </div>

      {/* Search + Filters */}
      <div className="flex flex-wrap items-center gap-4 mb-6 p-4 rounded-xl bg-surface-1 border border-border-subtle">
        {/* Search input */}
        <div className="relative">
          <Search
            size={14}
            className="absolute left-3 top-1/2 -translate-y-1/2 text-text-muted"
          />
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search news..."
            className="pl-8 pr-8 py-1.5 w-52 rounded-lg bg-surface-2 border border-border-subtle text-sm text-text-primary placeholder:text-text-muted/50 focus:outline-none focus:border-accent-cyan/40 transition-colors"
          />
          {searchQuery && (
            <button
              onClick={() => setSearchQuery("")}
              className="absolute right-2 top-1/2 -translate-y-1/2 text-text-muted hover:text-text-primary"
            >
              <X size={14} />
            </button>
          )}
        </div>

        <div className="w-px h-6 bg-border-subtle" />

        <Filter size={16} className="text-text-muted" />

        {/* Category type filter */}
        <div className="flex items-center gap-1">
          {categoryOptions.map((opt) => (
            <button
              key={opt.value}
              onClick={() => setFilter("type", opt.value)}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                categoryType === opt.value
                  ? "bg-accent-violet/10 text-accent-violet border border-accent-violet/20"
                  : "text-text-secondary hover:text-text-primary hover:bg-surface-3/50"
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* Articles list */}
      <div className="rounded-xl bg-surface-1 border border-border-subtle p-5">
        {isLoading ? (
          <div className="space-y-3">
            {Array.from({ length: 8 }).map((_, i) => (
              <div
                key={i}
                className="h-16 rounded-lg bg-surface-2 animate-pulse"
              />
            ))}
          </div>
        ) : displayedArticles.length > 0 ? (
          <div className="space-y-1">
            {displayedArticles.map((article) => {
              const isNew = newIds.has(article.id);
              return (
                <a
                  key={article.id}
                  href={article.link || "#"}
                  target="_blank"
                  rel="noopener noreferrer"
                  className={`group flex items-start gap-3 p-3 rounded-lg hover:bg-surface-2/50 transition-all duration-200 ${
                    isNew
                      ? "animate-[newsSlideIn_0.6s_ease-out] bg-accent-cyan/5 border-l-2 border-accent-cyan"
                      : ""
                  }`}
                >
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <p className="text-sm text-text-primary group-hover:text-accent-cyan transition-colors line-clamp-2 leading-snug">
                        {article.title}
                      </p>
                      {isNew && (
                        <span className="shrink-0 text-[9px] uppercase tracking-wider px-1.5 py-0.5 rounded font-semibold text-accent-cyan bg-accent-cyan/10 animate-pulse">
                          new
                        </span>
                      )}
                    </div>
                    {article.summary && (
                      <p className="text-xs text-text-muted mt-1 line-clamp-1">
                        {article.summary}
                      </p>
                    )}
                    <div className="flex items-center gap-2 mt-1.5 flex-wrap">
                      {categoryBadgeStyles[article.category] && (
                        <span
                          className={`text-[9px] uppercase tracking-wider px-1.5 py-0.5 rounded font-semibold ${categoryBadgeStyles[article.category].className}`}
                        >
                          {categoryBadgeStyles[article.category].label}
                        </span>
                      )}
                      {article.is_macro && (
                        <span className="text-[9px] uppercase tracking-wider px-1.5 py-0.5 rounded font-semibold text-accent-amber bg-accent-amber/10">
                          Macro
                        </span>
                      )}
                      <span className="text-[10px] text-text-muted font-medium">
                        {article.source}
                      </span>
                      <span className="text-[10px] text-text-muted/50">
                        &bull;
                      </span>
                      <span className="text-[10px] text-text-muted flex items-center gap-0.5">
                        <Clock size={8} />
                        {timeAgo(article.published_at)}
                      </span>
                    </div>
                  </div>
                  <div className="flex flex-col items-end gap-1 shrink-0">
                    {article.sentiment && (
                      <span
                        className={`text-[9px] uppercase tracking-wider px-1.5 py-0.5 rounded font-semibold ${sentimentColors[article.sentiment.label]}`}
                      >
                        {article.sentiment.label}
                      </span>
                    )}
                    <ExternalLink
                      size={12}
                      className="text-text-muted opacity-0 group-hover:opacity-100 transition-opacity"
                    />
                  </div>
                </a>
              );
            })}
          </div>
        ) : (
          <p className="text-text-muted text-sm text-center py-12">
            No news articles found for the selected filters
          </p>
        )}
      </div>
    </div>
  );
}
