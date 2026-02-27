/** TradeSignal - Entry Point */

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { Provider as JotaiProvider } from "jotai";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import "./index.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      retry: 2,
      refetchOnWindowFocus: false,
    },
  },
});

// Setup WebSocket for live streaming
let ws: WebSocket;
function connectWS() {
  const wsUrl =
    window.location.protocol === "https:"
      ? `wss://${window.location.host}/api/v1/ws/updates`
      : `ws://${window.location.host}/api/v1/ws/updates`;

  ws = new WebSocket(wsUrl);

  ws.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);
      const { type, data } = payload;

      if (!data) return;

      switch (type) {
        case "live_prices":
          queryClient.setQueryData(["live-prices"], data);
          // Also update dashboard if entries exist
          queryClient.setQueryData(["dashboard"], (old: any) => {
            if (!old) return old;
            return old.map((inst: any) => {
              const update = data.find((d: any) => d.instrument_id === inst.id);
              if (update) {
                return {
                  ...inst,
                  price: update.price,
                  change_amount: update.change_amount,
                  change_percent: update.change_percent,
                  market_status: update.market_status,
                };
              }
              return inst;
            });
          });
          break;

        case "news_updates": {
          // Helper to merge and deduplicate news arrays
          const mergeNews = (incoming: any[], existing: any[], limit: number) => {
            const merged = [...incoming, ...(Array.isArray(existing) ? existing : [])];
            const unique = Array.from(
              new Map(merged.map((a) => [a.id, a])).values(),
            );
            return unique
              .sort(
                (a, b) =>
                  new Date(b.published_at || 0).getTime() -
                  new Date(a.published_at || 0).getTime(),
              )
              .slice(0, limit);
          };

          // Update global news latest feed
          queryClient.setQueryData(["news-latest"], (old: any) => {
            if (!old) return data;
            return mergeNews(data, old, 50);
          });

          // Update macro-news feed (only if categories match)
          const macroCategories = [
            "us_politics",
            "uk_politics",
            "us_finance",
            "uk_finance",
          ];
          const macroOnly = data.filter((a: any) =>
            macroCategories.includes(a.category),
          );
          if (macroOnly.length > 0) {
            queryClient.setQueryData(["macro-news"], (old: any) => {
              if (!old) return macroOnly;
              return mergeNews(macroOnly, old, 20);
            });
          }

          // Update all news-page queries (News page real-time updates)
          // Query key format: ["news-page", region, categoryType]
          queryClient
            .getQueryCache()
            .findAll({ queryKey: ["news-page"] })
            .forEach((query) => {
              const [, qRegion, qCatType] = query.queryKey as string[];
              // Filter incoming articles to match the query's filters
              const filtered = data.filter((a: any) => {
                if (qRegion && qRegion !== "all" && !a.category.startsWith(`${qRegion}_`)) return false;
                if (qCatType && qCatType !== "all") {
                  if (qCatType === "macro") {
                    if (!macroCategories.includes(a.category)) return false;
                  } else {
                    // For specific region+type combo, the queryFn uses exact category
                    // For "all" region + type, we filter by suffix
                    if (qRegion && qRegion !== "all") {
                      if (a.category !== `${qRegion}_${qCatType}`) return false;
                    } else {
                      if (!a.category.endsWith(`_${qCatType}`)) return false;
                    }
                  }
                }
                return true;
              });
              if (filtered.length === 0) return;
              queryClient.setQueryData(query.queryKey, (old: any) => {
                if (!old) return old;
                return mergeNews(filtered, old, 200);
              });
            });

          // Update specific instrument news if applicable
          data.forEach((article: any) => {
            if (article.instrument_id) {
              queryClient.setQueryData(
                ["news", article.instrument_id],
                (old: any) => {
                  if (!old) return [article];
                  return mergeNews([article], old, 30);
                },
              );
            }
          });
          break;
        }

        case "grade_updates":
          queryClient.setQueryData(["grades"], data);
          // Update dashboard grades
          queryClient.setQueryData(["dashboard"], (old: any) => {
            if (!old) return old;
            return old.map((inst: any) => {
              const updates = data.filter(
                (d: any) => d.instrument_id === inst.id,
              );
              if (updates.length > 0) {
                const short = updates.find((u: any) => u.term === "short");
                const long = updates.find((u: any) => u.term === "long");
                return {
                  ...inst,
                  short_term_grade:
                    short?.overall_grade || inst.short_term_grade,
                  short_term_score:
                    short?.overall_score || inst.short_term_score,
                  long_term_grade: long?.overall_grade || inst.long_term_grade,
                  long_term_score: long?.overall_score || inst.long_term_score,
                  graded_at: updates[0].graded_at,
                };
              }
              return inst;
            });
          });
          // Update specific instrument grades if open
          data.forEach((grade: any) => {
            queryClient.setQueryData(
              ["grades", grade.instrument_id],
              (old: any) => {
                if (!old) return old;
                const others = old.filter((g: any) => g.term !== grade.term);
                return [...others, grade];
              },
            );
          });
          break;

        case "technical_updates":
          // Update specific technical queries for open instruments
          data.forEach((tech: any) => {
            queryClient.setQueryData(
              ["technical", tech.instrument_id],
              (old: any) => {
                if (!old) return old;
                // Add new indicator or update existing one
                const others = old.filter(
                  (t: any) => t.indicator_name !== tech.indicator_name,
                );
                return [tech, ...others].slice(0, 20); // Keep latest
              },
            );
          });
          break;

        case "macro_sentiment_updates":
          queryClient.setQueryData(["macro-sentiment"], data);
          break;
      }
    } catch (e) {
      console.error("WS parse error", e);
    }
  };

  ws.onclose = () => {
    setTimeout(connectWS, 5000);
  };
}
connectWS();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <JotaiProvider>
        <App />
      </JotaiProvider>
    </QueryClientProvider>
  </StrictMode>,
);
