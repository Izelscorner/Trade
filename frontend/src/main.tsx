/** TradeSignal - Entry Point */

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { Provider as JotaiProvider } from "jotai";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import { _setWs, _resendSubscription } from "./ws";
import type {
  DashboardInstrument,
  LivePrice,
  NewsArticle,
  Grade,
  TechnicalIndicator,
} from "./types";
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

// --- WebSocket connection ---

function connectWS() {
  const wsUrl =
    window.location.protocol === "https:"
      ? `wss://${window.location.host}/api/v1/ws/updates`
      : `ws://${window.location.host}/api/v1/ws/updates`;

  const socket = new WebSocket(wsUrl);
  _setWs(socket);

  socket.onopen = () => {
    _resendSubscription();
  };

  socket.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);
      const { type, data } = payload;

      if (!data) return;

      switch (type) {
        case "live_prices":
          queryClient.setQueryData(["live-prices"], data);
          // Update dashboard instrument prices
          queryClient.setQueryData(
            ["dashboard"],
            (old: DashboardInstrument[] | undefined) => {
              if (!old) return old;
              return old.map((inst: DashboardInstrument) => {
                const update = data.find(
                  (d: LivePrice) => d.instrument_id === inst.id,
                );
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
            },
          );
          // Update individual instrument live-price cache (for AssetDetail)
          data.forEach((price: LivePrice) => {
            queryClient.setQueryData(
              ["live-price", price.instrument_id],
              price,
            );
          });
          break;

        case "news_updates": {
          const mergeNews = (
            incoming: NewsArticle[],
            existing: NewsArticle[],
            limit: number,
          ) => {
            const merged = [
              ...incoming,
              ...(Array.isArray(existing) ? existing : []),
            ];
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
          queryClient.setQueryData(
            ["news-latest"],
            (old: NewsArticle[] | undefined) => {
              if (!old) return data;
              return mergeNews(data, old, 50);
            },
          );

          // Update macro-news feed
          const macroCategories = [
            "us_politics",
            "uk_politics",
            "us_finance",
            "uk_finance",
          ];
          const macroOnly = data.filter((a: NewsArticle) =>
            macroCategories.includes(a.category),
          );
          if (macroOnly.length > 0) {
            queryClient.setQueryData(
              ["macro-news"],
              (old: NewsArticle[] | undefined) => {
                if (!old) return macroOnly;
                return mergeNews(macroOnly, old, 20);
              },
            );
          }

          // Update news-page queries
          queryClient
            .getQueryCache()
            .findAll({ queryKey: ["news-page"] })
            .forEach((query) => {
              const [, qRegion, qCatType] = query.queryKey as string[];
              const filtered = data.filter((a: NewsArticle) => {
                if (
                  qRegion &&
                  qRegion !== "all" &&
                  !a.category.startsWith(`${qRegion}_`)
                )
                  return false;
                if (qCatType && qCatType !== "all") {
                  if (qCatType === "macro") {
                    if (!macroCategories.includes(a.category)) return false;
                  } else {
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
              queryClient.setQueryData(
                query.queryKey,
                (old: NewsArticle[] | undefined) => {
                  if (!old) return filtered;
                  return mergeNews(filtered, old, 200);
                },
              );
            });

          // Update specific instrument news
          data.forEach((article: NewsArticle) => {
            if (article.instrument_id) {
              queryClient.setQueryData(
                ["news", article.instrument_id],
                (old: NewsArticle[] | undefined) => {
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
          queryClient.setQueryData(
            ["dashboard"],
            (old: DashboardInstrument[] | undefined) => {
              if (!old) return old;
              return old.map((inst: DashboardInstrument) => {
                const updates = data.filter(
                  (d: Grade) => d.instrument_id === inst.id,
                );
                if (updates.length > 0) {
                  const short = updates.find(
                    (u: Grade) => u.term === "short",
                  );
                  const long = updates.find((u: Grade) => u.term === "long");
                  return {
                    ...inst,
                    short_term_grade:
                      short?.overall_grade ?? inst.short_term_grade,
                    short_term_score:
                      short?.overall_score ?? inst.short_term_score,
                    long_term_grade:
                      long?.overall_grade ?? inst.long_term_grade,
                    long_term_score:
                      long?.overall_score ?? inst.long_term_score,
                    graded_at: updates[0].graded_at,
                  };
                }
                return inst;
              });
            },
          );
          // Update specific instrument grades
          data.forEach((grade: Grade) => {
            queryClient.setQueryData(
              ["grades", grade.instrument_id],
              (old: Grade[] | undefined) => {
                if (!old) return [grade];
                const others = old.filter(
                  (g: Grade) => g.term !== grade.term,
                );
                return [...others, grade];
              },
            );
          });
          break;

        case "technical_updates":
          data.forEach((tech: TechnicalIndicator) => {
            queryClient.setQueryData(
              ["technical", tech.instrument_id],
              (old: TechnicalIndicator[] | undefined) => {
                if (!old) return [tech];
                const others = old.filter(
                  (t: TechnicalIndicator) =>
                    t.indicator_name !== tech.indicator_name,
                );
                return [tech, ...others];
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

  socket.onclose = () => {
    _setWs(null);
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
