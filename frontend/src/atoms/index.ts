/** Jotai atoms for TradeSignal state management */

import { atom } from "jotai";
import { atomWithStorage } from "jotai/utils";
import { atomWithQuery } from "jotai-tanstack-query";
import {
  fetchDashboard,
  fetchMacroSentiment,
  fetchMacroNews,
  fetchGrades,
  fetchNews,
  fetchLivePrices,
  fetchHistoricalPrices,
  fetchTechnicalIndicators,
  fetchGradeHistory,
  fetchAIAnalysis,
} from "../api/client";
import type {
  Category,
  DashboardInstrument,
  Grade,
  HistoricalPrice,
  LivePrice,
  MacroSentiment,
  NewsArticle,
  TechnicalIndicator,
} from "../types";

// --- UI State ---
export const selectedCategoryAtom = atom<Category | "all">("all");
export const selectedInstrumentIdAtom = atom<string | null>(null);
export const showSentimentAtom = atomWithStorage<boolean>("showSentiment", true);

// --- Dashboard ---
export const dashboardAtom = atomWithQuery<DashboardInstrument[]>(() => ({
  queryKey: ["dashboard"],
  queryFn: fetchDashboard,
  refetchInterval: 60_000,
}));

export const macroSentimentAtom = atomWithQuery<MacroSentiment[]>(() => ({
  queryKey: ["macro-sentiment"],
  queryFn: fetchMacroSentiment,
  refetchInterval: 120_000,
}));

export const macroNewsAtom = atomWithQuery<NewsArticle[]>(() => ({
  queryKey: ["macro-news"],
  queryFn: fetchMacroNews,
  refetchInterval: 120_000,
}));

// --- Live Prices ---
export const livePricesAtom = atomWithQuery<LivePrice[]>(() => ({
  queryKey: ["live-prices"],
  queryFn: fetchLivePrices,
  // refetchInterval is removed because WebSocket maintains live streaming
}));

// --- Grades ---
export const gradesAtom = atomWithQuery<Grade[]>(() => ({
  queryKey: ["grades"],
  queryFn: () => fetchGrades(),
  refetchInterval: 120_000,
}));

// --- News ---
export const latestNewsAtom = atomWithQuery<NewsArticle[]>(() => ({
  queryKey: ["news-latest"],
  queryFn: () => fetchNews(),
  refetchInterval: 120_000,
}));

// --- Instrument Detail Atoms (parameterized) ---
export const instrumentHistoricalAtom = (instrumentId: string, days = 365) =>
  atomWithQuery<HistoricalPrice[]>(() => ({
    queryKey: ["historical", instrumentId, days],
    queryFn: () => fetchHistoricalPrices(instrumentId, days),
    enabled: !!instrumentId,
  }));

export const instrumentTechnicalAtom = (instrumentId: string) =>
  atomWithQuery<TechnicalIndicator[]>(() => ({
    queryKey: ["technical", instrumentId],
    queryFn: () => fetchTechnicalIndicators(instrumentId),
    enabled: !!instrumentId,
  }));

export const instrumentNewsAtom = (instrumentId: string) =>
  atomWithQuery<NewsArticle[]>(() => ({
    queryKey: ["news", instrumentId],
    queryFn: () => fetchNews({ instrumentId }),
    enabled: !!instrumentId,
  }));

export const instrumentGradesAtom = (instrumentId: string) =>
  atomWithQuery<Grade[]>(() => ({
    queryKey: ["grades", instrumentId],
    queryFn: () => fetchGrades(instrumentId),
    enabled: !!instrumentId,
  }));

export const instrumentGradeHistoryAtom = (
  instrumentId: string,
  term: string,
) =>
  atomWithQuery<Grade[]>(() => ({
    queryKey: ["grade-history", instrumentId, term],
    queryFn: () => fetchGradeHistory(instrumentId, term),
    enabled: !!instrumentId,
  }));

export const instrumentAIAnalysisAtom = (instrumentId: string) =>
  atomWithQuery<{ analysis: string }>(() => ({
    queryKey: ["ai-analysis", instrumentId],
    queryFn: () => fetchAIAnalysis(instrumentId),
    enabled: false, // Only trigger on demand
    staleTime: 300_000, // Cache for 5 mins
  }));
