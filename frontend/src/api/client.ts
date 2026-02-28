/** Central API client for TradeSignal */

import type {
  APIResponse,
  DashboardInstrument,
  Grade,
  HistoricalPrice,
  Instrument,
  LivePrice,
  MacroSentiment,
  NewsArticle,
  TechnicalIndicator,
} from "../types";

const BASE = "/api/v1";

async function fetchAPI<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  const json: APIResponse<T> = await res.json();
  if (json.error) {
    throw new Error(json.error);
  }
  return json.data as T;
}

async function postAPI<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  const json: APIResponse<T> = await res.json();
  if (json.error) {
    throw new Error(json.error);
  }
  return json.data as T;
}

async function deleteAPI<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { method: "DELETE" });
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  const json: APIResponse<T> = await res.json();
  if (json.error) {
    throw new Error(json.error);
  }
  return json.data as T;
}

// Dashboard
export const fetchDashboard = () =>
  fetchAPI<DashboardInstrument[]>("/dashboard");
export const fetchMacroSentiment = () =>
  fetchAPI<MacroSentiment[]>("/dashboard/macro");
export const fetchMacroNews = (limit = 30) =>
  fetchAPI<NewsArticle[]>(`/dashboard/macro/news?limit=${limit}`);

// Instruments
export const fetchInstruments = (category?: string) =>
  fetchAPI<Instrument[]>(
    `/instruments${category ? `?category=${category}` : ""}`,
  );
export const fetchInstrument = (id: string) =>
  fetchAPI<Instrument>(`/instruments/${id}`);

// Grades
export const fetchGrades = (instrumentId?: string, term?: string) => {
  const params = new URLSearchParams();
  if (instrumentId) params.set("instrument_id", instrumentId);
  if (term) params.set("term", term);
  const qs = params.toString();
  return fetchAPI<Grade[]>(`/grades${qs ? `?${qs}` : ""}`);
};
export const fetchGradeHistory = (
  instrumentId: string,
  term = "short",
  limit = 30,
) =>
  fetchAPI<Grade[]>(
    `/grades/history/${instrumentId}?term=${term}&limit=${limit}`,
  );

// News
export const fetchNews = (opts?: {
  category?: string;
  region?: string;
  instrumentId?: string;
  limit?: number;
}) => {
  const params = new URLSearchParams();
  if (opts?.category) params.set("category", opts.category);
  if (opts?.region) params.set("region", opts.region);
  if (opts?.instrumentId) params.set("instrument_id", opts.instrumentId);
  if (opts?.limit) params.set("limit", String(opts.limit));
  const qs = params.toString();
  return fetchAPI<NewsArticle[]>(`/news${qs ? `?${qs}` : ""}`);
};

// Prices
export const fetchLivePrices = () => fetchAPI<LivePrice[]>("/prices/live");
export const fetchLivePrice = (instrumentId: string) =>
  fetchAPI<LivePrice>(`/prices/live/${instrumentId}`);
export const fetchHistoricalPrices = (instrumentId: string, days = 365) =>
  fetchAPI<HistoricalPrice[]>(
    `/prices/historical/${instrumentId}?days=${days}`,
  );

// Technical
export const fetchTechnicalIndicators = (instrumentId: string) =>
  fetchAPI<TechnicalIndicator[]>(`/technical/${instrumentId}`);

// AI Analysis
export const fetchAIAnalysis = (instrument_id: string) =>
  fetchAPI<{ analysis: string }>(`/ai-analysis/${instrument_id}`);

export const fetchIndependentAIAnalysis = (instrument_id: string) =>
  fetchAPI<{ analysis: string }>(`/ai-analysis/independent/${instrument_id}`);

// Add Instruments
export const addInstruments = (symbols: string) =>
  postAPI<{ created: Instrument[]; skipped: string[] }>("/instruments", {
    symbols,
  });

// Portfolio
export const fetchPortfolio = () => fetchAPI<string[]>("/portfolio");
export const addToPortfolio = (instrumentId: string) =>
  postAPI<{ ok: boolean }>("/portfolio", { instrument_id: instrumentId });
export const removeFromPortfolio = (instrumentId: string) =>
  deleteAPI<{ ok: boolean }>(`/portfolio/${instrumentId}`);
