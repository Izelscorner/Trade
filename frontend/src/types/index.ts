/** TradeSignal TypeScript types */

export type Category = "stock" | "etf" | "commodity";

export interface Instrument {
  id: string;
  symbol: string;
  name: string;
  category: Category;
}

export interface DashboardInstrument {
  id: string;
  symbol: string;
  name: string;
  category: Category;
  price: number | null;
  change_amount: number | null;
  change_percent: number | null;
  market_status: string | null;
  short_term_grade: string | null;
  short_term_score: number | null;
  long_term_grade: string | null;
  long_term_score: number | null;
  graded_at: string | null;
}

export interface Grade {
  id: string;
  instrument_id: string;
  symbol: string;
  name: string;
  term: "short" | "long";
  overall_grade: string;
  overall_score: number;
  technical_score: number;
  sentiment_score: number;
  macro_score: number;
  details: GradeDetails | null;
  graded_at: string;
}

export interface GradeDetails {
  weights: { technical: number; sentiment: number; macro: number };
  technical: Record<string, { signal: string; score: number }>;
  sentiment: {
    articles?: number;
    avg_positive?: number;
    avg_negative?: number;
    source?: string;
  };
  macro: Record<string, { score: number; label: string; articles: number }>;
}

export interface Sentiment {
  positive: number;
  negative: number;
  neutral: number;
  label:
    | "very positive"
    | "positive"
    | "neutral"
    | "negative"
    | "very negative";
}

export interface NewsArticle {
  id: string;
  instrument_id?: string;
  title: string;
  link: string | null;
  summary: string | null;
  source: string;
  category: string;
  is_macro?: boolean;
  is_asset_specific?: boolean;
  published_at: string | null;
  sentiment: Sentiment | null;
}

export interface LivePrice {
  id: string;
  instrument_id: string;
  symbol: string;
  name: string;
  price: number;
  change_amount: number | null;
  change_percent: number | null;
  market_status: string;
  fetched_at: string;
}

export interface HistoricalPrice {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface TechnicalIndicator {
  indicator_name: string;
  instrument_id?: string;
  value: Record<string, number>;
  signal: string;
  date: string;
  calculated_at: string;
}

export interface MacroSentiment {
  region: string;
  score: number;
  label: string;
  article_count: number;
  calculated_at: string;
}

export interface APIResponse<T> {
  data: T | null;
  error: string | null;
  timestamp: string;
}
