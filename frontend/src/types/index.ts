/** TradeSignal TypeScript types */

export type Category = "stock" | "etf" | "commodity";

export type Sector =
  | "technology"
  | "financials"
  | "healthcare"
  | "consumer_discretionary"
  | "consumer_staples"
  | "communication"
  | "energy"
  | "industrials"
  | "materials"
  | "utilities"
  | "real_estate";

export const SECTOR_LABELS: Record<Sector, string> = {
  technology: "Technology",
  financials: "Financials",
  healthcare: "Healthcare",
  consumer_discretionary: "Consumer Disc.",
  consumer_staples: "Consumer Staples",
  communication: "Communication",
  energy: "Energy",
  industrials: "Industrials",
  materials: "Materials",
  utilities: "Utilities",
  real_estate: "Real Estate",
};

export interface Instrument {
  id: string;
  symbol: string;
  name: string;
  category: Category;
  sector?: Sector | null;
}

export interface DashboardInstrument {
  id: string;
  symbol: string;
  name: string;
  category: Category;
  sector?: Sector | null;
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

/** Sigmoid buy-confidence from composite score ∈ [-3, 3] → (0, 100). */
export function scoreToBuyConfidence(score: number | null): number {
  if (score === null) return 50;
  const k = 1.5;
  return Math.round(100 / (1 + Math.exp(-k * score)) * 10) / 10;
}

/** Map buy-confidence to an action label. */
export function buyConfidenceToAction(confidence: number): string {
  if (confidence >= 78) return "Strong Buy";
  if (confidence >= 63) return "Buy";
  if (confidence >= 54) return "Slight Buy";
  if (confidence >= 46) return "Neutral";
  if (confidence >= 37) return "Slight Sell";
  if (confidence >= 22) return "Sell";
  return "Strong Sell";
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
  sector_score: number;
  fundamentals_score: number;
  details: GradeDetails | null;
  graded_at: string;
}

export interface MetricConfig {
  label: string;
  sublabel: string;
  expected_range: string;
  direction: "lower" | "higher" | "range";
  direction_text: string;
  good?: number;
  fair?: number;
  range_good?: [number, number];
}

export interface FundamentalMetrics {
  pe_ratio: number | null;
  roe: number | null;
  de_ratio: number | null;
  peg_ratio: number | null;
  fetched_at: string | null;
  sector: string | null;
  config: Record<string, MetricConfig>;
}

export interface MacroIndicatorConfig {
  expected_range: string;
  direction: "up" | "down" | "range";
  direction_text: string;
  impact: string;
  good_zone: [number, number];
  warn_zone: [number, number];
}

export interface MacroIndicator {
  name: string;
  value: number;
  label: string;
  unit: string;
  fetched_at: string | null;
  config: MacroIndicatorConfig;
}

export interface GradeDetails {
  weights: { technical: number; sentiment: number; sector: number; macro: number; fundamentals?: number };
  effective_weights?: { technical: number; sentiment: number; sector: number; macro: number; fundamentals?: number };
  buy_confidence?: number;  // 0–100 sigmoid-scaled buy probability
  action?: string;          // "Strong Buy" | "Buy" | "Slight Buy" | "Neutral" | "Slight Sell" | "Sell" | "Strong Sell"
  technical: {
    group_scores?: Record<string, { score: number; count: number; indicators: Record<string, string> }>;
    data_completeness?: number;
    adx_multiplier?: number;
    atr_risk_factor?: number;
    adx?: string;
    atr_pct?: number;
    raw_tech_score?: number;
    [key: string]: unknown;
  };
  sentiment: {
    articles?: number;
    non_neutral?: number;
    labels?: Record<string, number>;
    mean?: number;
    confidence?: number;
    consensus_adjustment?: number;
    avg_age_hours?: number;
    decay_half_life_h?: number;
    term?: string;
    [key: string]: unknown;
  };
  macro: {
    records?: number;
    articles?: number;
    mean?: number;
    confidence?: number;
    latest_label?: string;
    decay_half_life_h?: number;
    term?: string;
    [key: string]: unknown;
  };
  sector?: {
    sector?: string;
    records?: number;
    articles?: number;
    mean?: number;
    confidence?: number;
    latest_label?: string;
    decay_half_life_h?: number;
    term?: string;
    [key: string]: unknown;
  };
  fundamentals?: {
    has_data?: boolean;
    metrics?: Record<string, { value: number | null; score: number }>;
    raw_score?: number;
    confidence?: number;
    age_hours?: number;
    fetched_at?: string;
    [key: string]: unknown;
  };
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
  long_term_label?:
    | "very positive"
    | "positive"
    | "neutral"
    | "negative"
    | "very negative"
    | null;
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
  term?: string;
  score: number;
  label: string;
  article_count: number;
  calculated_at: string;
}

export interface SectorSentiment {
  sector: string;
  term: string;
  score: number;
  label: string;
  article_count: number;
  calculated_at: string;
}

export interface ETFConstituent {
  symbol: string;
  name: string;
  weight_percent: number;
  tracked_instrument_id: string | null;
  article_count: number;
}

export interface APIResponse<T> {
  data: T | null;
  error: string | null;
  timestamp: string;
}
