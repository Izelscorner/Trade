/** Asset Detail page */

import { useMemo } from "react";
import { useParams, Link } from "react-router-dom";
import { useAtom } from "jotai";
import { useQuery } from "@tanstack/react-query";
import {
  instrumentHistoricalAtom,
  instrumentTechnicalAtom,
  instrumentNewsAtom,
  instrumentGradesAtom,
  instrumentAIAnalysisAtom,
  macroSentimentAtom,
} from "../atoms";
import {
  fetchInstrument,
  fetchLivePrice,
  prioritizeInstrument,
  fetchConfig,
  fetchETFConstituents,
  fetchSectorSentiment,
  fetchNews,
  fetchFundamentals,
  fetchMacroIndicators,
} from "../api/client";
import { wsSubscribe } from "../ws";
import PriceChange from "../components/PriceChange";
import PriceChart from "../components/PriceChart";
import TechnicalPanel from "../components/TechnicalPanel";
import GradeDetail from "../components/GradeDetail";
import MacroSentimentCard from "../components/MacroSentimentCard";
import FundamentalsPanel from "../components/FundamentalsPanel";
import MacroIndicatorsCard from "../components/MacroIndicatorsCard";
import NewsFeed from "../components/NewsFeed";
import AIAnalysisModal from "../components/AIAnalysisModal";
import { PageSkeleton } from "../components/Skeletons";
import { ArrowLeft, CircleDot, Brain, Layers, Building2 } from "lucide-react";
import type { Instrument, LivePrice, Grade, ETFConstituent, SectorSentiment, FundamentalMetrics, MacroIndicator } from "../types";
import { SECTOR_LABELS, type Sector } from "../types";
import { useEffect, useState } from "react";

const marketStatusConfig: Record<string, { label: string; color: string }> = {
  active: { label: "Market Open", color: "text-accent-emerald" },
  pre_market: { label: "Pre-Market", color: "text-accent-amber" },
  after_hours: { label: "After Hours", color: "text-accent-violet" },
  closed: { label: "Market Closed", color: "text-text-muted" },
};

export default function AssetDetail() {
  const { id } = useParams<{ id: string }>();
  const [instrument, setInstrument] = useState<Instrument | null>(null);
  const [livePrice, setLivePrice] = useState<LivePrice | null>(null);
  const [loading, setLoading] = useState(true);

  const [chartDays, setChartDays] = useState(1);

  // Parameterized atoms
  const historicalAtom = useMemo(
    () => instrumentHistoricalAtom(id || "", chartDays),
    [id, chartDays],
  );
  const technicalAtom = useMemo(() => instrumentTechnicalAtom(id || ""), [id]);
  const newsAtom = useMemo(() => instrumentNewsAtom(id || ""), [id]);
  const gradesAtom = useMemo(() => instrumentGradesAtom(id || ""), [id]);
  const aiAtom = useMemo(() => instrumentAIAnalysisAtom(id || ""), [id]);

  const [{ data: historical }] = useAtom(historicalAtom);
  const [{ data: technical }] = useAtom(technicalAtom);
  const [{ data: news }] = useAtom(newsAtom);
  const [{ data: grades }] = useAtom(gradesAtom);
  const [{ data: macroSentiments }] = useAtom(macroSentimentAtom);
  const [{ data: aiAnalysis, isFetching: aiLoading, refetch: fetchAI }] =
    useAtom(aiAtom);

  const [aiModalOpen, setAiModalOpen] = useState(false);

  const { data: config } = useQuery({
    queryKey: ["config"],
    queryFn: fetchConfig,
    staleTime: Infinity,
  });

  const { data: etfConstituents } = useQuery<ETFConstituent[]>({
    queryKey: ["etf-constituents", id],
    queryFn: () => fetchETFConstituents(id!),
    enabled: !!id && instrument?.category === "etf",
    staleTime: 600_000,
  });

  const { data: sectorSentiments } = useQuery<SectorSentiment[]>({
    queryKey: ["sector-sentiment", instrument?.sector],
    queryFn: () => fetchSectorSentiment(instrument!.sector!),
    enabled: !!instrument?.sector,
    staleTime: 60_000,
  });

  const { data: sectorNews } = useQuery({
    queryKey: ["sector-news", instrument?.sector],
    queryFn: () => fetchNews({ category: `sector_${instrument!.sector}` }),
    enabled: !!instrument?.sector,
    staleTime: 120_000,
  });

  const { data: fundamentals } = useQuery<FundamentalMetrics>({
    queryKey: ["fundamentals", id],
    queryFn: () => fetchFundamentals(id!),
    enabled: !!id && instrument?.category !== "commodity",
    staleTime: 600_000,
  });

  const { data: macroIndicators } = useQuery<MacroIndicator[]>({
    queryKey: ["macro-indicators"],
    queryFn: fetchMacroIndicators,
    staleTime: 600_000,
  });

  const modelDisplay = useMemo(() => {
    const raw = config?.nim_model || "";
    const name = raw.includes("/") ? raw.split("/").pop()! : raw;
    return name.toUpperCase() || "AI";
  }, [config]);

  // Subscribe WS to this specific instrument + prioritize its unprocessed news
  useEffect(() => {
    if (id) {
      wsSubscribe({ page: "asset_detail", instrument_ids: [id] });
      prioritizeInstrument(id);
    }
  }, [id]);

  // Live price from WS-updated query cache
  const { data: wsLivePrice } = useQuery<LivePrice>({
    queryKey: ["live-price", id],
    queryFn: () => fetchLivePrice(id!),
    enabled: !!id,
  });

  useEffect(() => {
    if (!id) return;
    let isMounted = true;

    const fetchData = async () => {
      const inst = await fetchInstrument(id).catch(() => null);
      const price = await fetchLivePrice(id).catch(() => null);
      if (isMounted) {
        setInstrument(inst);
        setLivePrice(price);
        setLoading(false);
      }
    };

    queueMicrotask(() => {
      if (isMounted) setLoading(true);
    });
    fetchData();

    return () => {
      isMounted = false;
    };
  }, [id]);

  if (loading || !instrument) return <PageSkeleton />;

  const shortGrade = grades?.find((g: Grade) => g.term === "short") || null;
  const longGrade = grades?.find((g: Grade) => g.term === "long") || null;

  // Prefer WS-updated price, fall back to initial fetch
  const currentPrice = wsLivePrice || livePrice;
  const statusConfig =
    marketStatusConfig[currentPrice?.market_status || "closed"];

  return (
    <div className="max-w-[1400px] mx-auto px-6 py-8 space-y-8">
      {/* Back + Header */}
      <div className="animate-fade-in">
        <Link
          to="/assets"
          className="inline-flex items-center gap-1.5 text-sm text-text-secondary hover:text-accent-cyan transition-colors mb-4"
        >
          <ArrowLeft size={14} />
          Back to Assets
        </Link>

        <div className="flex flex-col md:flex-row md:items-end justify-between gap-4">
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-3xl font-bold text-text-primary">
                {instrument.symbol}
              </h1>
              <span className="text-[10px] uppercase tracking-wider px-2 py-0.5 rounded-md bg-surface-3 text-text-muted border border-border-subtle font-medium">
                {instrument.category}
              </span>

              <div className="flex items-center gap-2">
                <button
                  onClick={() => {
                    setAiModalOpen(true);
                    if (!aiAnalysis) fetchAI();
                  }}
                  className="flex items-center gap-1.5 px-3 py-1 rounded-full bg-accent-cyan/10 text-accent-cyan border border-accent-cyan/20 hover:bg-accent-cyan/20 transition-all duration-200 group"
                  title="Analysis based on local news, technicals and sentiment"
                >
                  <Brain
                    size={14}
                    className="group-hover:scale-110 transition-transform"
                  />
                  <span className="text-[10px] font-bold uppercase tracking-wider">
                    AI Analysis
                  </span>
                </button>
              </div>

              {statusConfig && (
                <div
                  className={`flex items-center gap-1 text-xs font-medium ${statusConfig.color}`}
                >
                  <CircleDot size={10} className="animate-pulse" />
                  {statusConfig.label}
                </div>
              )}
            </div>
            <p className="text-text-secondary mt-1">{instrument.name}</p>
          </div>

          {currentPrice && (
            <PriceChange
              symbol={instrument.symbol}
              price={currentPrice.price}
              changeAmount={currentPrice.change_amount}
              changePercent={currentPrice.change_percent}
              size="lg"
            />
          )}
        </div>
      </div>

      {/* Grades */}
      <div className="animate-slide-up" style={{ animationDelay: "100ms" }}>
        <GradeDetail shortGrade={shortGrade} longGrade={longGrade} />
      </div>

      {/* Fundamentals (stocks/ETFs only) */}
      {instrument.category !== "commodity" && fundamentals && (
        <div className="animate-slide-up" style={{ animationDelay: "125ms" }}>
          <FundamentalsPanel metrics={fundamentals} category={instrument.category} />
        </div>
      )}

      {/* Macro Sentiment */}
      <div className="animate-slide-up" style={{ animationDelay: "150ms" }}>
        <MacroSentimentCard sentiments={macroSentiments || []} />
      </div>

      {/* Macro Economic Indicators */}
      {macroIndicators && macroIndicators.length > 0 && (
        <div className="animate-slide-up" style={{ animationDelay: "155ms" }}>
          <MacroIndicatorsCard indicators={macroIndicators} />
        </div>
      )}

      {/* Sector Sentiment */}
      {instrument.sector && sectorSentiments && sectorSentiments.length > 0 && (
        <div className="animate-slide-up" style={{ animationDelay: "160ms" }}>
          <SectorSentimentCard
            sector={instrument.sector}
            sentiments={sectorSentiments}
          />
        </div>
      )}

      {/* ETF Constituents */}
      {instrument.category === "etf" &&
        etfConstituents &&
        etfConstituents.length > 0 && (
          <div className="animate-slide-up" style={{ animationDelay: "175ms" }}>
            <div className="rounded-xl bg-surface-1 border border-border-subtle p-5">
              <div className="flex items-center gap-2 mb-4">
                <Layers size={18} className="text-accent-cyan" />
                <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
                  ETF Holdings
                </h2>
                <span className="text-xs text-text-muted ml-auto">
                  {etfConstituents.length} constituents
                </span>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
                {etfConstituents.map((c) => (
                  <div
                    key={c.symbol}
                    className="flex items-center justify-between p-2.5 rounded-lg bg-surface-2/50 hover:bg-surface-2 transition-colors"
                  >
                    <div className="flex items-center gap-2 min-w-0">
                      {c.tracked_instrument_id ? (
                        <Link
                          to={`/asset/${c.tracked_instrument_id}`}
                          className="text-sm font-semibold text-accent-cyan hover:underline"
                        >
                          {c.symbol}
                        </Link>
                      ) : (
                        <span className="text-sm font-semibold text-text-primary">
                          {c.symbol}
                        </span>
                      )}
                      <span className="text-[10px] text-text-muted truncate">
                        {c.name}
                      </span>
                    </div>
                    <div className="flex items-center gap-2 shrink-0 ml-2">
                      <span
                        className={`text-[10px] font-mono ${c.article_count > 0 ? "text-accent-amber" : "text-text-muted/40"}`}
                      >
                        {c.article_count} art
                      </span>
                      <div
                        className="h-1.5 rounded-full bg-accent-cyan/30"
                        style={{
                          width: `${Math.max(12, c.weight_percent * 2)}px`,
                        }}
                      />
                      <span className="text-xs font-mono font-medium text-text-secondary">
                        {c.weight_percent.toFixed(1)}%
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

      {/* Chart + Technicals */}
      <div
        className="grid grid-cols-1 lg:grid-cols-2 gap-6 animate-slide-up"
        style={{ animationDelay: "200ms" }}
      >
        <PriceChart
          data={historical || []}
          symbol={instrument.symbol}
          days={chartDays}
          onDaysChange={setChartDays}
        />
        <TechnicalPanel indicators={technical || []} />
      </div>

      {/* News */}
      <div className="animate-slide-up" style={{ animationDelay: "300ms" }}>
        <NewsFeed
          articles={news || []}
          title={`${instrument.symbol} Related News`}
        />
      </div>

      {/* Sector News */}
      {instrument.sector && sectorNews && sectorNews.length > 0 && (
        <div className="animate-slide-up" style={{ animationDelay: "350ms" }}>
          <NewsFeed
            articles={sectorNews}
            title={`${SECTOR_LABELS[instrument.sector as Sector] || instrument.sector} Sector News`}
            icon={<Building2 size={18} className="text-accent-cyan" />}
          />
        </div>
      )}

      <AIAnalysisModal
        isOpen={aiModalOpen}
        onClose={() => setAiModalOpen(false)}
        analysis={aiAnalysis?.analysis || null}
        isLoading={aiLoading}
        symbol={instrument.symbol}
        title={`${modelDisplay} AI Analysis`}
      />
    </div>
  );
}

function SectorSentimentCard({
  sector,
  sentiments,
}: {
  sector: string;
  sentiments: SectorSentiment[];
}) {
  const sectorLabel = SECTOR_LABELS[sector as Sector] || sector;

  const sentimentColor = (label: string) => {
    if (label === "positive") return "text-emerald-400";
    if (label === "negative") return "text-red-400";
    return "text-slate-400";
  };

  const sentimentBg = (label: string) => {
    if (label === "positive") return "bg-emerald-500/10 border-emerald-500/20";
    if (label === "negative") return "bg-red-500/10 border-red-500/20";
    return "bg-surface-3 border-border-subtle";
  };

  return (
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5">
      <div className="flex items-center gap-2 mb-4">
        <Building2 size={18} className="text-accent-cyan" />
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
          {sectorLabel} Sector Sentiment
        </h2>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {sentiments.map((s) => (
          <div
            key={s.term}
            className={`rounded-lg border p-3 ${sentimentBg(s.label)}`}
          >
            <div className="flex items-center justify-between mb-1">
              <span className="text-xs text-text-muted uppercase tracking-wider font-medium">
                {s.term === "short" ? "Short-Term" : "Long-Term"}
              </span>
              <span className="text-[10px] text-text-muted font-mono bg-surface-3 px-1.5 py-0.5 rounded">
                {s.article_count} articles
              </span>
            </div>
            <div className="flex items-center gap-2">
              <span
                className={`text-lg font-bold font-mono ${sentimentColor(s.label)}`}
              >
                {s.score > 0 ? "+" : ""}
                {s.score.toFixed(4)}
              </span>
              <span
                className={`text-xs font-semibold uppercase tracking-wider ${sentimentColor(s.label)}`}
              >
                {s.label}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
