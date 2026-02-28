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
  instrumentIndependentAIAnalysisAtom,
} from "../atoms";
import { fetchInstrument, fetchLivePrice } from "../api/client";
import { wsSubscribe } from "../ws";
import PriceChange from "../components/PriceChange";
import PriceChart from "../components/PriceChart";
import TechnicalPanel from "../components/TechnicalPanel";
import GradeDetail from "../components/GradeDetail";
import NewsFeed from "../components/NewsFeed";
import AIAnalysisModal from "../components/AIAnalysisModal";
import { PageSkeleton } from "../components/Skeletons";
import { ArrowLeft, CircleDot, Brain, Network } from "lucide-react";
import type { Instrument, LivePrice, Grade } from "../types";
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
  const independentAiAtom = useMemo(
    () => instrumentIndependentAIAnalysisAtom(id || ""),
    [id],
  );

  const [{ data: historical }] = useAtom(historicalAtom);
  const [{ data: technical }] = useAtom(technicalAtom);
  const [{ data: news }] = useAtom(newsAtom);
  const [{ data: grades }] = useAtom(gradesAtom);
  const [{ data: aiAnalysis, isFetching: aiLoading, refetch: fetchAI }] =
    useAtom(aiAtom);
  const [
    {
      data: independentAnalysis,
      isFetching: independentLoading,
      refetch: fetchIndependentAI,
    },
  ] = useAtom(independentAiAtom);

  const [aiModalMode, setAiModalMode] = useState<
    "integrated" | "independent" | null
  >(null);

  // Subscribe WS to this specific instrument
  useEffect(() => {
    if (id) {
      wsSubscribe({ page: "asset_detail", instrument_ids: [id] });
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
  const statusConfig = marketStatusConfig[currentPrice?.market_status || "closed"];

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
                    setAiModalMode("integrated");
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
                    System AI
                  </span>
                </button>

                <button
                  onClick={() => {
                    setAiModalMode("independent");
                    if (!independentAnalysis) fetchIndependentAI();
                  }}
                  className="flex items-center gap-1.5 px-3 py-1 rounded-full bg-accent-violet/10 text-accent-violet border border-accent-violet/20 hover:bg-accent-violet/20 transition-all duration-200 group"
                  title="Independent analysis based on Gemini's general knowledge"
                >
                  <Network
                    size={14}
                    className="group-hover:scale-110 transition-transform"
                  />
                  <span className="text-[10px] font-bold uppercase tracking-wider">
                    Independent AI
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

      <AIAnalysisModal
        isOpen={!!aiModalMode}
        onClose={() => setAiModalMode(null)}
        analysis={
          aiModalMode === "integrated"
            ? aiAnalysis?.analysis || null
            : independentAnalysis?.analysis || null
        }
        isLoading={
          aiModalMode === "integrated" ? aiLoading : independentLoading
        }
        symbol={instrument.symbol}
        title={
          aiModalMode === "independent"
            ? "Gemini Independent Analysis"
            : "Gemini System Analysis"
        }
        subtitle={
          aiModalMode === "independent"
            ? "Based on Gemini's global knowledge base"
            : undefined
        }
      />
    </div>
  );
}
