/** Dashboard page - top graded and worst graded assets */

import { useAtom } from "jotai";
import { dashboardAtom, macroSentimentAtom, macroNewsAtom, latestNewsAtom } from "../atoms";
import InstrumentCard from "../components/InstrumentCard";
import MacroSentimentCard from "../components/MacroSentimentCard";
import NewsFeed from "../components/NewsFeed";
import { PageSkeleton } from "../components/Skeletons";
import { Trophy, AlertTriangle, Zap, Globe } from "lucide-react";
import type { DashboardInstrument } from "../types";

function sortByGrade(
  instruments: DashboardInstrument[],
  direction: "best" | "worst",
): DashboardInstrument[] {
  const sorted = [...instruments].sort((a, b) => {
    const scoreA = a.short_term_score ?? -999;
    const scoreB = b.short_term_score ?? -999;
    return direction === "best" ? scoreB - scoreA : scoreA - scoreB;
  });
  return sorted;
}

export default function Dashboard() {
  const [{ data: instruments, isLoading: loadingInstruments }] =
    useAtom(dashboardAtom);
  const [{ data: macroSentiments }] = useAtom(macroSentimentAtom);
  const [{ data: macroNews }] = useAtom(macroNewsAtom);
  const [{ data: news }] = useAtom(latestNewsAtom);

  if (loadingInstruments) return <PageSkeleton />;

  const instrumentList = instruments || [];
  const topGraded = sortByGrade(instrumentList, "best").slice(0, 4);
  const worstGraded = sortByGrade(instrumentList, "worst").slice(0, 4);

  return (
    <div className="max-w-[1400px] mx-auto px-6 py-8 space-y-8">
      {/* Page Header */}
      <div className="animate-fade-in">
        <h1 className="text-2xl font-bold text-text-primary mb-1">
          <span className="text-gradient-premium">Dashboard</span>
        </h1>
        <p className="text-sm text-text-secondary">
          Real-time investment analysis across stocks, ETFs, and commodities
        </p>
      </div>

      {/* Top Graded */}
      <section className="animate-slide-up" style={{ animationDelay: "100ms" }}>
        <div className="flex items-center gap-2 mb-4">
          <Trophy size={18} className="text-accent-emerald" />
          <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
            Top Graded Assets
          </h2>
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {topGraded.map((inst, i) => (
            <InstrumentCard key={inst.id} instrument={inst} index={i} />
          ))}
        </div>
      </section>

      {/* Worst Graded */}
      <section className="animate-slide-up" style={{ animationDelay: "200ms" }}>
        <div className="flex items-center gap-2 mb-4">
          <AlertTriangle size={18} className="text-accent-rose" />
          <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
            Weakest Assets
          </h2>
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {worstGraded.map((inst, i) => (
            <InstrumentCard key={inst.id} instrument={inst} index={i} />
          ))}
        </div>
      </section>

      {/* Macro Section: Sentiment + Macro News */}
      <div
        className="grid grid-cols-1 lg:grid-cols-3 gap-6 animate-slide-up"
        style={{ animationDelay: "300ms" }}
      >
        <div className="lg:col-span-1">
          <MacroSentimentCard sentiments={macroSentiments || []} />
        </div>
        <div className="lg:col-span-2">
          <NewsFeed
            articles={macroNews || []}
            title="Macro & Economic News"
            icon={<Globe size={18} className="text-accent-violet" />}
            compact
            className="h-full"
          />
        </div>
      </div>

      {/* Bottom Row: All Instruments + Asset News */}
      <div
        className="grid grid-cols-1 lg:grid-cols-2 gap-6 animate-slide-up"
        style={{ animationDelay: "400ms" }}
      >
        <div>
          {/* All Instruments */}
          <div className="rounded-xl bg-surface-1 border border-border-subtle p-5">
            <div className="flex items-center gap-2 mb-4">
              <Zap size={18} className="text-accent-cyan" />
              <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
                All Instruments
              </h2>
            </div>
            <div className="grid grid-cols-1 gap-2">
              {instrumentList.map((inst, i) => (
                <InstrumentCard key={inst.id} instrument={inst} index={i} />
              ))}
            </div>
          </div>
        </div>

        <NewsFeed articles={news || []} compact className="h-full" />
      </div>
    </div>
  );
}
