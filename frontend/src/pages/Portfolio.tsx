/** Portfolio page — user's watchlist of starred assets */

import { useEffect } from "react";
import { Link } from "react-router-dom";
import { useAtom } from "jotai";
import { dashboardAtom } from "../atoms";
import { wsSubscribe } from "../ws";
import { usePortfolio } from "../hooks/usePortfolio";
import InstrumentCard from "../components/InstrumentCard";
import { PageSkeleton } from "../components/Skeletons";
import { Briefcase, ArrowRight } from "lucide-react";

export default function Portfolio() {
  const [{ data: instruments, isLoading }] = useAtom(dashboardAtom);
  const { portfolioIds } = usePortfolio();

  useEffect(() => {
    wsSubscribe({ page: "dashboard" });
  }, []);

  if (isLoading) return <PageSkeleton />;

  const portfolioInstruments = (instruments || []).filter((inst) =>
    portfolioIds.includes(inst.id),
  );

  return (
    <div className="max-w-[1400px] mx-auto px-6 py-8 space-y-8">
      <div className="animate-fade-in">
        <div className="flex items-center gap-2 mb-1">
          <Briefcase size={22} className="text-accent-amber" />
          <h1 className="text-2xl font-bold text-text-primary">
            <span className="text-gradient-premium">Portfolio</span>
          </h1>
        </div>
        <p className="text-sm text-text-secondary">
          Your personal watchlist with live prices and grades
        </p>
      </div>

      {portfolioInstruments.length > 0 ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 animate-slide-up">
          {portfolioInstruments.map((inst, i) => (
            <InstrumentCard key={inst.id} instrument={inst} index={i} />
          ))}
        </div>
      ) : (
        <div className="flex flex-col items-center justify-center py-20 animate-fade-in">
          <Briefcase
            size={48}
            className="text-text-muted mb-4 opacity-40"
          />
          <p className="text-text-secondary mb-2">
            No assets in your portfolio yet
          </p>
          <Link
            to="/assets"
            className="flex items-center gap-2 text-accent-cyan hover:text-accent-cyan/80 transition-colors text-sm font-medium"
          >
            Go to Assets to add some
            <ArrowRight size={14} />
          </Link>
        </div>
      )}
    </div>
  );
}
