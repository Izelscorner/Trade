/** Instrument card for dashboard grid */

import { Link } from "react-router-dom";
import type { DashboardInstrument } from "../types";
import GradeBadge from "./GradeBadge";
import PriceChange from "./PriceChange";
import { Activity, CircleDot } from "lucide-react";

interface InstrumentCardProps {
  instrument: DashboardInstrument;
  index: number;
}

const categoryIcon: Record<string, string> = {
  stock: "📈",
  etf: "📊",
  commodity: "🏆",
};

const marketStatusColors: Record<string, string> = {
  active: "text-accent-emerald",
  pre_market: "text-accent-amber",
  after_hours: "text-accent-violet",
  closed: "text-text-muted",
};

export default function InstrumentCard({
  instrument,
  index,
}: InstrumentCardProps) {
  const statusColor =
    marketStatusColors[instrument.market_status || "closed"] ||
    marketStatusColors.closed;
  const statusLabel = (instrument.market_status || "closed").replace("_", " ");

  return (
    <Link
      to={`/asset/${instrument.id}`}
      id={`instrument-card-${instrument.symbol}`}
      className="group block rounded-xl bg-surface-1 border border-border-subtle p-5 hover:border-accent-cyan/30 hover:bg-surface-2/50 transition-all duration-300 animate-fade-in"
      style={{ animationDelay: `${index * 50}ms` }}
    >
      {/* Header */}
      <div className="flex justify-between items-start mb-4">
        <div className="flex items-center gap-2">
          <span className="text-lg">
            {categoryIcon[instrument.category] || "📈"}
          </span>
          <div>
            <h3 className="text-base font-bold text-text-primary group-hover:text-accent-cyan transition-colors">
              {instrument.symbol}
            </h3>
            <p className="text-xs text-text-muted truncate max-w-[140px]">
              {instrument.name}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-1.5">
          <GradeBadge
            grade={instrument.short_term_grade}
            size="sm"
            showLabel
            label="Short"
            gradedAt={instrument.graded_at}
          />
          <GradeBadge
            grade={instrument.long_term_grade}
            size="sm"
            showLabel
            label="Long"
            gradedAt={instrument.graded_at}
          />
        </div>
      </div>

      {/* Price */}
      <div className="mb-3">
        <PriceChange
          symbol={instrument.symbol}
          price={instrument.price}
          changeAmount={instrument.change_amount}
          changePercent={instrument.change_percent}
          size="md"
        />
      </div>

      {/* Footer */}
      <div className="flex items-center justify-between">
        <div
          className={`flex items-center gap-1 text-[10px] uppercase tracking-wider font-medium ${statusColor}`}
        >
          <CircleDot size={8} className="animate-pulse" />
          {statusLabel}
        </div>
        <div className="flex items-center gap-1 text-[10px] text-text-muted">
          <Activity size={10} />
          <span className="uppercase tracking-wider">
            {instrument.category}
          </span>
        </div>
      </div>
    </Link>
  );
}
