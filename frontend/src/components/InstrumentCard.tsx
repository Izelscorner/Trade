/** Instrument card for dashboard grid — buy-confidence first */

import { Link } from "react-router-dom";
import type { DashboardInstrument } from "../types";
import { scoreToBuyConfidence, buyConfidenceToAction } from "../types";
import PriceChange from "./PriceChange";
import { Activity, CircleDot } from "lucide-react";

interface InstrumentCardProps {
  instrument: DashboardInstrument;
  index: number;
}

const categoryIcon: Record<string, string> = {
  stock: "📈", etf: "📊", commodity: "🏆",
};

const marketStatusColors: Record<string, string> = {
  active:      "text-accent-emerald",
  pre_market:  "text-accent-amber",
  after_hours: "text-accent-violet",
  closed:      "text-text-muted",
};

/** Derive color classes from buy-confidence % */
function confidenceClasses(pct: number) {
  if (pct >= 78) return { badge: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30", bar: "bg-emerald-500" };
  if (pct >= 63) return { badge: "bg-green-500/15   text-green-400   border-green-500/30",   bar: "bg-green-500"   };
  if (pct >= 54) return { badge: "bg-lime-500/15    text-lime-400    border-lime-500/30",    bar: "bg-lime-500"    };
  if (pct >= 46) return { badge: "bg-slate-500/15   text-slate-400   border-slate-500/30",   bar: "bg-slate-500"   };
  if (pct >= 37) return { badge: "bg-amber-500/15   text-amber-400   border-amber-500/30",   bar: "bg-amber-500"   };
  if (pct >= 22) return { badge: "bg-orange-500/15  text-orange-400  border-orange-500/30",  bar: "bg-orange-500"  };
  return               { badge: "bg-red-500/15      text-red-400     border-red-500/30",     bar: "bg-red-500"     };
}

interface ConfBadgeProps { pct: number; label: string }
function ConfBadge({ pct, label }: ConfBadgeProps) {
  const cls = confidenceClasses(pct);
  const action = buyConfidenceToAction(pct);
  return (
    <div className="flex flex-col items-center gap-0.5 min-w-13">
      <span className={`inline-flex items-center justify-center rounded-lg border font-mono text-xs font-bold px-2 py-0.5 w-full ${cls.badge}`}>
        {pct.toFixed(0)}%
      </span>
      <span className={`text-[9px] font-semibold ${cls.badge.split(" ")[1]} text-center leading-tight`}>
        {action}
      </span>
      <span className="text-[8px] text-text-muted/60 uppercase tracking-wider">{label}</span>
    </div>
  );
}

export default function InstrumentCard({ instrument, index }: InstrumentCardProps) {
  const statusColor = marketStatusColors[instrument.market_status || "closed"] || marketStatusColors.closed;
  const statusLabel = (instrument.market_status || "closed").replace("_", " ");

  const shortPct = scoreToBuyConfidence(instrument.short_term_score ?? null);
  const longPct  = scoreToBuyConfidence(instrument.long_term_score  ?? null);

  // Composite bar: blend of short + long (50/50)
  const compositeConf = instrument.short_term_score !== null && instrument.long_term_score !== null
    ? (shortPct + longPct) / 2
    : instrument.short_term_score !== null ? shortPct : longPct;

  const cls = confidenceClasses(compositeConf);

  return (
    <Link
      to={`/asset/${instrument.id}`}
      id={`instrument-card-${instrument.symbol}`}
      className="group block rounded-xl bg-surface-1 border border-border-subtle p-5 hover:border-accent-cyan/30 hover:bg-surface-2/50 transition-all duration-300 animate-fade-in"
      style={{ animationDelay: `${index * 50}ms` }}
    >
      {/* Header */}
      <div className="flex justify-between items-start mb-3">
        <div className="flex items-center gap-2">
          <span className="text-lg">{categoryIcon[instrument.category] || "📈"}</span>
          <div>
            <h3 className="text-base font-bold text-text-primary group-hover:text-accent-cyan transition-colors">
              {instrument.symbol}
            </h3>
            <p className="text-xs text-text-muted truncate max-w-30">{instrument.name}</p>
          </div>
        </div>

        {/* Short + Long confidence badges */}
        <div className="flex items-start gap-2">
          {instrument.short_term_score !== null && (
            <ConfBadge pct={shortPct} label="Short" />
          )}
          {instrument.long_term_score !== null && (
            <ConfBadge pct={longPct} label="Long" />
          )}
        </div>
      </div>

      {/* Confidence bar */}
      <div className="h-1 rounded-full bg-surface-3 overflow-hidden mb-3">
        <div
          className={`h-full rounded-full transition-all duration-700 ${cls.bar}`}
          style={{ width: `${compositeConf}%` }}
        />
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
        <div className={`flex items-center gap-1 text-[10px] uppercase tracking-wider font-medium ${statusColor}`}>
          <CircleDot size={8} className="animate-pulse" />
          {statusLabel}
        </div>
        <div className="flex items-center gap-1 text-[10px] text-text-muted">
          <Activity size={10} />
          <span className="uppercase tracking-wider">{instrument.category}</span>
        </div>
      </div>
    </Link>
  );
}
