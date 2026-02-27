/** Asset List page - all assets with category filtering */

import { useState, useMemo } from "react";
import { Link } from "react-router-dom";
import { useAtom } from "jotai";
import { dashboardAtom } from "../atoms";
import CategoryFilter from "../components/CategoryFilter";
import GradeBadge from "../components/GradeBadge";
import PriceChange from "../components/PriceChange";
import { TableRowSkeleton } from "../components/Skeletons";
import type { Category, DashboardInstrument } from "../types";
import { ArrowUpDown } from "lucide-react";

type SortKey = "symbol" | "price" | "change" | "short_grade" | "long_grade";
type SortDir = "asc" | "desc";

const marketStatusColors: Record<string, string> = {
  active: "text-accent-emerald",
  pre_market: "text-accent-amber",
  after_hours: "text-accent-violet",
  closed: "text-text-muted",
};

export default function AssetList() {
  const [{ data: instruments, isLoading }] = useAtom(dashboardAtom);
  const [category, setCategory] = useState<Category | "all">("all");
  const [sortKey, setSortKey] = useState<SortKey>("symbol");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const filtered = useMemo(() => {
    let list = instruments || [];
    if (category !== "all") {
      list = list.filter((i) => i.category === category);
    }
    return [...list].sort((a, b) => {
      let cmp = 0;
      switch (sortKey) {
        case "symbol":
          cmp = a.symbol.localeCompare(b.symbol);
          break;
        case "price":
          cmp = (a.price ?? 0) - (b.price ?? 0);
          break;
        case "change":
          cmp = (a.change_percent ?? 0) - (b.change_percent ?? 0);
          break;
        case "short_grade":
          cmp = (a.short_term_score ?? -999) - (b.short_term_score ?? -999);
          break;
        case "long_grade":
          cmp = (a.long_term_score ?? -999) - (b.long_term_score ?? -999);
          break;
      }
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [instruments, category, sortKey, sortDir]);

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "symbol" ? "asc" : "desc");
    }
  };

  const SortHeader = ({
    label,
    sortId,
  }: {
    label: string;
    sortId: SortKey;
  }) => (
    <button
      onClick={() => toggleSort(sortId)}
      className={`flex items-center gap-1 text-xs uppercase tracking-wider font-semibold transition-colors ${
        sortKey === sortId
          ? "text-accent-cyan"
          : "text-text-muted hover:text-text-secondary"
      }`}
    >
      {label}
      <ArrowUpDown size={10} />
    </button>
  );

  return (
    <div className="max-w-[1400px] mx-auto px-6 py-8 space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 animate-fade-in">
        <div>
          <h1 className="text-2xl font-bold text-text-primary">
            <span className="text-gradient-premium">Assets</span>
          </h1>
          <p className="text-sm text-text-secondary">
            All tracked instruments with investment grades
          </p>
        </div>
        <CategoryFilter selected={category} onChange={setCategory} />
      </div>

      {/* Table */}
      <div className="rounded-xl bg-surface-1 border border-border-subtle overflow-hidden animate-slide-up">
        {/* Table Header */}
        <div className="grid grid-cols-[1fr_2fr_1fr_1fr_100px_100px] gap-4 items-center px-5 py-3 border-b border-border-subtle bg-surface-2/30">
          <SortHeader label="Symbol" sortId="symbol" />
          <span className="text-xs uppercase tracking-wider font-semibold text-text-muted">
            Name
          </span>
          <SortHeader label="Price" sortId="price" />
          <SortHeader label="Change" sortId="change" />
          <SortHeader label="Short" sortId="short_grade" />
          <SortHeader label="Long" sortId="long_grade" />
        </div>

        {/* Rows */}
        {isLoading ? (
          Array.from({ length: 7 }).map((_, i) => <TableRowSkeleton key={i} />)
        ) : filtered.length > 0 ? (
          filtered.map((inst, index) => (
            <InstrumentRow key={inst.id} instrument={inst} index={index} />
          ))
        ) : (
          <div className="px-5 py-12 text-center text-text-muted text-sm">
            No instruments found
          </div>
        )}
      </div>
    </div>
  );
}

function InstrumentRow({
  instrument,
  index,
}: {
  instrument: DashboardInstrument;
  index: number;
}) {
  const statusColor = marketStatusColors[instrument.market_status || "closed"];

  return (
    <Link
      to={`/asset/${instrument.id}`}
      id={`asset-row-${instrument.symbol}`}
      className="grid grid-cols-[1fr_2fr_1fr_1fr_100px_100px] gap-4 items-center px-5 py-4 border-b border-border-subtle hover:bg-surface-2/30 transition-all duration-200 group animate-fade-in"
      style={{ animationDelay: `${index * 30}ms` }}
    >
      <div className="flex items-center gap-2">
        <div className={`w-1.5 h-1.5 rounded-full ${statusColor} bg-current`} />
        <span className="font-semibold text-text-primary group-hover:text-accent-cyan transition-colors font-mono">
          {instrument.symbol}
        </span>
      </div>

      <div>
        <span className="text-sm text-text-secondary">{instrument.name}</span>
        <span className="text-[10px] text-text-muted ml-2 uppercase tracking-wider px-1.5 py-0.5 rounded bg-surface-3">
          {instrument.category}
        </span>
      </div>

      <PriceChange
        symbol={instrument.symbol}
        price={instrument.price}
        changeAmount={null}
        changePercent={null}
        size="sm"
      />

      <div>
        {instrument.change_percent !== null ? (
          <span
            className={`font-mono text-sm font-medium ${
              instrument.change_percent > 0
                ? "text-accent-emerald"
                : instrument.change_percent < 0
                  ? "text-accent-rose"
                  : "text-text-muted"
            }`}
          >
            {instrument.change_percent > 0 ? "+" : ""}
            {instrument.change_percent.toFixed(2)}%
          </span>
        ) : (
          <span className="text-text-muted text-sm">—</span>
        )}
      </div>

      <div className="flex justify-center">
        <GradeBadge
          grade={instrument.short_term_grade}
          size="sm"
          gradedAt={instrument.graded_at}
        />
      </div>

      <div className="flex justify-center">
        <GradeBadge
          grade={instrument.long_term_grade}
          size="sm"
          gradedAt={instrument.graded_at}
        />
      </div>
    </Link>
  );
}
