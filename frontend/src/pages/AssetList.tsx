/** Asset List page - all assets with category filtering, search, pagination, add asset, portfolio */

import { useState, useMemo, useEffect } from "react";
import { Link } from "react-router-dom";
import { useAtom } from "jotai";
import { useQueryClient } from "@tanstack/react-query";
import { dashboardAtom } from "../atoms";
import { wsSubscribe } from "../ws";
import { addInstruments } from "../api/client";
import { usePortfolio } from "../hooks/usePortfolio";
import CategoryFilter from "../components/CategoryFilter";
import GradeBadge from "../components/GradeBadge";
import PriceChange from "../components/PriceChange";
import { TableRowSkeleton } from "../components/Skeletons";
import type { Category, DashboardInstrument } from "../types";
import {
  ArrowUpDown,
  ChevronLeft,
  ChevronRight,
  Loader2,
  Plus,
  Search,
  Star,
  Trash2,
  X,
} from "lucide-react";
import { removeInstrument } from "../api/client";

type SortKey = "symbol" | "price" | "change" | "short_grade" | "long_grade";
type SortDir = "asc" | "desc";

const ITEMS_PER_PAGE = 20;

const marketStatusColors: Record<string, string> = {
  active: "text-accent-emerald",
  pre_market: "text-accent-amber",
  after_hours: "text-accent-violet",
  closed: "text-text-muted",
};

export default function AssetList() {
  const [{ data: instruments, isLoading }] = useAtom(dashboardAtom);
  const queryClient = useQueryClient();
  const { isInPortfolio, togglePortfolio } = usePortfolio();
  const [category, setCategory] = useState<Category | "all">("all");
  const [sortKey, setSortKey] = useState<SortKey>("symbol");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const [showAddModal, setShowAddModal] = useState(false);

  useEffect(() => {
    wsSubscribe({ page: "asset_list" });
  }, []);

  // Reset page when filters change
  useEffect(() => {
    setPage(1);
  }, [category, search]);

  const filtered = useMemo(() => {
    let list = instruments || [];
    if (category !== "all") {
      list = list.filter((i) => i.category === category);
    }
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      list = list.filter(
        (i) =>
          i.symbol.toLowerCase().includes(q) ||
          i.name.toLowerCase().includes(q),
      );
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
  }, [instruments, category, sortKey, sortDir, search]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / ITEMS_PER_PAGE));
  const paginated = filtered.slice(
    (page - 1) * ITEMS_PER_PAGE,
    page * ITEMS_PER_PAGE,
  );

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "symbol" ? "asc" : "desc");
    }
  };

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
        <div className="flex items-center gap-3">
          {/* Search */}
          <div className="relative">
            <Search
              size={14}
              className="absolute left-3 top-1/2 -translate-y-1/2 text-text-muted"
            />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search assets..."
              className="pl-8 pr-3 py-2 text-sm rounded-lg bg-surface-2 border border-border-subtle text-text-primary placeholder:text-text-muted focus:outline-none focus:border-accent-cyan/50 w-48"
            />
          </div>
          <CategoryFilter selected={category} onChange={setCategory} />
          <button
            onClick={() => setShowAddModal(true)}
            className="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-accent-cyan/10 border border-accent-cyan/30 text-accent-cyan text-sm font-medium hover:bg-accent-cyan/20 transition-colors"
          >
            <Plus size={16} />
            Add Asset
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="rounded-xl bg-surface-1 border border-border-subtle overflow-hidden animate-slide-up">
        {/* Table Header */}
        <div className="grid grid-cols-[40px_1fr_2fr_1fr_1fr_120px_120px_40px] gap-4 items-center px-5 py-3 border-b border-border-subtle bg-surface-2/30">
          <span />
          <SortHeader
            label="Symbol"
            sortId="symbol"
            currentSortKey={sortKey}
            onSort={handleSort}
          />
          <span className="text-xs uppercase tracking-wider font-semibold text-text-muted">
            Name
          </span>
          <SortHeader
            label="Price"
            sortId="price"
            currentSortKey={sortKey}
            onSort={handleSort}
          />
          <SortHeader
            label="Change"
            sortId="change"
            currentSortKey={sortKey}
            onSort={handleSort}
          />
          <SortHeader
            label="Short"
            sortId="short_grade"
            currentSortKey={sortKey}
            onSort={handleSort}
          />
          <SortHeader
            label="Long"
            sortId="long_grade"
            currentSortKey={sortKey}
            onSort={handleSort}
          />
          <span />
        </div>

        {/* Rows */}
        {isLoading ? (
          Array.from({ length: 7 }).map((_, i) => <TableRowSkeleton key={i} />)
        ) : paginated.length > 0 ? (
          paginated.map((inst, index) => (
            <InstrumentRow
              key={inst.id}
              instrument={inst}
              index={index}
              starred={isInPortfolio(inst.id)}
              onToggleStar={() => togglePortfolio(inst.id)}
              onRemove={async () => {
                if (
                  window.confirm(
                    `Are you sure you want to remove ${inst.symbol}? This will wipe price history and technical indicators.`,
                  )
                ) {
                  try {
                    await removeInstrument(inst.id);
                    queryClient.refetchQueries({ queryKey: ["dashboard"] });
                  } catch (e) {
                    alert(
                      e instanceof Error
                        ? e.message
                        : "Failed to remove instrument",
                    );
                  }
                }
              }}
            />
          ))
        ) : (
          <div className="px-5 py-12 text-center text-text-muted text-sm">
            No instruments found
            {instruments && (
              <div className="mt-2 text-[10px] opacity-30">
                Instruments count: {instruments.length}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-4 text-sm">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page === 1}
            className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-surface-2 border border-border-subtle text-text-secondary hover:text-text-primary disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            <ChevronLeft size={14} />
            Prev
          </button>
          <span className="text-text-muted">
            Page {page} of {totalPages}
          </span>
          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page === totalPages}
            className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-surface-2 border border-border-subtle text-text-secondary hover:text-text-primary disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            Next
            <ChevronRight size={14} />
          </button>
        </div>
      )}

      {/* Add Asset Modal */}
      {showAddModal && (
        <AddAssetModal
          onClose={() => setShowAddModal(false)}
          onSuccess={() => {
            queryClient.refetchQueries({ queryKey: ["dashboard"] });
            setShowAddModal(false);
          }}
        />
      )}
    </div>
  );
}

function InstrumentRow({
  instrument,
  index,
  starred,
  onToggleStar,
  onRemove,
}: {
  instrument: DashboardInstrument;
  index: number;
  starred: boolean;
  onToggleStar: () => void;
  onRemove: () => void;
}) {
  const statusColor = marketStatusColors[instrument.market_status || "closed"];

  return (
    <div
      className="grid grid-cols-[40px_1fr_2fr_1fr_1fr_120px_120px] gap-4 items-center px-5 py-4 border-b border-border-subtle hover:bg-surface-2/30 transition-all duration-200 group animate-fade-in"
      style={{ animationDelay: `${index * 30}ms` }}
    >
      <button
        onClick={(e) => {
          e.preventDefault();
          e.stopPropagation();
          onToggleStar();
        }}
        className="flex items-center justify-center"
      >
        <Star
          size={16}
          className={`transition-colors ${
            starred
              ? "text-accent-amber fill-accent-amber"
              : "text-text-muted hover:text-accent-amber"
          }`}
        />
      </button>

      <Link to={`/asset/${instrument.id}`} className="flex items-center gap-2">
        <div className={`w-1.5 h-1.5 rounded-full ${statusColor} bg-current`} />
        <span className="font-semibold text-text-primary group-hover:text-accent-cyan transition-colors font-mono">
          {instrument.symbol}
        </span>
      </Link>

      <Link to={`/asset/${instrument.id}`}>
        <span className="text-sm text-text-secondary">{instrument.name}</span>
        <span className="text-[10px] text-text-muted ml-2 uppercase tracking-wider px-1.5 py-0.5 rounded bg-surface-3">
          {instrument.category}
        </span>
      </Link>

      <Link to={`/asset/${instrument.id}`}>
        <PriceChange
          symbol={instrument.symbol}
          price={instrument.price}
          changeAmount={null}
          changePercent={null}
          size="sm"
        />
      </Link>

      <Link to={`/asset/${instrument.id}`}>
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
      </Link>

      <Link to={`/asset/${instrument.id}`} className="flex justify-center">
        <GradeBadge
          grade={instrument.short_term_grade}
          score={instrument.short_term_score}
          size="sm"
          gradedAt={instrument.graded_at}
        />
      </Link>

      <Link to={`/asset/${instrument.id}`} className="flex justify-center">
        <GradeBadge
          grade={instrument.long_term_grade}
          score={instrument.long_term_score}
          size="sm"
          gradedAt={instrument.graded_at}
        />
      </Link>

      <div className="flex justify-center opacity-0 group-hover:opacity-100 transition-opacity">
        <button
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            onRemove();
          }}
          className="p-1.5 rounded-lg text-text-muted hover:text-accent-rose hover:bg-accent-rose/10 transition-all"
          title="Remove asset"
        >
          <Trash2 size={14} />
        </button>
      </div>
    </div>
  );
}

function AddAssetModal({
  onClose,
  onSuccess,
}: {
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [symbols, setSymbols] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<{
    created: { symbol: string; name: string }[];
    skipped: string[];
  } | null>(null);

  const handleSubmit = async () => {
    if (!symbols.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const data = await addInstruments(symbols.trim());
      setResult(data);
      if (data.created.length > 0) {
        setTimeout(onSuccess, 2000);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to add instruments");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 animate-fade-in">
      <div className="bg-surface-1 border border-border-subtle rounded-xl p-6 w-full max-w-md shadow-xl">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-text-primary">
            Add Assets
          </h2>
          <button
            onClick={onClose}
            className="text-text-muted hover:text-text-primary transition-colors"
          >
            <X size={20} />
          </button>
        </div>

        <input
          type="text"
          value={symbols}
          onChange={(e) => setSymbols(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSubmit()}
          placeholder="TSLA, MSFT, META..."
          className="w-full px-4 py-3 rounded-lg bg-surface-2 border border-border-subtle text-text-primary placeholder:text-text-muted focus:outline-none focus:border-accent-cyan/50 text-sm font-mono"
          autoFocus
          disabled={loading}
        />
        <p className="text-xs text-text-muted mt-2">
          Enter one or more ticker symbols separated by commas
        </p>

        {error && (
          <div className="mt-3 p-3 rounded-lg bg-accent-rose/10 border border-accent-rose/30 text-accent-rose text-sm">
            {error}
          </div>
        )}

        {result && (
          <div className="mt-3 space-y-2">
            {result.created.length > 0 && (
              <div className="p-3 rounded-lg bg-accent-emerald/10 border border-accent-emerald/30 text-accent-emerald text-sm">
                Added:{" "}
                {result.created
                  .map((c) => `${c.symbol} (${c.name})`)
                  .join(", ")}
              </div>
            )}
            {result.skipped.length > 0 && (
              <div className="p-3 rounded-lg bg-accent-amber/10 border border-accent-amber/30 text-accent-amber text-sm">
                Already exist: {result.skipped.join(", ")}
              </div>
            )}
          </div>
        )}

        <div className="flex justify-end gap-3 mt-5">
          <button
            onClick={onClose}
            className="px-4 py-2 rounded-lg text-sm text-text-secondary hover:text-text-primary transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSubmit}
            disabled={loading || !symbols.trim()}
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-accent-cyan/20 border border-accent-cyan/30 text-accent-cyan text-sm font-medium hover:bg-accent-cyan/30 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            {loading && <Loader2 size={14} className="animate-spin" />}
            {loading ? "Resolving..." : "Add"}
          </button>
        </div>
      </div>
    </div>
  );
}

function SortHeader({
  label,
  sortId,
  currentSortKey,
  onSort,
}: {
  label: string;
  sortId: SortKey;
  currentSortKey: SortKey;
  onSort: (key: SortKey) => void;
}) {
  return (
    <button
      onClick={() => onSort(sortId)}
      className={`flex items-center gap-1 text-xs uppercase tracking-wider font-semibold transition-colors ${
        currentSortKey === sortId
          ? "text-accent-cyan"
          : "text-text-muted hover:text-text-secondary"
      }`}
    >
      {label}
      <ArrowUpDown size={10} />
    </button>
  );
}
