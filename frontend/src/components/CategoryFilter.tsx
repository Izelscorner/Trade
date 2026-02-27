/** Category filter tabs */

import type { Category } from "../types";

interface CategoryFilterProps {
  selected: Category | "all";
  onChange: (cat: Category | "all") => void;
}

const categories: { value: Category | "all"; label: string }[] = [
  { value: "all", label: "All" },
  { value: "stock", label: "Stocks" },
  { value: "etf", label: "ETFs" },
  { value: "commodity", label: "Commodities" },
];

export default function CategoryFilter({
  selected,
  onChange,
}: CategoryFilterProps) {
  return (
    <div className="flex items-center gap-1 p-1 rounded-lg bg-surface-1 border border-border-subtle">
      {categories.map(({ value, label }) => (
        <button
          key={value}
          id={`filter-${value}`}
          onClick={() => onChange(value)}
          className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all duration-200 ${
            selected === value
              ? "bg-accent-cyan/15 text-accent-cyan border border-accent-cyan/25 shadow-sm"
              : "text-text-secondary hover:text-text-primary hover:bg-surface-2/50 border border-transparent"
          }`}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
