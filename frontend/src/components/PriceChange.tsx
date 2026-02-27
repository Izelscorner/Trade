/** Price change display with color and arrow */

import { TrendingUp, TrendingDown, Minus } from "lucide-react";

interface PriceChangeProps {
  symbol?: string;
  price: number | null;
  changeAmount: number | null;
  changePercent: number | null;
  size?: "sm" | "md" | "lg";
}

function formatPrice(price: number, symbol?: string) {
  if (symbol === "IITU" || symbol === "IITU.L") {
    // GBp to GBP
    return `£${(price / 100).toLocaleString("en-GB", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}`;
  }
  return `$${price.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

export default function PriceChange({
  symbol,
  price,
  changeAmount,
  changePercent,
  size = "md",
}: PriceChangeProps) {
  if (price === null) {
    return <span className="text-text-muted">—</span>;
  }

  const isPositive = changePercent !== null && changePercent > 0;
  const isNegative = changePercent !== null && changePercent < 0;
  const isNeutral = !isPositive && !isNegative;

  const colorClass = isPositive
    ? "text-accent-emerald"
    : isNegative
      ? "text-accent-rose"
      : "text-text-secondary";

  const priceSize =
    size === "lg" ? "text-2xl" : size === "md" ? "text-lg" : "text-sm";
  const changeSize =
    size === "lg" ? "text-sm" : size === "md" ? "text-xs" : "text-[10px]";

  let displayChangeAmount = "";
  if (changeAmount !== null) {
    const rawVal = Math.abs(changeAmount);
    if (symbol === "IITU" || symbol === "IITU.L") {
      displayChangeAmount = `£${(rawVal / 100).toFixed(2)}`;
    } else {
      displayChangeAmount = `$${rawVal.toFixed(2)}`;
    }
  }

  return (
    <div className="flex flex-col items-end">
      <span
        className={`${priceSize} font-semibold font-mono text-text-primary`}
      >
        {formatPrice(price, symbol)}
      </span>
      {changePercent !== null && (
        <div className={`flex items-center gap-1 ${colorClass} ${changeSize}`}>
          {isPositive && <TrendingUp size={size === "sm" ? 10 : 12} />}
          {isNegative && <TrendingDown size={size === "sm" ? 10 : 12} />}
          {isNeutral && <Minus size={size === "sm" ? 10 : 12} />}
          <span className="font-mono font-medium">
            {changeAmount !== null && (
              <>
                {changeAmount > 0 ? "+" : "-"}
                {displayChangeAmount}{" "}
              </>
            )}
            ({changePercent > 0 ? "+" : ""}
            {changePercent.toFixed(2)}%)
          </span>
        </div>
      )}
    </div>
  );
}
