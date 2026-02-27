/** Price chart using Recharts */

import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from "recharts";
import type { HistoricalPrice } from "../types";
import { LineChart as ChartIcon } from "lucide-react";

interface PriceChartProps {
  data: HistoricalPrice[];
  title?: string;
  symbol?: string;
}

export default function PriceChart({
  data,
  title = "Price History",
  symbol,
}: PriceChartProps) {
  if (data.length === 0) {
    return (
      <div className="rounded-xl bg-surface-1 border border-border-subtle p-5">
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider mb-4">
          {title}
        </h2>
        <div className="h-64 flex items-center justify-center text-text-muted">
          No price data available
        </div>
      </div>
    );
  }

  const isUp = data.length >= 2 && data[data.length - 1].close >= data[0].close;
  const strokeColor = isUp ? "#10b981" : "#f43f5e";
  const gradientId = `priceGradient-${isUp ? "up" : "down"}`;

  const chartData = data.map((d) => ({
    ...d,
    date: new Date(d.date).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    }),
    rawDate: d.date,
  }));

  return (
    <div className="rounded-xl bg-surface-1 border border-border-subtle p-5">
      <div className="flex items-center gap-2 mb-4">
        <ChartIcon size={18} className="text-accent-blue" />
        <h2 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
          {title}
        </h2>
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <AreaChart
          data={chartData}
          margin={{ top: 5, right: 10, left: 0, bottom: 5 }}
        >
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={strokeColor} stopOpacity={0.3} />
              <stop offset="95%" stopColor={strokeColor} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis
            dataKey="date"
            tick={{ fill: "#5a6478", fontSize: 10 }}
            tickLine={false}
            axisLine={{ stroke: "#1e293b" }}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fill: "#5a6478", fontSize: 10 }}
            tickLine={false}
            axisLine={false}
            domain={["auto", "auto"]}
            tickFormatter={(v: number) => {
              if (symbol === "IITU" || symbol === "IITU.L") {
                return `£${(v / 100).toFixed(0)}`;
              }
              return `$${v.toFixed(0)}`;
            }}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: "#151d2d",
              border: "1px solid #2a3548",
              borderRadius: "8px",
              color: "#e8edf5",
              fontSize: "12px",
            }}
            formatter={(value: number | string | undefined) => {
              const num = typeof value === "number" ? value : 0;
              if (symbol === "IITU" || symbol === "IITU.L") {
                return [`£${(num / 100).toFixed(2)}`, "Close"];
              }
              return [`$${num.toFixed(2)}`, "Close"];
            }}
            labelStyle={{ color: "#8b95a8" }}
          />
          <Area
            type="monotone"
            dataKey="close"
            stroke={strokeColor}
            strokeWidth={2}
            fill={`url(#${gradientId})`}
            animationDuration={1000}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
