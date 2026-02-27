/** Signal badge for technical indicators */

interface SignalBadgeProps {
  signal: string;
}

const signalConfig: Record<string, { label: string; className: string }> = {
  strong_buy: {
    label: "STRONG BUY",
    className:
      "bg-signal-strong-buy/20 text-signal-strong-buy border-signal-strong-buy/30",
  },
  buy: {
    label: "BUY",
    className: "bg-signal-buy/20 text-signal-buy border-signal-buy/30",
  },
  neutral: {
    label: "NEUTRAL",
    className:
      "bg-signal-neutral/20 text-signal-neutral border-signal-neutral/30",
  },
  sell: {
    label: "SELL",
    className: "bg-signal-sell/20 text-signal-sell border-signal-sell/30",
  },
  strong_sell: {
    label: "STRONG SELL",
    className:
      "bg-signal-strong-sell/20 text-signal-strong-sell border-signal-strong-sell/30",
  },
};

export default function SignalBadge({ signal }: SignalBadgeProps) {
  const config = signalConfig[signal] || signalConfig.neutral;

  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-semibold uppercase tracking-wider border ${config.className} transition-all duration-200`}
    >
      {config.label}
    </span>
  );
}
