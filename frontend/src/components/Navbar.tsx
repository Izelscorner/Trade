import { Link, useLocation } from "react-router-dom";
import { BarChart3, Briefcase, LayoutDashboard, List, Newspaper, MessageSquareOff, MessageSquare } from "lucide-react";
import { useState } from "react";
import { useAtom } from "jotai";
import { showSentimentAtom } from "../atoms";

const navItems = [
  { path: "/", label: "Dashboard", icon: LayoutDashboard },
  { path: "/assets", label: "Assets", icon: List },
  { path: "/portfolio", label: "Portfolio", icon: Briefcase },
  { path: "/news", label: "News", icon: Newspaper },
];

export default function Navbar() {
  const { pathname } = useLocation();
  const [showSentiment, setShowSentiment] = useAtom(showSentimentAtom);
  const [loading, setLoading] = useState(false);

  const toggleSentiment = async () => {
    setLoading(true);
    const newValue = !showSentiment;
    
    // Update local state IMMEDIATELY for real-time switching
    setShowSentiment(newValue);

    try {
      // Still sync with backend so re-grades respect the current preference
      await fetch("/api/v1/settings/sentiment", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ value: newValue }),
      });
    } catch (err) {
      console.error("Failed to sync sentiment preference to backend", err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <header className="sticky top-0 z-50 glass-strong">
      <div className="max-w-[1400px] mx-auto px-6 h-16 flex items-center justify-between">
        {/* Logo */}
        <Link to="/" className="flex items-center gap-3 group">
          <div className="w-9 h-9 rounded-lg bg-accent-cyan/10 border border-accent-cyan/30 flex items-center justify-center group-hover:bg-accent-cyan/20 transition-colors">
            <BarChart3 size={20} className="text-accent-cyan" />
          </div>
          <div>
            <h1 className="text-lg font-bold leading-tight tracking-tight">
              <span className="text-gradient-premium">TradeSignal</span>
            </h1>
            <p className="text-[10px] text-text-muted uppercase tracking-[0.2em] leading-none -mt-0.5">
              Investment Analysis
            </p>
          </div>
        </Link>

        {/* Navigation */}
        <div className="flex items-center gap-6">
          <nav className="flex items-center gap-1">
            {navItems.map(({ path, label, icon: Icon }) => {
              const isActive =
                path === "/" ? pathname === "/" : pathname.startsWith(path);
              return (
                <Link
                  key={path}
                  to={path}
                  className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${
                    isActive
                      ? "bg-accent-cyan/10 text-accent-cyan border border-accent-cyan/20"
                      : "text-text-secondary hover:text-text-primary hover:bg-surface-3/50"
                  }`}
                >
                  <Icon size={16} />
                  {label}
                </Link>
              );
            })}
          </nav>

          {/* Sentiment Toggle */}
          <button
            onClick={toggleSentiment}
            disabled={loading}
            className={`flex items-center gap-2 px-4 py-1.5 rounded-full text-[11px] font-bold uppercase tracking-wider transition-all duration-300 border ${
              showSentiment
                ? "bg-success/10 text-success border-success/20 hover:bg-success/20"
                : "bg-surface-3 text-text-muted border-white/5 hover:bg-surface-4"
            } ${loading ? "opacity-30" : ""}`}
            title={showSentiment ? "Sentiment Analysis active" : "Sentiment Analysis disabled (Technicals/Fundamentals only)"}
          >
            {showSentiment ? <MessageSquare size={13} /> : <MessageSquareOff size={13} />}
            Sentiment: {showSentiment ? "On" : "Off"}
          </button>
        </div>
      </div>
    </header>
  );
}
