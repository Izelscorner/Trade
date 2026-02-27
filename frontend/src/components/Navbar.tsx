/** Navigation header */

import { Link, useLocation } from "react-router-dom";
import { BarChart3, LayoutDashboard, List, Newspaper } from "lucide-react";

const navItems = [
  { path: "/", label: "Dashboard", icon: LayoutDashboard },
  { path: "/assets", label: "Assets", icon: List },
  { path: "/news", label: "News", icon: Newspaper },
];

export default function Navbar() {
  const { pathname } = useLocation();

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
      </div>
    </header>
  );
}
