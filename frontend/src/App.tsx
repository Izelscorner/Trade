/** TradeSignal App - Root component with routing */

import { BrowserRouter, Routes, Route } from "react-router-dom";
import Navbar from "./components/Navbar";
import Dashboard from "./pages/Dashboard";
import AssetList from "./pages/AssetList";
import AssetDetail from "./pages/AssetDetail";

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-surface-0">
        <Navbar />
        <main>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/assets" element={<AssetList />} />
            <Route path="/asset/:id" element={<AssetDetail />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
