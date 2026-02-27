/** TradeSignal - Entry Point */

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { Provider as JotaiProvider } from "jotai";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import "./index.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      retry: 2,
      refetchOnWindowFocus: false,
    },
  },
});

// Setup WebSocket for live streaming
let ws: WebSocket;
function connectWS() {
  const wsUrl =
    window.location.protocol === "https:"
      ? `wss://${window.location.host}/api/v1/ws/prices`
      : `ws://${window.location.host}/api/v1/ws/prices`;
  ws = new WebSocket(wsUrl);

  ws.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);
      if (payload.type === "live_prices" && payload.data) {
        // Update queryClient cache seamlessly — all Jotai query atoms using this key instantly re-render
        queryClient.setQueryData(["live-prices"], payload.data);
      }
    } catch (e) {
      console.error("WS parse error", e);
    }
  };

  ws.onclose = () => {
    // Reconnect in 5 seconds
    setTimeout(connectWS, 5000);
  };
}

// Start WebSocket connection
connectWS();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <JotaiProvider>
        <App />
      </JotaiProvider>
    </QueryClientProvider>
  </StrictMode>,
);
