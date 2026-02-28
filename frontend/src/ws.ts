/** WebSocket subscription manager — used by page components to declare their data needs. */

export type WsPage = "dashboard" | "asset_detail" | "asset_list" | "news";

export interface WsSubscription {
  page: WsPage;
  instrument_ids?: string[];
  region?: string;
  category?: string;
}

let ws: WebSocket | null = null;
let currentSubscription: WsSubscription = { page: "dashboard" };

/** Update the active subscription.  Called by each page on mount / filter change. */
export function wsSubscribe(sub: WsSubscription) {
  currentSubscription = sub;
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ subscribe: sub }));
  }
}

/** Internal: called once from main.tsx to store the WS reference. */
export function _setWs(socket: WebSocket | null) {
  ws = socket;
}

/** Internal: re-send subscription on reconnect. */
export function _resendSubscription() {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ subscribe: currentSubscription }));
  }
}
