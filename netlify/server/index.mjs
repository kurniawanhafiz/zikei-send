// Zikei Send - signaling server for Netlify Server (Early Access).
// Docs: https://docs.netlify.com/build/server/  (entry file: netlify/server/index.mjs)
//
// Only these paths are routed to this server (see `config.path`); everything else
// is served as static files from `dist/`, so no SPA catch-all redirect is needed
// (a catch-all rewrite would swallow /ws before it reaches the server).
//
//   GET  /ws      WebSocket signaling (room create / join / relay SDP + ICE)
//   GET  /ice     ICE server list (STUN, optional TURN from env vars)
//   GET  /health  liveness probe

import { upgradeWebSocket, getWebSockets, onShutdown } from "@netlify/server";
import { createSignaling, iceConfig } from "./signaling.mjs";

export const config = {
  path: ["/ws", "/ice", "/health"],
  // Room state is held in this instance's memory, so never run more than one instance.
  scale: { max: 1 },
};

const core = createSignaling();
setInterval(() => core.sweep(), 15_000).unref?.();

onShutdown(() => {
  // 1012 = "service restart": clients reconnect immediately and re-join with their token.
  for (const socket of getWebSockets()) {
    try { socket.close(1012, "Server restarting"); } catch { /* ignore */ }
  }
});

function clientIp(request, context) {
  return (
    context?.ip ||
    request.headers.get("x-nf-client-connection-ip") ||
    request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ||
    "unknown"
  );
}

// Optional hardening: set ALLOWED_ORIGINS="send.example.com,zikei.netlify.app" (hosts, comma separated)
// to refuse WebSocket upgrades coming from pages on other origins.
function originAllowed(request) {
  const allowed = (process.env.ALLOWED_ORIGINS || "").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
  if (!allowed.length) return true;
  const origin = request.headers.get("origin");
  if (!origin) return false;
  try { return allowed.includes(new URL(origin).host.toLowerCase()); } catch { return false; }
}

const json = (data, init = {}) =>
  Response.json(data, { ...init, headers: { "cache-control": "no-store", ...(init.headers || {}) } });

export default async (request, context) => {
  const url = new URL(request.url);
  const ip = clientIp(request, context);

  if (url.pathname === "/health") return json({ ok: true, service: "zikei-send-signaling", rooms: core.rooms.size });

  if (url.pathname === "/ice") {
    if (!core.allowIce(ip)) return json({ error: "rate_limited" }, { status: 429 });
    const { iceServers, turn, ttl } = iceConfig(process.env);
    return json({ iceServers, turn, ttl });
  }

  if (url.pathname === "/ws") {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Zikei Send signaling. Connect with a WebSocket client.", { status: 426 });
    }
    if (!originAllowed(request)) return new Response("Origin not allowed", { status: 403 });
    if (!core.allowConnect(ip)) return new Response("Too many connections", { status: 429 });

    const { socket, response } = upgradeWebSocket(request, { idleTimeout: 30 });
    const conn = core.connect(
      {
        send: (data) => { if (socket.readyState === 1) socket.send(data); },
        close: (code, reason) => socket.close(code, reason),
      },
      { ip },
    );
    socket.addEventListener("message", (event) => conn.onMessage(event.data));
    socket.addEventListener("close", () => conn.onClose());
    socket.addEventListener("error", () => conn.onClose());
    return response;
  }

  return new Response("Not found", { status: 404 });
};

