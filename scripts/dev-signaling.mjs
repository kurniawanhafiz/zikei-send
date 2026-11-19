// Local stand-in for Netlify Server: same signaling core, served with the `ws` package.
// Usage: npm run dev:signal   (then `npm run dev` in another terminal, or `npm run dev:local` for both)
import http from "node:http";
import { WebSocketServer } from "ws";
import { createSignaling, iceConfig } from "../netlify/server/signaling.mjs";

const port = Number(process.env.SIGNAL_PORT || 8787);
const core = createSignaling();
setInterval(() => core.sweep(), 15_000).unref();

const server = http.createServer((req, res) => {
  const path = new URL(req.url, "http://x").pathname;
  res.setHeader("content-type", "application/json");
  if (path === "/ice") return res.end(JSON.stringify(iceConfig(process.env)));
  if (path === "/health") return res.end(JSON.stringify({ ok: true, rooms: core.rooms.size }));
  res.statusCode = 404; res.end("{}");
});
const wss = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 });
server.on("upgrade", (req, socket, head) => {
  if (new URL(req.url, "http://x").pathname !== "/ws") return socket.destroy();
  const ip = req.socket.remoteAddress || "local";
  if (!core.allowConnect(ip)) { socket.write("HTTP/1.1 429 Too Many Requests\r\n\r\n"); return socket.destroy(); }
  wss.handleUpgrade(req, socket, head, (ws) => {
    const conn = core.connect({ send: (d) => ws.readyState === 1 && ws.send(d), close: (c, r) => ws.close(c, r) }, { ip });
    ws.on("message", (data, isBinary) => conn.onMessage(isBinary ? data : data.toString()));
    ws.on("close", () => conn.onClose());
    ws.on("error", () => conn.onClose());
  });
});
server.listen(port, "127.0.0.1", () => console.log(`[zikei] local signaling on http://127.0.0.1:${port}`));
