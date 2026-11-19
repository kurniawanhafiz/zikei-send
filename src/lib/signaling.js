// WebSocket signaling client with heartbeat and automatic reconnect.
// It only transports JSON messages; room/session semantics live in session.js.

import { Emitter } from "./protocol.js";

export class SignalingClient extends Emitter {
  #url;
  #WS;
  #socket = null;
  #intentionalClose = false;
  #attempt = 0;
  #retryTimer = null;
  #pingTimer = null;
  #openTimer = null;
  maxAttempts = 12;
  state = "closed"; // closed | connecting | open | reconnecting

  constructor(url, WebSocketImpl = globalThis.WebSocket) {
    super();
    this.#url = url;
    this.#WS = WebSocketImpl;
  }

  #setState(s) {
    if (this.state === s) return;
    this.state = s;
    this.emit("state", s);
  }

  /** Opens the socket (resolves once open, rejects on failure/timeout). Later drops auto-reconnect. */
  connect(timeoutMs = 12_000) {
    this.#intentionalClose = false;
    return new Promise((resolve, reject) => {
      this.#open(timeoutMs, resolve, reject);
    });
  }

  #open(timeoutMs, resolve, reject) {
    clearTimeout(this.#retryTimer);
    this.#setState(this.#attempt > 0 ? "reconnecting" : "connecting");
    let settled = false;
    let socket;
    try { socket = new this.#WS(this.#url); } catch (err) { return reject?.(err); }
    this.#socket = socket;
    this.#openTimer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try { socket.close(); } catch { /* ignore */ }
      reject?.(new Error("Signaling server did not respond in time."));
      this.#handleDrop(socket);
    }, timeoutMs);

    socket.onopen = () => {
      clearTimeout(this.#openTimer);
      settled = true;
      const wasReconnect = this.#attempt > 0;
      this.#attempt = 0;
      this.#setState("open");
      this.#startHeartbeat();
      resolve?.();
      this.emit("open", { reconnect: wasReconnect });
    };
    socket.onmessage = (event) => {
      let msg;
      try { msg = JSON.parse(event.data); } catch { return; }
      if (msg && typeof msg.type === "string") this.emit("message", msg);
    };
    socket.onerror = () => { /* "close" always follows; handled there */ };
    socket.onclose = (event) => {
      clearTimeout(this.#openTimer);
      if (!settled) {
        settled = true;
        reject?.(new Error("Cannot reach the signaling server."));
      }
      this.#handleDrop(socket, event);
    };
  }

  #handleDrop(socket, event) {
    if (socket !== this.#socket) return; // a stale socket
    this.#stopHeartbeat();
    this.#socket = null;
    if (this.#intentionalClose) { this.#setState("closed"); return; }
    this.emit("drop", { code: event?.code ?? 0 });
    if (this.#attempt >= this.maxAttempts) {
      this.#setState("closed");
      this.emit("gave-up");
      return;
    }
    // 1012 = server restarting -> retry right away; otherwise exponential backoff with jitter.
    const delay = event?.code === 1012 && this.#attempt === 0 ? 0 : Math.min(8000, 500 * 2 ** this.#attempt) + Math.random() * 300;
    this.#attempt += 1;
    this.#setState("reconnecting");
    this.#retryTimer = setTimeout(() => this.#open(12_000, null, null), delay);
  }

  #startHeartbeat() {
    this.#stopHeartbeat();
    this.#pingTimer = setInterval(() => this.send({ type: "ping" }), 25_000);
  }
  #stopHeartbeat() { clearInterval(this.#pingTimer); this.#pingTimer = null; }

  send(obj) {
    if (this.#socket?.readyState !== 1) return false;
    try { this.#socket.send(JSON.stringify(obj)); return true; } catch { return false; }
  }

  get isOpen() { return this.#socket?.readyState === 1; }

  close() {
    this.#intentionalClose = true;
    clearTimeout(this.#retryTimer);
    clearTimeout(this.#openTimer);
    this.#stopHeartbeat();
    const s = this.#socket;
    this.#socket = null;
    try { s?.close(1000, "bye"); } catch { /* ignore */ }
    this.#setState("closed");
  }
}
