// ZikeiSession ties together signaling, WebRTC and the transfer engine and exposes
// an immutable snapshot for React (subscribe / getSnapshot, usable with useSyncExternalStore).
//
// phase: idle | connecting | waiting-peer | waiting-approval | negotiating | connected | ended | error

import { Emitter, normalizeCode, randomDeviceName } from "./protocol.js";
import { SignalingClient } from "./signaling.js";
import { PeerLink, FALLBACK_ICE } from "./peer.js";
import { TransferManager } from "./transfer.js";
import { prepareSinks, blockedFiles, canStreamToDisk } from "./sinks.js";

const ERROR_TEXT = {
  rate_limited: "Too many attempts. Wait a minute and try again.",
  room_not_found: "Room not found or expired. Check the code and ask the sender to create a new room if needed.",
  room_full: "This room already has a second device.",
  room_locked: "Too many wrong PINs. The room is locked for a few minutes.",
  capacity: "The signaling server is busy. Try again in a moment.",
  bad_code: "Room codes have 6 characters (letters and digits).",
};

export class ZikeiSession extends Emitter {
  #opts;
  #sig = null;
  #link = null;
  #xfer = null;
  #xferOff = null;
  #creds = null; // {room, peerId, token}
  #intent = null; // {kind:"create", pin} | {kind:"join", code, pin}
  #deviceName;
  #snap;
  #listeners = new Set();
  #iceServers = null;
  #ended = false;
  #first = null;

  constructor(opts = {}) {
    super();
    this.#opts = {
      signalUrl: opts.signalUrl,
      iceUrl: opts.iceUrl ?? "/ice",
      fetch: opts.fetch ?? globalThis.fetch?.bind(globalThis),
      WebSocketImpl: opts.WebSocketImpl,
      RTCPeerConnectionImpl: opts.RTCPeerConnectionImpl,
      prepareSinks: opts.prepareSinks ?? prepareSinks,
      blockedFiles: opts.blockedFiles ?? blockedFiles,
      canStream: opts.canStream ?? canStreamToDisk,
    };
    this.#deviceName = opts.deviceName ?? randomDeviceName();
    this.#snap = this.#build({ phase: "idle" });
  }

  // ---- store ---------------------------------------------------------------
  subscribe = (fn) => { this.#listeners.add(fn); return () => this.#listeners.delete(fn); };
  getSnapshot = () => this.#snap;

  #state = {
    phase: "idle", role: "", room: "", pinProtected: false, error: "", errorCode: "", notice: "",
    signaling: "closed", rtc: "new", route: "unknown", pendingGuest: "", hostName: "",
  };

  #build(patch) {
    this.#state = { ...this.#state, ...patch };
    return { ...this.#state, deviceName: this.#deviceName, transfer: this.#xfer?.snapshot() ?? { peer: null, batches: [], texts: [], closed: false }, canStream: this.#opts.canStream() };
  }
  #set(patch) {
    this.#snap = this.#build(patch);
    for (const fn of [...this.#listeners]) fn();
  }
  #fail(code, message) { this.#teardown(true); this.#set({ phase: "error", error: message, errorCode: code }); }

  // ---- public actions ------------------------------------------------------
  async createRoom({ pin = "" } = {}) {
    this.#intent = { kind: "create", pin };
    this.#set({ phase: "connecting", role: "host", error: "", errorCode: "", notice: "" });
    await this.#start((sig) => sig.send({ type: "create", pin: pin || undefined, label: this.#deviceName }));
  }

  async joinRoom(code, pin = "") {
    const c = normalizeCode(code);
    this.#intent = { kind: "join", code: c, pin };
    this.#set({ phase: "connecting", role: "guest", room: c, error: "", errorCode: "", notice: "" });
    await this.#start((sig) => sig.send({ type: "join", room: c, pin: pin || undefined, label: this.#deviceName }));
  }

  approve() { this.#sig?.send({ type: "approve" }); this.#set({ pendingGuest: "" }); }
  reject() { this.#sig?.send({ type: "reject" }); this.#set({ pendingGuest: "" }); }

  sendFiles(files) { return this.#xfer?.sendFiles(files); }
  sendText(text) { return this.#xfer?.sendText(text); }
  acceptBatch(id) { return this.#xfer?.acceptBatch(id); }
  declineBatch(id) { this.#xfer?.declineBatch(id); }
  cancelBatch(id) { this.#xfer?.cancelBatch(id); }

  leave() {
    this.#sig?.send({ type: "leave" });
    this.#teardown(true);
    this.#creds = null;
    this.#intent = null;
    this.#set({ phase: "idle", role: "", room: "", error: "", errorCode: "", notice: "", pendingGuest: "", rtc: "new", route: "unknown", signaling: "closed", pinProtected: false });
  }

  // ---- internals -----------------------------------------------------------
  async #loadIce() {
    if (this.#iceServers) return this.#iceServers;
    try {
      const res = await this.#opts.fetch(this.#opts.iceUrl, { cache: "no-store" });
      if (res.ok) {
        const data = await res.json();
        if (Array.isArray(data.iceServers) && data.iceServers.length) return (this.#iceServers = data.iceServers);
      }
    } catch { /* fall back */ }
    return (this.#iceServers = FALLBACK_ICE);
  }

  async #start(firstMessage) {
    this.#teardown(true);
    this.#ended = false;
    const sig = new SignalingClient(this.#opts.signalUrl, this.#opts.WebSocketImpl);
    this.#sig = sig;
    sig.on("state", (s) => this.#set({ signaling: s }));
    sig.on("message", (m) => this.#onMessage(m));
    sig.on("open", ({ reconnect }) => { if (reconnect) this.#onReconnect(); });
    sig.on("gave-up", () => {
      if (this.#state.phase === "connected") this.#set({ notice: "Signaling server unreachable. The direct connection keeps working, but a new device cannot join." });
      else this.#fail("signaling_lost", "Lost the connection to the signaling server.");
    });
    try {
      await Promise.all([sig.connect(), this.#loadIce()]);
    } catch (err) {
      return this.#fail("signaling_unreachable", `${err.message} If you host this yourself, check that Netlify Server is enabled and that /ws is routed to it.`);
    }
    this.#first = firstMessage;
    firstMessage(sig);
  }

  #onReconnect() {
    if (!this.#creds) { this.#first?.(this.#sig); return; } // dropped before the room existed: repeat the first request
    this.#sig.send({ type: "rejoin", room: this.#creds.room, peerId: this.#creds.peerId, token: this.#creds.token });
  }

  #onMessage(m) {
    switch (m.type) {
      case "room-created":
        this.#creds = { room: m.room, peerId: m.peerId, token: m.token };
        this.#set({ phase: "waiting-peer", room: m.room, pinProtected: !!m.pinProtected });
        break;
      case "join-pending":
        this.#creds = { room: m.room, peerId: m.peerId, token: m.token };
        this.#set({ phase: "waiting-approval", room: m.room });
        break;
      case "join-request":
        this.#set({ pendingGuest: m.label || "Device" });
        break;
      case "joined": // guest approved -> prepare to answer
        this.#set({ phase: "negotiating", hostName: m.hostLabel || "" });
        this.#startLink("guest");
        break;
      case "peer-joined": // host: guest approved -> make the offer
        this.#set({ phase: "negotiating", pendingGuest: "" });
        this.#startLink("host");
        break;
      case "signal":
        this.#link?.handleSignal(m.payload);
        break;
      case "peer-online":
        this.#link?.retryPending();
        break;
      case "peer-offline":
        if (this.#state.phase !== "connected") this.#set({ notice: "The other device lost its signaling connection. Waiting for it to return…" });
        break;
      case "join-rejected":
        this.#fail("rejected", "The room creator declined your request.");
        break;
      case "peer-left":
        this.#onPeerLeft(m.reason);
        break;
      case "room-expired":
        if (this.#state.phase !== "connected") this.#fail("room_expired", "The room expired.");
        else this.#set({ notice: "The signaling room expired. Your direct connection still works." });
        break;
      case "resumed":
        this.#set({ notice: "" });
        if (this.#state.role === "host" && m.peerState === "none" && this.#state.phase === "negotiating") this.#set({ phase: "waiting-peer" });
        this.#link?.retryPending();
        break;
      case "error":
        this.#onServerError(m);
        break;
      default:
        break;
    }
  }

  #onPeerLeft(reason) {
    if (this.#state.phase === "connected" || this.#state.phase === "negotiating") {
      this.#teardown(this.#state.role !== "host");
      if (this.#state.role === "host") {
        this.#set({ phase: "waiting-peer", rtc: "new", notice: "The other device left. The room is open for another device." });
      } else {
        this.#set({ phase: "ended", notice: "The other device left the room." });
        this.#teardown(true);
      }
    } else if (this.#state.role === "host") {
      this.#set({ pendingGuest: "", notice: reason === "join_cancelled" ? "The device that wanted to join went away." : "" });
    } else {
      this.#set({ phase: "ended", notice: "The room was closed." });
      this.#teardown(true);
    }
  }

  #onServerError(m) {
    const text = ERROR_TEXT[m.code] ?? m.message ?? "Something went wrong.";
    if (m.code === "pin_required" || m.code === "pin_invalid") {
      this.#teardown(true);
      return this.#set({ phase: "error", error: text, errorCode: m.code });
    }
    if (m.code === "peer_offline" || m.code === "not_paired") return; // transient; ICE restart will retry
    if (m.code === "room_not_found" && this.#creds && this.#state.phase === "connected") {
      return this.#set({ notice: "Signaling session lost (server restarted). The direct connection keeps working; create a new room for more devices." });
    }
    this.#fail(m.code || "error", text);
  }

  #startLink(role) {
    this.#disposeLink();
    const link = new PeerLink({
      role,
      iceServers: this.#iceServers,
      RTCPeerConnectionImpl: this.#opts.RTCPeerConnectionImpl,
      signal: (payload) => this.#sig?.send({ type: "signal", payload }) ?? false,
    });
    this.#link = link;
    link.on("state", (s) => {
      if (this.#link !== link) return;
      this.#set({ rtc: s });
      if (s === "connected") link.getRouteType().then((r) => { if (this.#link === link) this.#set({ route: r }); });
    });
    link.on("channel-open", (ch) => {
      if (this.#link !== link) return;
      this.#xfer = new TransferManager({
        channel: ch,
        localName: this.#deviceName,
        getChunkSize: () => link.maxMessageSize,
        prepareSinks: this.#opts.prepareSinks,
        blockedFiles: this.#opts.blockedFiles,
        canStream: this.#opts.canStream(),
      });
      this.#xferOff = this.#xfer.on("change", () => this.#set({}));
      this.#xfer.on("fatal", (msg) => this.#fail("incompatible", msg));
      this.#set({ phase: "connected", notice: "", error: "" });
    });
    link.on("channel-close", () => {
      if (this.#link !== link || this.#ended) return;
      this.#set({ phase: "ended", notice: "The direct connection closed." });
      this.#xfer?.dispose();
    });
    link.on("failed", ({ reason }) => {
      if (this.#link !== link) return;
      if (this.#state.phase === "connected") return this.#set({ notice: "The direct connection is unstable or lost." });
      this.#fail("rtc_failed", reason === "timeout"
        ? "Could not establish a direct connection in time. Some networks (strict NAT, corporate or mobile carrier firewalls) block peer-to-peer traffic. A TURN relay server is needed for those cases."
        : "The direct connection failed. Some networks block peer-to-peer traffic; a TURN relay server is needed for those cases.");
    });
    link.on("error", (err) => console.warn("[zikei] peer error", err));
    link.start();
  }

  #disposeLink() {
    this.#xferOff?.();
    this.#xferOff = null;
    this.#xfer?.dispose();
    this.#xfer = null;
    this.#link?.close();
    this.#link = null;
  }

  #teardown(closeSignaling) {
    if (closeSignaling) this.#ended = true;
    this.#disposeLink();
    if (closeSignaling) { this.#sig?.close(); this.#sig = null; }
  }

  destroy() { this.#sig?.send({ type: "leave" }); this.#teardown(true); }
}
