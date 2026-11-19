// WebRTC connection management.
//
// Roles are fixed: the room creator ("host") always creates the data channel and
// the offer; the joiner ("guest") only answers. That removes offer glare entirely.
// ICE candidates that arrive before the remote description are buffered. Messages
// are processed strictly one after another.

import { Emitter, LIMITS } from "./protocol.js";

export const FALLBACK_ICE = [{ urls: ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"] }];

export class PeerLink extends Emitter {
  #role;
  #iceServers;
  #signal;
  #RTC;
  #pc = null;
  #channel = null;
  #queue = Promise.resolve();
  #pendingCandidates = [];
  #closed = false;
  #restartTimer = null;
  #connectTimer = null;
  #restarts = 0;
  #needsRestart = false;

  constructor({ role, iceServers, signal, RTCPeerConnectionImpl = globalThis.RTCPeerConnection }) {
    super();
    this.#role = role;
    this.#iceServers = iceServers?.length ? iceServers : FALLBACK_ICE;
    this.#signal = signal; // (payload) => boolean (true if handed to the signaling socket)
    this.#RTC = RTCPeerConnectionImpl;
  }

  get channel() { return this.#channel; }
  get connectionState() { return this.#pc?.connectionState ?? "new"; }
  get maxMessageSize() { return this.#pc?.sctp?.maxMessageSize || 0; }

  /** Host: create channel + first offer. Guest: prepare the peer connection and wait for the offer. */
  start() {
    this.#ensurePc();
    this.#connectTimer = setTimeout(() => {
      if (this.#pc && this.#pc.connectionState !== "connected") {
        this.emit("failed", { reason: "timeout" });
      }
    }, LIMITS.CONNECT_TIMEOUT_MS);
    if (this.#role === "host") {
      this.#attachChannel(this.#pc.createDataChannel("zikei", { ordered: true }));
      return this.#enqueue(async () => { if (!(await this.#makeOffer(false))) this.#needsRestart = true; });
    }
    return Promise.resolve();
  }

  #ensurePc() {
    if (this.#pc) return this.#pc;
    const pc = new this.#RTC({ iceServers: this.#iceServers });
    this.#pc = pc;
    pc.onicecandidate = (e) => {
      // e.candidate === null marks end-of-candidates; remote side does not need it.
      if (e.candidate) this.#signal({ kind: "candidate", candidate: e.candidate.toJSON ? e.candidate.toJSON() : e.candidate });
    };
    pc.onconnectionstatechange = () => this.#onConnectionState();
    pc.ondatachannel = (e) => { if (e.channel.label === "zikei") this.#attachChannel(e.channel); };
    return pc;
  }

  #onConnectionState() {
    const state = this.#pc?.connectionState;
    this.emit("state", state);
    if (state === "connected") {
      clearTimeout(this.#connectTimer);
      clearTimeout(this.#restartTimer);
      this.#restarts = 0;
      this.#needsRestart = false;
    } else if (state === "disconnected") {
      // Often transient (Wi-Fi roaming). Give it a few seconds, then restart ICE.
      clearTimeout(this.#restartTimer);
      this.#restartTimer = setTimeout(() => this.#restartIce(), 4000);
    } else if (state === "failed") {
      clearTimeout(this.#restartTimer);
      this.#restartIce();
    }
  }

  /** Host-only: ICE restart through signaling. Retries are limited; afterwards the link is reported failed. */
  #restartIce() {
    if (this.#closed || !this.#pc) return;
    if (this.#role !== "host") return; // guest waits for host's restart offer
    if (this.#pc.connectionState === "connected") return;
    if (this.#restarts >= 3) { this.emit("failed", { reason: "ice" }); return; }
    this.#restarts += 1;
    this.#enqueue(async () => {
      const ok = await this.#makeOffer(true);
      if (!ok) this.#needsRestart = true; // signaling unavailable: retry via retryPending()
    });
  }

  /** Called by the session when signaling is back. */
  retryPending() {
    if (this.#needsRestart && this.#role === "host") { this.#needsRestart = false; this.#restartIce(); }
  }

  async #makeOffer(iceRestart) {
    const pc = this.#pc;
    if (!pc || this.#closed) return false;
    const offer = await pc.createOffer(iceRestart ? { iceRestart: true } : undefined);
    await pc.setLocalDescription(offer);
    return this.#signal({ kind: "offer", description: { type: pc.localDescription.type, sdp: pc.localDescription.sdp } });
  }

  #attachChannel(ch) {
    this.#channel = ch;
    ch.binaryType = "arraybuffer";
    ch.bufferedAmountLowThreshold = LIMITS.SEND_LOW_WATER;
    let opened = false;
    const announce = () => { if (opened) return; opened = true; this.emit("channel-open", ch); };
    ch.addEventListener("open", announce);
    ch.addEventListener("close", () => this.emit("channel-close", ch));
    ch.addEventListener("error", () => this.emit("channel-error", ch));
    if (ch.readyState === "open") queueMicrotask(announce);
  }

  /** Feed a signaling payload (offer / answer / candidate). Processed serially. */
  handleSignal(payload) {
    return this.#enqueue(() => this.#process(payload));
  }

  #enqueue(fn) {
    this.#queue = this.#queue.then(fn).catch((err) => {
      if (!this.#closed) this.emit("error", err);
    });
    return this.#queue;
  }

  async #process(payload) {
    if (this.#closed || !payload) return;
    const pc = this.#ensurePc();
    if (payload.kind === "offer") {
      if (this.#role !== "guest") return; // only the host offers
      await pc.setRemoteDescription(payload.description);
      await this.#flushCandidates();
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      this.#signal({ kind: "answer", description: { type: pc.localDescription.type, sdp: pc.localDescription.sdp } });
    } else if (payload.kind === "answer") {
      if (this.#role !== "host" || pc.signalingState !== "have-local-offer") return;
      await pc.setRemoteDescription(payload.description);
      await this.#flushCandidates();
    } else if (payload.kind === "candidate") {
      if (!payload.candidate) return;
      if (pc.remoteDescription) {
        try { await pc.addIceCandidate(payload.candidate); } catch (err) { console.warn("[zikei] addIceCandidate failed", err); }
      } else if (this.#pendingCandidates.length < 200) {
        this.#pendingCandidates.push(payload.candidate);
      }
    }
  }

  async #flushCandidates() {
    const list = this.#pendingCandidates;
    this.#pendingCandidates = [];
    for (const c of list) {
      try { await this.#pc.addIceCandidate(c); } catch (err) { console.warn("[zikei] buffered candidate rejected", err); }
    }
  }

  /** "direct" | "relay" | "unknown" - based on the selected candidate pair. */
  async getRouteType() {
    try {
      const stats = await this.#pc.getStats();
      let pair = null;
      const byId = new Map();
      stats.forEach((r) => byId.set(r.id, r));
      stats.forEach((r) => {
        if (r.type === "transport" && r.selectedCandidatePairId) pair = byId.get(r.selectedCandidatePairId);
      });
      if (!pair) stats.forEach((r) => { if (r.type === "candidate-pair" && r.nominated && r.state === "succeeded") pair = r; });
      if (!pair) return "unknown";
      const local = byId.get(pair.localCandidateId);
      const remote = byId.get(pair.remoteCandidateId);
      return local?.candidateType === "relay" || remote?.candidateType === "relay" ? "relay" : "direct";
    } catch { return "unknown"; }
  }

  close() {
    this.#closed = true;
    clearTimeout(this.#restartTimer);
    clearTimeout(this.#connectTimer);
    try { this.#channel?.close(); } catch { /* ignore */ }
    try { this.#pc?.close(); } catch { /* ignore */ }
    this.#channel = null;
    this.#pc = null;
  }
}
