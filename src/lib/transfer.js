// File + text transfer over one ordered, reliable RTCDataChannel.
//
// Wire protocol (control frames are JSON strings, file bytes are binary frames):
//   hello         {t,v,name,stream}                      capabilities, sent by both sides when the channel opens
//   batch         {t,bid,files:[{fid,name,size,type}]}   sender proposes files
//   batch-accept  {t,bid} | batch-decline {t,bid,reason} receiver's answer (accepting needs a user click)
//   file-start    {t,bid,fid}   then binary chunks, then   file-end {t,fid,size}
//   ack           {t,fid,bytes}                          receiver -> sender, bytes WRITTEN so far (flow control + progress)
//   file-done     {t,fid,ok,error?}                      receiver -> sender after the sink is closed
//   cancel        {t,bid,by:"sender"|"receiver"}
//   text          {t,id,text}
//
// Flow control: the sender never has more than LIMITS.WINDOW unacknowledged bytes in flight and also
// respects RTCDataChannel.bufferedAmount, so receiver memory stays bounded.
// Integrity: the channel is reliable/ordered (SCTP with its own checksum); on top of that the receiver
// verifies that the exact announced byte count arrived. No per-file hash is computed (WebCrypto cannot hash
// incrementally), which is documented as a known limitation.

import { Emitter, LIMITS, randomId, sanitizeFilename } from "./protocol.js";

const ID_RE = /^[A-Za-z0-9_-]{1,64}$/;

class Aborted extends Error {
  constructor(reason) { super(reason); this.name = "Aborted"; this.reason = reason; }
}

export class TransferManager extends Emitter {
  #ch;
  #opts;
  #now;
  peer = null; // {name, stream}
  #sendBatches = new Map(); // bid -> batch
  #recvBatches = new Map();
  #sendQueue = [];
  #sending = false;
  #files = new Map(); // fid -> File (sender side)
  #acked = new Map(); // fid -> bytes acknowledged by receiver
  #fileDone = new Map(); // fid -> {resolve}
  #waiters = new Set();
  #cur = null; // current file being received
  #texts = [];
  #closed = false;
  #emitTimer = null;
  #listeners = [];

  /**
   * @param {object} o
   * @param {RTCDataChannel} o.channel
   * @param {string} o.localName
   * @param {() => number} [o.getChunkSize]
   * @param {(batch) => Promise<{open(info): Promise<sink>}>} o.prepareSinks
   * @param {(batch) => Array} [o.blockedFiles]
   * @param {boolean} [o.canStream]
   */
  constructor(o) {
    super();
    this.#ch = o.channel;
    this.#opts = o;
    this.#now = o.now ?? (() => Date.now());
    this.#ch.binaryType = "arraybuffer";
    this.#ch.bufferedAmountLowThreshold = LIMITS.SEND_LOW_WATER;
    const on = (type, fn) => { this.#ch.addEventListener(type, fn); this.#listeners.push([type, fn]); };
    on("message", (e) => this.#onMessage(e.data));
    on("close", () => this.#onClosed("Connection closed"));
    if (this.#ch.readyState === "open") this.#hello();
    else on("open", () => this.#hello());
  }

  // ------------------------------------------------------------------ public API

  /** Queue files for sending. Returns the batch id. */
  sendFiles(fileList) {
    const files = Array.from(fileList ?? []);
    if (!files.length) return null;
    if (files.length > LIMITS.MAX_FILES_PER_BATCH) throw new Error(`Send at most ${LIMITS.MAX_FILES_PER_BATCH} files at a time.`);
    const bid = randomId("b");
    const items = files.map((f) => {
      const fid = randomId("f");
      this.#files.set(fid, f);
      return { id: fid, name: f.name || "file", size: f.size, type: f.type || "", status: "queued", loaded: 0, speed: 0, eta: 0, error: "" };
    });
    const batch = { id: bid, direction: "send", status: "queued", reason: "", items, aborted: false, abortReason: "", decision: null, createdAt: this.#now() };
    this.#sendBatches.set(bid, batch);
    this.#sendQueue.push(batch);
    this.#emitNow();
    this.#pump();
    return bid;
  }

  sendText(text) {
    const t = String(text ?? "");
    if (!t.trim()) return false;
    if (t.length > LIMITS.MAX_TEXT) throw new Error(`Text is limited to ${LIMITS.MAX_TEXT.toLocaleString()} characters.`);
    if (!this.#ctrl({ t: "text", id: randomId("t"), text: t })) throw new Error("Not connected.");
    this.#pushText({ from: "me", text: t });
    return true;
  }

  /** Receiver clicks "Accept". Must run inside the click handler (file pickers need a user gesture). */
  async acceptBatch(bid) {
    const b = this.#recvBatches.get(bid);
    if (!b || b.status !== "pending") return { ok: false };
    if (b.blocked.length) return { ok: false, error: "blocked" };
    b.status = "accepting";
    this.#emitNow();
    let sinks;
    try {
      sinks = await this.#opts.prepareSinks({ files: b.items.map((i) => ({ name: i.name, size: i.size })) });
    } catch (err) {
      b.status = "pending"; // picker dismissed: stay pending
      this.#emitNow();
      return { ok: false, cancelled: err?.name === "AbortError", error: err?.message };
    }
    if (b.status !== "accepting" || this.#closed) { return { ok: false }; }
    b.sinks = sinks;
    b.status = "receiving";
    b.sinkKind = sinks.kind;
    this.#ctrl({ t: "batch-accept", bid });
    this.#emitNow();
    return { ok: true };
  }

  declineBatch(bid, reason = "declined") {
    const b = this.#recvBatches.get(bid);
    if (!b || !["pending", "accepting"].includes(b.status)) return;
    b.status = "declined";
    b.reason = reason;
    this.#ctrl({ t: "batch-decline", bid, reason });
    this.#emitNow();
  }

  /** Cancel an outgoing or incoming batch. */
  cancelBatch(bid) {
    const sb = this.#sendBatches.get(bid);
    if (sb) { this.#abortSend(sb, "cancelled", true); return; }
    const rb = this.#recvBatches.get(bid);
    if (rb && ["receiving", "pending", "accepting"].includes(rb.status)) {
      if (rb.status !== "receiving") return this.declineBatch(bid);
      this.#ctrl({ t: "cancel", bid, by: "receiver" });
      this.#abortReceive(rb, "cancelled", "You cancelled this transfer.");
    }
  }

  snapshot() {
    const pubItem = (i) => ({ id: i.id, name: i.name, size: i.size, status: i.status, loaded: i.loaded, speed: i.speed, eta: i.eta, error: i.error, savedAs: i.savedAs, location: i.location });
    const pubBatch = (b) => ({
      id: b.id, direction: b.direction, status: b.status, reason: b.reason, blocked: b.blocked?.map((f) => f.name) ?? [],
      sinkKind: b.sinkKind ?? "", total: b.items.reduce((n, i) => n + i.size, 0), items: b.items.map(pubItem), createdAt: b.createdAt,
    });
    const all = [...this.#sendBatches.values(), ...this.#recvBatches.values()].sort((a, b) => a.createdAt - b.createdAt);
    return { peer: this.peer, batches: all.map(pubBatch), texts: [...this.#texts], closed: this.#closed };
  }

  /** Stop everything and detach from the channel. */
  dispose() {
    this.#onClosed("Disconnected");
    for (const [type, fn] of this.#listeners) { try { this.#ch.removeEventListener(type, fn); } catch { /* ignore */ } }
    this.#listeners = [];
  }

  // ------------------------------------------------------------------ plumbing

  #hello() {
    this.#ctrl({ t: "hello", v: LIMITS.PROTOCOL_VERSION, name: this.#opts.localName, stream: !!this.#opts.canStream });
  }

  #ctrl(obj) {
    if (this.#ch.readyState !== "open") return false;
    try { this.#ch.send(JSON.stringify(obj)); return true; } catch { return false; }
  }

  #emitNow() {
    clearTimeout(this.#emitTimer);
    this.#emitTimer = null;
    this.emit("change");
  }
  #emitSoon() {
    if (this.#emitTimer) return;
    this.#emitTimer = setTimeout(() => { this.#emitTimer = null; this.emit("change"); }, 120);
  }

  #pushText(t) {
    this.#texts.push({ id: randomId("t"), at: this.#now(), ...t });
    if (this.#texts.length > 50) this.#texts.shift();
    this.#emitNow();
  }

  #notify() {
    const list = [...this.#waiters];
    this.#waiters.clear();
    for (const fn of list) fn();
  }
  #waitSignal() { return new Promise((res) => this.#waiters.add(res)); }

  #progress(item, loaded) {
    const t = this.#now();
    item.loaded = loaded;
    const s = (item._s ??= { t, bytes: loaded, ema: 0 });
    const dt = t - s.t;
    if (dt >= 250) {
      const inst = ((loaded - s.bytes) / dt) * 1000;
      s.ema = s.ema ? 0.7 * s.ema + 0.3 * inst : inst;
      s.t = t; s.bytes = loaded;
    }
    item.speed = s.ema;
    item.eta = s.ema > 0 ? (item.size - loaded) / s.ema : 0;
    this.#emitSoon();
  }

  // ------------------------------------------------------------------ sending

  async #pump() {
    if (this.#sending) return;
    this.#sending = true;
    try {
      while (this.#sendQueue.length && !this.#closed) {
        const b = this.#sendQueue.shift();
        if (b.aborted) continue;
        await this.#runSend(b);
      }
    } finally { this.#sending = false; }
  }

  async #runSend(b) {
    try {
      b.status = "awaiting";
      this.#emitNow();
      if (!this.#ctrl({ t: "batch", bid: b.id, files: b.items.map((i) => ({ fid: i.id, name: i.name, size: i.size, type: i.type })) })) throw new Aborted("Not connected.");
      const accepted = await new Promise((resolve) => { b.decision = resolve; });
      b.decision = null;
      if (b.aborted) throw new Aborted(b.abortReason);
      if (accepted !== true) {
        b.status = "declined";
        b.reason = typeof accepted === "string" ? accepted : "declined";
        for (const i of b.items) i.status = "declined";
        return;
      }
      b.status = "sending";
      this.#emitNow();
      for (const item of b.items) {
        this.#assertLive(b);
        await this.#sendFile(b, item);
      }
      b.status = "done";
    } catch (err) {
      if (b.status !== "cancelled" && b.status !== "declined") {
        b.status = err instanceof Aborted && err.reason === "cancelled" ? "cancelled" : "failed";
        b.reason = err?.message || "Transfer failed";
      }
      for (const i of b.items) if (["queued", "sending"].includes(i.status)) { i.status = b.status === "cancelled" ? "cancelled" : "failed"; i.error ||= b.reason; }
    } finally {
      for (const i of b.items) { this.#files.delete(i.id); this.#acked.delete(i.id); this.#fileDone.delete(i.id); }
      this.#emitNow();
    }
  }

  #assertLive(b) {
    if (b.aborted) throw new Aborted(b.abortReason || "cancelled");
    if (this.#closed || this.#ch.readyState !== "open") throw new Aborted("Connection lost");
  }

  async #drain(b) {
    const ch = this.#ch;
    while (ch.bufferedAmount > LIMITS.SEND_HIGH_WATER) {
      await new Promise((resolve) => {
        const done = () => { ch.removeEventListener("bufferedamountlow", done); ch.removeEventListener("close", done); this.#waiters.delete(done); resolve(); };
        ch.addEventListener("bufferedamountlow", done);
        ch.addEventListener("close", done);
        this.#waiters.add(done); // lets cancel/close wake us immediately
      });
      this.#assertLive(b);
    }
  }

  async #sendFile(b, item) {
    const file = this.#files.get(item.id);
    const chunkSize = Math.max(1024, Math.min(LIMITS.CHUNK_SIZE, this.#opts.getChunkSize?.() || LIMITS.CHUNK_SIZE));
    item.status = "sending";
    item.startedAt = this.#now();
    this.#acked.set(item.id, 0);
    const done = new Promise((resolve) => this.#fileDone.set(item.id, { resolve }));
    if (!this.#ctrl({ t: "file-start", bid: b.id, fid: item.id })) throw new Aborted("Connection lost");

    let offset = 0;
    while (offset < item.size) {
      this.#assertLive(b);
      while (offset - (this.#acked.get(item.id) ?? 0) > LIMITS.WINDOW) { // flow control window
        await this.#waitSignal();
        this.#assertLive(b);
      }
      const end = Math.min(offset + LIMITS.READ_BLOCK, item.size);
      const block = new Uint8Array(await file.slice(offset, end).arrayBuffer());
      if (block.byteLength !== end - offset) throw new Error(`Could not read "${item.name}" (file changed or became unavailable).`);
      for (let p = 0; p < block.byteLength; p += chunkSize) {
        await this.#drain(b);
        this.#assertLive(b);
        this.#ch.send(block.subarray(p, Math.min(p + chunkSize, block.byteLength)));
      }
      offset = end;
    }
    this.#assertLive(b);
    this.#ctrl({ t: "file-end", fid: item.id, size: item.size });
    // Wait for the receiver to confirm it has written + closed the file (or for cancel/close).
    let result;
    while (!(result = await Promise.race([done, this.#waitSignal().then(() => null)]))) this.#assertLive(b);
    if (!result.ok) throw new Error(result.error || `Receiver could not save "${item.name}".`);
    item.status = "done";
    this.#progress(item, item.size);
    item.speed = 0; item.eta = 0;
    this.#emitNow();
  }

  #abortSend(b, reason, notifyPeer) {
    if (b.aborted || ["done", "failed", "cancelled", "declined"].includes(b.status)) return;
    b.aborted = true;
    b.abortReason = reason;
    if (notifyPeer && b.status !== "queued") this.#ctrl({ t: "cancel", bid: b.id, by: "sender" });
    if (b.status === "queued") {
      b.status = "cancelled";
      for (const i of b.items) i.status = "cancelled";
      this.#files.forEach((_, k) => { if (b.items.some((i) => i.id === k)) this.#files.delete(k); });
    }
    b.decision?.(false);
    this.#notify();
    this.#emitNow();
  }

  // ------------------------------------------------------------------ receiving

  #onMessage(data) {
    if (typeof data === "string") {
      if (data.length > LIMITS.MAX_CONTROL_JSON) return;
      let msg;
      try { msg = JSON.parse(data); } catch { return; }
      if (!msg || typeof msg !== "object" || typeof msg.t !== "string") return;
      try { this.#onControl(msg); } catch (err) { console.warn("[zikei] control frame error", err); }
      return;
    }
    if (data instanceof ArrayBuffer) this.#onChunk(new Uint8Array(data));
    else if (ArrayBuffer.isView(data)) this.#onChunk(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
  }

  #onControl(m) {
    switch (m.t) {
      case "hello": {
        if (m.v !== LIMITS.PROTOCOL_VERSION) { this.#fatal("The other device runs an incompatible version of Zikei Send."); return; }
        this.peer = { name: String(m.name ?? "Device").replace(/[\u0000-\u001f<>]/g, "").slice(0, 32) || "Device", stream: !!m.stream };
        this.#emitNow();
        return;
      }
      case "text": {
        if (typeof m.text !== "string" || m.text.length > LIMITS.MAX_TEXT || !m.text.trim()) return;
        this.#pushText({ from: "peer", text: m.text });
        return;
      }
      case "batch": return this.#onBatch(m);
      case "batch-accept": {
        const b = this.#sendBatches.get(m.bid);
        if (b?.decision) b.decision(true);
        return;
      }
      case "batch-decline": {
        const b = this.#sendBatches.get(m.bid);
        if (b?.decision) b.decision(typeof m.reason === "string" ? m.reason.slice(0, 80) : "declined");
        return;
      }
      case "ack": {
        if (typeof m.fid !== "string" || !this.#acked.has(m.fid) || !Number.isFinite(m.bytes)) return;
        const bytes = Math.max(this.#acked.get(m.fid), m.bytes);
        this.#acked.set(m.fid, bytes);
        const item = this.#findSendItem(m.fid);
        if (item && item.status === "sending") this.#progress(item, Math.min(bytes, item.size));
        this.#notify();
        return;
      }
      case "file-done": {
        const w = this.#fileDone.get(m.fid);
        if (w) { w.resolve({ ok: !!m.ok, error: typeof m.error === "string" ? m.error.slice(0, 200) : "" }); this.#fileDone.delete(m.fid); }
        return;
      }
      case "cancel": {
        if (m.by === "sender") {
          const rb = this.#recvBatches.get(m.bid);
          if (rb && ["receiving", "pending", "accepting"].includes(rb.status)) this.#abortReceive(rb, "cancelled", "The sender cancelled this transfer.");
        } else {
          const sb = this.#sendBatches.get(m.bid);
          if (sb) this.#abortSend(sb, "cancelled", false);
        }
        return;
      }
      case "file-start": return this.#onFileStart(m);
      case "file-end": return this.#onFileEnd(m);
      default: return;
    }
  }

  #findSendItem(fid) {
    for (const b of this.#sendBatches.values()) { const i = b.items.find((x) => x.id === fid); if (i) return i; }
    return null;
  }

  #onBatch(m) {
    const decline = (reason) => this.#ctrl({ t: "batch-decline", bid: m.bid, reason });
    if (typeof m.bid !== "string" || !ID_RE.test(m.bid) || this.#recvBatches.has(m.bid)) return decline("invalid");
    if (!Array.isArray(m.files) || m.files.length < 1 || m.files.length > LIMITS.MAX_FILES_PER_BATCH) return decline("invalid");
    const seen = new Set();
    const items = [];
    for (const f of m.files) {
      if (!f || typeof f.fid !== "string" || !ID_RE.test(f.fid) || seen.has(f.fid)) return decline("invalid");
      if (!Number.isSafeInteger(f.size) || f.size < 0) return decline("invalid");
      seen.add(f.fid);
      items.push({ id: f.fid, name: sanitizeFilename(f.name), size: f.size, type: typeof f.type === "string" ? f.type.slice(0, 100) : "", status: "pending", loaded: 0, speed: 0, eta: 0, error: "" });
    }
    const active = [...this.#recvBatches.values()];
    if (active.some((x) => x.status === "receiving" || x.status === "accepting")) return decline("busy");
    if (active.filter((x) => x.status === "pending").length >= LIMITS.MAX_PENDING_BATCHES) return decline("busy");
    const batch = { id: m.bid, direction: "receive", status: "pending", reason: "", items, sinks: null, blocked: [], cursor: 0, createdAt: this.#now() };
    batch.blocked = this.#opts.blockedFiles ? this.#opts.blockedFiles({ files: items }) : [];
    this.#recvBatches.set(batch.id, batch);
    this.#emitNow();
    this.emit("incoming", batch.id);
  }

  async #onFileStart(m) {
    const b = this.#recvBatches.get(m.bid);
    if (!b || b.status !== "receiving" || this.#cur) return this.#ctrl({ t: "file-done", fid: String(m.fid), ok: false, error: "Unexpected file" });
    const item = b.items[b.cursor];
    if (!item || item.id !== m.fid) return this.#ctrl({ t: "file-done", fid: String(m.fid), ok: false, error: "Unexpected file order" });
    b.cursor += 1;
    item.status = "receiving";
    const cur = { batch: b, item, received: 0, written: 0, lastAck: 0, failed: false, sink: null, queue: Promise.resolve() };
    this.#cur = cur;
    cur.ready = b.sinks.open({ name: item.name, size: item.size, type: item.type }).then((s) => { cur.sink = s; }, (err) => { this.#failCurrent(cur, `Could not open the destination file: ${err?.message || err}`); });
    cur.queue = cur.ready;
    this.#emitNow();
  }

  #onChunk(u8) {
    const cur = this.#cur;
    if (!cur || cur.failed) return;
    cur.received += u8.byteLength;
    if (cur.received > cur.item.size) return this.#failCurrent(cur, "Received more data than announced.");
    cur.queue = cur.queue.then(async () => {
      if (cur.failed || !cur.sink) return;
      try {
        await cur.sink.write(u8);
      } catch (err) {
        return this.#failCurrent(cur, `Write failed: ${err?.message || err}`);
      }
      cur.written += u8.byteLength;
      this.#progress(cur.item, cur.written);
      if (cur.written - cur.lastAck >= LIMITS.ACK_STEP) {
        cur.lastAck = cur.written;
        this.#ctrl({ t: "ack", fid: cur.item.id, bytes: cur.written });
      }
    });
  }

  #onFileEnd(m) {
    const cur = this.#cur;
    if (!cur || cur.item.id !== m.fid) return;
    cur.queue = cur.queue.then(async () => {
      if (cur.failed) return;
      if (cur.received !== cur.item.size || cur.written !== cur.item.size || m.size !== cur.item.size) {
        return this.#failCurrent(cur, `Size mismatch (expected ${cur.item.size} bytes, got ${cur.written}).`);
      }
      let info;
      try { info = await cur.sink.close(); } catch (err) { return this.#failCurrent(cur, `Could not finish writing: ${err?.message || err}`); }
      cur.item.status = "done";
      cur.item.savedAs = info?.savedAs ?? cur.item.name;
      cur.item.location = info?.location ?? "";
      this.#progress(cur.item, cur.item.size);
      cur.item.speed = 0; cur.item.eta = 0;
      this.#cur = null;
      this.#ctrl({ t: "file-done", fid: cur.item.id, ok: true });
      if (cur.batch.items.every((i) => i.status === "done")) cur.batch.status = "done";
      this.#emitNow();
    });
  }

  #failCurrent(cur, message) {
    if (cur.failed) return;
    cur.failed = true;
    cur.item.status = "failed";
    cur.item.error = message;
    cur.batch.status = "failed";
    cur.batch.reason = message;
    for (const i of cur.batch.items) if (i.status === "pending") i.status = "failed";
    Promise.resolve(cur.sink?.abort?.()).catch(() => {});
    if (this.#cur === cur) this.#cur = null;
    this.#ctrl({ t: "file-done", fid: cur.item.id, ok: false, error: message });
    this.#emitNow();
  }

  #abortReceive(b, status, message) {
    const cur = this.#cur;
    if (cur && cur.batch === b) {
      cur.failed = true;
      Promise.resolve(cur.sink?.abort?.()).catch(() => {});
      this.#cur = null;
    }
    b.status = status;
    b.reason = message;
    for (const i of b.items) if (["pending", "receiving"].includes(i.status)) { i.status = status; i.error = message; }
    this.#emitNow();
  }

  #fatal(message) {
    this.emit("fatal", message);
    this.#onClosed(message);
  }

  #onClosed(reason) {
    if (this.#closed) return;
    this.#closed = true;
    for (const b of this.#sendBatches.values()) {
      if (!["done", "failed", "cancelled", "declined"].includes(b.status)) {
        b.aborted = true;
        b.abortReason = reason;
        b.decision?.(false);
        b.status = "failed";
        b.reason = reason;
        for (const i of b.items) if (!["done"].includes(i.status)) { i.status = "failed"; i.error = reason; }
      }
    }
    for (const b of this.#recvBatches.values()) {
      if (["receiving", "pending", "accepting"].includes(b.status)) this.#abortReceive(b, "failed", reason);
    }
    for (const w of this.#fileDone.values()) w.resolve({ ok: false, error: reason });
    this.#fileDone.clear();
    this.#notify();
    this.#emitNow();
  }
}
