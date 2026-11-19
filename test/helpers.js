// Test doubles: a connected pair of fake RTCDataChannels with bufferedAmount semantics.
export class FakeChannel extends EventTarget {
  constructor(name) {
    super();
    this.name = name;
    this.readyState = "open";
    this.binaryType = "arraybuffer";
    this.bufferedAmount = 0;
    this.bufferedAmountLowThreshold = 0;
    this.peer = null;
    this.sent = [];
    this.maxBuffered = 0;
    this.latency = 0;
  }
  send(data) {
    if (this.readyState !== "open") throw new Error("InvalidStateError");
    const size = typeof data === "string" ? data.length : data.byteLength;
    const payload = typeof data === "string" ? data : data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
    this.bufferedAmount += size;
    this.maxBuffered = Math.max(this.maxBuffered, this.bufferedAmount);
    setTimeout(() => {
      if (this.readyState !== "open" && !this.peer) return;
      this.bufferedAmount -= size;
      if (this.bufferedAmount <= this.bufferedAmountLowThreshold) this.dispatchEvent(new Event("bufferedamountlow"));
      const p = this.peer;
      if (p && p.readyState === "open") p.dispatchEvent(Object.assign(new Event("message"), { data: payload }));
    }, this.latency);
  }
  close() {
    if (this.readyState === "closed") return;
    this.readyState = "closed";
    this.dispatchEvent(new Event("close"));
    if (this.peer && this.peer.readyState !== "closed") setTimeout(() => this.peer.close(), 0);
  }
}
export function channelPair() {
  const a = new FakeChannel("a"), b = new FakeChannel("b");
  a.peer = b; b.peer = a;
  return [a, b];
}
export class FakeFile {
  constructor(name, bytes, type = "application/octet-stream") { this.name = name; this.bytes = bytes; this.size = bytes.length; this.type = type; }
  slice(a, b) { const part = this.bytes.slice(a, b); return { arrayBuffer: async () => part.buffer.slice(part.byteOffset, part.byteOffset + part.byteLength) }; }
}
export function memorySinks(store, { failWriteAt } = {}) {
  return async () => ({
    kind: "memory",
    async open(info) {
      const parts = [];
      let n = 0;
      return {
        async write(u8) { n += u8.byteLength; if (failWriteAt && n >= failWriteAt) throw new Error("disk full"); parts.push(u8.slice()); },
        async close() { store.set(info.name, Buffer.concat(parts)); return { savedAs: info.name, location: "mem" }; },
        async abort() { store.set(info.name + ":aborted", true); },
      };
    },
  });
}
