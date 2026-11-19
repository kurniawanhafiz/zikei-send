// Shared constants and pure helpers (no browser globals at import time, so they are unit-testable in Node).

export const LIMITS = {
  CHUNK_SIZE: 64 * 1024, // capped further by pc.sctp.maxMessageSize
  READ_BLOCK: 512 * 1024, // bytes read from disk per slice
  SEND_HIGH_WATER: 2 * 1024 * 1024, // pause sending above this bufferedAmount
  SEND_LOW_WATER: 512 * 1024, // bufferedamountlow threshold
  ACK_STEP: 256 * 1024, // receiver acknowledges every N written bytes
  WINDOW: 8 * 1024 * 1024, // max un-acknowledged bytes in flight (bounds receiver memory)
  MAX_FILES_PER_BATCH: 200,
  MAX_PENDING_BATCHES: 5,
  MAX_MEMORY_FILE: 512 * 1024 * 1024, // largest single file buffered in RAM when streaming-to-disk is unavailable
  MAX_TEXT: 100_000,
  MAX_NAME: 180,
  MAX_CONTROL_JSON: 256 * 1024, // reject absurdly large control frames
  CONNECT_TIMEOUT_MS: 25_000,
  PROTOCOL_VERSION: 1,
};

export const CODE_RE = /^[A-HJ-NP-Z2-9]{6}$/;
export const PIN_RE = /^\d{4,8}$/;

export function normalizeCode(value) {
  return String(value ?? "").toUpperCase().replace(/[^A-HJ-NP-Z2-9]/g, "").slice(0, 6);
}

export function makeInviteUrl(origin, code) {
  return `${origin}/#join=${code}`;
}

/** Extract a room code from a location hash ("#join=ABC234") - the code never reaches server logs. */
export function parseInviteHash(hash) {
  const m = /^#?join=([A-Za-z0-9]{6})$/.exec(String(hash ?? ""));
  if (!m) return "";
  const code = m[1].toUpperCase();
  return CODE_RE.test(code) ? code : "";
}

export function formatBytes(bytes = 0) {
  if (!Number.isFinite(bytes) || bytes < 0) return "—";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = bytes / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`;
}

export function formatSpeed(bps) {
  return bps > 0 ? `${formatBytes(bps)}/s` : "—";
}

export function formatEta(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) return "—";
  if (seconds < 60) return `${Math.ceil(seconds)}s`;
  const m = Math.floor(seconds / 60);
  if (m < 60) return `${m}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

const RESERVED = /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\..*)?$/i;

/** Make a remote-supplied file name safe to use as a local file name. */
export function sanitizeFilename(name, fallback = "file") {
  let s = String(name ?? "");
  s = s.normalize("NFC").replace(/[\u0000-\u001f\u007f<>:"/\\|?*‪-‮⁦-⁩]/g, "_");
  s = s.replace(/^[\s.]+|[\s.]+$/g, "");
  if (!s || RESERVED.test(s)) s = s ? `_${s}` : fallback;
  if (s.length > LIMITS.MAX_NAME) {
    const dot = s.lastIndexOf(".");
    const ext = dot > 0 && s.length - dot <= 16 ? s.slice(dot) : "";
    s = s.slice(0, LIMITS.MAX_NAME - ext.length) + ext;
  }
  return s;
}

/** "photo.jpg" -> "photo (2).jpg" until `exists(name)` is false. */
export async function uniqueName(name, exists) {
  if (!(await exists(name))) return name;
  const dot = name.lastIndexOf(".");
  const base = dot > 0 ? name.slice(0, dot) : name;
  const ext = dot > 0 ? name.slice(dot) : "";
  for (let n = 2; n < 1000; n++) {
    const candidate = `${base} (${n})${ext}`;
    if (!(await exists(candidate))) return candidate;
  }
  return `${base}-${Date.now()}${ext}`;
}

export function randomId(prefix = "") {
  const a = new Uint8Array(8);
  globalThis.crypto.getRandomValues(a);
  return prefix + Array.from(a, (b) => b.toString(16).padStart(2, "0")).join("");
}

const ADJ = ["Swift", "Calm", "Bright", "Brave", "Quiet", "Lucky", "Sunny", "Misty", "Bold", "Keen"];
const ANIMAL = ["Fox", "Otter", "Heron", "Lynx", "Panda", "Falcon", "Gecko", "Koala", "Orca", "Wren"];
export function randomDeviceName() {
  const a = new Uint8Array(2);
  globalThis.crypto.getRandomValues(a);
  return `${ADJ[a[0] % ADJ.length]} ${ANIMAL[a[1] % ANIMAL.length]}`;
}

/** Tiny event emitter. */
export class Emitter {
  #handlers = new Map();
  on(event, fn) {
    if (!this.#handlers.has(event)) this.#handlers.set(event, new Set());
    this.#handlers.get(event).add(fn);
    return () => this.#handlers.get(event)?.delete(fn);
  }
  emit(event, ...args) {
    for (const fn of [...(this.#handlers.get(event) ?? [])]) {
      try { fn(...args); } catch (err) { console.error(`[zikei] handler for "${event}" threw`, err); }
    }
  }
}
