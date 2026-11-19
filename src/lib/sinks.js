// Where received bytes go.
//  - FileSystem Access API (Chromium desktop): streamed straight to disk -> constant memory, any size.
//  - Fallback (Firefox, Safari, mobile): buffered in RAM per file, then handed to the browser as a download.
//    Limited to LIMITS.MAX_MEMORY_FILE per file; larger files are refused up front.

import { LIMITS, sanitizeFilename, uniqueName } from "./protocol.js";

class MemorySink {
  #parts = [];
  #size = 0;
  constructor(info, env) { this.info = info; this.env = env; }
  async write(u8) { this.#parts.push(u8); this.#size += u8.byteLength; }
  async close() {
    const { document, URL: U, setTimeout: st } = this.env;
    const blob = new Blob(this.#parts, { type: "application/octet-stream" });
    this.#parts = [];
    const url = U.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = this.info.name;
    a.rel = "noopener";
    document.body.appendChild(a);
    a.click();
    a.remove();
    st(() => U.revokeObjectURL(url), 60_000);
    return { savedAs: this.info.name, location: "Downloads" };
  }
  async abort() { this.#parts = []; }
}

class FsSink {
  constructor(info, handle, writable, dir) { this.info = info; this.handle = handle; this.writable = writable; this.dir = dir; }
  async write(u8) { await this.writable.write(u8); }
  async close() { await this.writable.close(); return { savedAs: this.handle.name, location: this.dir ? "chosen folder" : "chosen location" }; }
  async abort() {
    try { await this.writable.abort(); } catch { /* ignore */ }
    if (this.dir) { try { await this.dir.removeEntry(this.handle.name); } catch { /* ignore */ } }
  }
}

async function dirHasEntry(dir, name) {
  try { await dir.getFileHandle(name); return true; } catch (e) {
    if (e?.name === "NotFoundError") return false;
    if (e?.name === "TypeMismatchError") return true;
    throw e;
  }
}

/**
 * Must be called from a user gesture (the "Accept" click) because it may open a picker.
 * Returns { kind, open(fileInfo) -> sink }. Throws {name:"AbortError"} if the user cancels the picker.
 */
export async function prepareSinks(batch, env = globalThis.window ?? globalThis) {
  const files = batch.files;
  if (files.length === 1 && typeof env.showSaveFilePicker === "function") {
    const f = files[0];
    const handle = await env.showSaveFilePicker({ suggestedName: sanitizeFilename(f.name) });
    return {
      kind: "disk",
      async open(info) { return new FsSink(info, handle, await handle.createWritable(), null); },
    };
  }
  if (files.length > 1 && typeof env.showDirectoryPicker === "function") {
    const dir = await env.showDirectoryPicker({ mode: "readwrite" });
    return {
      kind: "disk",
      async open(info) {
        const name = await uniqueName(sanitizeFilename(info.name), (n) => dirHasEntry(dir, n));
        const handle = await dir.getFileHandle(name, { create: true });
        return new FsSink(info, handle, await handle.createWritable(), dir);
      },
    };
  }
  return {
    kind: "memory",
    async open(info) { return new MemorySink({ ...info, name: sanitizeFilename(info.name) }, env); },
  };
}

/** Which files of a batch cannot be received by this browser? (only relevant for the in-memory fallback) */
export function blockedFiles(batch, env = globalThis.window ?? globalThis) {
  const n = batch.files.length;
  const streams = (n === 1 && typeof env.showSaveFilePicker === "function") || (n > 1 && typeof env.showDirectoryPicker === "function");
  return streams ? [] : batch.files.filter((f) => f.size > LIMITS.MAX_MEMORY_FILE);
}

export function canStreamToDisk(env = globalThis.window ?? globalThis) {
  return typeof env.showSaveFilePicker === "function" && typeof env.showDirectoryPicker === "function";
}
