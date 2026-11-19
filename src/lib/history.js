// Local-only transfer history (file names, sizes, direction, time). Never contains file contents and never leaves the browser.
const KEY = "zikei:history:v1";
const MAX = 50;

export function loadHistory() {
  try {
    const raw = globalThis.localStorage?.getItem(KEY);
    const list = raw ? JSON.parse(raw) : [];
    return Array.isArray(list) ? list.slice(0, MAX) : [];
  } catch { return []; }
}

export function addHistory(entries) {
  const next = [...entries, ...loadHistory()].slice(0, MAX);
  try { globalThis.localStorage?.setItem(KEY, JSON.stringify(next)); } catch { /* storage unavailable or full */ }
  return next;
}

export function clearHistory() {
  try { globalThis.localStorage?.removeItem(KEY); } catch { /* ignore */ }
  return [];
}
