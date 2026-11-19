import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { QRCodeSVG } from "qrcode.react";
import { ZikeiSession } from "./lib/session.js";
import { addHistory, clearHistory, loadHistory } from "./lib/history.js";
import { PIN_RE, formatBytes, formatEta, formatSpeed, makeInviteUrl, normalizeCode, parseInviteHash } from "./lib/protocol.js";

function signalUrl() {
  const env = import.meta.env.VITE_SIGNAL_URL;
  if (env) return env;
  return `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`;
}
function iceUrl() {
  const env = import.meta.env.VITE_SIGNAL_URL;
  if (env) { try { const u = new URL(env); u.protocol = u.protocol === "wss:" ? "https:" : "http:"; u.pathname = "/ice"; return u.toString(); } catch { /* fall through */ } }
  return "/ice";
}

function useSession() {
  const [session] = useState(() => new ZikeiSession({ signalUrl: signalUrl(), iceUrl: iceUrl() }));
  useEffect(() => () => session.destroy(), [session]);
  const snap = useSyncExternalStore(session.subscribe, session.getSnapshot);
  return [session, snap];
}

function useTheme() {
  const [theme, setTheme] = useState(() => { try { return localStorage.getItem("zikei:theme") || "auto"; } catch { return "auto"; } });
  useEffect(() => {
    const root = document.documentElement;
    if (theme === "auto") root.removeAttribute("data-theme"); else root.setAttribute("data-theme", theme);
    try { localStorage.setItem("zikei:theme", theme); } catch { /* ignore */ }
  }, [theme]);
  const next = { auto: "light", light: "dark", dark: "auto" }[theme];
  return [theme, () => setTheme(next)];
}

const TERMINAL = new Set(["done", "failed", "cancelled", "declined"]);

export default function App() {
  const [session, snap] = useSession();
  const [theme, cycleTheme] = useTheme();
  const [history, setHistory] = useState(loadHistory);
  const recorded = useRef(new Set());
  const active = snap.transfer.batches.some((b) => ["sending", "receiving", "awaiting", "accepting"].includes(b.status));

  // Record finished files in the local history (once each).
  useEffect(() => {
    const fresh = [];
    for (const b of snap.transfer.batches) {
      for (const i of b.items) {
        if (TERMINAL.has(i.status) && !recorded.current.has(`${b.id}:${i.id}`)) {
          recorded.current.add(`${b.id}:${i.id}`);
          fresh.push({ id: `${b.id}:${i.id}`, dir: b.direction, name: i.savedAs || i.name, size: i.size, status: i.status, peer: snap.transfer.peer?.name || "", at: Date.now() });
        }
      }
    }
    if (fresh.length) setHistory(addHistory(fresh.reverse()));
  }, [snap.transfer]);

  // Keep the screen awake and warn before closing the tab while transferring.
  useEffect(() => {
    if (!active) return undefined;
    let lock = null;
    navigator.wakeLock?.request("screen").then((l) => { lock = l; }).catch(() => {});
    const warn = (e) => { e.preventDefault(); e.returnValue = ""; };
    window.addEventListener("beforeunload", warn);
    return () => { window.removeEventListener("beforeunload", warn); lock?.release?.().catch(() => {}); };
  }, [active]);

  const inRoom = !["idle", "error", "ended"].includes(snap.phase) || (snap.phase === "connecting");

  return (
    <div className="shell">
      <Header theme={theme} onTheme={cycleTheme} />
      <main id="main">
        {snap.phase === "idle" && <Home session={session} history={history} onClearHistory={() => setHistory(clearHistory())} />}
        {(snap.phase === "error" || snap.phase === "ended") && <Ended snap={snap} onRestart={() => session.leave()} />}
        {inRoom && snap.phase !== "idle" && <Room session={session} snap={snap} />}
      </main>
      <footer className="footer">
        <span>Files go straight from device to device over an encrypted WebRTC connection — they are never uploaded to a server.</span>
        <span>Direct connections can fail on strict networks unless a TURN relay is configured.</span>
      </footer>
    </div>
  );
}

function Header({ theme, onTheme }) {
  const label = { auto: "Theme: automatic", light: "Theme: light", dark: "Theme: dark" }[theme];
  return (
    <header className="topbar">
      <a className="brand" href="/" aria-label="Zikei Send home"><span className="brand-mark" aria-hidden="true">Z</span><span>zikei<span className="brand-light">send</span></span></a>
      <div className="topbar-right">
        <span className="top-badge"><span className="pulse" aria-hidden="true" /> Peer-to-peer</span>
        <button className="icon-button" onClick={onTheme} aria-label={`${label}. Click to change.`} title={label}>{theme === "dark" ? "☾" : theme === "light" ? "☀" : "◐"}</button>
      </div>
    </header>
  );
}

function Home({ session, history, onClearHistory }) {
  const invited = useMemo(() => parseInviteHash(location.hash), []);
  const [code, setCode] = useState(invited);
  const [joinPin, setJoinPin] = useState("");
  const [createPin, setCreatePin] = useState("");
  const [usePin, setUsePin] = useState(false);
  const [err, setErr] = useState("");

  const create = () => {
    if (usePin && !PIN_RE.test(createPin)) return setErr("Choose a PIN of 4 to 8 digits, or turn the PIN off.");
    setErr("");
    session.createRoom({ pin: usePin ? createPin : "" });
  };
  const join = (e) => {
    e.preventDefault();
    if (code.length !== 6) return setErr("Enter the 6-character room code.");
    if (joinPin && !PIN_RE.test(joinPin)) return setErr("PINs are 4 to 8 digits.");
    setErr("");
    history.replaceState?.(null, "", location.pathname);
    session.joinRoom(code, joinPin);
  };

  return (
    <>
      <section className="hero">
        <div className="eyebrow">Simple · Private · Direct</div>
        <h1>Send files.<br /><span>No cloud in between.</span></h1>
        <p className="subtitle">Pair two browsers with a code or QR, then send files and text straight between them. No account, no upload.</p>
      </section>
      <section className="grid-2" aria-label="Start">
        <div className="card">
          <h2>Send from this device</h2>
          <p className="muted">Create a room, then share the code or QR with the other device.</p>
          <label className="check"><input type="checkbox" checked={usePin} onChange={(e) => setUsePin(e.target.checked)} /> Protect the room with a PIN</label>
          {usePin && (
            <div className="field">
              <label htmlFor="create-pin">PIN (4–8 digits)</label>
              <input id="create-pin" inputMode="numeric" autoComplete="off" maxLength={8} value={createPin} onChange={(e) => setCreatePin(e.target.value.replace(/\D/g, ""))} />
            </div>
          )}
          <button className="button primary" onClick={create}>Create a room <span aria-hidden="true">→</span></button>
        </div>
        <form className="card" onSubmit={join}>
          <h2>Receive on this device</h2>
          <p className="muted">{invited ? `You were invited to room ${invited}. Press Join to ask for access.` : "Enter the code shown on the sending device."}</p>
          <div className="field">
            <label htmlFor="room-code">Room code</label>
            <input id="room-code" className="code-input" autoCapitalize="characters" autoComplete="off" spellCheck={false} maxLength={6} placeholder="ABC234" value={code} onChange={(e) => setCode(normalizeCode(e.target.value))} />
          </div>
          <div className="field">
            <label htmlFor="join-pin">PIN <span className="muted">(only if the room has one)</span></label>
            <input id="join-pin" inputMode="numeric" autoComplete="off" maxLength={8} value={joinPin} onChange={(e) => setJoinPin(e.target.value.replace(/\D/g, ""))} />
          </div>
          <button className="button secondary" type="submit">Join room</button>
        </form>
      </section>
      {err && <p className="error-box" role="alert">{err}</p>}
      <History history={history} onClear={onClearHistory} />
    </>
  );
}

function History({ history, onClear }) {
  if (!history.length) return null;
  return (
    <section className="card history" aria-labelledby="hist-h">
      <div className="row-between">
        <h2 id="hist-h">Recent transfers <span className="muted">(stored only in this browser)</span></h2>
        <button className="text-button" onClick={onClear}>Clear</button>
      </div>
      <ul className="plain-list">
        {history.slice(0, 8).map((h) => (
          <li key={h.id} className="row-between">
            <span className="ellipsis">{h.dir === "send" ? "↑ Sent" : "↓ Received"} · {h.name}</span>
            <span className="muted nowrap">{formatBytes(h.size)} · {h.status === "done" ? "completed" : h.status}</span>
          </li>
        ))}
      </ul>
    </section>
  );
}

function Ended({ snap, onRestart }) {
  const failed = snap.phase === "error";
  return (
    <section className="card center" role={failed ? "alert" : "status"}>
      <h2>{failed ? "Could not connect" : "Disconnected"}</h2>
      <p className={failed ? "error-text" : "muted"}>{failed ? snap.error : snap.notice || "The session ended."}</p>
      {failed && snap.errorCode === "rtc_failed" && (
        <ul className="hint-list">
          <li>Try both devices on the same Wi-Fi, or switch one to a different network.</li>
          <li>Turn off VPNs and ad-blockers that block WebRTC.</li>
          <li>The site owner can enable a TURN relay (see the README).</li>
        </ul>
      )}
      <button className="button primary" onClick={onRestart}>Start over</button>
    </section>
  );
}

const PHASE_TEXT = {
  connecting: "Contacting the signaling server…",
  "waiting-peer": "Waiting for another device to join…",
  "waiting-approval": "Waiting for the room creator to approve you…",
  negotiating: "Establishing a direct connection…",
  connected: "Connected",
};

function StatusBar({ snap }) {
  const connected = snap.phase === "connected";
  const route = snap.route === "relay" ? "via relay (TURN)" : snap.route === "direct" ? "direct" : "";
  return (
    <div className="status" role="status" aria-live="polite">
      <span className={`dot ${connected ? "ok" : "wait"}`} aria-hidden="true">{connected ? "✓" : "…"}</span>
      <span>{PHASE_TEXT[snap.phase] ?? snap.phase}{connected && snap.transfer.peer ? ` · ${snap.transfer.peer.name}` : ""}{connected && route ? ` · ${route}` : ""}</span>
      {snap.signaling === "reconnecting" && <span className="tag">signaling reconnecting…</span>}
    </div>
  );
}

function Room({ session, snap }) {
  const connected = snap.phase === "connected";
  const isHost = snap.role === "host";
  return (
    <section className="room" aria-label="Room">
      <div className="card">
        <div className="row-between">
          <div>
            <div className="label">Room</div>
            <div className="room-code" aria-label={`Room code ${snap.room.split("").join(" ")}`}>{snap.room || "······"}</div>
          </div>
          <button className="text-button" onClick={() => session.leave()}>Leave room</button>
        </div>
        <StatusBar snap={snap} />
        {snap.notice && <p className="notice" role="status">{snap.notice}</p>}
        {snap.pendingGuest && isHost && (
          <div className="join-request" role="alertdialog" aria-labelledby="jr-t">
            <p id="jr-t"><b>{snap.pendingGuest}</b> wants to join this room. Only accept a device you recognize.</p>
            <div className="row-gap">
              <button className="button primary" onClick={() => session.approve()}>Allow</button>
              <button className="button secondary" onClick={() => session.reject()}>Decline</button>
            </div>
          </div>
        )}
        {isHost && !connected && snap.room && <Invite room={snap.room} pin={snap.pinProtected} />}
      </div>
      {connected && <Transfer session={session} snap={snap} />}
    </section>
  );
}

function Invite({ room, pin }) {
  const url = makeInviteUrl(location.origin, room);
  const [copied, setCopied] = useState("");
  const copy = async (text, what) => {
    try { await navigator.clipboard.writeText(text); setCopied(what); setTimeout(() => setCopied(""), 2000); } catch { setCopied("failed"); }
  };
  return (
    <div className="invite">
      <div className="qr" aria-hidden="true"><QRCodeSVG value={url} size={156} bgColor="#ffffff" fgColor="#111827" level="M" marginSize={2} /></div>
      <div>
        <h2>Invite the other device</h2>
        <p className="muted">Scan the QR code or type the code on the other device.{pin ? " It will also need the PIN you chose." : ""} You approve every device before it connects. The link only works while this page stays open.</p>
        <div className="row-gap">
          <button className="button secondary" onClick={() => copy(url, "link")}>Copy invite link</button>
          <button className="button secondary" onClick={() => copy(room, "code")}>Copy code</button>
        </div>
        <p className="muted small" role="status">{copied === "failed" ? "Copy failed — select the text manually." : copied ? `Copied ${copied}.` : " "}</p>
      </div>
    </div>
  );
}

function Transfer({ session, snap }) {
  const [dragging, setDragging] = useState(false);
  const input = useRef(null);
  const [text, setText] = useState("");
  const [err, setErr] = useState("");
  const add = (list) => {
    setErr("");
    try { if (list?.length) session.sendFiles(list); } catch (e) { setErr(e.message); }
  };

  useEffect(() => {
    const onPaste = (e) => { const f = e.clipboardData?.files; if (f?.length && !/input|textarea/i.test(e.target.tagName)) add(f); };
    window.addEventListener("paste", onPaste);
    return () => window.removeEventListener("paste", onPaste);
  });

  const sendText = (e) => {
    e.preventDefault();
    try { if (session.sendText(text)) setText(""); } catch (ex) { setErr(ex.message); }
  };
  const { batches, texts, peer } = snap.transfer;

  return (
    <>
      <div className="card">
        <div
          className={`dropzone${dragging ? " dragging" : ""}`}
          role="button" tabIndex={0} aria-label="Choose files to send, or drop them here"
          onClick={() => input.current?.click()}
          onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); input.current?.click(); } }}
          onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
          onDragLeave={() => setDragging(false)}
          onDrop={(e) => { e.preventDefault(); setDragging(false); add(e.dataTransfer.files); }}
        >
          <div className="upload-icon" aria-hidden="true">↑</div>
          <h2>Drop files to send{peer ? ` to ${peer.name}` : ""}</h2>
          <p className="muted">or click to choose · you can also paste files with Ctrl/⌘+V</p>
          <input ref={input} type="file" multiple hidden onChange={(e) => { add(e.target.files); e.target.value = ""; }} />
        </div>
        {err && <p className="error-box" role="alert">{err}</p>}
        {peer && !peer.stream && <p className="muted small">{peer.name}'s browser buffers files in memory (files over {formatBytes(512 * 1024 * 1024)} are refused). Chrome or Edge on desktop can receive any size by saving directly to disk.</p>}
      </div>

      {batches.length > 0 && (
        <div className="card" aria-label="Transfers">
          <h2>Transfers</h2>
          <div className="stack">{[...batches].reverse().map((b) => <Batch key={b.id} batch={b} session={session} peerName={peer?.name} />)}</div>
        </div>
      )}

      <form className="card" onSubmit={sendText}>
        <h2>Share text</h2>
        <label htmlFor="text" className="sr-only">Text to send</label>
        <textarea id="text" rows={3} maxLength={100000} placeholder="Paste a link or a note…" value={text} onChange={(e) => setText(e.target.value)} />
        <div className="row-gap"><button className="button secondary" type="submit" disabled={!text.trim()}>Send text</button></div>
        {texts.length > 0 && (
          <ul className="plain-list texts">
            {[...texts].reverse().map((t) => (
              <li key={t.id}>
                <div className="row-between"><span className="muted small">{t.from === "me" ? "You" : peer?.name || "Other device"}</span>
                  <button type="button" className="text-button" onClick={() => navigator.clipboard?.writeText(t.text)}>Copy</button></div>
                <pre className="text-body">{t.text}</pre>
              </li>
            ))}
          </ul>
        )}
      </form>
    </>
  );
}

const STATUS_LABEL = {
  queued: "Queued", awaiting: "Waiting for the other device to accept…", pending: "Waiting for your decision", accepting: "Choosing where to save…",
  sending: "Sending", receiving: "Receiving", done: "Completed", failed: "Failed", cancelled: "Cancelled", declined: "Declined",
};

function Batch({ batch, session, peerName }) {
  const send = batch.direction === "send";
  const live = ["queued", "awaiting", "sending", "receiving"].includes(batch.status);
  const pending = batch.status === "pending" || batch.status === "accepting";
  const [note, setNote] = useState("");
  const accept = async () => {
    const r = await session.acceptBatch(batch.id);
    if (r && !r.ok && !r.cancelled && r.error && r.error !== "blocked") setNote(r.error);
  };
  return (
    <div className="batch">
      <div className="row-between">
        <div>
          <b>{send ? "↑ Sending" : "↓ Incoming"} · {batch.items.length} file{batch.items.length === 1 ? "" : "s"} · {formatBytes(batch.total)}</b>
          <div className="muted small">{STATUS_LABEL[batch.status] ?? batch.status}{batch.reason && batch.reason !== "cancelled" && ["failed", "declined", "cancelled"].includes(batch.status) ? ` — ${batch.reason}` : ""}</div>
        </div>
        {live && <button className="text-button" onClick={() => session.cancelBatch(batch.id)}>Cancel</button>}
      </div>
      {pending && !send && (
        <div className="join-request">
          <p>{peerName || "The other device"} wants to send you these files.{batch.items.length > 1 ? " You will pick a folder to save them in (or they download one by one)." : ""}</p>
          {batch.blocked.length > 0 && <p className="error-text">Too large for this browser to receive: {batch.blocked.join(", ")}. Use Chrome or Edge on desktop for big files.</p>}
          <div className="row-gap">
            <button className="button primary" disabled={batch.status === "accepting" || batch.blocked.length > 0} onClick={accept}>Accept &amp; save</button>
            <button className="button secondary" onClick={() => session.declineBatch(batch.id)}>Decline</button>
          </div>
          {note && <p className="error-text" role="alert">{note}</p>}
        </div>
      )}
      <ul className="plain-list">
        {batch.items.map((i) => <FileRow key={i.id} item={i} />)}
      </ul>
    </div>
  );
}

function FileRow({ item }) {
  const pct = item.size ? Math.min(100, Math.floor((item.loaded / item.size) * 100)) : item.status === "done" ? 100 : 0;
  const showBar = ["sending", "receiving", "done"].includes(item.status);
  return (
    <li className="file-row">
      <div className="row-between">
        <span className="ellipsis" title={item.name}>{item.savedAs && item.savedAs !== item.name ? `${item.name} → ${item.savedAs}` : item.name}</span>
        <span className="muted nowrap small">{formatBytes(item.size)}</span>
      </div>
      {showBar && (
        <>
          <div className="bar" role="progressbar" aria-label={`Progress for ${item.name}`} aria-valuemin={0} aria-valuemax={100} aria-valuenow={pct}><div style={{ width: `${pct}%` }} /></div>
          <div className="muted small row-between">
            <span>{item.status === "done" ? "✓ Completed" : `${pct}%`}{item.status === "done" && item.location ? ` · saved to ${item.location}` : ""}</span>
            {["sending", "receiving"].includes(item.status) && <span>{formatSpeed(item.speed)} · {formatEta(item.eta)} left</span>}
          </div>
        </>
      )}
      {["failed", "cancelled", "declined"].includes(item.status) && <div className="error-text small">{item.status === "failed" ? "✕ Failed" : item.status === "cancelled" ? "Cancelled" : "Declined"}{item.error && item.error !== "cancelled" ? ` — ${item.error}` : ""}</div>}
    </li>
  );
}
