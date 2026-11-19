// Transport-agnostic signaling core for Zikei Send.
//
// This module never touches files or WebRTC media: it only pairs two peers in a
// short-lived room and relays validated SDP / ICE messages between them.
// Room state lives in memory of ONE server instance (see README: the server is
// pinned to scale.max = 1 because rooms are not shared between instances).

import { createHmac, randomBytes, randomInt, timingSafeEqual } from "node:crypto";

export const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
export const CODE_RE = /^[A-HJ-NP-Z2-9]{6}$/;
export const PIN_RE = /^\d{4,8}$/;

export const DEFAULTS = {
  maxMessageBytes: 32 * 1024,
  maxSdpChars: 20_000,
  maxCandidateChars: 2_000,
  maxRooms: 300,
  idleTtlMs: 30 * 60 * 1000, // room dies after this long without any activity/ping
  maxAgeMs: 6 * 60 * 60 * 1000, // absolute lifetime of a room
  offlineGraceMs: 2 * 60 * 1000, // time a peer may stay disconnected before being dropped
  maxPinFailures: 5,
  pinLockMs: 5 * 60 * 1000,
  msgBurst: 120, // messages per socket per window (ICE can be bursty)
  msgWindowMs: 10_000,
  rate: {
    connect: { max: 30, windowMs: 60_000 },
    create: { max: 10, windowMs: 60_000 },
    join: { max: 20, windowMs: 60_000 },
    ice: { max: 60, windowMs: 60_000 },
  },
};

const SIGNAL_KINDS = new Set(["offer", "answer", "candidate"]);

export function normalizeCode(value) {
  return String(value ?? "").trim().toUpperCase();
}

function makeCode() {
  let out = "";
  for (let i = 0; i < 6; i++) out += CODE_CHARS[randomInt(CODE_CHARS.length)];
  return out;
}

function safeEqual(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  return timingSafeEqual(ba, bb);
}

function cleanLabel(value) {
  const s = String(value ?? "").replace(/[\u0000-\u001f\u007f<>]/g, "").trim().slice(0, 32);
  return s || "Device";
}

/** Validate and rebuild a signaling payload so nothing unexpected is relayed. */
export function sanitizeSignal(payload, cfg = DEFAULTS) {
  if (!payload || typeof payload !== "object") return null;
  const { kind } = payload;
  if (!SIGNAL_KINDS.has(kind)) return null;
  if (kind === "offer" || kind === "answer") {
    const d = payload.description;
    if (!d || d.type !== kind || typeof d.sdp !== "string" || d.sdp.length > cfg.maxSdpChars) return null;
    return { kind, description: { type: d.type, sdp: d.sdp } };
  }
  // candidate (null candidate object = end-of-candidates)
  if (payload.candidate === null) return { kind, candidate: null };
  const c = payload.candidate;
  if (!c || typeof c !== "object" || typeof c.candidate !== "string" || c.candidate.length > cfg.maxCandidateChars) return null;
  const out = { candidate: c.candidate };
  if (typeof c.sdpMid === "string" && c.sdpMid.length <= 64) out.sdpMid = c.sdpMid;
  else if (c.sdpMid === null) out.sdpMid = null;
  if (Number.isInteger(c.sdpMLineIndex) && c.sdpMLineIndex >= 0 && c.sdpMLineIndex < 64) out.sdpMLineIndex = c.sdpMLineIndex;
  return { kind, candidate: out };
}

/**
 * ICE server list handed to clients through GET /ice.
 * STUN is always included (public Google STUN by default - fine for development,
 * no delivery guarantee). TURN is only added when configured through env vars:
 *   TURN_URLS       comma separated, e.g. "turn:turn.example.com:3478,turns:turn.example.com:5349"
 *   TURN_SECRET     coturn `static-auth-secret` -> short-lived HMAC credentials (recommended)
 *   or TURN_USERNAME + TURN_CREDENTIAL (static credentials)
 *   STUN_URLS       optional comma separated override of the default STUN servers
 */
export function iceConfig(env = {}, nowMs = Date.now()) {
  const split = (v) => String(v ?? "").split(",").map((s) => s.trim()).filter(Boolean);
  const stun = split(env.STUN_URLS);
  const iceServers = [{ urls: stun.length ? stun : ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"] }];
  const turnUrls = split(env.TURN_URLS);
  let turn = false;
  let ttl = 3600;
  if (turnUrls.length) {
    if (env.TURN_SECRET) {
      const username = `${Math.floor(nowMs / 1000) + ttl}:zikei`;
      const credential = createHmac("sha1", env.TURN_SECRET).update(username).digest("base64");
      iceServers.push({ urls: turnUrls, username, credential });
      turn = true;
    } else if (env.TURN_USERNAME && env.TURN_CREDENTIAL) {
      iceServers.push({ urls: turnUrls, username: env.TURN_USERNAME, credential: env.TURN_CREDENTIAL });
      turn = true;
    }
  }
  return { iceServers, turn, ttl };
}

export function createSignaling(options = {}) {
  const cfg = { ...DEFAULTS, ...options, rate: { ...DEFAULTS.rate, ...(options.rate ?? {}) } };
  const now = options.now ?? Date.now;
  const rooms = new Map();
  const rateBuckets = new Map(); // `${kind}:${ip}` -> { count, resetAt }

  // ---- rate limiting -------------------------------------------------------
  function rateAllow(kind, ip) {
    const rule = cfg.rate[kind];
    const key = `${kind}:${ip}`;
    const t = now();
    let b = rateBuckets.get(key);
    if (!b || t >= b.resetAt) {
      b = { count: 0, resetAt: t + rule.windowMs };
      rateBuckets.set(key, b);
    }
    b.count += 1;
    return b.count <= rule.max;
  }

  // ---- helpers -------------------------------------------------------------
  function out(session, obj) {
    if (!session) return;
    try { session.transport.send(JSON.stringify(obj)); } catch { /* socket already gone */ }
  }
  const outPeer = (peer, obj) => peer?.session && out(peer.session, obj);

  function newPeer(role, label) {
    return {
      id: randomBytes(8).toString("hex"),
      token: randomBytes(16).toString("hex"),
      role,
      label: cleanLabel(label),
      state: role === "host" ? "active" : "pending",
      session: null,
      offlineSince: null,
    };
  }

  function attach(session, room, peer) {
    session.roomCode = room.code;
    session.peerId = peer.id;
    peer.session = session;
    peer.offlineSince = null;
  }

  function detachSession(session) {
    session.roomCode = null;
    session.peerId = null;
  }

  function lookup(session) {
    if (!session.roomCode) return {};
    const room = rooms.get(session.roomCode);
    if (!room) return {};
    const peer = room.host.id === session.peerId ? room.host : room.guest?.id === session.peerId ? room.guest : null;
    if (!peer || peer.session !== session) return {};
    return { room, peer, other: peer === room.host ? room.guest : room.host };
  }

  function destroyRoom(room, reason, notify = true) {
    rooms.delete(room.code);
    for (const p of [room.host, room.guest]) {
      if (!p?.session) continue;
      if (notify) out(p.session, { type: reason === "expired" ? "room-expired" : "peer-left", reason });
      detachSession(p.session);
      p.session = null;
    }
  }

  function peerState(room) {
    if (!room.guest) return "none";
    return room.guest.state; // pending | active
  }

  function fail(session, code, message) {
    out(session, { type: "error", code, message });
  }

  // ---- housekeeping --------------------------------------------------------
  function sweep() {
    const t = now();
    for (const [key, b] of rateBuckets) if (t >= b.resetAt) rateBuckets.delete(key);
    for (const room of [...rooms.values()]) {
      const expired = t - room.lastActivity > cfg.idleTtlMs || t - room.createdAt > cfg.maxAgeMs;
      if (expired) { destroyRoom(room, "expired"); continue; }
      // drop peers that stayed offline too long
      for (const p of [room.host, room.guest]) {
        if (p && !p.session && p.offlineSince !== null && t - p.offlineSince > cfg.offlineGraceMs) {
          if (p === room.host) { destroyRoom(room, "host_offline"); break; }
          room.guest = null;
          outPeer(room.host, { type: "peer-left", reason: "guest_offline" });
        }
      }
    }
  }

  // ---- message handlers ----------------------------------------------------
  function onCreate(s, msg) {
    if (s.roomCode) return fail(s, "already_in_room", "Already in a room.");
    if (!rateAllow("create", s.ip)) return fail(s, "rate_limited", "Too many rooms created. Wait a minute and try again.");
    sweep();
    if (rooms.size >= cfg.maxRooms) return fail(s, "capacity", "The server is busy. Try again later.");
    let pin = null;
    if (msg.pin !== undefined && msg.pin !== null && msg.pin !== "") {
      if (typeof msg.pin !== "string" || !PIN_RE.test(msg.pin)) return fail(s, "bad_message", "PIN must be 4-8 digits.");
      pin = msg.pin;
    }
    let code = makeCode();
    for (let i = 0; rooms.has(code) && i < 20; i++) code = makeCode();
    if (rooms.has(code)) return fail(s, "capacity", "Could not allocate a room. Try again.");
    const host = newPeer("host", msg.label);
    const t = now();
    const room = { code, pin, host, guest: null, createdAt: t, lastActivity: t, pinFailures: 0, lockedUntil: 0 };
    rooms.set(code, room);
    attach(s, room, host);
    out(s, { type: "room-created", room: code, peerId: host.id, token: host.token, pinProtected: !!pin, ttlMs: cfg.idleTtlMs });
  }

  function onJoin(s, msg) {
    if (s.roomCode) return fail(s, "already_in_room", "Already in a room.");
    if (!rateAllow("join", s.ip)) return fail(s, "rate_limited", "Too many attempts. Wait a minute and try again.");
    const code = normalizeCode(msg.room);
    if (!CODE_RE.test(code)) return fail(s, "bad_code", "Room codes have 6 characters (letters and digits).");
    sweep();
    const room = rooms.get(code);
    if (!room) return fail(s, "room_not_found", "Room not found or expired.");
    const t = now();
    if (room.lockedUntil > t) return fail(s, "room_locked", "Too many wrong PINs. This room is locked for a few minutes.");
    if (room.pin) {
      if (msg.pin === undefined || msg.pin === null || msg.pin === "") return fail(s, "pin_required", "This room is protected by a PIN.");
      if (typeof msg.pin !== "string" || !safeEqual(msg.pin, room.pin)) {
        room.pinFailures += 1;
        if (room.pinFailures >= cfg.maxPinFailures) {
          room.pinFailures = 0;
          room.lockedUntil = t + cfg.pinLockMs;
          return fail(s, "room_locked", "Too many wrong PINs. This room is locked for a few minutes.");
        }
        return fail(s, "pin_invalid", "Wrong PIN.");
      }
    }
    if (room.guest) return fail(s, "room_full", "This room already has a second device.");
    const guest = newPeer("guest", msg.label);
    room.guest = guest;
    room.lastActivity = t;
    attach(s, room, guest);
    out(s, { type: "join-pending", room: code, peerId: guest.id, token: guest.token });
    outPeer(room.host, { type: "join-request", label: guest.label });
  }

  function onApprove(s) {
    const { room, peer } = lookup(s);
    if (!room || peer !== room.host) return fail(s, "forbidden", "Only the room creator can approve.");
    if (!room.guest || room.guest.state !== "pending") return fail(s, "no_pending", "No device is waiting for approval.");
    room.guest.state = "active";
    room.lastActivity = now();
    outPeer(room.guest, { type: "joined", room: room.code, hostLabel: room.host.label });
    out(s, { type: "peer-joined", label: room.guest.label, peerOnline: !!room.guest.session });
  }

  function onReject(s) {
    const { room, peer } = lookup(s);
    if (!room || peer !== room.host) return fail(s, "forbidden", "Only the room creator can reject.");
    if (!room.guest || room.guest.state !== "pending") return fail(s, "no_pending", "No device is waiting for approval.");
    const g = room.guest;
    room.guest = null;
    if (g.session) { out(g.session, { type: "join-rejected" }); detachSession(g.session); g.session = null; }
  }

  function onSignal(s, msg) {
    const { room, peer, other } = lookup(s);
    if (!room) return fail(s, "not_in_room", "Not in a room.");
    if (!other || other.state !== "active" || peer.state !== "active") return fail(s, "not_paired", "The other device has not been approved yet.");
    const payload = sanitizeSignal(msg.payload, cfg);
    if (!payload) return fail(s, "bad_message", "Invalid signaling payload.");
    room.lastActivity = now();
    if (!other.session) return fail(s, "peer_offline", "The other device is offline.");
    out(other.session, { type: "signal", payload });
  }

  function onRejoin(s, msg) {
    if (s.roomCode) return fail(s, "already_in_room", "Already in a room.");
    if (!rateAllow("join", s.ip)) return fail(s, "rate_limited", "Too many attempts. Wait a minute and try again.");
    const room = rooms.get(normalizeCode(msg.room));
    if (!room) return fail(s, "room_not_found", "Room not found or expired.");
    const peer = [room.host, room.guest].find((p) => p && p.id === msg.peerId && typeof msg.token === "string" && safeEqual(msg.token, p.token));
    if (!peer) return fail(s, "room_not_found", "Room not found or expired.");
    if (peer.session) { // a stale socket for the same peer: replace it
      const old = peer.session;
      detachSession(old);
      peer.session = null;
      try { old.transport.close(4000, "replaced"); } catch { /* ignore */ }
    }
    attach(s, room, peer);
    room.lastActivity = now();
    const other = peer === room.host ? room.guest : room.host;
    out(s, {
      type: "resumed",
      role: peer.role,
      peerState: peer === room.host ? peerState(room) : peer.state,
      peerOnline: !!other?.session,
      pinProtected: !!room.pin,
    });
    outPeer(other, { type: "peer-online" });
    if (peer === room.host && room.guest?.state === "pending") out(s, { type: "join-request", label: room.guest.label });
  }

  function onLeave(s) {
    const { room, peer, other } = lookup(s);
    if (!room) return;
    if (peer === room.host) {
      destroyRoom(room, "host_left");
    } else {
      room.guest = null;
      detachSession(s);
      peer.session = null;
      outPeer(other, { type: "peer-left", reason: "guest_left" });
    }
  }

  function onClose(s) {
    const { room, peer, other } = lookup(s);
    detachSession(s);
    if (!room) return;
    peer.session = null;
    peer.offlineSince = now();
    if (peer.role === "guest" && peer.state === "pending") {
      room.guest = null; // never approved: forget it
      outPeer(other, { type: "peer-left", reason: "join_cancelled" });
      return;
    }
    outPeer(other, { type: "peer-offline" });
  }

  function onMessage(s, raw) {
    if (s.closed) return;
    const t = now();
    s.stamps = s.stamps.filter((x) => t - x < cfg.msgWindowMs);
    s.stamps.push(t);
    if (s.stamps.length > cfg.msgBurst) {
      fail(s, "rate_limited", "Too many messages.");
      try { s.transport.close(1008, "rate limited"); } catch { /* ignore */ }
      return;
    }
    if (typeof raw !== "string") return fail(s, "bad_message", "Text frames only.");
    if (raw.length > cfg.maxMessageBytes) return fail(s, "too_large", "Message too large.");
    let msg;
    try { msg = JSON.parse(raw); } catch { return fail(s, "bad_message", "Invalid JSON."); }
    if (!msg || typeof msg !== "object" || typeof msg.type !== "string") return fail(s, "bad_message", "Invalid message.");

    switch (msg.type) {
      case "create": return onCreate(s, msg);
      case "join": return onJoin(s, msg);
      case "approve": return onApprove(s);
      case "reject": return onReject(s);
      case "signal": return onSignal(s, msg);
      case "rejoin": return onRejoin(s, msg);
      case "leave": return onLeave(s);
      case "ping": {
        const { room } = lookup(s);
        if (room) room.lastActivity = now();
        return out(s, { type: "pong" });
      }
      default: return fail(s, "bad_message", "Unknown message type.");
    }
  }

  return {
    rooms,
    sweep,
    /** Call before accepting a socket. */
    allowConnect(ip) { return rateAllow("connect", ip); },
    allowIce(ip) { return rateAllow("ice", ip); },
    /** transport: { send(string), close(code, reason) }. Returns handlers to wire to the socket events. */
    connect(transport, { ip = "unknown" } = {}) {
      const session = { transport, ip, roomCode: null, peerId: null, stamps: [], closed: false };
      return {
        onMessage: (raw) => onMessage(session, raw),
        onClose: () => { if (session.closed) return; session.closed = true; onClose(session); },
      };
    },
  };
}
