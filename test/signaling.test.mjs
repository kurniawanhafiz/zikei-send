import test from "node:test";
import assert from "node:assert/strict";
import { createSignaling, iceConfig, sanitizeSignal, CODE_RE } from "../netlify/server/signaling.mjs";

function mk(opts) {
  let t = 1_000_000;
  const core = createSignaling({ now: () => t, ...opts });
  const advance = (ms) => { t += ms; };
  function client(ip = "1.1.1.1") {
    const inbox = [];
    let closed = null;
    const conn = core.connect({ send: (d) => inbox.push(JSON.parse(d)), close: (c) => { closed = c; } }, { ip });
    return {
      inbox, conn,
      get closed() { return closed; },
      say: (o) => conn.onMessage(typeof o === "string" ? o : JSON.stringify(o)),
      last: (type) => [...inbox].reverse().find((m) => m.type === type),
      drop: () => conn.onClose(),
    };
  }
  return { core, client, advance };
}
const SDP = { kind: "offer", description: { type: "offer", sdp: "v=0\r\n" } };

function pair(env = mk()) {
  const host = env.client(), guest = env.client("2.2.2.2");
  host.say({ type: "create", label: "Host" });
  const room = host.last("room-created");
  guest.say({ type: "join", room: room.room, label: "Guest" });
  return { ...env, host, guest, room };
}

test("create returns a valid code and secrets", () => {
  const { client } = mk();
  const h = client(); h.say({ type: "create" });
  const m = h.last("room-created");
  assert.match(m.room, CODE_RE);
  assert.ok(m.token.length >= 32 && m.peerId);
});

test("join needs host approval, then both sides are paired", () => {
  const { host, guest } = pair();
  assert.equal(host.last("join-request").label, "Guest");
  assert.ok(guest.last("join-pending"));
  assert.equal(guest.last("joined"), undefined);
  // signaling is refused until approved
  guest.say({ type: "signal", payload: SDP });
  assert.equal(guest.last("error").code, "not_paired");
  host.say({ type: "approve" });
  assert.ok(guest.last("joined"));
  assert.ok(host.last("peer-joined"));
  host.say({ type: "signal", payload: SDP });
  assert.deepEqual(guest.last("signal").payload, SDP);
});

test("only the host can approve / reject", () => {
  const { host, guest } = pair();
  guest.say({ type: "approve" });
  assert.equal(guest.last("error").code, "forbidden");
  host.say({ type: "reject" });
  assert.ok(guest.last("join-rejected"));
});

test("third device gets room_full; unknown room gets room_not_found; bad code rejected", () => {
  const env = pair();
  const third = env.client("3.3.3.3");
  third.say({ type: "join", room: env.room.room });
  assert.equal(third.last("error").code, "room_full");
  third.say({ type: "join", room: "ZZZZZZ" });
  assert.equal(third.last("error").code, "room_not_found");
  third.say({ type: "join", room: "abc" });
  assert.equal(third.last("error").code, "bad_code");
});

test("PIN: required, wrong, correct, lockout after repeated failures", () => {
  const env = mk();
  const host = env.client(); host.say({ type: "create", pin: "1234" });
  const code = host.last("room-created").room;
  const g = env.client("9.9.9.9");
  g.say({ type: "join", room: code });
  assert.equal(g.last("error").code, "pin_required");
  for (let i = 0; i < 4; i++) { g.say({ type: "join", room: code, pin: "0000" }); assert.equal(g.last("error").code, "pin_invalid"); }
  g.say({ type: "join", room: code, pin: "0000" });
  assert.equal(g.last("error").code, "room_locked");
  g.say({ type: "join", room: code, pin: "1234" }); // even the right PIN is refused while locked
  assert.equal(g.last("error").code, "room_locked");
  env.advance(5 * 60 * 1000 + 1);
  g.say({ type: "join", room: code, pin: "1234" });
  assert.ok(g.last("join-pending"));
});

test("invalid PIN format on create is rejected", () => {
  const { client } = mk();
  const h = client(); h.say({ type: "create", pin: "12" });
  assert.equal(h.last("error").code, "bad_message");
});

test("rate limits: room creation and join attempts per IP", () => {
  const { client } = mk();
  const h = client();
  const rooms = [];
  for (let i = 0; i < 10; i++) { const c = client(); c.say({ type: "create" }); rooms.push(c.last("room-created")); }
  assert.ok(rooms.every(Boolean));
  const c = client(); c.say({ type: "create" });
  assert.equal(c.last("error").code, "rate_limited");
  for (let i = 0; i < 20; i++) h.say({ type: "join", room: "ZZZZZZ" });
  h.say({ type: "join", room: "ZZZZZZ" });
  assert.equal(h.last("error").code, "rate_limited");
});

test("message validation: size, JSON, unknown type, payload shape", () => {
  const { client } = mk();
  const h = client(); h.say({ type: "create" });
  h.say("{nope"); assert.equal(h.last("error").code, "bad_message");
  h.say({ type: "bogus" }); assert.equal(h.last("error").code, "bad_message");
  h.say("x".repeat(40_000)); assert.equal(h.last("error").code, "too_large");
  assert.equal(sanitizeSignal({ kind: "offer", description: { type: "answer", sdp: "x" } }), null);
  assert.equal(sanitizeSignal({ kind: "offer", description: { type: "offer", sdp: "x".repeat(30_000) } }), null);
  assert.equal(sanitizeSignal({ kind: "evil" }), null);
  const c = sanitizeSignal({ kind: "candidate", candidate: { candidate: "candidate:1 1 udp 1 1.2.3.4 5 typ host", sdpMid: "0", sdpMLineIndex: 0, junk: "<script>" } });
  assert.deepEqual(Object.keys(c.candidate).sort(), ["candidate", "sdpMLineIndex", "sdpMid"]);
});

test("flood protection closes the socket", () => {
  const { client } = mk();
  const h = client();
  for (let i = 0; i < 200; i++) h.say({ type: "ping" });
  assert.equal(h.closed, 1008);
});

test("rejoin with token restores a dropped peer; wrong token refused; stale socket replaced", () => {
  const env = pair();
  const { host, guest, room } = env;
  host.say({ type: "approve" });
  const creds = host.last("room-created");
  guest.drop();
  assert.ok(host.last("peer-offline"));
  const g2 = env.client("2.2.2.2");
  const gcreds = guest.last("join-pending");
  g2.say({ type: "rejoin", room: room.room, peerId: gcreds.peerId, token: "wrong" });
  assert.equal(g2.last("error").code, "room_not_found");
  g2.say({ type: "rejoin", room: room.room, peerId: gcreds.peerId, token: gcreds.token });
  assert.equal(g2.last("resumed").role, "guest");
  assert.ok(host.last("peer-online"));
  // host reconnects on a new socket; old one is closed
  const h2 = env.client();
  h2.say({ type: "rejoin", room: room.room, peerId: creds.peerId, token: creds.token });
  assert.equal(host.closed, 4000);
  assert.equal(h2.last("resumed").peerState, "active");
});

test("pending guest that disconnects is forgotten and the host is told", () => {
  const { host, guest } = pair();
  guest.drop();
  assert.equal(host.last("peer-left").reason, "join_cancelled");
});

test("host leaving closes the room; guest is notified", () => {
  const { host, guest, core } = pair();
  host.say({ type: "approve" });
  host.say({ type: "leave" });
  assert.equal(guest.last("peer-left").reason, "host_left");
  assert.equal(core.rooms.size, 0);
});

test("rooms expire by idle TTL and are swept; pings keep them alive", () => {
  const { client, core, advance } = mk();
  const a = client(), b = client("5.5.5.5");
  a.say({ type: "create" }); b.say({ type: "create" });
  advance(20 * 60 * 1000); a.say({ type: "ping" });
  advance(15 * 60 * 1000);
  core.sweep();
  assert.equal(core.rooms.size, 1);
  assert.ok(b.last("room-expired"));
});

test("offline peers are dropped after the grace period", () => {
  const { host, guest, core, advance } = pair();
  host.say({ type: "approve" });
  guest.drop();
  advance(3 * 60 * 1000);
  host.say({ type: "ping" });
  core.sweep();
  assert.equal(host.last("peer-left").reason, "guest_offline");
  assert.equal(core.rooms.size, 1);
});

test("capacity limit", () => {
  const { client } = mk({ maxRooms: 2, rate: { create: { max: 100, windowMs: 1000 } } });
  for (let i = 0; i < 2; i++) client(`10.0.0.${i}`).say({ type: "create" });
  const c = client("10.0.0.9"); c.say({ type: "create" });
  assert.equal(c.last("error").code, "capacity");
});

test("iceConfig: STUN default, TURN only when configured, HMAC credentials", () => {
  assert.equal(iceConfig({}).turn, false);
  assert.equal(iceConfig({}).iceServers.length, 1);
  const st = iceConfig({ TURN_URLS: "turn:t.example.com:3478", TURN_USERNAME: "u", TURN_CREDENTIAL: "p" });
  assert.equal(st.turn, true);
  assert.equal(st.iceServers[1].username, "u");
  const eph = iceConfig({ TURN_URLS: "turn:t.example.com:3478,turns:t.example.com:5349", TURN_SECRET: "s" }, 1_700_000_000_000);
  assert.equal(eph.iceServers[1].urls.length, 2);
  assert.equal(eph.iceServers[1].username, `${1_700_000_000 + 3600}:zikei`);
  assert.ok(eph.iceServers[1].credential.length > 10);
  assert.equal(iceConfig({ TURN_URLS: "turn:x" }).turn, false); // URLs without credentials -> not used
});
