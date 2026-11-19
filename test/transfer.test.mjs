import test from "node:test";
import assert from "node:assert/strict";
import { TransferManager } from "../src/lib/transfer.js";
import { LIMITS, sanitizeFilename, uniqueName, parseInviteHash, normalizeCode, formatBytes } from "../src/lib/protocol.js";
import { channelPair, FakeFile, memorySinks } from "./helpers.js";

const bytes = (n, seed = 1) => { const b = new Uint8Array(n); for (let i = 0; i < n; i++) b[i] = (i * 31 + seed) & 255; return b; };
const until = async (fn, ms = 5000) => { const t = Date.now(); while (!fn()) { if (Date.now() - t > ms) throw new Error("timeout waiting for condition"); await new Promise((r) => setTimeout(r, 5)); } };

function setup({ sinkOpts, blockedFiles } = {}) {
  const [a, b] = channelPair();
  const store = new Map();
  const sender = new TransferManager({ channel: a, localName: "A", prepareSinks: memorySinks(new Map()) });
  const receiver = new TransferManager({ channel: b, localName: "B", prepareSinks: memorySinks(store, sinkOpts), blockedFiles });
  return { a, b, sender, receiver, store };
}
const incoming = (r) => r.snapshot().batches.find((x) => x.direction === "receive");
const outgoing = (s) => s.snapshot().batches.find((x) => x.direction === "send");

test("hello exchange", async () => {
  const { sender, receiver } = setup();
  await until(() => sender.peer && receiver.peer);
  assert.equal(sender.peer.name, "B");
  assert.equal(receiver.peer.name, "A");
});

test("multi-file transfer incl. 0-byte file arrives intact and in order", async () => {
  const { sender, receiver, store } = setup();
  const files = [new FakeFile("a.bin", bytes(300_000, 1)), new FakeFile("empty.txt", new Uint8Array(0)), new FakeFile("b.bin", bytes(1, 7)), new FakeFile("c.bin", bytes(1_500_000, 9))];
  sender.sendFiles(files);
  await until(() => incoming(receiver));
  assert.equal((await receiver.acceptBatch(incoming(receiver).id)).ok, true);
  await until(() => outgoing(sender).status === "done");
  assert.equal(incoming(receiver).status, "done");
  for (const f of files) assert.ok(Buffer.compare(store.get(f.name), Buffer.from(f.bytes)) === 0, f.name);
  assert.equal(store.get("empty.txt").length, 0);
  assert.deepEqual(outgoing(sender).items.map((i) => i.status), ["done", "done", "done", "done"]);
  assert.equal(outgoing(sender).items[3].loaded, 1_500_000);
});

test("backpressure: sender never lets bufferedAmount run away; in-flight bytes bounded by window", async () => {
  const { a, sender, receiver } = setup();
  a.latency = 2;
  const big = new FakeFile("big.bin", bytes(40 * 1024 * 1024, 3));
  sender.sendFiles([big]);
  await until(() => incoming(receiver));
  await receiver.acceptBatch(incoming(receiver).id);
  await until(() => outgoing(sender).status === "done", 60000);
  assert.ok(a.maxBuffered <= LIMITS.SEND_HIGH_WATER + LIMITS.CHUNK_SIZE, `max buffered ${a.maxBuffered}`);
});

test("receiver can decline; sender learns the reason", async () => {
  const { sender, receiver } = setup();
  sender.sendFiles([new FakeFile("x", bytes(10))]);
  await until(() => incoming(receiver));
  receiver.declineBatch(incoming(receiver).id);
  await until(() => outgoing(sender).status === "declined");
  assert.equal(outgoing(sender).reason, "declined");
});

test("sender cancel mid-transfer aborts the receiver sink; next batch still works", async () => {
  const { a, sender, receiver, store } = setup();
  a.latency = 1;
  sender.sendFiles([new FakeFile("big", bytes(30 * 1024 * 1024))]);
  await until(() => incoming(receiver));
  await receiver.acceptBatch(incoming(receiver).id);
  await until(() => outgoing(sender).items[0].loaded > 0);
  sender.cancelBatch(outgoing(sender).id);
  await until(() => outgoing(sender).status === "cancelled");
  await until(() => incoming(receiver).status === "cancelled");
  assert.ok(store.get("big:aborted"));
  // follow-up transfer
  sender.sendFiles([new FakeFile("ok.txt", bytes(500))]);
  await until(() => receiver.snapshot().batches.filter((b) => b.direction === "receive").length === 2);
  const second = receiver.snapshot().batches.filter((b) => b.direction === "receive")[1];
  await receiver.acceptBatch(second.id);
  await until(() => sender.snapshot().batches.at(-1).status === "done");
  assert.equal(store.get("ok.txt").length, 500);
});

test("receiver cancel mid-transfer stops the sender", async () => {
  const { a, sender, receiver } = setup();
  a.latency = 1;
  sender.sendFiles([new FakeFile("big", bytes(30 * 1024 * 1024))]);
  await until(() => incoming(receiver));
  await receiver.acceptBatch(incoming(receiver).id);
  await until(() => outgoing(sender).items[0].loaded > 0);
  receiver.cancelBatch(incoming(receiver).id);
  await until(() => outgoing(sender).status === "cancelled");
  assert.equal(incoming(receiver).status, "cancelled");
});

test("queued second batch waits for the first one", async () => {
  const { sender, receiver, store } = setup();
  sender.sendFiles([new FakeFile("one", bytes(1000))]);
  sender.sendFiles([new FakeFile("two", bytes(2000))]);
  await until(() => incoming(receiver));
  await receiver.acceptBatch(incoming(receiver).id);
  await until(() => receiver.snapshot().batches.filter((b) => b.direction === "receive").length === 2);
  await receiver.acceptBatch(receiver.snapshot().batches.filter((b) => b.direction === "receive")[1].id);
  await until(() => sender.snapshot().batches.every((b) => b.status === "done"));
  assert.equal(store.get("one").length, 1000);
  assert.equal(store.get("two").length, 2000);
});

test("write failure on receiver fails the batch on both sides", async () => {
  const { sender, receiver } = setup({ sinkOpts: { failWriteAt: 100_000 } });
  sender.sendFiles([new FakeFile("f", bytes(500_000)), new FakeFile("g", bytes(10))]);
  await until(() => incoming(receiver));
  await receiver.acceptBatch(incoming(receiver).id);
  await until(() => outgoing(sender).status === "failed");
  assert.match(outgoing(sender).reason, /Write failed/);
  assert.equal(incoming(receiver).status, "failed");
});

test("connection loss fails active transfers", async () => {
  const { a, sender, receiver } = setup();
  a.latency = 1;
  sender.sendFiles([new FakeFile("big", bytes(30 * 1024 * 1024))]);
  await until(() => incoming(receiver));
  await receiver.acceptBatch(incoming(receiver).id);
  await until(() => outgoing(sender).items[0].loaded > 0);
  a.close();
  await until(() => outgoing(sender).status === "failed");
  await until(() => incoming(receiver).status === "failed");
});

test("hostile sender: malicious names/sizes are sanitized or declined; overflow data is rejected", async () => {
  const [a, b] = channelPair();
  const store = new Map();
  const receiver = new TransferManager({ channel: b, localName: "B", prepareSinks: memorySinks(store) });
  const raw = (o) => a.send(JSON.stringify(o));
  raw({ t: "batch", bid: "x1", files: [{ fid: "f1", name: "../../etc/passwd", size: -5 }] });
  raw({ t: "batch", bid: "x2", files: [{ fid: "f1", name: "../../etc/passwd", size: 3 }] });
  raw({ t: "batch", bid: "x3", files: Array.from({ length: 500 }, (_, i) => ({ fid: "f" + i, name: "n", size: 1 })) });
  await until(() => receiver.snapshot().batches.length === 1);
  const batch = receiver.snapshot().batches[0];
  assert.equal(batch.items[0].name, "_.._etc_passwd");
  await receiver.acceptBatch(batch.id);
  raw({ t: "file-start", bid: "x2", fid: "f1" });
  a.send(new Uint8Array(10)); // announced 3 bytes, sends 10
  await until(() => receiver.snapshot().batches[0].status === "failed");
  assert.match(receiver.snapshot().batches[0].reason, /more data/);
});

test("text sharing, with limits", async () => {
  const { sender, receiver } = setup();
  sender.sendText("hello <b>world</b>");
  await until(() => receiver.snapshot().texts.length === 1);
  assert.equal(receiver.snapshot().texts[0].text, "hello <b>world</b>");
  assert.equal(receiver.snapshot().texts[0].from, "peer");
  assert.throws(() => sender.sendText("x".repeat(LIMITS.MAX_TEXT + 1)));
});

test("blocked files (too large for in-memory fallback) cannot be accepted", async () => {
  const { sender, receiver } = setup({ blockedFiles: (b) => b.files.filter((f) => f.size > 5) });
  sender.sendFiles([new FakeFile("big", bytes(100))]);
  await until(() => incoming(receiver));
  assert.deepEqual(incoming(receiver).blocked, ["big"]);
  assert.equal((await receiver.acceptBatch(incoming(receiver).id)).error, "blocked");
});

test("helpers: filename sanitizing, unique names, invite hash, codes, bytes", async () => {
  assert.equal(sanitizeFilename("a/b\\c:d*.txt"), "a_b_c_d_.txt");
  assert.equal(sanitizeFilename("..."), "file");
  assert.equal(sanitizeFilename("CON.txt"), "_CON.txt");
  assert.equal(sanitizeFilename("x".repeat(400) + ".png").length, LIMITS.MAX_NAME);
  const taken = new Set(["a.txt", "a (2).txt"]);
  assert.equal(await uniqueName("a.txt", async (n) => taken.has(n)), "a (3).txt");
  assert.equal(parseInviteHash("#join=abc234"), "ABC234");
  assert.equal(parseInviteHash("#join=ABC10O"), "");
  assert.equal(parseInviteHash("#x"), "");
  assert.equal(normalizeCode(" ab-c2 34z9 "), "ABC234");
  assert.equal(formatBytes(1536), "1.50 KB");
});
