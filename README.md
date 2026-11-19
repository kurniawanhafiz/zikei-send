# Zikei Send

Browser-to-browser file sharing over WebRTC, hosted **entirely on Netlify** (static frontend + Netlify Server for signaling). No accounts, and files are never uploaded to any server: they travel directly between the two browsers.

## How it works

1. Device A creates a room (6-character code, optional PIN) and shows a code + QR.
2. Device B enters the code / scans the QR. Device A must **approve** B before anything is negotiated.
3. The signaling server (`netlify/server/`) relays only SDP/ICE messages. Then the WebRTC DataChannel carries files and text.

```
src/lib/        protocol helpers, signaling client, WebRTC link, transfer engine, sinks, session controller
src/App.jsx     React UI
netlify/server/ signaling server (index.mjs = Netlify entry, signaling.mjs = transport-agnostic core)
public/         PWA manifest, service worker, icons
test/           node:test unit tests (signaling core + transfer engine)
scripts/        local signaling server for development without Netlify CLI
```

## Features (what is implemented)

- Room create/join, QR invite (`/#join=CODE`: the code is in the URL fragment, so it is not sent to servers), host approval of each joiner, optional PIN (5 wrong tries lock the room for 5 minutes).
- Deterministic WebRTC negotiation (creator always offers), buffered ICE candidates, ICE restart, connect timeout, signaling auto-reconnect with token re-join.
- Multi-file transfer, 0-byte files, drag & drop, paste, progress / speed / ETA, cancel from either side, accept/decline per batch, text sharing.
- Flow control: ack window (8 MiB) + `bufferedAmount` backpressure, so memory stays bounded.
- Receiving: Chrome/Edge desktop stream straight to disk (any size, folder picker for multi-file). Other browsers buffer **one file at a time in RAM** and download it; files > 512 MiB are refused up front.
- Local-only transfer history, dark/light theme, PWA (installable, offline shell), keyboard/ARIA support.
- Security: payload validation, message size caps, per-IP rate limits, room caps and TTL, constant-time PIN compare, optional `ALLOWED_ORIGINS`, CSP + security headers.

## Limitations (please read)

- **Netlify Server is Early Access** (announced 2026-11-10): behavior and limits may change; verify the [docs](https://docs.netlify.com/build/server/) before relying on it. It needs a credit-based plan; Free/Personal is capped to **1 instance**. Requires **Node 24+**.
- Rooms live in the memory of one instance (`scale.max = 1` is set in the entry file). A restart/replacement loses rooms. An established WebRTC transfer is **not** affected (it no longer needs the server), but new joins need a new room.
- Documented limits: a WebSocket is closed at the request duration limit (30 min on Free, 2 h on Pro+) and instances live at most 8 h; the client reconnects and re-joins automatically. Instances stop after 5 min without connections and cold-start on the next visit.
- **STUN only by default** (public Google STUN, fine for development). Peer-to-peer can fail behind symmetric NAT, some corporate/campus firewalls and some mobile carriers. A **TURN relay** fixes this but is an external service (see below). Do not assume it works on every network.
- File integrity = reliable ordered SCTP + exact byte-count check; no per-file hash.
- No folder transfer, no resume after disconnect, 2 devices per room.
- WebRTC encrypts the transport (DTLS), but this project has not been security-audited. A 6-character code is not strong authentication: rely on host approval and the optional PIN.
- File size is limited by browser, RAM/disk and network, not by this app.

## Develop

```bash
npm install
npm test                 # unit tests (node:test)
npm run dev:local        # Vite + local signaling server (no Netlify account needed), http://localhost:5173
npx netlify dev          # or: the real Netlify Server locally, http://localhost:8888  (CLI >= 27.17.0)
```

Test with two browser windows (or phone + laptop on the same site): create a room in one, join in the other, approve, send files. Use a private window for the second profile.

## Deploy on Netlify

**Auto deploy (recommended):** push to GitHub, then in Netlify: *Add new project → Import an existing project* → pick the repo. `netlify.toml` already sets `npm test && npm run build`, publish dir `dist`, Node 24 and headers. Every push to the production branch redeploys; other branches/PRs get deploy previews.

**CLI deploy:**
```bash
npx netlify login
npx netlify link          # or: npx netlify init
npm run deploy            # netlify deploy --build --prod  (use deploy:preview for a draft)
```

Netlify detects `netlify/server/index.mjs` during the build; its `config.path` routes only `/ws`, `/ice`, `/health` to the server, everything else is static. **Do not add a `/* → /index.html` rewrite**: it would hijack `/ws`.

Verify after deploy: open `https://YOUR-SITE/health` (JSON `ok:true`), then test two devices.

### Custom domain
Site configuration → Domain management → Add a domain, then create the DNS records Netlify shows (A/ALIAS for the apex or a CNAME for a subdomain such as `send.example.com`). HTTPS certificate is automatic. WebRTC needs HTTPS (already the case).

### Optional: TURN relay
Set these Netlify env vars (scope **Functions**, then redeploy): `TURN_URLS`, and either `TURN_SECRET` (coturn `static-auth-secret`, short-lived credentials generated by `/ice`) or `TURN_USERNAME` + `TURN_CREDENTIAL`. A TURN server is a separate service (self-hosted coturn or a managed provider) and may cost money; relayed traffic passes through it (still encrypted end to end by DTLS). The UI shows "direct" or "via relay".

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| "Signaling server did not respond" | Netlify Server not enabled for the account/site, wrong Node version (needs 24), a `/* /index.html` rewrite, or the deploy failed to start the server (see Netlify function/server logs). |
| `/health` returns 404 or HTML | The server was not deployed or `netlify/server/index.mjs` is missing/duplicated (only one index file is allowed). |
| Joins fine, then "Could not establish a direct connection" | NAT/firewall blocks P2P; configure TURN, try another network, disable VPN. |
| "Room not found" | Wrong/expired code, or the instance restarted. Create a new room. |
| Big file refused on phone/Firefox/Safari | In-RAM fallback limit (512 MiB); use Chrome/Edge desktop to receive. |
| Receiver gets no download | The browser blocked multiple downloads; allow them for the site. |

Environment variables: see `.env.example`.
