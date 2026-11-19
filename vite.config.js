import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Plain `vite` (npm run dev) has no Netlify Server, so /ws and /ice are proxied to the local
// signaling server started by `npm run dev:local`. With `netlify dev` (port 8888) Netlify routes them itself.
export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    proxy: {
      "/ws": { target: "ws://127.0.0.1:8787", ws: true },
      "/ice": "http://127.0.0.1:8787",
      "/health": "http://127.0.0.1:8787",
    },
  },
  build: { target: "es2022" },
});
