import { defineConfig } from "vite";

// The Go server proxies nothing: during `npm run dev`, API calls are forwarded
// to a running dagviz instance (default http://127.0.0.1:8080).
export default defineConfig({
  base: "./",
  build: {
    outDir: "dist",
    emptyOutDir: true,
    chunkSizeWarningLimit: 3000,
  },
  server: {
    proxy: {
      "/api": process.env.DAGVIZ_BACKEND ?? "http://127.0.0.1:8080",
    },
  },
});
