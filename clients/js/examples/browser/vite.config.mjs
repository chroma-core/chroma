import * as path from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
  },
  // This manual remapping is only needed because we're loading a locally linked version of the JS client
  resolve: {
    alias: [
      {
        find: "chromadb-default-embed",
        replacement: path.resolve(
          __dirname,
          "node_modules",
          "chromadb-default-embed",
        ),
      },
    ],
  },
});
