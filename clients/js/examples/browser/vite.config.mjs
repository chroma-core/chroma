import * as path from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  // Get package type from environment variable
  const packageType = process.env.PACKAGE || 'bundled';

  console.log(`Building with package type: ${packageType}`);

  return {
    plugins: [react()],
    define: {
      // Pass the package type to the client code
      'process.env.PACKAGE': JSON.stringify(packageType),
    },
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
  };
});
