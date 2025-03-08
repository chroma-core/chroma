# Browser Example

This is an example of how to use ChromaDB in a browser environment.

## Getting Started

1. Make sure you have Chroma running locally at `http://localhost:8000`
2. Run `pnpm install` to install dependencies
3. Run one of the following commands:

- `pnpm dev` - Run the example with the default bundled package
- `pnpm dev:bundled` - Run the example with the bundled chromadb package
- `pnpm dev:client` - Run the example with the chromadb-client package (peer dependencies)

4. Visit `http://localhost:3000` in your browser

## Package Options

This example demonstrates both ChromaDB package options:

1. **chromadb** (Bundled Package): Includes all embedding libraries as dependencies.
   - Good for simple projects or when you want everything included.
   - Import with: `import { ChromaClient } from "chromadb";`

2. **chromadb-client** (Client Package): Uses peer dependencies.
   - Good for projects that already use specific versions of embedding libraries.
   - Keeps your dependency tree lean if you only need specific embedding libraries.
   - Import with: `import { ChromaClient } from "chromadb-client";`

The example code dynamically chooses which package to use based on the environment variable set by the dev script.
