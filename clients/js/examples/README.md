# ChromaDB Examples

This directory contains examples for using both ChromaDB package options:

1. `chromadb`: Bundled package with all dependencies included
2. `chromadb-client`: Package with peer dependencies that you install separately

## Node.js Example

The Node.js example demonstrates how to use ChromaDB in a Node.js environment.

```bash
cd node
pnpm install

# Run with the default bundled package
pnpm dev

# Run with the bundled package explicitly
pnpm dev:bundled

# Run with the client package (peer dependencies)
pnpm dev:client
```

## Browser Example

The browser example demonstrates how to use ChromaDB in a browser environment.

```bash
cd browser
pnpm install

# Run with the default bundled package
pnpm dev

# Run with the bundled package explicitly
pnpm dev:bundled

# Run with the client package (peer dependencies)
pnpm dev:client
```

## Differences Between Package Options

- The **bundled package** (`chromadb`) includes all embedding libraries as dependencies, making it easier to get started.
- The **client package** (`chromadb-client`) uses peer dependencies, giving you more control over which versions of embedding libraries you use. It also keeps your dependency tree leaner if you only need specific embedding libraries.

Both packages offer identical functionality and API.
