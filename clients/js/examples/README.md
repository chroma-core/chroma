# ChromaDB JavaScript Client Examples

This directory contains examples for using the ChromaDB JavaScript client.

## Available Packages

ChromaDB now offers two different packages for different use cases:

### 1. `chromadb` Package (Bundled Dependencies)

The main package with all embedding model dependencies bundled.

```javascript
import { ChromaClient } from "chromadb";
const client = new ChromaClient({ path: "http://localhost:8000" });
```

Use this package if you want:
- A simpler setup
- All embedding models available without managing peer dependencies
- Less potential for dependency conflicts

### 2. `chromadb-client` Package (Peer Dependencies)

An alternative package where embedding model libraries are peer dependencies.

```javascript
import { ChromaClient } from "chromadb-client";
const client = new ChromaClient({ path: "http://localhost:8000" });
```

Use this package if you want:
- More control over which embedding model libraries to install
- Smaller bundle size (only install what you need)
- Ability to use specific versions of embedding libraries

## Examples

This directory includes example applications that demonstrate how to use the ChromaDB JavaScript client:

- **browser/**: Example web application using ChromaDB in the browser with React
- **node/**: Example Node.js application using ChromaDB

## Running the Examples

1. Build the ChromaDB packages:

```bash
# From the repository root
pnpm install
pnpm build
```

2. Run a specific example:

```bash
cd examples/browser
# or
cd examples/node

pnpm install
pnpm dev
```

## Making Changes

If you make changes to the ChromaDB code, you'll need to rebuild the packages:

```bash
# From the repository root
pnpm build
```

Then restart your example application to see the changes.