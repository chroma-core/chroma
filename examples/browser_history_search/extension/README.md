# Chroma History Search - Browser Extension

A Chrome extension that semantically searches your browser history using WASM-based embeddings. Everything runs locally in your browser - no server, no API keys, complete privacy.

## Architecture

```
extension/
├── manifest.json          # Chrome Manifest V3 config
├── background.js          # Service worker: indexes history, handles search
├── popup.html/css/js      # Search UI popup
├── lib/
│   ├── embeddings.js      # WASM embedding via Transformers.js (ONNX Runtime)
│   └── vector-store.js    # IndexedDB-backed vector store with cosine similarity
└── icons/                 # Extension icons
```

### How It Works

1. **Indexing**: On install, the background service worker reads your Chrome history via the `chrome.history` API and generates embeddings using the [all-MiniLM-L6-v2](https://huggingface.co/Xenova/all-MiniLM-L6-v2) model running in ONNX Runtime WASM (~23MB, downloaded once and cached)

2. **Storage**: Embeddings and metadata are stored in IndexedDB for persistence across browser sessions

3. **Search**: When you type a query, it's embedded using the same model and compared against stored embeddings using cosine similarity

4. **Updates**: An alarm triggers incremental re-indexing every hour to capture new history

### WASM Details

The extension uses [Transformers.js](https://huggingface.co/docs/transformers.js) which bundles ONNX Runtime compiled to WebAssembly. This allows running the `all-MiniLM-L6-v2` sentence transformer model directly in the browser with no backend server.

The `wasm-unsafe-eval` CSP directive in the manifest enables WASM execution within the extension context.

## Installation

1. Open Chrome and navigate to `chrome://extensions/`

2. Enable **Developer mode** (toggle in the top right)

3. Click **Load unpacked** and select this `extension/` directory

4. The extension icon will appear in your toolbar

5. Click the icon to open the search popup

## First Run

On first install, the extension will:

1. Download the embedding model (~23MB, cached after first download)
2. Read your last 90 days of browser history
3. Generate embeddings for each page (this may take a few minutes)

Progress is shown in the popup status bar.

## Usage

1. Click the extension icon in your toolbar
2. Type a natural language query, e.g.:
   - "that article about React hooks"
   - "python documentation"
   - "recipe for chocolate cake"
   - "stackoverflow git rebase"
3. Optionally filter by domain (e.g., "github.com")
4. Click a result to open the page

Search happens as you type (after a 500ms pause) or when you press Enter.

## Permissions

- **history**: Read browser history for indexing
- **storage**: Store extension settings
- **alarms**: Schedule periodic re-indexing
- **offscreen**: Required for WASM execution context

## Privacy

- All processing happens locally in your browser
- The embedding model is downloaded from HuggingFace CDN and cached locally
- No browsing data is sent to any server
- Embeddings and history data are stored only in your browser's IndexedDB
- Uninstalling the extension removes all stored data

## Limitations

- Initial indexing can take a few minutes for large histories
- The embedding model download requires an internet connection on first use
- Service workers may be unloaded by Chrome after periods of inactivity (the alarm handles restart)
- Currently Chrome only (Firefox extension support would need manifest changes)

## Development

To modify and test:

1. Make your changes to the source files
2. Go to `chrome://extensions/`
3. Click the refresh icon on the extension card
4. Click the extension icon to test

Check the background service worker console for logs:
- Go to `chrome://extensions/`
- Click "Service worker" under the extension
