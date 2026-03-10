# Browser History Search with Chroma

Semantically search your browser history using natural language queries. Instead of exact keyword matching, find pages by meaning - for example, search "that article about machine learning" to find relevant pages even if they don't contain those exact words.

## Features

- Supports Chrome and Firefox browser history
- Semantic search using embeddings (finds pages by meaning, not just keywords)
- Metadata filtering by date, domain, or visit count
- Fast local search with persistent storage

## Setup

1. Install dependencies:

```bash
pip install chromadb tqdm
```

2. Load your browser history:

```bash
# Auto-detect browser (tries Chrome, then Firefox)
python load_history.py

# Specify browser explicitly
python load_history.py --browser chrome
python load_history.py --browser firefox

# Specify custom history file path
python load_history.py --history_path /path/to/History
```

3. Search your history:

```bash
python search.py
```

## Usage Examples

Once loaded, you can search with natural language:

```
Query: that article about machine learning I was reading
Query: recipes for pasta
Query: documentation for python async
Query: news about climate change
Query: stackoverflow question about git merge
```

You can also filter by metadata:

```
Query: python tutorials (filter by domain: medium.com)
```

## How It Works

1. **History Extraction**: Reads your browser's SQLite history database
2. **Embedding**: Creates semantic embeddings for each page title + URL
3. **Storage**: Stores embeddings in Chroma for fast retrieval
4. **Search**: Queries use the same embedding model to find semantically similar pages

## Browser History Locations

The script automatically looks for history in default locations:

### Chrome
- **macOS**: `~/Library/Application Support/Google/Chrome/Default/History`
- **Linux**: `~/.config/google-chrome/Default/History`
- **Windows**: `%LOCALAPPDATA%\Google\Chrome\User Data\Default\History`

### Firefox
- **macOS**: `~/Library/Application Support/Firefox/Profiles/*/places.sqlite`
- **Linux**: `~/.mozilla/firefox/*/places.sqlite`
- **Windows**: `%APPDATA%\Mozilla\Firefox\Profiles\*\places.sqlite`

## Browser Extension (Chroma WASM)

There's also a Chrome browser extension that runs Chroma compiled to WebAssembly. See the [`extension/`](./extension/) directory.

Features:
- **Chroma in WASM** - the Rust vector store runs natively in your browser
- Uses ONNX Runtime WASM for embedding generation (all-MiniLM-L6-v2 model)
- Automatically indexes your history in the background
- Full Chroma API: `add()`, `query()`, `delete()`, `save()`, `load()`
- All data stays local in your browser

The WASM module is built from [`rust/wasm/`](../../rust/wasm/) using `wasm-pack`. See [`extension/README.md`](./extension/README.md) for build and setup instructions.

## Notes

- The Python script copies your history database before reading (browser locks prevent direct access while running)
- Your data stays local - no external APIs required for basic functionality
- For larger histories, initial loading may take a few minutes

## Privacy

All data is processed and stored locally. No browser history is sent to external services.
