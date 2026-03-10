/**
 * Chroma WASM-backed vector store.
 *
 * Uses the chroma-wasm Rust module (compiled to WebAssembly) for vector
 * storage and cosine similarity search. Persists the collection to
 * IndexedDB as a serialized snapshot so data survives browser restarts.
 */

const DB_NAME = "chroma_history_store";
const DB_VERSION = 1;
const STORE_NAME = "snapshots";
const META_STORE = "metadata";
const COLLECTION_NAME = "browser_history";

let wasmModule = null;

/**
 * Initialize the Chroma WASM module.
 *
 * The WASM binary must be built from rust/wasm/ using wasm-pack and placed
 * in the extension's pkg/ directory.
 */
async function initWasm() {
  if (wasmModule) return wasmModule;

  const { default: init, ChromaCollection } = await import(
    chrome.runtime.getURL("pkg/chroma_wasm.js")
  );
  await init(chrome.runtime.getURL("pkg/chroma_wasm_bg.wasm"));

  wasmModule = { ChromaCollection };
  return wasmModule;
}

/**
 * Open (or create) the IndexedDB database used for persisting snapshots.
 */
function openDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onupgradeneeded = (event) => {
      const db = event.target.result;

      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: "name" });
      }
      if (!db.objectStoreNames.contains(META_STORE)) {
        db.createObjectStore(META_STORE, { keyPath: "key" });
      }
    };

    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

export class VectorStore {
  constructor() {
    this.db = null;
    this.collection = null;
  }

  /**
   * Initialize the store: load WASM module and restore from IndexedDB if available.
   */
  async init() {
    const { ChromaCollection } = await initWasm();
    this.db = await openDB();

    // Try to restore from a persisted snapshot
    const snapshot = await this._loadSnapshot();
    if (snapshot) {
      this.collection = ChromaCollection.load(snapshot);
    } else {
      this.collection = new ChromaCollection(COLLECTION_NAME, "cosine");
    }

    return this;
  }

  /**
   * Add a single entry to the Chroma collection.
   */
  async add(id, embedding, metadata) {
    this.collection.add(
      [id],
      [Array.from(embedding)],
      [metadata.title || ""],
      [metadata],
    );
  }

  /**
   * Add entries in bulk.
   */
  async addBatch(entries) {
    const ids = [];
    const embeddings = [];
    const documents = [];
    const metadatas = [];

    for (const { id, embedding, metadata } of entries) {
      ids.push(id);
      embeddings.push(Array.from(embedding));
      documents.push(`${metadata.title || ""} - ${metadata.url || ""}`);
      metadatas.push(metadata);
    }

    this.collection.add(ids, embeddings, documents, metadatas);
  }

  /**
   * Check if an entry exists by ID.
   */
  async has(id) {
    return this.collection.contains(id);
  }

  /**
   * Get the total number of entries.
   */
  async count() {
    return this.collection.count();
  }

  /**
   * Search for the most similar entries to the query embedding.
   *
   * Uses Chroma's brute-force cosine similarity search running in WASM.
   */
  async search(queryEmbedding, nResults = 10, domainFilter = null) {
    const whereFilter = domainFilter ? { domain: domainFilter } : null;

    const results = this.collection.query(
      Array.from(queryEmbedding),
      nResults,
      whereFilter,
    );

    // Map Chroma results to the format expected by the popup
    return results.map((r) => ({
      id: r.id,
      title: r.metadata?.title || "",
      url: r.metadata?.url || "",
      domain: r.metadata?.domain || "",
      visitCount: r.metadata?.visitCount || 0,
      lastVisit: r.metadata?.lastVisit || 0,
      similarity: 1.0 - r.distance, // Convert distance to similarity
    }));
  }

  /**
   * Persist the collection to IndexedDB.
   *
   * Serializes the entire Chroma collection (including all embeddings)
   * to a JSON string and stores it in IndexedDB.
   */
  async persist() {
    const snapshot = this.collection.save();
    await this._saveSnapshot(snapshot);
  }

  /**
   * Get metadata about the index.
   */
  async getMeta(key) {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(META_STORE, "readonly");
      const store = tx.objectStore(META_STORE);
      const request = store.get(key);

      request.onsuccess = () => resolve(request.result?.value);
      request.onerror = () => reject(request.error);
    });
  }

  /**
   * Set metadata.
   */
  async setMeta(key, value) {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(META_STORE, "readwrite");
      const store = tx.objectStore(META_STORE);
      store.put({ key, value });

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  /**
   * Clear all entries and re-create the collection.
   */
  async clear() {
    const { ChromaCollection } = await initWasm();
    this.collection = new ChromaCollection(COLLECTION_NAME, "cosine");
    await this._clearSnapshot();
  }

  // --- Internal persistence helpers ---

  async _loadSnapshot() {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_NAME, "readonly");
      const store = tx.objectStore(STORE_NAME);
      const request = store.get(COLLECTION_NAME);

      request.onsuccess = () => resolve(request.result?.data || null);
      request.onerror = () => reject(request.error);
    });
  }

  async _saveSnapshot(data) {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_NAME, "readwrite");
      const store = tx.objectStore(STORE_NAME);
      store.put({ name: COLLECTION_NAME, data });

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async _clearSnapshot() {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_NAME, "readwrite");
      const store = tx.objectStore(STORE_NAME);
      store.delete(COLLECTION_NAME);

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }
}
