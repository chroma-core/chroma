/**
 * IndexedDB-backed vector store with cosine similarity search.
 *
 * Stores embeddings persistently in the browser and provides fast
 * approximate nearest neighbor search using cosine similarity.
 */

const DB_NAME = "chroma_history_search";
const DB_VERSION = 1;
const STORE_NAME = "vectors";
const META_STORE = "metadata";

/**
 * Open (or create) the IndexedDB database.
 */
function openDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onupgradeneeded = (event) => {
      const db = event.target.result;

      if (!db.objectStoreNames.contains(STORE_NAME)) {
        const store = db.createObjectStore(STORE_NAME, { keyPath: "id" });
        store.createIndex("domain", "domain", { unique: false });
        store.createIndex("lastVisit", "lastVisit", { unique: false });
      }

      if (!db.objectStoreNames.contains(META_STORE)) {
        db.createObjectStore(META_STORE, { keyPath: "key" });
      }
    };

    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

/**
 * Compute cosine similarity between two vectors.
 */
function cosineSimilarity(a, b) {
  let dotProduct = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }

  const denominator = Math.sqrt(normA) * Math.sqrt(normB);
  if (denominator === 0) return 0;

  return dotProduct / denominator;
}

export class VectorStore {
  constructor() {
    this.db = null;
  }

  async init() {
    this.db = await openDB();
    return this;
  }

  /**
   * Add a single entry to the store.
   */
  async add(id, embedding, metadata) {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_NAME, "readwrite");
      const store = tx.objectStore(STORE_NAME);

      store.put({
        id,
        embedding: Array.from(embedding),
        title: metadata.title || "",
        url: metadata.url || "",
        domain: metadata.domain || "",
        visitCount: metadata.visitCount || 0,
        lastVisit: metadata.lastVisit || 0,
      });

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  /**
   * Add entries in bulk (much faster than individual adds).
   */
  async addBatch(entries) {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_NAME, "readwrite");
      const store = tx.objectStore(STORE_NAME);

      for (const { id, embedding, metadata } of entries) {
        store.put({
          id,
          embedding: Array.from(embedding),
          title: metadata.title || "",
          url: metadata.url || "",
          domain: metadata.domain || "",
          visitCount: metadata.visitCount || 0,
          lastVisit: metadata.lastVisit || 0,
        });
      }

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  /**
   * Check if an entry exists by ID.
   */
  async has(id) {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_NAME, "readonly");
      const store = tx.objectStore(STORE_NAME);
      const request = store.getKey(id);

      request.onsuccess = () => resolve(request.result !== undefined);
      request.onerror = () => reject(request.error);
    });
  }

  /**
   * Get the total number of entries.
   */
  async count() {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_NAME, "readonly");
      const store = tx.objectStore(STORE_NAME);
      const request = store.count();

      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  /**
   * Search for the most similar entries to the query embedding.
   */
  async search(queryEmbedding, nResults = 10, domainFilter = null) {
    const queryArr = Array.from(queryEmbedding);

    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_NAME, "readonly");
      const store = tx.objectStore(STORE_NAME);

      const results = [];
      let cursorSource;

      if (domainFilter) {
        const index = store.index("domain");
        cursorSource = index.openCursor(IDBKeyRange.only(domainFilter));
      } else {
        cursorSource = store.openCursor();
      }

      cursorSource.onsuccess = (event) => {
        const cursor = event.target.result;
        if (cursor) {
          const record = cursor.value;
          const similarity = cosineSimilarity(queryArr, record.embedding);

          results.push({
            id: record.id,
            title: record.title,
            url: record.url,
            domain: record.domain,
            visitCount: record.visitCount,
            lastVisit: record.lastVisit,
            similarity,
          });

          cursor.continue();
        } else {
          // Sort by similarity (descending) and return top N
          results.sort((a, b) => b.similarity - a.similarity);
          resolve(results.slice(0, nResults));
        }
      };

      cursorSource.onerror = () => reject(cursorSource.error);
    });
  }

  /**
   * Get metadata about the index (for UI display).
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
   * Clear all entries.
   */
  async clear() {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_NAME, "readwrite");
      const store = tx.objectStore(STORE_NAME);
      const request = store.clear();

      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }
}
