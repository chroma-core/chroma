/**
 * Background service worker for Chroma History Search.
 *
 * Handles:
 * - Initial indexing of browser history on install
 * - Periodic incremental indexing of new history
 * - Message passing between popup and background
 */

import { VectorStore } from "./lib/vector-store.js";
import { initEmbeddings, embedBatch, isReady } from "./lib/embeddings.js";

const HISTORY_DAYS = 90; // Index last 90 days of history
const INDEX_ALARM = "index-history";
const INDEX_INTERVAL_MINUTES = 60; // Re-index every hour

let indexingInProgress = false;

/**
 * Fetch browser history entries from the Chrome History API.
 */
async function fetchHistory(startTime) {
  return new Promise((resolve) => {
    chrome.history.search(
      {
        text: "",
        startTime,
        maxResults: 100000,
      },
      (results) => {
        // Filter out entries without titles and common noise
        const filtered = results.filter(
          (item) =>
            item.title &&
            item.title.trim() !== "" &&
            !item.url.startsWith("chrome://") &&
            !item.url.startsWith("chrome-extension://") &&
            !item.url.startsWith("about:") &&
            !item.url.startsWith("edge://")
        );
        resolve(filtered);
      }
    );
  });
}

/**
 * Index browser history into the vector store.
 */
async function indexHistory(fullReindex = false) {
  if (indexingInProgress) {
    console.log("[Chroma] Indexing already in progress, skipping.");
    return;
  }

  indexingInProgress = true;

  try {
    console.log("[Chroma] Starting history indexing...");

    // Initialize embedding model (downloads on first run)
    await initEmbeddings((progress) => {
      console.log("[Chroma] Model:", progress.message || progress.status);
    });

    const store = await new VectorStore().init();

    // Determine time range
    let startTime;
    if (fullReindex) {
      await store.clear();
      startTime = Date.now() - HISTORY_DAYS * 24 * 60 * 60 * 1000;
    } else {
      const lastIndexed = await store.getMeta("lastIndexedTime");
      startTime = lastIndexed || Date.now() - HISTORY_DAYS * 24 * 60 * 60 * 1000;
    }

    // Fetch history
    const historyItems = await fetchHistory(startTime);
    console.log(`[Chroma] Found ${historyItems.length} history entries to index`);

    if (historyItems.length === 0) {
      await store.setMeta("lastIndexedTime", Date.now());
      return;
    }

    // Filter out already-indexed entries
    const newItems = [];
    for (const item of historyItems) {
      const id = `url_${hashCode(item.url)}`;
      const exists = await store.has(id);
      if (!exists) {
        newItems.push(item);
      }
    }

    console.log(`[Chroma] ${newItems.length} new entries to embed`);

    if (newItems.length === 0) {
      await store.setMeta("lastIndexedTime", Date.now());
      return;
    }

    // Create text for embedding (title + URL for better semantic matching)
    const texts = newItems.map(
      (item) => `${item.title} - ${new URL(item.url).hostname}${new URL(item.url).pathname}`
    );

    // Generate embeddings
    const embeddings = await embedBatch(texts, 16, (progress) => {
      console.log(`[Chroma] Embedding: ${progress.percent}%`);
      // Notify popup if open
      chrome.runtime.sendMessage({
        type: "indexing-progress",
        ...progress,
      }).catch(() => {});
    });

    // Store in batches
    const batchSize = 100;
    for (let i = 0; i < newItems.length; i += batchSize) {
      const batch = [];
      for (let j = i; j < Math.min(i + batchSize, newItems.length); j++) {
        const item = newItems[j];
        let domain;
        try {
          domain = new URL(item.url).hostname;
        } catch {
          domain = "";
        }

        batch.push({
          id: `url_${hashCode(item.url)}`,
          embedding: embeddings[j],
          metadata: {
            title: item.title,
            url: item.url,
            domain,
            visitCount: item.visitCount || 0,
            lastVisit: item.lastVisitTime || 0,
          },
        });
      }
      await store.addBatch(batch);
    }

    // Persist the Chroma collection to IndexedDB
    await store.persist();
    await store.setMeta("lastIndexedTime", Date.now());
    const totalCount = await store.count();
    console.log(`[Chroma] Indexing complete. Total entries: ${totalCount}`);

    // Notify popup
    chrome.runtime.sendMessage({
      type: "indexing-complete",
      count: totalCount,
      newEntries: newItems.length,
    }).catch(() => {});
  } catch (error) {
    console.error("[Chroma] Indexing error:", error);
  } finally {
    indexingInProgress = false;
  }
}

/**
 * Simple string hash for creating IDs.
 */
function hashCode(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash |= 0; // Convert to 32-bit integer
  }
  return Math.abs(hash).toString(36);
}

// --- Event Listeners ---

// Index on install
chrome.runtime.onInstalled.addListener((details) => {
  console.log("[Chroma] Extension installed, starting initial index...");

  // Set up periodic indexing
  chrome.alarms.create(INDEX_ALARM, {
    periodInMinutes: INDEX_INTERVAL_MINUTES,
  });

  // Start initial indexing
  indexHistory(true);
});

// Periodic re-indexing
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === INDEX_ALARM) {
    indexHistory(false);
  }
});

// Handle messages from popup
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "search") {
    handleSearch(message.query, message.nResults, message.domainFilter)
      .then(sendResponse)
      .catch((err) => sendResponse({ error: err.message }));
    return true; // Keep the message channel open for async response
  }

  if (message.type === "get-status") {
    handleGetStatus()
      .then(sendResponse)
      .catch((err) => sendResponse({ error: err.message }));
    return true;
  }

  if (message.type === "reindex") {
    indexHistory(true);
    sendResponse({ status: "started" });
    return false;
  }
});

/**
 * Handle a search request from the popup.
 */
async function handleSearch(query, nResults = 10, domainFilter = null) {
  if (!isReady()) {
    await initEmbeddings();
  }

  const { embed } = await import("./lib/embeddings.js");
  const queryEmbedding = await embed(query);

  const store = await new VectorStore().init();
  const results = await store.search(queryEmbedding, nResults, domainFilter);

  return { results };
}

/**
 * Get the current status of the index.
 */
async function handleGetStatus() {
  const store = await new VectorStore().init();
  const count = await store.count();
  const lastIndexed = await store.getMeta("lastIndexedTime");

  return {
    count,
    lastIndexed,
    isIndexing: indexingInProgress,
    modelReady: isReady(),
  };
}
