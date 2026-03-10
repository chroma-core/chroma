/**
 * Popup UI logic for Chroma History Search.
 *
 * Communicates with the background service worker to perform
 * semantic searches over indexed browser history.
 */

const searchInput = document.getElementById("search-input");
const domainFilter = document.getElementById("domain-filter");
const searchBtn = document.getElementById("search-btn");
const resultsContainer = document.getElementById("results");
const statusEl = document.getElementById("status");
const entryCountEl = document.getElementById("entry-count");
const reindexBtn = document.getElementById("reindex-btn");

let debounceTimer = null;

/**
 * Format a timestamp into a human-readable relative time.
 */
function formatTime(timestamp) {
  if (!timestamp) return "Unknown";

  const now = Date.now();
  const diff = now - timestamp;

  const minutes = Math.floor(diff / 60000);
  const hours = Math.floor(diff / 3600000);
  const days = Math.floor(diff / 86400000);

  if (minutes < 1) return "Just now";
  if (minutes < 60) return `${minutes}m ago`;
  if (hours < 24) return `${hours}h ago`;
  if (days < 7) return `${days}d ago`;
  if (days < 30) return `${Math.floor(days / 7)}w ago`;

  return new Date(timestamp).toLocaleDateString();
}

/**
 * Render search results.
 */
function renderResults(results) {
  if (!results || results.length === 0) {
    resultsContainer.innerHTML = `
      <div class="empty-state">
        <p>No results found.</p>
        <p class="hint">Try a different search or broaden your query.</p>
      </div>
    `;
    return;
  }

  const html = results.map((result, index) => {
    const similarityWidth = Math.round(result.similarity * 100);

    return `
      <div class="result-item" data-url="${escapeHtml(result.url)}" title="${escapeHtml(result.url)}">
        <div class="result-title">${escapeHtml(result.title)}</div>
        <div class="result-url">${escapeHtml(result.url)}</div>
        <div class="result-meta">
          <span>${escapeHtml(result.domain)}</span>
          <span>${result.visitCount} visit${result.visitCount !== 1 ? "s" : ""}</span>
          <span>${formatTime(result.lastVisit)}</span>
          <span>
            <span class="similarity-bar" style="width: ${similarityWidth}px"></span>
            ${Math.round(result.similarity * 100)}%
          </span>
        </div>
      </div>
    `;
  }).join("");

  resultsContainer.innerHTML = html;

  // Add click handlers to open URLs
  resultsContainer.querySelectorAll(".result-item").forEach((item) => {
    item.addEventListener("click", () => {
      chrome.tabs.create({ url: item.dataset.url });
    });
  });
}

/**
 * Show a loading state.
 */
function showLoading(message = "Searching...") {
  resultsContainer.innerHTML = `
    <div class="loading">
      <div class="spinner"></div>
      <p>${escapeHtml(message)}</p>
    </div>
  `;
}

/**
 * Show an error message.
 */
function showError(message) {
  resultsContainer.innerHTML = `
    <div class="error-message">${escapeHtml(message)}</div>
  `;
}

/**
 * Perform a search.
 */
async function performSearch() {
  const query = searchInput.value.trim();
  if (!query) return;

  const domain = domainFilter.value.trim() || null;

  showLoading("Searching your history...");
  searchBtn.disabled = true;

  try {
    const response = await chrome.runtime.sendMessage({
      type: "search",
      query,
      nResults: 15,
      domainFilter: domain,
    });

    if (response.error) {
      showError(response.error);
    } else {
      renderResults(response.results);
    }
  } catch (error) {
    showError(`Search failed: ${error.message}`);
  } finally {
    searchBtn.disabled = false;
  }
}

/**
 * Update the status display.
 */
async function updateStatus() {
  try {
    const status = await chrome.runtime.sendMessage({ type: "get-status" });

    if (status.error) {
      statusEl.textContent = "Error";
      statusEl.className = "status";
      return;
    }

    if (status.isIndexing) {
      statusEl.textContent = "Indexing...";
      statusEl.className = "status indexing";
    } else if (status.modelReady) {
      statusEl.textContent = "Ready";
      statusEl.className = "status ready";
    } else {
      statusEl.textContent = "Loading model...";
      statusEl.className = "status indexing";
    }

    entryCountEl.textContent = `${status.count.toLocaleString()} entries indexed`;
  } catch {
    statusEl.textContent = "Starting...";
    statusEl.className = "status";
  }
}

/**
 * Escape HTML to prevent XSS.
 */
function escapeHtml(text) {
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

// --- Event Listeners ---

// Search on Enter
searchInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    performSearch();
  }
});

// Search on button click
searchBtn.addEventListener("click", performSearch);

// Debounced search as you type (after 500ms pause)
searchInput.addEventListener("input", () => {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    if (searchInput.value.trim().length >= 3) {
      performSearch();
    }
  }, 500);
});

// Re-index button
reindexBtn.addEventListener("click", () => {
  chrome.runtime.sendMessage({ type: "reindex" });
  statusEl.textContent = "Re-indexing...";
  statusEl.className = "status indexing";
});

// Listen for indexing progress from background
chrome.runtime.onMessage.addListener((message) => {
  if (message.type === "indexing-progress") {
    statusEl.textContent = `Indexing: ${message.percent}%`;
    statusEl.className = "status indexing";
  } else if (message.type === "indexing-complete") {
    statusEl.textContent = "Ready";
    statusEl.className = "status ready";
    entryCountEl.textContent = `${message.count.toLocaleString()} entries indexed`;
  }
});

// Initialize
updateStatus();
