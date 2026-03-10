/**
 * WASM-based text embedding using Transformers.js (ONNX Runtime WASM).
 *
 * Uses the all-MiniLM-L6-v2 model (~23MB) which runs entirely in the browser
 * via WebAssembly. No data is sent to external servers.
 *
 * Transformers.js: https://huggingface.co/docs/transformers.js
 */

// We dynamically import Transformers.js from a CDN.
// In production, you'd bundle this with the extension.
let pipeline = null;
let embedder = null;

const MODEL_NAME = "Xenova/all-MiniLM-L6-v2";

/**
 * Initialize the embedding pipeline. Downloads the model on first use
 * (~23MB), then caches it in the browser's Cache API.
 *
 * @param {function} onProgress - Optional progress callback
 * @returns {Promise<void>}
 */
export async function initEmbeddings(onProgress = null) {
  if (embedder) return;

  // Dynamically import Transformers.js
  const { pipeline: createPipeline, env } = await import(
    "https://cdn.jsdelivr.net/npm/@xenova/transformers@2.17.2"
  );

  // Configure to use WASM backend
  env.backends.onnx.wasm.numThreads = 1;

  pipeline = createPipeline;

  if (onProgress) {
    onProgress({ status: "loading", message: "Loading embedding model..." });
  }

  embedder = await pipeline("feature-extraction", MODEL_NAME, {
    progress_callback: onProgress,
    quantized: true, // Use quantized model for smaller size + faster inference
  });

  if (onProgress) {
    onProgress({ status: "ready", message: "Model loaded!" });
  }
}

/**
 * Generate an embedding for a single text string.
 *
 * @param {string} text - Text to embed
 * @returns {Promise<Float32Array>} - Embedding vector
 */
export async function embed(text) {
  if (!embedder) {
    throw new Error("Embeddings not initialized. Call initEmbeddings() first.");
  }

  const output = await embedder(text, {
    pooling: "mean",
    normalize: true,
  });

  return output.data;
}

/**
 * Generate embeddings for multiple texts (batched for efficiency).
 *
 * @param {string[]} texts - Array of texts to embed
 * @param {number} batchSize - Number of texts to process at once
 * @param {function} onProgress - Optional progress callback
 * @returns {Promise<Float32Array[]>} - Array of embedding vectors
 */
export async function embedBatch(texts, batchSize = 32, onProgress = null) {
  if (!embedder) {
    throw new Error("Embeddings not initialized. Call initEmbeddings() first.");
  }

  const results = [];

  for (let i = 0; i < texts.length; i += batchSize) {
    const batch = texts.slice(i, i + batchSize);

    for (const text of batch) {
      const output = await embedder(text, {
        pooling: "mean",
        normalize: true,
      });
      results.push(output.data);
    }

    if (onProgress) {
      const processed = Math.min(i + batchSize, texts.length);
      onProgress({
        status: "embedding",
        processed,
        total: texts.length,
        percent: Math.round((processed / texts.length) * 100),
      });
    }
  }

  return results;
}

/**
 * Check if the embedding model is loaded and ready.
 */
export function isReady() {
  return embedder !== null;
}
