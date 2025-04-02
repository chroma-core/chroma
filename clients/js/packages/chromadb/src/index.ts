/**
 * Re-exports functionality from the core package with bundled dependencies
 */

// Import core components
import * as core from '@internal/chromadb-core';

// Force inclusion of all embedding packages in the bundle
require('openai');
require('@google/generative-ai');
require('@xenova/transformers');
require('chromadb-default-embed');
require('cohere-ai');
require('voyageai');
require('ollama');

// Re-export everything from core
export * from '@internal/chromadb-core';