//go:build ef

package embeddings_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
	// Import all providers to register them
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/bm25"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/chromacloud"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/chromacloudsplade"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/cloudflare"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/cohere"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/default_ef"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/gemini"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/hf"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/jina"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/mistral"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/morph"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/nomic"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/ollama"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/openai"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/together"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/voyage"
)

// TestBuildDenseFromJSON tests that dense embedding functions can be instantiated
// from JSON configs as would be received from Chroma server.
func TestBuildDenseFromJSON(t *testing.T) {
	// Test cases for each dense embedding function with sample JSON configs
	testCases := []struct {
		name           string
		efName         string
		jsonConfig     string
		requiresAPIKey bool
		envVar         string
	}{
		{
			name:   "OpenAI",
			efName: "openai",
			jsonConfig: `{
				"api_key_env_var": "OPENAI_API_KEY",
				"model_name": "text-embedding-3-small",
				"dimensions": 1536
			}`,
			requiresAPIKey: true,
			envVar:         "OPENAI_API_KEY",
		},
		{
			name:   "Cohere",
			efName: "cohere",
			jsonConfig: `{
				"api_key_env_var": "COHERE_API_KEY",
				"model_name": "embed-english-v3.0"
			}`,
			requiresAPIKey: true,
			envVar:         "COHERE_API_KEY",
		},
		{
			name:   "Jina",
			efName: "jina",
			jsonConfig: `{
				"api_key_env_var": "JINA_API_KEY",
				"model_name": "jina-embeddings-v3"
			}`,
			requiresAPIKey: true,
			envVar:         "JINA_API_KEY",
		},
		{
			name:   "VoyageAI",
			efName: "voyageai",
			jsonConfig: `{
				"api_key_env_var": "VOYAGE_API_KEY",
				"model_name": "voyage-3"
			}`,
			requiresAPIKey: true,
			envVar:         "VOYAGE_API_KEY",
		},
		{
			name:   "Mistral",
			efName: "mistral",
			jsonConfig: `{
				"api_key_env_var": "MISTRAL_API_KEY",
				"model_name": "mistral-embed"
			}`,
			requiresAPIKey: true,
			envVar:         "MISTRAL_API_KEY",
		},
		{
			name:   "Morph",
			efName: "morph",
			jsonConfig: `{
				"api_key_env_var": "MORPH_API_KEY",
				"model_name": "morph-embedding-v2"
			}`,
			requiresAPIKey: true,
			envVar:         "MORPH_API_KEY",
		},
		{
			name:   "Nomic",
			efName: "nomic",
			jsonConfig: `{
				"api_key_env_var": "NOMIC_API_KEY",
				"model_name": "nomic-embed-text-v1.5"
			}`,
			requiresAPIKey: true,
			envVar:         "NOMIC_API_KEY",
		},
		{
			name:   "TogetherAI",
			efName: "together_ai",
			jsonConfig: `{
				"api_key_env_var": "TOGETHER_API_KEY",
				"model_name": "BAAI/bge-large-en-v1.5"
			}`,
			requiresAPIKey: true,
			envVar:         "TOGETHER_API_KEY",
		},
		{
			name:   "HuggingFace",
			efName: "huggingface",
			jsonConfig: `{
				"api_key_env_var": "HF_API_KEY",
				"model_name": "sentence-transformers/all-MiniLM-L6-v2"
			}`,
			requiresAPIKey: true,
			envVar:         "HF_API_KEY",
		},
		{
			name:   "Gemini",
			efName: "google_genai",
			jsonConfig: `{
				"api_key_env_var": "GEMINI_API_KEY",
				"model_name": "text-embedding-004"
			}`,
			requiresAPIKey: true,
			envVar:         "GEMINI_API_KEY",
		},
		{
			name:   "ChromaCloud",
			efName: "chroma_cloud",
			jsonConfig: `{
				"api_key_env_var": "CHROMA_API_KEY",
				"model_name": "chroma-embedding-v1"
			}`,
			requiresAPIKey: true,
			envVar:         "CHROMA_API_KEY",
		},
		{
			name:   "Ollama",
			efName: "ollama",
			jsonConfig: `{
				"model_name": "nomic-embed-text",
				"url": "http://localhost:11434"
			}`,
			requiresAPIKey: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Verify the EF is registered
			require.True(t, embeddings.HasDense(tc.efName), "EF %s should be registered", tc.efName)

			// Parse JSON config
			var cfg embeddings.EmbeddingFunctionConfig
			err := json.Unmarshal([]byte(tc.jsonConfig), &cfg)
			require.NoError(t, err, "JSON should parse successfully")

			if tc.requiresAPIKey {
				// Save current env var value (CI may have it set) and unset it for this test
				originalValue := os.Getenv(tc.envVar)
				os.Unsetenv(tc.envVar)

				// Without API key set, should get an error
				_, err = embeddings.BuildDense(tc.efName, cfg)
				require.Error(t, err, "Should fail without API key")
				assert.Contains(t, err.Error(), "not set", "Error should mention env var not set")

				// With API key set (using a dummy value), should succeed in creating the EF
				// (actual API calls would fail, but instantiation should work)
				os.Setenv(tc.envVar, "test-api-key-dummy")

				ef, err := embeddings.BuildDense(tc.efName, cfg)
				require.NoError(t, err, "Should succeed with API key set")
				require.NotNil(t, ef)
				assert.Equal(t, tc.efName, ef.Name())

				// Restore original env var value
				if originalValue != "" {
					os.Setenv(tc.envVar, originalValue)
				} else {
					os.Unsetenv(tc.envVar)
				}
			} else {
				// For EFs without API key requirement (like Ollama)
				ef, err := embeddings.BuildDense(tc.efName, cfg)
				require.NoError(t, err, "Should succeed without API key")
				require.NotNil(t, ef)
				assert.Equal(t, tc.efName, ef.Name())
			}
		})
	}
}

// TestBuildSparseFromJSON tests that sparse embedding functions can be instantiated
// from JSON configs as would be received from Chroma server.
func TestBuildSparseFromJSON(t *testing.T) {
	testCases := []struct {
		name           string
		efName         string
		expectedName   string // Name() may differ from registration name (aliases)
		jsonConfig     string
		requiresAPIKey bool
		envVar         string
	}{
		{
			name:         "BM25",
			efName:       "chroma_bm25",
			expectedName: "chroma_bm25",
			jsonConfig: `{
				"k": 1.2,
				"b": 0.75,
				"avg_len": 256.0,
				"token_max_length": 40,
				"stopwords": ["the", "a", "an"],
				"include_tokens": false
			}`,
			requiresAPIKey: false,
		},
		{
			name:         "BM25_alias",
			efName:       "bm25",
			expectedName: "chroma_bm25",
			jsonConfig: `{
				"k": 1.2,
				"b": 0.75
			}`,
			requiresAPIKey: false,
		},
		{
			name:         "ChromaCloudSplade",
			efName:       "chroma-cloud-splade",
			expectedName: "chroma-cloud-splade",
			jsonConfig: `{
				"api_key_env_var": "CHROMA_API_KEY",
				"model": "prithivida/Splade_PP_en_v1"
			}`,
			requiresAPIKey: true,
			envVar:         "CHROMA_API_KEY",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Verify the EF is registered
			require.True(t, embeddings.HasSparse(tc.efName), "EF %s should be registered", tc.efName)

			// Parse JSON config
			var cfg embeddings.EmbeddingFunctionConfig
			err := json.Unmarshal([]byte(tc.jsonConfig), &cfg)
			require.NoError(t, err, "JSON should parse successfully")

			if tc.requiresAPIKey {
				// Save current env var value (CI may have it set) and unset it for this test
				originalValue := os.Getenv(tc.envVar)
				os.Unsetenv(tc.envVar)

				// Without API key set, should get an error
				_, err = embeddings.BuildSparse(tc.efName, cfg)
				require.Error(t, err, "Should fail without API key")

				// With API key set
				os.Setenv(tc.envVar, "test-api-key-dummy")

				ef, err := embeddings.BuildSparse(tc.efName, cfg)
				require.NoError(t, err, "Should succeed with API key set")
				require.NotNil(t, ef)
				assert.Equal(t, tc.expectedName, ef.Name())

				// Restore original env var value
				if originalValue != "" {
					os.Setenv(tc.envVar, originalValue)
				} else {
					os.Unsetenv(tc.envVar)
				}
			} else {
				// For EFs without API key requirement (like BM25)
				ef, err := embeddings.BuildSparse(tc.efName, cfg)
				require.NoError(t, err, "Should succeed without API key")
				require.NotNil(t, ef)
				assert.Equal(t, tc.expectedName, ef.Name())
			}
		})
	}
}

// TestBM25FullJSONRoundTrip tests the complete flow for BM25:
// JSON config -> BuildSparse -> embed -> verify output
func TestBM25FullJSONRoundTrip(t *testing.T) {
	// Sample JSON config as would come from Chroma server
	jsonConfig := `{
		"k": 1.5,
		"b": 0.8,
		"avg_len": 200.0,
		"token_max_length": 50,
		"include_tokens": true
	}`

	// Parse JSON
	var cfg embeddings.EmbeddingFunctionConfig
	err := json.Unmarshal([]byte(jsonConfig), &cfg)
	require.NoError(t, err)

	// Build EF using registry
	ef, err := embeddings.BuildSparse("bm25", cfg)
	require.NoError(t, err)
	require.NotNil(t, ef)

	// Verify we can embed
	ctx := context.Background()
	sv, err := ef.EmbedQuerySparse(ctx, "hello world test")
	require.NoError(t, err)
	require.NotNil(t, sv)
	assert.Greater(t, len(sv.Indices), 0)
	assert.Equal(t, len(sv.Indices), len(sv.Values))
	assert.Equal(t, len(sv.Indices), len(sv.Labels)) // include_tokens=true

	// Verify config was applied by checking GetConfig
	resultCfg := ef.GetConfig()
	assert.Equal(t, 1.5, resultCfg["k"])
	assert.Equal(t, 0.8, resultCfg["b"])
	assert.Equal(t, 200.0, resultCfg["avg_len"])
	assert.Equal(t, 50, resultCfg["token_max_length"])
	assert.Equal(t, true, resultCfg["include_tokens"])
}

// TestAllRegisteredProvidersHaveFactories verifies all expected providers are registered
func TestAllRegisteredProvidersHaveFactories(t *testing.T) {
	expectedDense := []string{
		"openai",
		"cohere",
		"jina",
		"voyageai",
		"mistral",
		"morph",
		"nomic",
		"together_ai",
		"huggingface",
		"google_genai",
		"chroma_cloud",
		"ollama",
		"cloudflare_workers_ai",
		"default",            // Primary name (matches Python client)
		"onnx_mini_lm_l6_v2", // Alias for backward compatibility
	}

	expectedSparse := []string{
		"chroma_bm25",         // Primary name (matches Python client)
		"bm25",                // Alias for backward compatibility
		"chroma-cloud-splade",
	}

	registeredDense := embeddings.ListDense()
	for _, name := range expectedDense {
		assert.Contains(t, registeredDense, name, "Dense EF %s should be registered", name)
	}

	registeredSparse := embeddings.ListSparse()
	for _, name := range expectedSparse {
		assert.Contains(t, registeredSparse, name, "Sparse EF %s should be registered", name)
	}
}
