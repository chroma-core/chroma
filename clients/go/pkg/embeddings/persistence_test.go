//go:build ef

package embeddings_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/chromacloud"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/cloudflare"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/cohere"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/gemini"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/hf"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/jina"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/mistral"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/nomic"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/ollama"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/openai"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/together"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/voyage"
)

// TestEmbeddingFunctionPersistence verifies that all embedding functions can be:
// 1. Created with a config
// 2. Serialized via Name() and GetConfig()
// 3. Rebuilt from the serialized config via BuildDense()
// 4. The rebuilt EF has matching name and config
//
// Note: Most EFs return a hardcoded env var name in GetConfig() for security,
// so we set the standard env var for each provider to enable rebuild.

func TestEmbeddingFunctionPersistence_ConsistentHash(t *testing.T) {
	// ConsistentHash doesn't require API keys
	ef := embeddings.NewConsistentHashEmbeddingFunction()

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "consistent_hash", name)
	assert.NotNil(t, config)

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "consistent_hash should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
	assert.Equal(t, config["dim"], rebuilt.GetConfig()["dim"])
}

func TestEmbeddingFunctionPersistence_OpenAI(t *testing.T) {
	// Set the standard env var that GetConfig() returns
	t.Setenv("OPENAI_API_KEY", "test-key-123")

	ef, err := openai.NewOpenAIEmbeddingFunction("", openai.WithEnvAPIKey())
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "openai", name)
	assert.Equal(t, "OPENAI_API_KEY", config["api_key_env_var"])
	assert.NotEmpty(t, config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "openai should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
	assert.Equal(t, config["api_key_env_var"], rebuilt.GetConfig()["api_key_env_var"])
	assert.Equal(t, config["model_name"], rebuilt.GetConfig()["model_name"])
}

func TestEmbeddingFunctionPersistence_OpenAI_WithOptions(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key-123")

	ef, err := openai.NewOpenAIEmbeddingFunction("",
		openai.WithEnvAPIKey(),
		openai.WithModel(openai.TextEmbedding3Large),
		openai.WithDimensions(256),
	)
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "openai", name)
	assert.Equal(t, "OPENAI_API_KEY", config["api_key_env_var"])
	assert.Equal(t, string(openai.TextEmbedding3Large), config["model_name"])
	assert.Equal(t, 256, config["dimensions"])

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	rebuiltConfig := rebuilt.GetConfig()
	assert.Equal(t, name, rebuilt.Name())
	assert.Equal(t, config["api_key_env_var"], rebuiltConfig["api_key_env_var"])
	assert.Equal(t, config["model_name"], rebuiltConfig["model_name"])
	assert.Equal(t, config["dimensions"], rebuiltConfig["dimensions"])
}

func TestEmbeddingFunctionPersistence_Cohere(t *testing.T) {
	t.Setenv("COHERE_API_KEY", "test-key-123")

	ef, err := cohere.NewCohereEmbeddingFunction(cohere.WithEnvAPIKey())
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "cohere", name)
	assert.Equal(t, "COHERE_API_KEY", config["api_key_env_var"])
	assert.NotEmpty(t, config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "cohere should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
	assert.Equal(t, config["model_name"], rebuilt.GetConfig()["model_name"])
}

func TestEmbeddingFunctionPersistence_Jina(t *testing.T) {
	t.Setenv("JINA_API_KEY", "test-key-123")

	ef, err := jina.NewJinaEmbeddingFunction(jina.WithEnvAPIKey())
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "jina", name)
	assert.NotEmpty(t, config["api_key_env_var"])
	assert.NotEmpty(t, config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "jina should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
}

func TestEmbeddingFunctionPersistence_Mistral(t *testing.T) {
	t.Setenv("MISTRAL_API_KEY", "test-key-123")

	ef, err := mistral.NewMistralEmbeddingFunction(mistral.WithEnvAPIKey())
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "mistral", name)
	assert.NotEmpty(t, config["api_key_env_var"])
	assert.NotEmpty(t, config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "mistral should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
}

func TestEmbeddingFunctionPersistence_Gemini(t *testing.T) {
	t.Setenv("GEMINI_API_KEY", "test-key-123")

	ef, err := gemini.NewGeminiEmbeddingFunction(gemini.WithEnvAPIKey())
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "google_genai", name)
	assert.NotEmpty(t, config["api_key_env_var"])
	assert.NotEmpty(t, config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "google_genai should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
}

func TestEmbeddingFunctionPersistence_Voyage(t *testing.T) {
	t.Setenv("VOYAGE_API_KEY", "test-key-123")

	ef, err := voyage.NewVoyageAIEmbeddingFunction(voyage.WithEnvAPIKey())
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "voyageai", name)
	assert.NotEmpty(t, config["api_key_env_var"])
	assert.NotEmpty(t, config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "voyageai should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
}

func TestEmbeddingFunctionPersistence_Ollama(t *testing.T) {
	// Ollama doesn't require API keys, just a base URL
	ef, err := ollama.NewOllamaEmbeddingFunction(
		ollama.WithBaseURL("http://localhost:11434"),
		ollama.WithModel("nomic-embed-text"),
	)
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "ollama", name)
	// Note: Ollama uses "url" not "base_url" in GetConfig
	assert.Equal(t, "http://localhost:11434", config["url"])
	assert.Equal(t, "nomic-embed-text", config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "ollama should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
	assert.Equal(t, config["url"], rebuilt.GetConfig()["url"])
	assert.Equal(t, config["model_name"], rebuilt.GetConfig()["model_name"])
}

func TestEmbeddingFunctionPersistence_HuggingFace(t *testing.T) {
	t.Setenv("HF_API_KEY", "test-key-123")

	ef, err := hf.NewHuggingFaceEmbeddingFunctionFromOptions(
		hf.WithEnvAPIKey(),
		hf.WithModel("sentence-transformers/all-MiniLM-L6-v2"),
	)
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "huggingface", name)
	assert.NotEmpty(t, config["api_key_env_var"])
	assert.Equal(t, "sentence-transformers/all-MiniLM-L6-v2", config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "huggingface should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
}

func TestEmbeddingFunctionPersistence_Cloudflare(t *testing.T) {
	// Cloudflare uses CLOUDFLARE_API_TOKEN as the standard env var
	t.Setenv("CLOUDFLARE_API_TOKEN", "test-key-123")

	ef, err := cloudflare.NewCloudflareEmbeddingFunction(
		cloudflare.WithAPIKeyFromEnvVar("CLOUDFLARE_API_TOKEN"),
		cloudflare.WithAccountID("test-account-id"),
	)
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "cloudflare_workers_ai", name)
	assert.NotEmpty(t, config["api_key_env_var"])
	assert.NotEmpty(t, config["model_name"])
	assert.Equal(t, "test-account-id", config["account_id"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "cloudflare_workers_ai should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
	assert.Equal(t, config["account_id"], rebuilt.GetConfig()["account_id"])
}

func TestEmbeddingFunctionPersistence_Together(t *testing.T) {
	t.Setenv("TOGETHER_API_KEY", "test-key-123")

	ef, err := together.NewTogetherEmbeddingFunction(together.WithEnvAPIToken())
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "together_ai", name)
	assert.NotEmpty(t, config["api_key_env_var"])
	assert.NotEmpty(t, config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "together_ai should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
}

func TestEmbeddingFunctionPersistence_Nomic(t *testing.T) {
	t.Setenv("NOMIC_API_KEY", "test-key-123")

	ef, err := nomic.NewNomicEmbeddingFunction(nomic.WithEnvAPIKey())
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "nomic", name)
	assert.NotEmpty(t, config["api_key_env_var"])
	assert.NotEmpty(t, config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "nomic should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
}

func TestEmbeddingFunctionPersistence_ChromaCloud(t *testing.T) {
	t.Setenv("CHROMA_API_KEY", "test-key-123")

	ef, err := chromacloud.NewEmbeddingFunction(chromacloud.WithEnvAPIKey())
	require.NoError(t, err)

	name := ef.Name()
	config := ef.GetConfig()

	assert.Equal(t, "chroma_cloud", name)
	assert.NotEmpty(t, config["api_key_env_var"])
	assert.NotEmpty(t, config["model_name"])

	// Verify registry has this EF
	assert.True(t, embeddings.HasDense(name), "chroma_cloud should be registered")

	// Rebuild from config
	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF matches
	assert.Equal(t, name, rebuilt.Name())
	assert.Equal(t, config["model_name"], rebuilt.GetConfig()["model_name"])
}

// TestAllRegisteredEFsHaveFactories verifies that all known EF names are registered
func TestAllRegisteredEFsHaveFactories(t *testing.T) {
	expectedEFs := []string{
		"consistent_hash",
		"openai",
		"cohere",
		"jina",
		"mistral",
		"google_genai",
		"voyageai",
		"ollama",
		"huggingface",
		"cloudflare_workers_ai",
		"together_ai",
		"nomic",
		"chroma_cloud",
		"default",            // Primary name (matches Python client)
		"onnx_mini_lm_l6_v2", // Alias for backward compatibility
	}

	for _, name := range expectedEFs {
		t.Run(name, func(t *testing.T) {
			assert.True(t, embeddings.HasDense(name), "%s should be registered in the dense registry", name)
		})
	}
}

// TestCustomEnvVarPersistence verifies that custom env var names are persisted in GetConfig()
// This is critical for the auto-wire feature - users can use custom env var names like
// "MY_OPENAI_KEY" and these should be preserved when the collection is retrieved
func TestCustomEnvVarPersistence(t *testing.T) {
	testCases := []struct {
		name          string
		customEnvVar  string
		createEF      func(envVar string) (embeddings.EmbeddingFunction, error)
		expectedName  string
		defaultEnvVar string
	}{
		{
			name:         "openai",
			customEnvVar: "MY_CUSTOM_OPENAI_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return openai.NewOpenAIEmbeddingFunction("", openai.WithAPIKeyFromEnvVar(envVar))
			},
			expectedName:  "openai",
			defaultEnvVar: "OPENAI_API_KEY",
		},
		{
			name:         "cohere",
			customEnvVar: "MY_CUSTOM_COHERE_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return cohere.NewCohereEmbeddingFunction(cohere.WithAPIKeyFromEnvVar(envVar))
			},
			expectedName:  "cohere",
			defaultEnvVar: "COHERE_API_KEY",
		},
		{
			name:         "jina",
			customEnvVar: "MY_CUSTOM_JINA_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return jina.NewJinaEmbeddingFunction(jina.WithAPIKeyFromEnvVar(envVar))
			},
			expectedName:  "jina",
			defaultEnvVar: "JINA_API_KEY",
		},
		{
			name:         "mistral",
			customEnvVar: "MY_CUSTOM_MISTRAL_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return mistral.NewMistralEmbeddingFunction(mistral.WithAPIKeyFromEnvVar(envVar))
			},
			expectedName:  "mistral",
			defaultEnvVar: "MISTRAL_API_KEY",
		},
		{
			name:         "gemini",
			customEnvVar: "MY_CUSTOM_GEMINI_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return gemini.NewGeminiEmbeddingFunction(gemini.WithAPIKeyFromEnvVar(envVar))
			},
			expectedName:  "google_genai",
			defaultEnvVar: "GEMINI_API_KEY",
		},
		{
			name:         "voyage",
			customEnvVar: "MY_CUSTOM_VOYAGE_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return voyage.NewVoyageAIEmbeddingFunction(voyage.WithAPIKeyFromEnvVar(envVar))
			},
			expectedName:  "voyageai",
			defaultEnvVar: "VOYAGE_API_KEY",
		},
		{
			name:         "together",
			customEnvVar: "MY_CUSTOM_TOGETHER_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return together.NewTogetherEmbeddingFunction(together.WithAPITokenFromEnvVar(envVar))
			},
			expectedName:  "together_ai",
			defaultEnvVar: "TOGETHER_API_KEY",
		},
		{
			name:         "nomic",
			customEnvVar: "MY_CUSTOM_NOMIC_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return nomic.NewNomicEmbeddingFunction(nomic.WithAPIKeyFromEnvVar(envVar))
			},
			expectedName:  "nomic",
			defaultEnvVar: "NOMIC_API_KEY",
		},
		{
			name:         "huggingface",
			customEnvVar: "MY_CUSTOM_HF_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return hf.NewHuggingFaceEmbeddingFunctionFromOptions(
					hf.WithAPIKeyFromEnvVar(envVar),
					hf.WithModel("sentence-transformers/all-MiniLM-L6-v2"),
				)
			},
			expectedName:  "huggingface",
			defaultEnvVar: "HF_API_KEY",
		},
		{
			name:         "cloudflare",
			customEnvVar: "MY_CUSTOM_CF_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return cloudflare.NewCloudflareEmbeddingFunction(
					cloudflare.WithAPIKeyFromEnvVar(envVar),
					cloudflare.WithAccountID("test-account"),
				)
			},
			expectedName:  "cloudflare_workers_ai",
			defaultEnvVar: "CLOUDFLARE_API_TOKEN",
		},
		{
			name:         "chromacloud",
			customEnvVar: "MY_CUSTOM_CHROMA_KEY",
			createEF: func(envVar string) (embeddings.EmbeddingFunction, error) {
				return chromacloud.NewEmbeddingFunction(chromacloud.WithAPIKeyFromEnvVar(envVar))
			},
			expectedName:  "chroma_cloud",
			defaultEnvVar: "CHROMA_API_KEY",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set the custom env var
			t.Setenv(tc.customEnvVar, "test-secret-value")

			// Create EF with custom env var
			ef, err := tc.createEF(tc.customEnvVar)
			require.NoError(t, err)

			// Verify GetConfig() returns the custom env var name, not the default
			config := ef.GetConfig()
			assert.Equal(t, tc.customEnvVar, config["api_key_env_var"],
				"GetConfig() should return custom env var name '%s', not default '%s'",
				tc.customEnvVar, tc.defaultEnvVar)
			assert.Equal(t, tc.expectedName, ef.Name())

			// Verify the EF can be rebuilt using the config
			// This simulates the auto-wire scenario
			rebuilt, err := embeddings.BuildDense(ef.Name(), config)
			require.NoError(t, err)
			require.NotNil(t, rebuilt)

			// The rebuilt EF should also have the custom env var
			rebuiltConfig := rebuilt.GetConfig()
			assert.Equal(t, tc.customEnvVar, rebuiltConfig["api_key_env_var"],
				"Rebuilt EF should preserve custom env var name '%s'", tc.customEnvVar)
		})
	}
}

// TestConfigRoundTrip tests that config can be serialized to JSON and back
func TestConfigRoundTrip(t *testing.T) {
	// Set all required env vars for the test
	t.Setenv("OPENAI_API_KEY", "test-key-123")

	testCases := []struct {
		name     string
		createEF func() (embeddings.EmbeddingFunction, error)
	}{
		{
			name: "consistent_hash",
			createEF: func() (embeddings.EmbeddingFunction, error) {
				return embeddings.NewConsistentHashEmbeddingFunction(), nil
			},
		},
		{
			name: "openai",
			createEF: func() (embeddings.EmbeddingFunction, error) {
				return openai.NewOpenAIEmbeddingFunction("", openai.WithEnvAPIKey())
			},
		},
		{
			name: "ollama",
			createEF: func() (embeddings.EmbeddingFunction, error) {
				return ollama.NewOllamaEmbeddingFunction(
					ollama.WithBaseURL("http://localhost:11434"),
					ollama.WithModel("nomic-embed-text"),
				)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ef, err := tc.createEF()
			require.NoError(t, err)

			name := ef.Name()
			config := ef.GetConfig()

			// Verify we can rebuild
			rebuilt, err := embeddings.BuildDense(name, config)
			require.NoError(t, err)
			require.NotNil(t, rebuilt)

			// Names should match
			assert.Equal(t, name, rebuilt.Name())

			// Get rebuilt config and compare key fields
			rebuiltConfig := rebuilt.GetConfig()
			for key, val := range config {
				if key != "api_key" { // Skip sensitive fields
					assert.Equal(t, val, rebuiltConfig[key], "config key %s should match", key)
				}
			}
		})
	}
}

// Negative tests for EF persistence failure paths

func TestBuildDense_UnknownName(t *testing.T) {
	config := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": "SOME_API_KEY",
	}

	ef, err := embeddings.BuildDense("nonexistent_provider_xyz", config)
	assert.Error(t, err)
	assert.Nil(t, ef)
	assert.Contains(t, err.Error(), "unknown")
}

func TestBuildDense_MissingEnvVar(t *testing.T) {
	// Ensure the env var is NOT set
	t.Setenv("OPENAI_API_KEY", "")

	config := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": "OPENAI_API_KEY",
		"model_name":      "text-embedding-3-small",
	}

	ef, err := embeddings.BuildDense("openai", config)
	assert.Error(t, err)
	assert.Nil(t, ef)
	assert.Contains(t, err.Error(), "OPENAI_API_KEY")
}

func TestBuildDense_MissingRequiredConfig(t *testing.T) {
	// Missing api_key_env_var should cause failure
	config := embeddings.EmbeddingFunctionConfig{
		"model_name": "text-embedding-3-small",
		// Missing api_key_env_var
	}

	ef, err := embeddings.BuildDense("openai", config)
	assert.Error(t, err)
	assert.Nil(t, ef)
}

func TestBuildDense_EmptyConfig(t *testing.T) {
	// Empty config for a provider that requires config
	config := embeddings.EmbeddingFunctionConfig{}

	ef, err := embeddings.BuildDense("openai", config)
	assert.Error(t, err)
	assert.Nil(t, ef)
}

func TestBuildDense_ConsistentHashAlwaysWorks(t *testing.T) {
	// consistent_hash doesn't require env vars, should always work
	config := embeddings.EmbeddingFunctionConfig{
		"dim": float64(128),
	}

	ef, err := embeddings.BuildDense("consistent_hash", config)
	assert.NoError(t, err)
	assert.NotNil(t, ef)
	assert.Equal(t, "consistent_hash", ef.Name())
}

func TestBuildDense_NilConfig(t *testing.T) {
	// nil config for consistent_hash should work (uses defaults)
	ef, err := embeddings.BuildDense("consistent_hash", nil)
	assert.NoError(t, err)
	assert.NotNil(t, ef)
}

func TestBuildDense_NilConfigForProviderRequiringConfig(t *testing.T) {
	// nil config for OpenAI should fail
	ef, err := embeddings.BuildDense("openai", nil)
	assert.Error(t, err)
	assert.Nil(t, ef)
}

func TestBuildDense_WrongEnvVarSet(t *testing.T) {
	// Set a different env var than what the config expects
	t.Setenv("DIFFERENT_API_KEY", "some-value")
	t.Setenv("OPENAI_API_KEY", "") // Ensure target is not set

	config := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": "OPENAI_API_KEY",
		"model_name":      "text-embedding-3-small",
	}

	ef, err := embeddings.BuildDense("openai", config)
	assert.Error(t, err)
	assert.Nil(t, ef)
	assert.Contains(t, err.Error(), "OPENAI_API_KEY")
}

func TestHasDense_UnknownProvider(t *testing.T) {
	// HasDense should return false for unknown providers
	assert.False(t, embeddings.HasDense("unknown_provider_xyz"))
	assert.False(t, embeddings.HasDense(""))
	assert.False(t, embeddings.HasDense("not_a_real_ef"))
}

func TestHasDense_KnownProviders(t *testing.T) {
	// HasDense should return true for known providers
	knownProviders := []string{
		"openai",
		"cohere",
		"jina",
		"mistral",
		"google_genai",
		"voyageai",
		"huggingface",
		"cloudflare_workers_ai",
		"together_ai",
		"nomic",
		"chroma_cloud",
		"ollama",
		"consistent_hash",
	}

	for _, provider := range knownProviders {
		assert.True(t, embeddings.HasDense(provider), "Provider %s should be registered", provider)
	}
}

func TestEFPersistence_FailureRecovery(t *testing.T) {
	// Test that after a failure, we can still create EFs with correct config
	t.Setenv("OPENAI_API_KEY", "") // Start with no key

	config := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": "OPENAI_API_KEY",
		"model_name":      "text-embedding-3-small",
	}

	// First attempt should fail
	ef1, err := embeddings.BuildDense("openai", config)
	assert.Error(t, err)
	assert.Nil(t, ef1)

	// Now set the key
	t.Setenv("OPENAI_API_KEY", "now-valid-key")

	// Second attempt should succeed
	ef2, err := embeddings.BuildDense("openai", config)
	assert.NoError(t, err)
	assert.NotNil(t, ef2)
	assert.Equal(t, "openai", ef2.Name())
}

// Test insecure flag persistence and env var fallback

func TestInsecureConfigPersistence_OpenAI(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key-123")

	// Create EF with insecure mode and HTTP URL
	ef, err := openai.NewOpenAIEmbeddingFunction("",
		openai.WithEnvAPIKey(),
		openai.WithBaseURL("http://localhost:8080"),
		openai.WithInsecure(),
	)
	require.NoError(t, err)

	// Verify insecure flag is in config
	config := ef.GetConfig()
	assert.Equal(t, true, config["insecure"])
	assert.Equal(t, "http://localhost:8080", config["api_base"])

	// Rebuild from config - should succeed because insecure: true is in config
	rebuilt, err := embeddings.BuildDense("openai", config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	// Verify rebuilt EF preserves insecure flag
	rebuiltConfig := rebuilt.GetConfig()
	assert.Equal(t, true, rebuiltConfig["insecure"])
	assert.Equal(t, "http://localhost:8080", rebuiltConfig["api_base"])
}

func TestInsecureConfigPersistence_Jina(t *testing.T) {
	t.Setenv("JINA_API_KEY", "test-key-123")

	ef, err := jina.NewJinaEmbeddingFunction(
		jina.WithEnvAPIKey(),
		jina.WithEmbeddingEndpoint("http://localhost:8080"),
		jina.WithInsecure(),
	)
	require.NoError(t, err)

	config := ef.GetConfig()
	assert.Equal(t, true, config["insecure"])
	assert.Equal(t, "http://localhost:8080", config["base_url"])

	rebuilt, err := embeddings.BuildDense("jina", config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	rebuiltConfig := rebuilt.GetConfig()
	assert.Equal(t, true, rebuiltConfig["insecure"])
}

func TestInsecureConfigPersistence_Nomic(t *testing.T) {
	t.Setenv("NOMIC_API_KEY", "test-key-123")

	ef, err := nomic.NewNomicEmbeddingFunction(
		nomic.WithEnvAPIKey(),
		nomic.WithBaseURL("http://localhost:8080"),
		nomic.WithInsecure(),
	)
	require.NoError(t, err)

	config := ef.GetConfig()
	assert.Equal(t, true, config["insecure"])
	assert.Equal(t, "http://localhost:8080", config["base_url"])

	rebuilt, err := embeddings.BuildDense("nomic", config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	rebuiltConfig := rebuilt.GetConfig()
	assert.Equal(t, true, rebuiltConfig["insecure"])
	assert.Equal(t, "http://localhost:8080", rebuiltConfig["base_url"])
}

func TestInsecureEnvVarFallback_OpenAI(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key-123")
	t.Setenv(embeddings.AllowInsecureEnvVar, "true")

	// Config with HTTP URL but WITHOUT insecure: true
	config := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": "OPENAI_API_KEY",
		"model_name":      "text-embedding-3-small",
		"api_base":        "http://localhost:8080",
		// Note: no "insecure": true
	}

	// Should succeed because env var fallback is enabled
	ef, err := embeddings.BuildDense("openai", config)
	require.NoError(t, err)
	require.NotNil(t, ef)
}

func TestInsecureEnvVarFallback_Disabled(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key-123")
	t.Setenv(embeddings.AllowInsecureEnvVar, "") // Ensure env var is NOT set

	// Config with HTTP URL but WITHOUT insecure: true
	config := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": "OPENAI_API_KEY",
		"model_name":      "text-embedding-3-small",
		"api_base":        "http://localhost:8080",
	}

	// Should fail because neither insecure config nor env var is set
	ef, err := embeddings.BuildDense("openai", config)
	require.Error(t, err)
	require.Nil(t, ef)
	assert.Contains(t, err.Error(), "HTTPS")
}

func TestInsecureConfigFalse_NotPersisted(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key-123")

	// Create EF without insecure flag (default HTTPS URL)
	ef, err := openai.NewOpenAIEmbeddingFunction("", openai.WithEnvAPIKey())
	require.NoError(t, err)

	// insecure should NOT be present in config when false (Go zero value)
	config := ef.GetConfig()
	_, hasInsecure := config["insecure"]
	assert.False(t, hasInsecure, "insecure key should not be present when false")

	// Rebuild should still work (HTTPS URL)
	rebuilt, err := embeddings.BuildDense("openai", config)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)
}
