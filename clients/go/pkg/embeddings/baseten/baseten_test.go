//go:build ef

package baseten

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

func TestNewBasetenEmbeddingFunction_RequiresBaseURL(t *testing.T) {
	_, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "base URL is required")
}

func TestNewBasetenEmbeddingFunction_RequiresAPIKey(t *testing.T) {
	_, err := NewBasetenEmbeddingFunction(
		WithBaseURL("https://example.baseten.co"),
	)
	require.Error(t, err)
}

func TestNewBasetenEmbeddingFunction_HTTPRejectedWithoutInsecure(t *testing.T) {
	_, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("http://localhost:8000"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "base URL must use HTTPS")
}

func TestNewBasetenEmbeddingFunction_HTTPAcceptedWithInsecure(t *testing.T) {
	_, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("http://localhost:8000"),
		WithInsecure(),
	)
	require.NoError(t, err)
}

func TestNewBasetenEmbeddingFunction_HTTPSAccepted(t *testing.T) {
	_, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("https://example.baseten.co"),
	)
	require.NoError(t, err)
}

func TestNewBasetenEmbeddingFunction_WithModelID(t *testing.T) {
	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("https://example.baseten.co"),
		WithModelID("my-model"),
	)
	require.NoError(t, err)
	require.Equal(t, "my-model", ef.apiClient.Model)
}

func TestBasetenEmbeddingFunction_Name(t *testing.T) {
	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("https://example.baseten.co"),
	)
	require.NoError(t, err)
	require.Equal(t, "baseten", ef.Name())
}

func TestBasetenEmbeddingFunction_DefaultSpace(t *testing.T) {
	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("https://example.baseten.co"),
	)
	require.NoError(t, err)
	require.Equal(t, embeddings.COSINE, ef.DefaultSpace())
}

func TestBasetenEmbeddingFunction_SupportedSpaces(t *testing.T) {
	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("https://example.baseten.co"),
	)
	require.NoError(t, err)
	spaces := ef.SupportedSpaces()
	require.Contains(t, spaces, embeddings.COSINE)
	require.Contains(t, spaces, embeddings.L2)
	require.Contains(t, spaces, embeddings.IP)
}

func TestBasetenEmbeddingFunction_GetConfig(t *testing.T) {
	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("https://example.baseten.co"),
		WithModelID("test-model"),
	)
	require.NoError(t, err)

	cfg := ef.GetConfig()
	require.Equal(t, APIKeyEnvVar, cfg["api_key_env_var"])
	require.Equal(t, "https://example.baseten.co", cfg["api_base"])
	require.Equal(t, "test-model", cfg["model_name"])
}

func TestBasetenEmbeddingFunction_GetConfigWithInsecure(t *testing.T) {
	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("http://localhost:8000"),
		WithInsecure(),
	)
	require.NoError(t, err)

	cfg := ef.GetConfig()
	require.Equal(t, true, cfg["insecure"])
}

func TestBasetenEmbeddingFunction_GetConfigWithEnvVar(t *testing.T) {
	customEnvVar := "MY_CUSTOM_BASETEN_KEY"
	t.Setenv(customEnvVar, "test-api-key")

	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKeyFromEnvVar(customEnvVar),
		WithBaseURL("https://example.baseten.co"),
	)
	require.NoError(t, err)

	cfg := ef.GetConfig()
	require.Equal(t, customEnvVar, cfg["api_key_env_var"])
}

func TestWithEnvAPIKey_Success(t *testing.T) {
	t.Setenv(APIKeyEnvVar, "test-api-key")

	ef, err := NewBasetenEmbeddingFunction(
		WithEnvAPIKey(),
		WithBaseURL("https://example.baseten.co"),
	)
	require.NoError(t, err)
	require.Equal(t, APIKeyEnvVar, ef.apiClient.APIKeyEnvVar)
}

func TestWithEnvAPIKey_NotSet(t *testing.T) {
	os.Unsetenv(APIKeyEnvVar)

	_, err := NewBasetenEmbeddingFunction(
		WithEnvAPIKey(),
		WithBaseURL("https://example.baseten.co"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), APIKeyEnvVar+" not set")
}

func TestWithAPIKeyFromEnvVar_CustomEnvVar(t *testing.T) {
	customEnvVar := "CUSTOM_BASETEN_KEY"
	t.Setenv(customEnvVar, "test-api-key")

	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKeyFromEnvVar(customEnvVar),
		WithBaseURL("https://example.baseten.co"),
	)
	require.NoError(t, err)
	require.Equal(t, customEnvVar, ef.apiClient.APIKeyEnvVar)
}

func TestWithAPIKeyFromEnvVar_NotSet(t *testing.T) {
	customEnvVar := "NONEXISTENT_KEY_12345"
	os.Unsetenv(customEnvVar)

	_, err := NewBasetenEmbeddingFunction(
		WithAPIKeyFromEnvVar(customEnvVar),
		WithBaseURL("https://example.baseten.co"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), customEnvVar+" not set")
}

func TestBasetenEmbeddingFunction_EmbedDocuments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/embeddings", r.URL.Path)
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Contains(t, r.Header.Get("Authorization"), "Bearer ")

		var reqBody map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&reqBody)
		require.NoError(t, err)
		require.Contains(t, reqBody, "input")

		response := CreateEmbeddingResponse{
			Object: "list",
			Data: []EmbeddingData{
				{Object: "embedding", Index: 0, Embedding: []float32{0.1, 0.2, 0.3}},
				{Object: "embedding", Index: 1, Embedding: []float32{0.4, 0.5, 0.6}},
			},
			Model: "test-model",
			Usage: Usage{PromptTokens: 10, TotalTokens: 10},
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL(server.URL),
		WithInsecure(),
	)
	require.NoError(t, err)

	documents := []string{"doc1", "doc2"}
	embs, err := ef.EmbedDocuments(context.Background(), documents)
	require.NoError(t, err)
	require.Len(t, embs, 2)
	require.Equal(t, 3, embs[0].Len())
	require.Equal(t, 3, embs[1].Len())
}

func TestBasetenEmbeddingFunction_EmbedQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/embeddings", r.URL.Path)

		response := CreateEmbeddingResponse{
			Object: "list",
			Data: []EmbeddingData{
				{Object: "embedding", Index: 0, Embedding: []float32{0.1, 0.2, 0.3, 0.4}},
			},
			Model: "test-model",
			Usage: Usage{PromptTokens: 5, TotalTokens: 5},
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL(server.URL),
		WithInsecure(),
	)
	require.NoError(t, err)

	emb, err := ef.EmbedQuery(context.Background(), "test query")
	require.NoError(t, err)
	require.Equal(t, 4, emb.Len())
}

func TestBasetenEmbeddingFunction_EmbedQuery_EmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := CreateEmbeddingResponse{
			Object: "list",
			Data:   []EmbeddingData{},
			Model:  "test-model",
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL(server.URL),
		WithInsecure(),
	)
	require.NoError(t, err)

	_, err = ef.EmbedQuery(context.Background(), "test query")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no embedding returned")
}

func TestBasetenEmbeddingFunction_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "bad request"}`))
	}))
	defer server.Close()

	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL(server.URL),
		WithInsecure(),
	)
	require.NoError(t, err)

	_, err = ef.EmbedDocuments(context.Background(), []string{"test"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected response")
}

func TestBasetenEmbeddingFunction_ModelInRequest(t *testing.T) {
	var capturedModel string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req CreateEmbeddingRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		capturedModel = req.Model

		response := CreateEmbeddingResponse{
			Object: "list",
			Data:   []EmbeddingData{{Embedding: []float32{0.1}}},
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL(server.URL),
		WithModelID("my-custom-model"),
		WithInsecure(),
	)
	require.NoError(t, err)

	_, err = ef.EmbedQuery(context.Background(), "test")
	require.NoError(t, err)
	require.Equal(t, "my-custom-model", capturedModel)
}

func TestBasetenEmbeddingFunction_ContextModel(t *testing.T) {
	var capturedModel string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req CreateEmbeddingRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		capturedModel = req.Model

		response := CreateEmbeddingResponse{
			Object: "list",
			Data:   []EmbeddingData{{Embedding: []float32{0.1}}},
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL(server.URL),
		WithModelID("default-model"),
		WithInsecure(),
	)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), ModelContextVar, "context-override-model")
	_, err = ef.EmbedQuery(ctx, "test")
	require.NoError(t, err)
	require.Equal(t, "context-override-model", capturedModel)
}

func TestNewBasetenEmbeddingFunctionFromConfig(t *testing.T) {
	t.Setenv("TEST_BASETEN_KEY", "test-api-key")

	cfg := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": "TEST_BASETEN_KEY",
		"api_base":        "https://example.baseten.co",
		"model_name":      "test-model",
	}

	ef, err := NewBasetenEmbeddingFunctionFromConfig(cfg)
	require.NoError(t, err)
	require.Equal(t, "baseten", ef.Name())
	require.Equal(t, "test-model", ef.apiClient.Model)
	require.Equal(t, "https://example.baseten.co", ef.apiClient.BaseURL)
}

func TestNewBasetenEmbeddingFunctionFromConfig_MissingEnvVar(t *testing.T) {
	cfg := embeddings.EmbeddingFunctionConfig{
		"api_base": "https://example.baseten.co",
	}

	_, err := NewBasetenEmbeddingFunctionFromConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "api_key_env_var is required")
}

func TestNewBasetenEmbeddingFunctionFromConfig_MissingBaseURL(t *testing.T) {
	t.Setenv("TEST_BASETEN_KEY", "test-api-key")

	cfg := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": "TEST_BASETEN_KEY",
	}

	_, err := NewBasetenEmbeddingFunctionFromConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "api_base is required")
}

func TestNewBasetenEmbeddingFunctionFromConfig_WithInsecure(t *testing.T) {
	t.Setenv("TEST_BASETEN_KEY", "test-api-key")

	cfg := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": "TEST_BASETEN_KEY",
		"api_base":        "http://localhost:8000",
		"insecure":        true,
	}

	ef, err := NewBasetenEmbeddingFunctionFromConfig(cfg)
	require.NoError(t, err)
	require.True(t, ef.apiClient.Insecure)
}

func TestBuildDense_Baseten(t *testing.T) {
	t.Setenv("TEST_BASETEN_KEY", "test-api-key")

	cfg := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": "TEST_BASETEN_KEY",
		"api_base":        "https://example.baseten.co",
	}

	ef, err := embeddings.BuildDense("baseten", cfg)
	require.NoError(t, err)
	require.Equal(t, "baseten", ef.Name())
}

func TestConfigRoundTrip(t *testing.T) {
	t.Setenv("TEST_BASETEN_KEY", "test-api-key")

	ef1, err := NewBasetenEmbeddingFunction(
		WithAPIKeyFromEnvVar("TEST_BASETEN_KEY"),
		WithBaseURL("https://example.baseten.co"),
		WithModelID("test-model"),
	)
	require.NoError(t, err)

	cfg := ef1.GetConfig()

	ef2, err := NewBasetenEmbeddingFunctionFromConfig(cfg)
	require.NoError(t, err)

	require.Equal(t, ef1.Name(), ef2.Name())
	require.Equal(t, ef1.apiClient.BaseURL, ef2.apiClient.BaseURL)
	require.Equal(t, ef1.apiClient.Model, ef2.apiClient.Model)
	require.Equal(t, ef1.apiClient.APIKeyEnvVar, ef2.apiClient.APIKeyEnvVar)
}

func TestWithHTTPClient(t *testing.T) {
	customClient := &http.Client{}
	ef, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("https://example.baseten.co"),
		WithHTTPClient(customClient),
	)
	require.NoError(t, err)
	require.Same(t, customClient, ef.apiClient.Client)
}

func TestWithHTTPClient_Nil(t *testing.T) {
	_, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("https://example.baseten.co"),
		WithHTTPClient(nil),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP client cannot be nil")
}

func TestWithAPIKey_Empty(t *testing.T) {
	_, err := NewBasetenEmbeddingFunction(
		WithAPIKey(""),
		WithBaseURL("https://example.baseten.co"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "API key cannot be empty")
}

func TestWithBaseURL_Empty(t *testing.T) {
	_, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL(""),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "base URL cannot be empty")
}

func TestWithBaseURL_Invalid(t *testing.T) {
	_, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("://invalid"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid base URL")
}

func TestWithModelID_Empty(t *testing.T) {
	_, err := NewBasetenEmbeddingFunction(
		WithAPIKey("test-key"),
		WithBaseURL("https://example.baseten.co"),
		WithModelID(""),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "model ID cannot be empty")
}

// Integration test - skipped unless credentials are available
func TestBasetenIntegration(t *testing.T) {
	apiKey := os.Getenv("BASETEN_API_KEY")
	baseURL := os.Getenv("BASETEN_BASE_URL")
	if apiKey == "" || baseURL == "" {
		t.Skip("Skipping integration test: BASETEN_API_KEY and BASETEN_BASE_URL not set")
	}

	ef, err := NewBasetenEmbeddingFunction(
		WithEnvAPIKey(),
		WithBaseURL(baseURL),
	)
	require.NoError(t, err)

	t.Run("EmbedDocuments", func(t *testing.T) {
		documents := []string{
			"Hello, world!",
			"How are you?",
		}
		embs, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.Len(t, embs, 2)
		require.Greater(t, embs[0].Len(), 0)
		require.Greater(t, embs[1].Len(), 0)
	})

	t.Run("EmbedQuery", func(t *testing.T) {
		emb, err := ef.EmbedQuery(context.Background(), "Test query")
		require.NoError(t, err)
		require.Greater(t, emb.Len(), 0)
	})
}
