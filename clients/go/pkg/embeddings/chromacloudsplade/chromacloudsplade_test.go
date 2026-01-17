//go:build ef && cloud

package chromacloudsplade

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadEnv(t *testing.T) {
	if os.Getenv(APIKeyEnvVar) == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
	}
}

func TestClient_Validation(t *testing.T) {
	t.Run("fails without API key", func(t *testing.T) {
		_, err := NewClient()
		require.Error(t, err)
		require.Contains(t, err.Error(), "API key is required")
	})

	t.Run("fails with empty API key option", func(t *testing.T) {
		_, err := NewClient(WithAPIKey(""))
		require.Error(t, err)
		require.Contains(t, err.Error(), "API key cannot be empty")
	})

	t.Run("fails with nil HTTP client", func(t *testing.T) {
		_, err := NewClient(WithAPIKey("test-key"), WithHTTPClient(nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "HTTP client cannot be nil")
	})

	t.Run("fails with empty model", func(t *testing.T) {
		_, err := NewClient(WithAPIKey("test-key"), WithModel(""))
		require.Error(t, err)
		require.Contains(t, err.Error(), "model cannot be empty")
	})

	t.Run("fails with empty base URL", func(t *testing.T) {
		_, err := NewClient(WithAPIKey("test-key"), WithBaseURL(""))
		require.Error(t, err)
		require.Contains(t, err.Error(), "base URL cannot be empty")
	})

	t.Run("fails with invalid base URL", func(t *testing.T) {
		_, err := NewClient(WithAPIKey("test-key"), WithBaseURL("not a valid url"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid base URL")
	})

	t.Run("fails with HTTP base URL without WithInsecure", func(t *testing.T) {
		_, err := NewClient(WithAPIKey("test-key"), WithBaseURL("http://example.com"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "base URL must use HTTPS")
	})

	t.Run("succeeds with HTTP base URL when WithInsecure is set", func(t *testing.T) {
		client, err := NewClient(WithAPIKey("test-key"), WithBaseURL("http://example.com"), WithInsecure())
		require.NoError(t, err)
		require.Equal(t, "http://example.com", client.BaseURL)
		require.True(t, client.Insecure)
	})
}

func TestClient_Options(t *testing.T) {
	t.Run("creates client with API key", func(t *testing.T) {
		client, err := NewClient(WithAPIKey("test-key"))
		require.NoError(t, err)
		require.Equal(t, "test-key", client.APIKey.Value())
		require.Equal(t, defaultBaseURL, client.BaseURL)
		require.Equal(t, defaultModel, string(client.Model))
	})

	t.Run("creates client with custom model", func(t *testing.T) {
		client, err := NewClient(WithAPIKey("test-key"), WithModel("custom-model"))
		require.NoError(t, err)
		require.Equal(t, "custom-model", string(client.Model))
	})

	t.Run("creates client with custom HTTP client", func(t *testing.T) {
		httpClient := &http.Client{}
		client, err := NewClient(WithAPIKey("test-key"), WithHTTPClient(httpClient))
		require.NoError(t, err)
		require.Same(t, httpClient, client.HTTPClient)
	})

	t.Run("creates client with custom base URL", func(t *testing.T) {
		client, err := NewClient(WithAPIKey("test-key"), WithBaseURL("https://custom.example.com"))
		require.NoError(t, err)
		require.Equal(t, "https://custom.example.com", client.BaseURL)
	})
}

func TestEmbeddingFunction(t *testing.T) {
	loadEnv(t)

	t.Run("EmbedDocumentsSparse with default options", func(t *testing.T) {
		ef, err := NewEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)

		documents := []string{
			"The quick brown fox jumps over the lazy dog while the sun sets behind the mountains.",
			"Machine learning models can process natural language to extract meaningful representations from text.",
		}
		embeddings, err := ef.EmbedDocumentsSparse(context.Background(), documents)
		require.NoError(t, err)
		require.Len(t, embeddings, 2)
		require.Greater(t, embeddings[0].Len(), 0)
		require.Greater(t, embeddings[1].Len(), 0)
		require.Equal(t, len(embeddings[0].Indices), len(embeddings[0].Values))
		require.Equal(t, len(embeddings[1].Indices), len(embeddings[1].Values))
	})

	t.Run("EmbedDocumentsSparse with empty list", func(t *testing.T) {
		ef, err := NewEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)

		embeddings, err := ef.EmbedDocumentsSparse(context.Background(), []string{})
		require.NoError(t, err)
		require.Len(t, embeddings, 0)
	})

	t.Run("EmbedQuerySparse", func(t *testing.T) {
		ef, err := NewEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)

		query := "How do neural networks learn to recognize patterns in images and text data?"
		embedding, err := ef.EmbedQuerySparse(context.Background(), query)
		require.NoError(t, err)
		require.Greater(t, embedding.Len(), 0)
		require.Equal(t, len(embedding.Indices), len(embedding.Values))
	})
}
