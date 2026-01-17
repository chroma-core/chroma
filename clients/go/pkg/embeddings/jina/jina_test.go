//go:build ef

package jina

import (
	"context"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJinaEmbeddingFunction(t *testing.T) {
	apiKey := os.Getenv("JINA_API_KEY")
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
		apiKey = os.Getenv("JINA_API_KEY")
	}

	t.Run("Test with defaults", func(t *testing.T) {
		ef, err := NewJinaEmbeddingFunction(WithAPIKey(apiKey))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
			"Document 2 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test with env API key", func(t *testing.T) {
		ef, err := NewJinaEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test with normalized off", func(t *testing.T) {
		ef, err := NewJinaEmbeddingFunction(WithEnvAPIKey(), WithNormalized(false))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test with model", func(t *testing.T) {
		ef, err := NewJinaEmbeddingFunction(WithEnvAPIKey(), WithModel("jina-embeddings-v3"))
		require.NoError(t, err)
		documents := []string{
			"import chromadb;client=chromadb.Client();collection=client.get_or_create_collection('col_name')",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test with EmbeddingType float", func(t *testing.T) {
		ef, err := NewJinaEmbeddingFunction(WithEnvAPIKey(), WithEmbeddingType(EmbeddingTypeFloat))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test with embedding endpoint", func(t *testing.T) {
		ef, err := NewJinaEmbeddingFunction(WithEnvAPIKey(), WithEmbeddingEndpoint(DefaultBaseAPIEndpoint))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test EmbedQuery", func(t *testing.T) {
		ef, err := NewJinaEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		resp, err := ef.EmbedQuery(context.Background(), "What is the meaning of life?")
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 1024, resp.Len())
	})

	t.Run("Test with task", func(t *testing.T) {
		ef, err := NewJinaEmbeddingFunction(WithEnvAPIKey(), WithTask(TaskClassification))
		require.NoError(t, err)
		documents := []string{
			"This is a positive review",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test missing API key", func(t *testing.T) {
		_, err := NewJinaEmbeddingFunction()
		require.Error(t, err)
		require.Contains(t, err.Error(), "'APIKey' failed on the 'required'")
	})

	t.Run("Test HTTP endpoint rejected without WithInsecure", func(t *testing.T) {
		_, err := NewJinaEmbeddingFunction(WithAPIKey(apiKey), WithEmbeddingEndpoint("http://example.com"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "base URL must use HTTPS")
	})

	t.Run("Test HTTP endpoint accepted with WithInsecure", func(t *testing.T) {
		_, err := NewJinaEmbeddingFunction(WithAPIKey(apiKey), WithEmbeddingEndpoint("http://example.com"), WithInsecure())
		require.NoError(t, err)
	})

	t.Run("Test HTTPS endpoint accepted", func(t *testing.T) {
		_, err := NewJinaEmbeddingFunction(WithAPIKey(apiKey), WithEmbeddingEndpoint("https://example.com"))
		require.NoError(t, err)
	})
}
