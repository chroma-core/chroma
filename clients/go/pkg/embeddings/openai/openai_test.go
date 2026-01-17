//go:build ef

package openai

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_openai_client(t *testing.T) {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
		apiKey = os.Getenv("OPENAI_API_KEY")
	}

	t.Run("Test DefaultApiService Add", func(t *testing.T) {
		ef, efErr := NewOpenAIEmbeddingFunction(apiKey)
		require.NoError(t, efErr)

		documents := []string{
			"Document 1 content here",
			"Document 2 content here",
			// Add more documents as needed
		}
		resp, reqErr := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, reqErr)
		require.NotNil(t, resp)
		require.Empty(t, ef.apiClient.OrgID)
	})

	t.Run("Test Adding Organization CollectionID with NewOpenAIClient", func(t *testing.T) {
		apiClient, efError := NewOpenAIClient(apiKey, WithOpenAIOrganizationID("org-123"))
		require.NoError(t, efError)

		require.Equal(t, "org-123", apiClient.OrgID)
	})

	t.Run("Test Adding Organization CollectionID with NewOpenAIEmbeddingFunction", func(t *testing.T) {
		ef, efError := NewOpenAIEmbeddingFunction(apiKey, WithOpenAIOrganizationID("org-123"))
		require.NoError(t, efError)

		require.Equal(t, "org-123", ef.apiClient.OrgID)
	})

	t.Run("Test With Model text-embedding-3-small", func(t *testing.T) {
		ef, erErr := NewOpenAIEmbeddingFunction(apiKey, WithModel(TextEmbedding3Small))
		require.NoError(t, erErr)
		documents := []string{
			"Document 1 content here",
		}
		resp, reqErr := ef.EmbedDocuments(context.Background(), documents)
		require.Nil(t, reqErr)
		require.NotNil(t, resp)
		require.Empty(t, ef.apiClient.OrgID)
		require.Equal(t, 1, len(resp))
		require.Equal(t, 1536, resp[0].Len())
	})

	t.Run("Test With Model text-embedding-3-large", func(t *testing.T) {
		ef, efErr := NewOpenAIEmbeddingFunction(apiKey, WithModel(TextEmbedding3Large))
		require.NoError(t, efErr)
		documents := []string{
			"Document 1 content here",
		}
		resp, reqErr := ef.EmbedDocuments(context.Background(), documents)
		require.Nil(t, reqErr)
		require.NotNil(t, resp)
		require.Empty(t, ef.apiClient.OrgID)
		require.Equal(t, 3072, resp[0].Len())
	})

	t.Run("Test With Invalid Model", func(t *testing.T) {
		_, efErr := NewOpenAIEmbeddingFunction(apiKey, WithModel("invalid-model"))
		require.Error(t, efErr)
		require.Contains(t, efErr.Error(), "invalid model name invalid-model")
	})

	t.Run("Test With Model text-embedding-3-large and reduced dimensions", func(t *testing.T) {
		ef, err := NewOpenAIEmbeddingFunction(apiKey, WithModel(TextEmbedding3Large), WithDimensions(512))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Empty(t, ef.apiClient.OrgID)
		require.Equal(t, 512, resp[0].Len())
	})

	t.Run("Test With Model legacy model and reduced dimensions", func(t *testing.T) {
		ef, err := NewOpenAIEmbeddingFunction(apiKey, WithDimensions(512))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
		}
		_, err = ef.EmbedDocuments(context.Background(), documents)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "This model does not support specifying dimensions")
	})

	t.Run("Test With BaseURL", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"data": [{"embedding": [1, 2, 3]}]}`))
			if err != nil {
				return
			}
		}))
		defer server.Close()
		// httptest.NewServer creates HTTP URLs, so we need WithInsecure()
		ef, err := NewOpenAIEmbeddingFunction(apiKey, WithBaseURL(server.URL), WithInsecure())
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
		}
		_, err = ef.EmbedDocuments(context.Background(), documents)
		require.Nil(t, err)
	})

	t.Run("Test HTTP URL rejected without WithInsecure", func(t *testing.T) {
		_, err := NewOpenAIEmbeddingFunction(apiKey, WithBaseURL("http://example.com"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "base URL must use HTTPS")
	})

	t.Run("Test HTTP URL accepted with WithInsecure", func(t *testing.T) {
		_, err := NewOpenAIEmbeddingFunction(apiKey, WithBaseURL("http://example.com"), WithInsecure())
		require.NoError(t, err)
	})

	t.Run("Test HTTPS URL accepted", func(t *testing.T) {
		_, err := NewOpenAIEmbeddingFunction(apiKey, WithBaseURL("https://example.com"))
		require.NoError(t, err)
	})

	t.Run("Test Embed query With Model text-embedding-3-large", func(t *testing.T) {
		ef, efErr := NewOpenAIEmbeddingFunction(apiKey, WithModel(TextEmbedding3Large))
		require.NoError(t, efErr)
		resp, reqErr := ef.EmbedQuery(context.Background(), "Document 1 content here")
		require.Nil(t, reqErr)
		require.NotNil(t, resp)
		require.Empty(t, ef.apiClient.OrgID)
		require.Equal(t, 3072, resp.Len())
	})

	t.Run("Test Embed query With Model text-embedding-3-small", func(t *testing.T) {
		ef, efErr := NewOpenAIEmbeddingFunction(apiKey, WithModel(TextEmbedding3Small))
		require.NoError(t, efErr)
		resp, reqErr := ef.EmbedQuery(context.Background(), "Document 1 content here")
		require.Nil(t, reqErr)
		require.NotNil(t, resp)
		require.Empty(t, ef.apiClient.OrgID)
		require.Equal(t, 1536, resp.Len())
	})

}
