//go:build ef

package together

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_client(t *testing.T) {
	apiKey := os.Getenv("TOGETHER_API_KEY")
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
		apiKey = os.Getenv("TOGETHER_API_KEY")
	}
	client, err := NewTogetherClient(WithEnvAPIToken())
	require.NoError(t, err)

	t.Run("Test CreateEmbedding", func(t *testing.T) {
		req := CreateEmbeddingRequest{
			Model: "BAAI/bge-base-en-v1.5",
			Input: &EmbeddingInputs{Input: "Test document"},
		}
		resp, rerr := client.CreateEmbedding(context.Background(), &req)

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Data)
		require.Len(t, resp.Data, 1)
	})
}

func Test_together_embedding_function(t *testing.T) {
	apiKey := os.Getenv("TOGETHER_API_KEY")
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
	}

	t.Run("Test EmbedDocuments with env-based API Key", func(t *testing.T) {
		client, err := NewTogetherEmbeddingFunction(WithEnvAPIToken())
		require.NoError(t, err)
		resp, rerr := client.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Equal(t, 1024, resp[0].Len())

	})

	t.Run("Test EmbedDocuments for model with env-based API Key", func(t *testing.T) {
		client, err := NewTogetherEmbeddingFunction(WithEnvAPIToken(), WithDefaultModel("BAAI/bge-base-en-v1.5"))
		require.NoError(t, err)
		resp, rerr := client.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Equal(t, 768, resp[0].Len())
	})

	t.Run("Test EmbedDocuments with too large init batch", func(t *testing.T) {
		_, err := NewTogetherEmbeddingFunction(WithEnvAPIToken(), WithMaxBatchSize(200))
		require.Error(t, err)
		require.Contains(t, err.Error(), "max batch size must be less than")
	})

	t.Run("Test EmbedDocuments with too large batch at inference", func(t *testing.T) {
		client, err := NewTogetherEmbeddingFunction(WithEnvAPIToken())
		require.NoError(t, err)
		docs200 := make([]string, 200)
		for i := 0; i < 200; i++ {
			docs200[i] = "Test document"
		}
		_, err = client.EmbedDocuments(context.Background(), docs200)
		require.Error(t, err)
		require.Contains(t, err.Error(), "number of documents exceeds the maximum batch")
	})

	t.Run("Test EmbedQuery", func(t *testing.T) {
		client, err := NewTogetherEmbeddingFunction(WithEnvAPIToken())
		require.NoError(t, err)
		resp, err := client.EmbedQuery(context.Background(), "Test query")
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 1024, resp.Len())
	})

	t.Run("Test EmbedDocuments with env-based API Key and WithDefaultHeaders", func(t *testing.T) {
		client, err := NewTogetherEmbeddingFunction(WithEnvAPIToken(), WithDefaultModel("BAAI/bge-base-en-v1.5"), WithDefaultHeaders(map[string]string{"X-Test-Header": "test"}))
		require.NoError(t, err)
		resp, rerr := client.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Equal(t, 768, resp[0].Len())
	})

	t.Run("Test EmbedDocuments with var API Key", func(t *testing.T) {
		client, err := NewTogetherEmbeddingFunction(WithAPIToken(os.Getenv("TOGETHER_API_KEY")))
		require.NoError(t, err)
		resp, rerr := client.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test EmbedDocuments with var token and account id and http client", func(t *testing.T) {
		client, err := NewTogetherEmbeddingFunction(WithAPIToken(os.Getenv("TOGETHER_API_KEY")), WithHTTPClient(http.DefaultClient))
		require.NoError(t, err)
		resp, rerr := client.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Equal(t, 2, len(resp))
		require.Equal(t, 1024, resp[0].Len())
	})
}
