//go:build ef

package voyage

import (
	"context"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "

func generateRandomString(length int) string {
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func Test_client(t *testing.T) {
	apiKey := os.Getenv("VOYAGE_API_KEY")
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
		apiKey = os.Getenv("VOYAGE_API_KEY")
	}
	client, err := NewVoyageAIClient(WithEnvAPIKey())
	require.NoError(t, err)

	t.Run("Test CreateEmbedding", func(t *testing.T) {
		req := CreateEmbeddingRequest{
			Model: "voyage-large-2",
			Input: &EmbeddingInputs{Input: "Test document"},
		}
		resp, rerr := client.CreateEmbedding(context.Background(), &req)

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Data)
		require.Len(t, resp.Data, 1)
	})
}

func Test_voyage_embedding_function(t *testing.T) {
	apiKey := os.Getenv("VOYAGE_API_KEY")
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
	}

	t.Run("Test EmbedDocuments with env-based API Key and default model", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		resp, rerr := client.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Equal(t, 2, len(resp))
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test EmbedDocuments with env-based API Key and default model override with context", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		ctx := context.Background()
		ctx = context.WithValue(ctx, ModelContextVar, "voyage-large-2")
		resp, rerr := client.EmbedDocuments(ctx, []string{"Test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Equal(t, 1, len(resp))
		require.Equal(t, 1536, resp[0].Len())
	})

	t.Run("Test EmbedDocuments for model with env-based API Key", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey(), WithDefaultModel("voyage-large-2"))
		require.NoError(t, err)
		resp, rerr := client.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Equal(t, 2, len(resp))
		require.Equal(t, 1536, resp[0].Len())
	})

	t.Run("Test EmbedDocuments with too large init batch", func(t *testing.T) {
		_, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey(), WithMaxBatchSize(200))
		require.Error(t, err)
		require.Contains(t, err.Error(), "max batch size must be less than")
	})

	t.Run("Test EmbedDocuments with too large batch at inference", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey())
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
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		resp, err := client.EmbedQuery(context.Background(), "Test query")
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 1024, resp.Len())
	})

	t.Run("Test EmbedDocuments with input type document", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		ctx := context.Background()
		ctx = context.WithValue(ctx, InputTypeContextVar, InputTypeDocument)
		resp, err := client.EmbedDocuments(ctx, []string{"Test document", "Another test document"})
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test EmbedDocuments with input type query", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		ctx := context.Background()
		ctx = context.WithValue(ctx, InputTypeContextVar, InputTypeQuery)
		resp, err := client.EmbedDocuments(ctx, []string{"Test document", "Another test document"})
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test EmbedDocuments with default truncation true", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey(), WithTruncation(true), WithDefaultModel("voyage-2"))
		require.NoError(t, err)
		resp, err := client.EmbedDocuments(context.Background(), []string{generateRandomString(20000)})
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test EmbedDocuments with default truncation true", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey(), WithTruncation(false), WithDefaultModel("voyage-2"))
		require.NoError(t, err)
		_, err = client.EmbedDocuments(context.Background(), []string{generateRandomString(20000)})
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "your batch has too many tokens")
	})

	t.Run("Test EmbedDocuments with env-based API Key and WithDefaultHeaders", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey(), WithDefaultModel("voyage-large-2"), WithDefaultHeaders(map[string]string{"X-Test-Header": "test"}))
		require.NoError(t, err)
		resp, rerr := client.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Equal(t, 2, len(resp))
		require.Equal(t, 1536, resp[0].Len())
	})

	t.Run("Test EmbedDocuments with var API Key", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithAPIKey(os.Getenv("VOYAGE_API_KEY")))
		require.NoError(t, err)
		resp, rerr := client.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Equal(t, 2, len(resp))
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test EmbedDocuments with var token and http client", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithAPIKey(os.Getenv("VOYAGE_API_KEY")), WithHTTPClient(http.DefaultClient))
		require.NoError(t, err)
		resp, rerr := client.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Equal(t, 2, len(resp))
		require.Equal(t, 1024, resp[0].Len())
	})

	t.Run("Test EmbedDocuments embedding format base64", func(t *testing.T) {
		client, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey(), WithEncodingFormat(EncodingFormatBase64))
		require.NoError(t, err)
		resp, err := client.EmbedDocuments(context.Background(), []string{"Test document"})

		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 1, len(resp))
		require.Equal(t, 1024, resp[0].Len())

		clientNoEncoding, err := NewVoyageAIEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		respNoEncoding, err := clientNoEncoding.EmbedDocuments(context.Background(), []string{"Test document"})
		require.Nil(t, err)
		require.NotNil(t, respNoEncoding)
	})

	t.Run("Test HTTP URL rejected without WithInsecure", func(t *testing.T) {
		_, err := NewVoyageAIEmbeddingFunction(WithAPIKey(apiKey), WithBaseURL("http://example.com"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "base URL must use HTTPS")
	})

	t.Run("Test HTTP URL accepted with WithInsecure", func(t *testing.T) {
		_, err := NewVoyageAIEmbeddingFunction(WithAPIKey(apiKey), WithBaseURL("http://example.com"), WithInsecure())
		require.NoError(t, err)
	})

	t.Run("Test HTTPS URL accepted", func(t *testing.T) {
		_, err := NewVoyageAIEmbeddingFunction(WithAPIKey(apiKey), WithBaseURL("https://example.com"))
		require.NoError(t, err)
	})
}
