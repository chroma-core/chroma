//go:build ef

package gemini

import (
	"context"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_gemini_client(t *testing.T) {
	apiKey := os.Getenv(APIKeyEnvVar)
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
		apiKey = os.Getenv(APIKeyEnvVar)
	}
	client, err := NewGeminiClient(WithEnvAPIKey())
	require.NoError(t, err)
	defer func(client *Client) {
		err := client.Close()
		if err != nil {

		}
	}(client)

	t.Run("Test CreateEmbedding", func(t *testing.T) {
		resp, rerr := client.CreateEmbedding(context.Background(), []string{"Test document"})
		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
	})
}

func Test_gemini_embedding_function(t *testing.T) {
	apiKey := os.Getenv(APIKeyEnvVar)
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
		apiKey = os.Getenv(APIKeyEnvVar)
	}

	t.Run("Test EmbedDocuments with env-based api key", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithEnvAPIKey())
		defer func(embeddingFunction *GeminiEmbeddingFunction) {
			err := embeddingFunction.Close()
			if err != nil {

			}
		}(embeddingFunction)
		require.NoError(t, err)
		resp, rerr := embeddingFunction.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})
		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Len(t, resp[0].ContentAsFloat32(), 3072)

	})

	t.Run("Test EmbedDocuments with provided API key", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithAPIKey(apiKey))
		defer func(embeddingFunction *GeminiEmbeddingFunction) {
			err := embeddingFunction.Close()
			if err != nil {

			}
		}(embeddingFunction)
		require.NoError(t, err)
		resp, rerr := embeddingFunction.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Len(t, resp[0].ContentAsFloat32(), 3072)

	})

	t.Run("Test EmbedDocuments with provided model", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithEnvAPIKey(), WithDefaultModel(DefaultEmbeddingModel))
		defer func(embeddingFunction *GeminiEmbeddingFunction) {
			err := embeddingFunction.Close()
			if err != nil {

			}
		}(embeddingFunction)
		require.NoError(t, err)
		resp, rerr := embeddingFunction.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Len(t, resp[0].ContentAsFloat32(), 3072)

	})

	t.Run("Test EmbedQuery", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithEnvAPIKey(), WithDefaultModel(DefaultEmbeddingModel))
		defer func(embeddingFunction *GeminiEmbeddingFunction) {
			err := embeddingFunction.Close()
			if err != nil {

			}
		}(embeddingFunction)
		require.NoError(t, err)
		resp, rerr := embeddingFunction.EmbedQuery(context.Background(), "this is my query")
		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp.ContentAsFloat32(), 3072)
	})

	t.Run("Test wrong model", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithEnvAPIKey(), WithDefaultModel("model-does-not-exist"))
		defer func(embeddingFunction *GeminiEmbeddingFunction) {
			err := embeddingFunction.Close()
			if err != nil {

			}
		}(embeddingFunction)
		require.NoError(t, err)
		_, rerr := embeddingFunction.EmbedQuery(context.Background(), "this is my query")
		require.Contains(t, rerr.Error(), "404")
		require.Error(t, rerr)
	})

	t.Run("Test wrong API key", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithAPIKey("wrong-api-key"))
		defer func(embeddingFunction *GeminiEmbeddingFunction) {
			err := embeddingFunction.Close()
			if err != nil {

			}
		}(embeddingFunction)
		require.NoError(t, err)
		_, rerr := embeddingFunction.EmbedQuery(context.Background(), "this is my query")
		require.Contains(t, rerr.Error(), "API key not valid")
		require.Error(t, rerr)
	})
}
