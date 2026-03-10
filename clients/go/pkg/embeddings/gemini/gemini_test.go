//go:build ef

package gemini

import (
	"context"
	"math"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireGeminiAPIKey(t *testing.T) string {
	t.Helper()
	apiKey := os.Getenv(APIKeyEnvVar)
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
		apiKey = os.Getenv(APIKeyEnvVar)
	}
	require.NotEmpty(t, apiKey, "%s must be set", APIKeyEnvVar)
	return apiKey
}

func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot float64
	var normA float64
	var normB float64
	for i := range a {
		ai := float64(a[i])
		bi := float64(b[i])
		dot += ai * bi
		normA += ai * ai
		normB += bi * bi
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

func Test_gemini_client(t *testing.T) {
	_ = requireGeminiAPIKey(t)
	client, err := NewGeminiClient(WithEnvAPIKey())
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	t.Run("Test CreateEmbedding", func(t *testing.T) {
		resp, rerr := client.CreateEmbedding(context.Background(), []string{"Test document"})
		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
	})
}

func Test_gemini_embedding_function(t *testing.T) {
	apiKey := requireGeminiAPIKey(t)

	t.Run("Test EmbedDocuments with env-based api key", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		defer func() { _ = embeddingFunction.Close() }()
		resp, rerr := embeddingFunction.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})
		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Len(t, resp[0].ContentAsFloat32(), 3072)

	})

	t.Run("Test EmbedDocuments with provided API key", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithAPIKey(apiKey))
		require.NoError(t, err)
		defer func() { _ = embeddingFunction.Close() }()
		resp, rerr := embeddingFunction.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Len(t, resp[0].ContentAsFloat32(), 3072)

	})

	t.Run("Test EmbedDocuments with provided model", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithEnvAPIKey(), WithDefaultModel(DefaultEmbeddingModel))
		require.NoError(t, err)
		defer func() { _ = embeddingFunction.Close() }()
		resp, rerr := embeddingFunction.EmbedDocuments(context.Background(), []string{"Test document", "Another test document"})

		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Len(t, resp[0].ContentAsFloat32(), 3072)

	})

	t.Run("Test EmbedQuery", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithEnvAPIKey(), WithDefaultModel(DefaultEmbeddingModel))
		require.NoError(t, err)
		defer func() { _ = embeddingFunction.Close() }()
		resp, rerr := embeddingFunction.EmbedQuery(context.Background(), "this is my query")
		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp.ContentAsFloat32(), 3072)
	})

	t.Run("Test wrong model", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithEnvAPIKey(), WithDefaultModel("model-does-not-exist"))
		require.NoError(t, err)
		defer func() { _ = embeddingFunction.Close() }()
		_, rerr := embeddingFunction.EmbedQuery(context.Background(), "this is my query")
		require.Contains(t, rerr.Error(), "404")
		require.Error(t, rerr)
	})

	t.Run("Test wrong API key", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(WithAPIKey("wrong-api-key"))
		require.NoError(t, err)
		defer func() { _ = embeddingFunction.Close() }()
		_, rerr := embeddingFunction.EmbedQuery(context.Background(), "this is my query")
		require.Contains(t, rerr.Error(), "API key not valid")
		require.Error(t, rerr)
	})
}

func Test_gemini_task_type_and_dimension_examples(t *testing.T) {
	apiKey := requireGeminiAPIKey(t)

	t.Run("Test output dimensionality from docs", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(
			WithAPIKey(apiKey),
			WithDefaultModel(DefaultEmbeddingModel),
			WithDimension(768),
		)
		require.NoError(t, err)
		defer func() { _ = embeddingFunction.Close() }()

		resp, rerr := embeddingFunction.EmbedQuery(context.Background(), "What is the meaning of life?")
		require.NoError(t, rerr)
		require.NotNil(t, resp)
		require.Len(t, resp.ContentAsFloat32(), 768)
	})

	t.Run("Test semantic similarity task type example", func(t *testing.T) {
		embeddingFunction, err := NewGeminiEmbeddingFunction(
			WithAPIKey(apiKey),
			WithDefaultModel(DefaultEmbeddingModel),
			WithTaskType(TaskTypeSemanticSimilarity),
		)
		require.NoError(t, err)
		defer func() { _ = embeddingFunction.Close() }()

		inputs := []string{
			"What is the meaning of life?",
			"What is the purpose of existence?",
			"How do I bake a cake?",
		}
		resp, rerr := embeddingFunction.EmbedDocuments(context.Background(), inputs)
		require.NoError(t, rerr)
		require.Len(t, resp, 3)

		simRelated := cosineSimilarity(resp[0].ContentAsFloat32(), resp[1].ContentAsFloat32())
		simUnrelated := cosineSimilarity(resp[0].ContentAsFloat32(), resp[2].ContentAsFloat32())
		require.Greater(t, simRelated, simUnrelated)
	})

	t.Run("Test retrieval query and retrieval document task types", func(t *testing.T) {
		documentEF, err := NewGeminiEmbeddingFunction(
			WithAPIKey(apiKey),
			WithDefaultModel(DefaultEmbeddingModel),
			WithTaskType(TaskTypeRetrievalDocument),
		)
		require.NoError(t, err)
		defer func() { _ = documentEF.Close() }()

		queryEF, err := NewGeminiEmbeddingFunction(
			WithAPIKey(apiKey),
			WithDefaultModel(DefaultEmbeddingModel),
			WithTaskType(TaskTypeRetrievalQuery),
		)
		require.NoError(t, err)
		defer func() { _ = queryEF.Close() }()

		docs := []string{
			"The meaning of life and human existence are classic philosophy questions.",
			"Chocolate cake recipe: flour, sugar, eggs, butter, and cocoa powder.",
		}
		docEmbeddings, rerr := documentEF.EmbedDocuments(context.Background(), docs)
		require.NoError(t, rerr)
		require.Len(t, docEmbeddings, 2)

		queryEmbedding, qerr := queryEF.EmbedQuery(context.Background(), "What is the meaning of life?")
		require.NoError(t, qerr)
		require.NotNil(t, queryEmbedding)

		simLife := cosineSimilarity(queryEmbedding.ContentAsFloat32(), docEmbeddings[0].ContentAsFloat32())
		simCake := cosineSimilarity(queryEmbedding.ContentAsFloat32(), docEmbeddings[1].ContentAsFloat32())
		require.Greater(t, simLife, simCake)
	})
}
