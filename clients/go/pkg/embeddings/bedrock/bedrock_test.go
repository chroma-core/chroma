//go:build ef

package bedrock

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// mockInvoker is a test double for the Bedrock runtime client.
type mockInvoker struct {
	response []byte
	err      error
}

func (m *mockInvoker) InvokeModel(_ context.Context, _ *bedrockruntime.InvokeModelInput,
	_ ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &bedrockruntime.InvokeModelOutput{Body: m.response}, nil
}

func newMockResponse(t *testing.T, vec []float32) []byte {
	t.Helper()
	resp := titanResponse{Embedding: vec, InputTextTokenCount: 3}
	b, err := json.Marshal(resp)
	require.NoError(t, err)
	return b
}

func TestBedrockEmbeddingFunction_MockEmbedQuery(t *testing.T) {
	expected := []float32{0.1, 0.2, 0.3}
	ef, err := NewBedrockEmbeddingFunction(
		WithBedrockClient(&mockInvoker{response: newMockResponse(t, expected)}),
	)
	require.NoError(t, err)

	emb, err := ef.EmbedQuery(context.Background(), "hello world")
	require.NoError(t, err)
	assert.Equal(t, expected, emb.ContentAsFloat32())
}

func TestBedrockEmbeddingFunction_MockEmbedDocuments(t *testing.T) {
	expected := []float32{0.4, 0.5, 0.6}
	ef, err := NewBedrockEmbeddingFunction(
		WithBedrockClient(&mockInvoker{response: newMockResponse(t, expected)}),
	)
	require.NoError(t, err)

	embs, err := ef.EmbedDocuments(context.Background(), []string{"doc1", "doc2"})
	require.NoError(t, err)
	require.Len(t, embs, 2)
	for _, e := range embs {
		assert.Equal(t, expected, e.ContentAsFloat32())
	}
}

func TestBedrockEmbeddingFunction_EmptyDocuments(t *testing.T) {
	ef, err := NewBedrockEmbeddingFunction(
		WithBedrockClient(&mockInvoker{response: newMockResponse(t, []float32{0.1})}),
	)
	require.NoError(t, err)

	embs, err := ef.EmbedDocuments(context.Background(), []string{})
	require.NoError(t, err)
	assert.Empty(t, embs)
}

func TestBedrockEmbeddingFunction_Name(t *testing.T) {
	ef, err := NewBedrockEmbeddingFunction(
		WithBedrockClient(&mockInvoker{response: newMockResponse(t, []float32{0.1})}),
	)
	require.NoError(t, err)
	assert.Equal(t, "amazon_bedrock", ef.Name())
}

func TestBedrockEmbeddingFunction_GetConfig(t *testing.T) {
	ef, err := NewBedrockEmbeddingFunction(
		WithBedrockClient(&mockInvoker{response: newMockResponse(t, []float32{0.1})}),
		WithModel("amazon.titan-embed-text-v2:0"),
		WithRegion("us-west-2"),
		WithProfile("myprofile"),
		WithDimensions(256),
		WithNormalize(true),
	)
	require.NoError(t, err)

	cfg := ef.GetConfig()
	assert.Equal(t, "amazon.titan-embed-text-v2:0", cfg["model_name"])
	assert.Equal(t, "us-west-2", cfg["region"])
	assert.Equal(t, "myprofile", cfg["profile"])
	assert.Equal(t, 256, cfg["dimensions"])
	assert.Equal(t, true, cfg["normalize"])
}

func TestBedrockEmbeddingFunction_GetConfig_BearerToken(t *testing.T) {
	t.Setenv("MY_BEDROCK_TOKEN", "test-token")
	ef, err := NewBedrockEmbeddingFunction(
		WithBearerTokenFromEnvVar("MY_BEDROCK_TOKEN"),
	)
	require.NoError(t, err)

	cfg := ef.GetConfig()
	assert.Equal(t, "MY_BEDROCK_TOKEN", cfg["api_key_env_var"])
	assert.Equal(t, DefaultModel, cfg["model_name"])
	assert.Equal(t, DefaultRegion, cfg["region"])
}

func TestBedrockEmbeddingFunction_ConfigRoundTrip(t *testing.T) {
	ef, err := NewBedrockEmbeddingFunction(
		WithBedrockClient(&mockInvoker{response: newMockResponse(t, []float32{0.1})}),
		WithModel("amazon.titan-embed-text-v2:0"),
		WithRegion("us-west-2"),
		WithDimensions(512),
		WithNormalize(true),
	)
	require.NoError(t, err)

	name := ef.Name()
	cfg := ef.GetConfig()

	assert.True(t, embeddings.HasDense(name))

	rebuilt, err := embeddings.BuildDense(name, cfg)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	assert.Equal(t, name, rebuilt.Name())
	rebuiltCfg := rebuilt.GetConfig()
	assert.Equal(t, cfg["model_name"], rebuiltCfg["model_name"])
	assert.Equal(t, cfg["region"], rebuiltCfg["region"])
	assert.Equal(t, cfg["dimensions"], rebuiltCfg["dimensions"])
	assert.Equal(t, cfg["normalize"], rebuiltCfg["normalize"])
}

func TestBedrockEmbeddingFunction_ConfigRoundTrip_BearerToken(t *testing.T) {
	t.Setenv("MY_BEDROCK_TOKEN", "test-token-value")
	ef, err := NewBedrockEmbeddingFunction(
		WithBearerTokenFromEnvVar("MY_BEDROCK_TOKEN"),
		WithModel("amazon.titan-embed-text-v2:0"),
		WithRegion("us-west-2"),
	)
	require.NoError(t, err)

	name := ef.Name()
	cfg := ef.GetConfig()

	assert.Equal(t, "MY_BEDROCK_TOKEN", cfg["api_key_env_var"])

	rebuilt, err := embeddings.BuildDense(name, cfg)
	require.NoError(t, err)
	require.NotNil(t, rebuilt)

	assert.Equal(t, name, rebuilt.Name())
	rebuiltCfg := rebuilt.GetConfig()
	assert.Equal(t, "MY_BEDROCK_TOKEN", rebuiltCfg["api_key_env_var"])
	assert.Equal(t, cfg["model_name"], rebuiltCfg["model_name"])
	assert.Equal(t, cfg["region"], rebuiltCfg["region"])
}

func TestBedrockEmbeddingFunction_DefaultSpaceAndSupported(t *testing.T) {
	ef, err := NewBedrockEmbeddingFunction(
		WithBedrockClient(&mockInvoker{response: newMockResponse(t, []float32{0.1})}),
	)
	require.NoError(t, err)
	assert.Equal(t, embeddings.COSINE, ef.DefaultSpace())
	assert.Contains(t, ef.SupportedSpaces(), embeddings.COSINE)
	assert.Contains(t, ef.SupportedSpaces(), embeddings.L2)
	assert.Contains(t, ef.SupportedSpaces(), embeddings.IP)
}

func TestBedrockEmbeddingFunction_OptionValidation(t *testing.T) {
	t.Run("empty model rejected", func(t *testing.T) {
		_, err := NewBedrockEmbeddingFunction(WithModel(""))
		require.Error(t, err)
	})
	t.Run("empty region rejected", func(t *testing.T) {
		_, err := NewBedrockEmbeddingFunction(WithRegion(""))
		require.Error(t, err)
	})
	t.Run("invalid region format rejected", func(t *testing.T) {
		_, err := NewBedrockEmbeddingFunction(
			WithBedrockClient(&mockInvoker{response: newMockResponse(t, []float32{0.1})}),
			WithRegion("not-a-valid-region!"),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid AWS region")
	})
	t.Run("empty profile rejected", func(t *testing.T) {
		_, err := NewBedrockEmbeddingFunction(WithProfile(""))
		require.Error(t, err)
	})
	t.Run("nil client rejected", func(t *testing.T) {
		_, err := NewBedrockEmbeddingFunction(WithBedrockClient(nil))
		require.Error(t, err)
	})
	t.Run("zero dimensions rejected", func(t *testing.T) {
		_, err := NewBedrockEmbeddingFunction(WithDimensions(0))
		require.Error(t, err)
	})
	t.Run("empty bearer token rejected", func(t *testing.T) {
		_, err := NewBedrockEmbeddingFunction(WithBearerToken(""))
		require.Error(t, err)
	})
	t.Run("missing bearer token env var rejected", func(t *testing.T) {
		t.Setenv("NONEXISTENT_VAR", "")
		_, err := NewBedrockEmbeddingFunction(WithBearerTokenFromEnvVar("NONEXISTENT_VAR"))
		require.Error(t, err)
	})
}

// Integration tests — only run when AWS credentials are available.

func loadEnv() {
	_ = godotenv.Load("../../../.env")
}

func TestBedrockEmbeddingFunction_Integration_BearerToken(t *testing.T) {
	if os.Getenv(BearerTokenEnvVar) == "" {
		loadEnv()
	}
	if os.Getenv(BearerTokenEnvVar) == "" {
		t.Skip("AWS_BEARER_TOKEN_BEDROCK not set, skipping Bedrock bearer token integration tests")
	}

	ef, err := NewBedrockEmbeddingFunction(
		WithEnvBearerToken(),
	)
	require.NoError(t, err)

	t.Run("EmbedDocuments", func(t *testing.T) {
		embs, err := ef.EmbedDocuments(context.Background(), []string{"hello world", "another document"})
		require.NoError(t, err)
		require.Len(t, embs, 2)
		assert.Greater(t, embs[0].Len(), 0)
		assert.Greater(t, embs[1].Len(), 0)
	})

	t.Run("EmbedQuery", func(t *testing.T) {
		emb, err := ef.EmbedQuery(context.Background(), "search query")
		require.NoError(t, err)
		assert.Greater(t, emb.Len(), 0)
	})
}

func TestBedrockEmbeddingFunction_Integration_SDK(t *testing.T) {
	if os.Getenv("AWS_REGION") == "" {
		loadEnv()
	}
	if os.Getenv("AWS_REGION") == "" {
		t.Skip("AWS_REGION not set, skipping Bedrock SDK integration tests")
	}

	ef, err := NewBedrockEmbeddingFunction(
		WithRegion(os.Getenv("AWS_REGION")),
	)
	require.NoError(t, err)

	t.Run("EmbedDocuments", func(t *testing.T) {
		embs, err := ef.EmbedDocuments(context.Background(), []string{"hello world", "another document"})
		require.NoError(t, err)
		require.Len(t, embs, 2)
		assert.Greater(t, embs[0].Len(), 0)
		assert.Greater(t, embs[1].Len(), 0)
	})

	t.Run("EmbedQuery", func(t *testing.T) {
		emb, err := ef.EmbedQuery(context.Background(), "search query")
		require.NoError(t, err)
		assert.Greater(t, emb.Len(), 0)
	})
}
