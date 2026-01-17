//go:build ef

package morph

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

func TestMorphEmbeddingFunction_Persistence(t *testing.T) {
	t.Setenv("MORPH_API_KEY", "test-key-123")

	ef, err := NewMorphEmbeddingFunction(WithEnvAPIKey())
	require.NoError(t, err)

	name := ef.Name()
	assert.Equal(t, "morph", name)

	config := ef.GetConfig()
	assert.Equal(t, "MORPH_API_KEY", config["api_key_env_var"])
	assert.Equal(t, "morph-embedding-v2", config["model_name"])
	assert.Equal(t, "https://api.morphllm.com/v1/", config["api_base"])

	rebuilt, err := embeddings.BuildDense(name, config)
	require.NoError(t, err)
	assert.Equal(t, name, rebuilt.Name())
	assert.Equal(t, config["model_name"], rebuilt.GetConfig()["model_name"])
}

func TestMorphEmbeddingFunction_PersistenceWithCustomModel(t *testing.T) {
	t.Setenv("MORPH_API_KEY", "test-key-123")

	ef, err := NewMorphEmbeddingFunction(
		WithEnvAPIKey(),
		WithModel("morph-embedding-v4"),
	)
	require.NoError(t, err)

	config := ef.GetConfig()
	assert.Equal(t, "morph-embedding-v4", config["model_name"])

	rebuilt, err := embeddings.BuildDense(ef.Name(), config)
	require.NoError(t, err)
	assert.Equal(t, "morph-embedding-v4", rebuilt.GetConfig()["model_name"])
}

func TestMorphEmbeddingFunction_PersistenceWithCustomEnvVar(t *testing.T) {
	t.Setenv("MY_MORPH_KEY", "test-key-456")

	ef, err := NewMorphEmbeddingFunction(WithAPIKeyFromEnvVar("MY_MORPH_KEY"))
	require.NoError(t, err)

	config := ef.GetConfig()
	assert.Equal(t, "MY_MORPH_KEY", config["api_key_env_var"])

	rebuilt, err := embeddings.BuildDense(ef.Name(), config)
	require.NoError(t, err)
	assert.Equal(t, "MY_MORPH_KEY", rebuilt.GetConfig()["api_key_env_var"])
}

func TestMorphEmbeddingFunction_DefaultSpace(t *testing.T) {
	t.Setenv("MORPH_API_KEY", "test-key")

	ef, err := NewMorphEmbeddingFunction(WithEnvAPIKey())
	require.NoError(t, err)

	assert.Equal(t, embeddings.COSINE, ef.DefaultSpace())
	assert.Contains(t, ef.SupportedSpaces(), embeddings.COSINE)
	assert.Contains(t, ef.SupportedSpaces(), embeddings.L2)
	assert.Contains(t, ef.SupportedSpaces(), embeddings.IP)
}

func TestMorphEmbeddingFunction_RequiresAPIKey(t *testing.T) {
	_, err := NewMorphEmbeddingFunction()
	require.Error(t, err)
}

func TestMorphEmbeddingFunction_EmptyDocuments(t *testing.T) {
	t.Setenv("MORPH_API_KEY", "test-key")

	ef, err := NewMorphEmbeddingFunction(WithEnvAPIKey())
	require.NoError(t, err)

	ctx := context.Background()
	result, err := ef.EmbedDocuments(ctx, []string{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestMorphEmbeddingFunction_LiveAPI(t *testing.T) {
	apiKey := os.Getenv("MORPH_API_KEY")
	if apiKey == "" {
		t.Skip("MORPH_API_KEY not set, skipping live API test")
	}

	ef, err := NewMorphEmbeddingFunction(WithEnvAPIKey())
	require.NoError(t, err)

	ctx := context.Background()

	embedding, err := ef.EmbedQuery(ctx, "Hello, world!")
	require.NoError(t, err)
	assert.NotEmpty(t, embedding.ContentAsFloat32())

	docs := []string{"First document", "Second document"}
	embeddingsResult, err := ef.EmbedDocuments(ctx, docs)
	require.NoError(t, err)
	assert.Len(t, embeddingsResult, 2)
	for _, emb := range embeddingsResult {
		assert.NotEmpty(t, emb.ContentAsFloat32())
	}
}
