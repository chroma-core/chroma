//go:build ef

package bm25

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

func TestTokenizer(t *testing.T) {
	t.Run("basic tokenization", func(t *testing.T) {
		tokenizer := NewTokenizer(DefaultStopwords, 40)
		tokens := tokenizer.Tokenize("Hello World")
		require.Len(t, tokens, 2)
		assert.Equal(t, "hello", tokens[0])
		assert.Equal(t, "world", tokens[1])
	})

	t.Run("filters stopwords", func(t *testing.T) {
		tokenizer := NewTokenizer(DefaultStopwords, 40)
		tokens := tokenizer.Tokenize("the quick brown fox")
		require.Len(t, tokens, 3)
		assert.Equal(t, "quick", tokens[0])
		assert.Equal(t, "brown", tokens[1])
		assert.Equal(t, "fox", tokens[2])
	})

	t.Run("applies stemming", func(t *testing.T) {
		tokenizer := NewTokenizer(DefaultStopwords, 40)
		tokens := tokenizer.Tokenize("running runs runner")
		require.Len(t, tokens, 3)
		assert.Equal(t, "run", tokens[0])
		assert.Equal(t, "run", tokens[1])
		assert.Equal(t, "runner", tokens[2])
	})

	t.Run("filters long tokens", func(t *testing.T) {
		tokenizer := NewTokenizer(DefaultStopwords, 5)
		tokens := tokenizer.Tokenize("hello wonderful world")
		require.Len(t, tokens, 2)
		assert.Equal(t, "hello", tokens[0])
		assert.Equal(t, "world", tokens[1])
	})

	t.Run("removes non-alphanumeric", func(t *testing.T) {
		tokenizer := NewTokenizer(DefaultStopwords, 40)
		tokens := tokenizer.Tokenize("hello, world! how are you?")
		require.Len(t, tokens, 2)
		assert.Equal(t, "hello", tokens[0])
		assert.Equal(t, "world", tokens[1])
	})

	t.Run("empty input", func(t *testing.T) {
		tokenizer := NewTokenizer(DefaultStopwords, 40)
		tokens := tokenizer.Tokenize("")
		require.Len(t, tokens, 0)
	})
}

func TestOptionValidation(t *testing.T) {
	t.Run("valid defaults", func(t *testing.T) {
		client, err := NewClient()
		require.NoError(t, err)
		assert.Equal(t, defaultK, client.K)
		assert.Equal(t, defaultB, client.B)
		assert.Equal(t, defaultAvgDocLength, client.AvgDocLength)
		assert.Equal(t, defaultTokenMaxLength, client.TokenMaxLength)
		assert.Equal(t, DefaultStopwords, client.Stopwords)
	})

	t.Run("custom options", func(t *testing.T) {
		client, err := NewClient(
			WithK(1.5),
			WithB(0.5),
			WithAvgDocLength(100.0),
			WithTokenMaxLength(50),
			WithStopwords([]string{"custom"}),
		)
		require.NoError(t, err)
		assert.Equal(t, 1.5, client.K)
		assert.Equal(t, 0.5, client.B)
		assert.Equal(t, 100.0, client.AvgDocLength)
		assert.Equal(t, 50, client.TokenMaxLength)
		assert.Equal(t, []string{"custom"}, client.Stopwords)
	})

	t.Run("negative k", func(t *testing.T) {
		_, err := NewClient(WithK(-1))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "k must be non-negative")
	})

	t.Run("invalid b", func(t *testing.T) {
		_, err := NewClient(WithB(-0.1))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "b must be between 0 and 1")

		_, err = NewClient(WithB(1.1))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "b must be between 0 and 1")
	})

	t.Run("invalid avgDocLength", func(t *testing.T) {
		_, err := NewClient(WithAvgDocLength(0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avgDocLength must be positive")

		_, err = NewClient(WithAvgDocLength(-10))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "avgDocLength must be positive")
	})

	t.Run("invalid tokenMaxLength", func(t *testing.T) {
		_, err := NewClient(WithTokenMaxLength(0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tokenMaxLength must be positive")
	})
}

func TestBM25Embedding(t *testing.T) {
	t.Run("basic embedding", func(t *testing.T) {
		ef, err := NewEmbeddingFunction()
		require.NoError(t, err)

		sv, err := ef.EmbedQuerySparse(context.Background(), "hello world")
		require.NoError(t, err)
		require.NotNil(t, sv)
		assert.Greater(t, len(sv.Indices), 0)
		assert.Equal(t, len(sv.Indices), len(sv.Values))

		for _, idx := range sv.Indices {
			assert.GreaterOrEqual(t, idx, 0)
		}
		for _, val := range sv.Values {
			assert.Greater(t, val, float32(0))
		}
	})

	t.Run("empty input", func(t *testing.T) {
		ef, err := NewEmbeddingFunction()
		require.NoError(t, err)

		sv, err := ef.EmbedQuerySparse(context.Background(), "")
		require.NoError(t, err)
		require.NotNil(t, sv)
		assert.Len(t, sv.Indices, 0)
		assert.Len(t, sv.Values, 0)
	})

	t.Run("only stopwords", func(t *testing.T) {
		ef, err := NewEmbeddingFunction()
		require.NoError(t, err)

		sv, err := ef.EmbedQuerySparse(context.Background(), "the a an")
		require.NoError(t, err)
		require.NotNil(t, sv)
		assert.Len(t, sv.Indices, 0)
		assert.Len(t, sv.Values, 0)
	})

	t.Run("multiple documents", func(t *testing.T) {
		ef, err := NewEmbeddingFunction()
		require.NoError(t, err)

		texts := []string{"hello world", "goodbye world", "hello again"}
		results, err := ef.EmbedDocumentsSparse(context.Background(), texts)
		require.NoError(t, err)
		require.Len(t, results, 3)

		for _, sv := range results {
			require.NotNil(t, sv)
			assert.Equal(t, len(sv.Indices), len(sv.Values))
		}
	})

	t.Run("include tokens", func(t *testing.T) {
		ef, err := NewEmbeddingFunction(WithIncludeTokens(true))
		require.NoError(t, err)

		sv, err := ef.EmbedQuerySparse(context.Background(), "hello world")
		require.NoError(t, err)
		require.NotNil(t, sv)
		assert.Equal(t, len(sv.Indices), len(sv.Labels))
		assert.Contains(t, sv.Labels, "hello")
		assert.Contains(t, sv.Labels, "world")
	})

	t.Run("repeated terms", func(t *testing.T) {
		ef, err := NewEmbeddingFunction(WithIncludeTokens(true))
		require.NoError(t, err)

		sv, err := ef.EmbedQuerySparse(context.Background(), "hello hello hello world")
		require.NoError(t, err)
		require.NotNil(t, sv)

		helloIdx := -1
		worldIdx := -1
		for i, label := range sv.Labels {
			if label == "hello" {
				helloIdx = i
			}
			if label == "world" {
				worldIdx = i
			}
		}
		require.NotEqual(t, -1, helloIdx)
		require.NotEqual(t, -1, worldIdx)

		assert.Greater(t, sv.Values[helloIdx], sv.Values[worldIdx])
	})

	t.Run("deterministic output", func(t *testing.T) {
		ef, err := NewEmbeddingFunction()
		require.NoError(t, err)

		text := "the quick brown fox jumps over lazy dog"
		sv1, err := ef.EmbedQuerySparse(context.Background(), text)
		require.NoError(t, err)
		sv2, err := ef.EmbedQuerySparse(context.Background(), text)
		require.NoError(t, err)

		assert.Equal(t, sv1.Indices, sv2.Indices)
		assert.Equal(t, sv1.Values, sv2.Values)
	})
}

func TestBM25Scoring(t *testing.T) {
	t.Run("score increases with term frequency", func(t *testing.T) {
		ef, err := NewEmbeddingFunction(WithIncludeTokens(true))
		require.NoError(t, err)

		sv1, err := ef.EmbedQuerySparse(context.Background(), "cat")
		require.NoError(t, err)
		sv2, err := ef.EmbedQuerySparse(context.Background(), "cat cat cat")
		require.NoError(t, err)

		var score1, score2 float32
		for i, label := range sv1.Labels {
			if label == "cat" {
				score1 = sv1.Values[i]
				break
			}
		}
		for i, label := range sv2.Labels {
			if label == "cat" {
				score2 = sv2.Values[i]
				break
			}
		}

		assert.Greater(t, score2, score1)
	})
}

func TestBM25PythonCompatibility(t *testing.T) {
	// Test compatibility with Python ChromaBm25EmbeddingFunction output
	// Python code:
	//   from chromadb.utils.embedding_functions.chroma_bm25_embedding_function import ChromaBm25EmbeddingFunction
	//   ef = ChromaBm25EmbeddingFunction()
	//   embeddings = ef(["the quick brown fox jumped over the lazy dog"])
	// Expected output:
	//   SparseVector(indices=[226376294, 741580288, 771291085, 1312749093, 1621867415, 1913189942],
	//                values=[1.6652868125369606, ...], labels=None)
	t.Run("matches Python output", func(t *testing.T) {
		ef, err := NewEmbeddingFunction()
		require.NoError(t, err)

		sv, err := ef.EmbedQuerySparse(context.Background(), "the quick brown fox jumped over the lazy dog")
		require.NoError(t, err)
		require.NotNil(t, sv)

		expectedIndices := []int{226376294, 741580288, 771291085, 1312749093, 1621867415, 1913189942}
		expectedValue := float32(1.6652868125369606)

		assert.Equal(t, expectedIndices, sv.Indices)
		require.Len(t, sv.Values, len(expectedIndices))
		for i, val := range sv.Values {
			assert.InDelta(t, expectedValue, val, 0.0001, "value at index %d", i)
		}
	})
}

func TestZeroParameters(t *testing.T) {
	t.Run("K=0 disables saturation", func(t *testing.T) {
		client, err := NewClient(WithK(0))
		require.NoError(t, err)
		assert.Equal(t, float64(0), client.K)
		assert.True(t, client.kSet)
	})

	t.Run("B=0 disables length normalization", func(t *testing.T) {
		client, err := NewClient(WithB(0))
		require.NoError(t, err)
		assert.Equal(t, float64(0), client.B)
		assert.True(t, client.bSet)
	})

	t.Run("K=0 and B=0 together", func(t *testing.T) {
		client, err := NewClient(WithK(0), WithB(0))
		require.NoError(t, err)
		assert.Equal(t, float64(0), client.K)
		assert.Equal(t, float64(0), client.B)
	})
}

func TestBM25ConfigJSONRoundTrip(t *testing.T) {
	t.Run("config survives JSON round-trip", func(t *testing.T) {
		// Create EF with custom config
		ef, err := NewEmbeddingFunction(
			WithK(1.5),
			WithB(0.8),
			WithAvgDocLength(150.0),
			WithTokenMaxLength(50),
			WithStopwords([]string{"custom", "stopwords"}),
			WithIncludeTokens(true),
		)
		require.NoError(t, err)

		// Get config
		cfg := ef.GetConfig()

		// Marshal to JSON (simulating persistence/network transfer)
		jsonBytes, err := json.Marshal(cfg)
		require.NoError(t, err)

		// Unmarshal from JSON
		var restoredCfg embeddings.EmbeddingFunctionConfig
		err = json.Unmarshal(jsonBytes, &restoredCfg)
		require.NoError(t, err)

		// Create new EF from restored config
		ef2, err := NewEmbeddingFunctionFromConfig(restoredCfg)
		require.NoError(t, err)

		// Verify config matches
		cfg2 := ef2.GetConfig()
		assert.Equal(t, 1.5, cfg2["k"])
		assert.Equal(t, 0.8, cfg2["b"])
		assert.Equal(t, 150.0, cfg2["avg_len"])
		assert.Equal(t, 50, cfg2["token_max_length"])
		assert.Equal(t, []string{"custom", "stopwords"}, cfg2["stopwords"])
		assert.Equal(t, true, cfg2["include_tokens"])
	})

	t.Run("embedding output matches after round-trip", func(t *testing.T) {
		// Create EF with custom config
		ef1, err := NewEmbeddingFunction(
			WithK(1.2),
			WithB(0.75),
			WithTokenMaxLength(30),
		)
		require.NoError(t, err)

		// Get config and round-trip through JSON
		cfg := ef1.GetConfig()
		jsonBytes, err := json.Marshal(cfg)
		require.NoError(t, err)

		var restoredCfg embeddings.EmbeddingFunctionConfig
		err = json.Unmarshal(jsonBytes, &restoredCfg)
		require.NoError(t, err)

		ef2, err := NewEmbeddingFunctionFromConfig(restoredCfg)
		require.NoError(t, err)

		// Both should produce identical embeddings
		text := "the quick brown fox jumps over lazy dog"
		sv1, err := ef1.EmbedQuerySparse(context.Background(), text)
		require.NoError(t, err)
		sv2, err := ef2.EmbedQuerySparse(context.Background(), text)
		require.NoError(t, err)

		assert.Equal(t, sv1.Indices, sv2.Indices)
		assert.Equal(t, sv1.Values, sv2.Values)
	})
}
