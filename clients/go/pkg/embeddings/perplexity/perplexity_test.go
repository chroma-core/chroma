//go:build ef

package perplexity

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Status:     fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func TestDecodeBase64Int8Embedding(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte{128, 255, 0, 1, 127})

	decoded, err := decodeBase64Int8Embedding(encoded)
	require.NoError(t, err)
	assert.Equal(t, []float32{-128, -1, 0, 1, 127}, decoded)
}

func TestPerplexityEmbeddingFunction_RequiresAPIKey(t *testing.T) {
	t.Setenv(APIKeyEnvVar, "")

	_, err := NewPerplexityEmbeddingFunction()
	require.Error(t, err)

	_, err = NewPerplexityEmbeddingFunction(WithEnvAPIKey())
	require.Error(t, err)
	require.Contains(t, err.Error(), APIKeyEnvVar)
}

func TestPerplexityEmbeddingFunction_EmptyDocuments(t *testing.T) {
	ef, err := NewPerplexityEmbeddingFunction(WithAPIKey("test-key"))
	require.NoError(t, err)

	result, err := ef.EmbedDocuments(context.Background(), []string{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestPerplexityEmbeddingFunction_EmbedQueryDecodesBase64Int8(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte{255, 1, 127})

	var requestPayload map[string]any
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			require.Equal(t, "Bearer test-key", req.Header.Get("Authorization"))
			require.Equal(t, "application/json", req.Header.Get("Content-Type"))

			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(body, &requestPayload))

			return newResponse(http.StatusOK, `{
				"object": "list",
				"data": [
					{"object":"embedding","index":0,"embedding":"`+encoded+`"}
				],
				"model":"pplx-embed-v1-4b"
			}`), nil
		}),
	}

	ef, err := NewPerplexityEmbeddingFunction(
		WithAPIKey("test-key"),
		WithHTTPClient(httpClient),
		WithModel("pplx-embed-v1-0.6b"),
		WithDimensions(3),
	)
	require.NoError(t, err)

	ctx := ContextWithModel(context.Background(), "pplx-embed-v1-4b")
	emb, err := ef.EmbedQuery(ctx, "hello world")
	require.NoError(t, err)
	assert.Equal(t, []float32{-1, 1, 127}, emb.ContentAsFloat32())

	assert.Equal(t, "pplx-embed-v1-4b", requestPayload["model"])
	assert.Equal(t, "base64_int8", requestPayload["encoding_format"])
	assert.Equal(t, "hello world", requestPayload["input"])
	assert.Equal(t, float64(3), requestPayload["dimensions"]) // JSON numbers decode as float64
}

func TestEmbeddingTypeResult_UnmarshalJSONFloatFallback(t *testing.T) {
	var result EmbeddingTypeResult
	err := json.Unmarshal([]byte(`[1.25,-2,3]`), &result)
	require.NoError(t, err)
	assert.Equal(t, []float32{1.25, -2, 3}, result.Floats)
}

func TestPerplexityEmbeddingFunction_HTTPErrorResponse(t *testing.T) {
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return newResponse(http.StatusTooManyRequests, `{"error":{"message":"Rate limit exceeded"}}`), nil
		}),
	}
	ef, err := NewPerplexityEmbeddingFunction(
		WithAPIKey("test-key"),
		WithHTTPClient(httpClient),
	)
	require.NoError(t, err)

	_, err = ef.EmbedQuery(context.Background(), "hello")
	require.Error(t, err)
	require.Contains(t, err.Error(), "429")
}

func TestPerplexityEmbeddingFunction_HTTPErrorResponse_TruncatedBody(t *testing.T) {
	longMsg := strings.Repeat("x", maxErrorBodyChars+200)
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return newResponse(http.StatusBadRequest, `{"error":"`+longMsg+`"}`), nil
		}),
	}
	ef, err := NewPerplexityEmbeddingFunction(
		WithAPIKey("test-key"),
		WithHTTPClient(httpClient),
	)
	require.NoError(t, err)

	_, err = ef.EmbedQuery(context.Background(), "hello")
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "...(truncated)")
	assert.Less(t, len(msg), len(longMsg)+100)
}

func TestSanitizeErrorBody_UTF8Safe(t *testing.T) {
	rune3byte := "☺"
	body := []byte(strings.Repeat(rune3byte, maxErrorBodyChars+10))
	result := sanitizeErrorBody(body)

	require.Contains(t, result, "...(truncated)")

	prefix := strings.TrimSuffix(result, "...(truncated)")
	runes := []rune(prefix)
	assert.Equal(t, maxErrorBodyChars, len(runes))
	for _, r := range runes {
		assert.Equal(t, '☺', r, "rune should not be corrupted")
	}
}

func TestPerplexityClient_CreateEmbedding_NilInputRejected(t *testing.T) {
	called := false
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return newResponse(http.StatusOK, `{"data":[]}`), nil
		}),
	}
	client, err := NewPerplexityClient(
		WithAPIKey("test-key"),
		WithHTTPClient(httpClient),
	)
	require.NoError(t, err)

	_, err = client.CreateEmbedding(context.Background(), &CreateEmbeddingRequest{
		Model: "pplx-embed-v1-0.6b",
		Input: nil,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "input is required")
	assert.False(t, called, "HTTP request should not be made for invalid input")
}

func TestPerplexityClient_CreateEmbedding_DimensionsPointerIsolation(t *testing.T) {
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return newResponse(http.StatusOK, `{
				"data":[{"index":0,"embedding":"AQ=="}]
			}`), nil
		}),
	}
	client, err := NewPerplexityClient(
		WithAPIKey("test-key"),
		WithHTTPClient(httpClient),
		WithDimensions(256),
	)
	require.NoError(t, err)

	reqA := &CreateEmbeddingRequest{
		Input: &EmbeddingInputs{Input: "doc-a"},
	}
	_, err = client.CreateEmbedding(context.Background(), reqA)
	require.NoError(t, err)
	require.NotNil(t, reqA.Dimensions)
	*reqA.Dimensions = 1
	require.NotNil(t, client.dimensions)
	assert.Equal(t, 256, *client.dimensions)

	externalDims := 777
	reqB := &CreateEmbeddingRequest{
		Input:      &EmbeddingInputs{Input: "doc-b"},
		Dimensions: &externalDims,
	}
	_, err = client.CreateEmbedding(context.Background(), reqB)
	require.NoError(t, err)
	require.NotNil(t, reqB.Dimensions)
	*reqB.Dimensions = 2
	assert.Equal(t, 777, externalDims)
}

func TestPerplexityEmbeddingFunction_EmbedDocumentsResponseValidation(t *testing.T) {
	t.Run("count mismatch", func(t *testing.T) {
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				enc := base64.StdEncoding.EncodeToString([]byte{1, 2, 3})
				return newResponse(http.StatusOK, `{
					"data":[
						{"index":0,"embedding":"`+enc+`"}
					]
				}`), nil
			}),
		}
		ef, err := NewPerplexityEmbeddingFunction(WithAPIKey("test-key"), WithHTTPClient(httpClient))
		require.NoError(t, err)

		_, err = ef.EmbedDocuments(context.Background(), []string{"doc1", "doc2"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "embedding count mismatch")
	})

	t.Run("nil embedding", func(t *testing.T) {
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return newResponse(http.StatusOK, `{
					"data":[
						{"index":0,"embedding":null}
					]
				}`), nil
			}),
		}
		ef, err := NewPerplexityEmbeddingFunction(WithAPIKey("test-key"), WithHTTPClient(httpClient))
		require.NoError(t, err)

		_, err = ef.EmbedDocuments(context.Background(), []string{"doc1"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "nil embedding")
	})

	t.Run("invalid index", func(t *testing.T) {
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				enc := base64.StdEncoding.EncodeToString([]byte{1, 2, 3})
				return newResponse(http.StatusOK, `{
					"data":[
						{"index":1,"embedding":"`+enc+`"}
					]
				}`), nil
			}),
		}
		ef, err := NewPerplexityEmbeddingFunction(WithAPIKey("test-key"), WithHTTPClient(httpClient))
		require.NoError(t, err)

		_, err = ef.EmbedDocuments(context.Background(), []string{"doc1"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid embedding index")
	})

	t.Run("duplicate index leading to missing slot", func(t *testing.T) {
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				encA := base64.StdEncoding.EncodeToString([]byte{1, 2, 3})
				encB := base64.StdEncoding.EncodeToString([]byte{4, 5, 6})
				return newResponse(http.StatusOK, `{
					"data":[
						{"index":0,"embedding":"`+encA+`"},
						{"index":0,"embedding":"`+encB+`"}
					]
				}`), nil
			}),
		}
		ef, err := NewPerplexityEmbeddingFunction(WithAPIKey("test-key"), WithHTTPClient(httpClient))
		require.NoError(t, err)

		_, err = ef.EmbedDocuments(context.Background(), []string{"doc1", "doc2"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing embedding at index 1")
	})

	t.Run("out-of-order indices are reordered correctly", func(t *testing.T) {
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				enc0 := base64.StdEncoding.EncodeToString([]byte{1, 2})
				enc1 := base64.StdEncoding.EncodeToString([]byte{255, 0})
				enc2 := base64.StdEncoding.EncodeToString([]byte{128, 127})
				return newResponse(http.StatusOK, `{
					"data":[
						{"index":2,"embedding":"`+enc2+`"},
						{"index":0,"embedding":"`+enc0+`"},
						{"index":1,"embedding":"`+enc1+`"}
					]
				}`), nil
			}),
		}
		ef, err := NewPerplexityEmbeddingFunction(WithAPIKey("test-key"), WithHTTPClient(httpClient))
		require.NoError(t, err)

		embs, err := ef.EmbedDocuments(context.Background(), []string{"docA", "docB", "docC"})
		require.NoError(t, err)
		require.Len(t, embs, 3)
		assert.Equal(t, []float32{1, 2}, embs[0].ContentAsFloat32())
		assert.Equal(t, []float32{-1, 0}, embs[1].ContentAsFloat32())
		assert.Equal(t, []float32{-128, 127}, embs[2].ContentAsFloat32())
	})
}

func TestPerplexityEmbeddingFunction_ConfigRoundTrip(t *testing.T) {
	t.Setenv("MY_PERPLEXITY_KEY", "test-perplexity-key")

	ef, err := NewPerplexityEmbeddingFunction(
		WithAPIKeyFromEnvVar("MY_PERPLEXITY_KEY"),
		WithModel("pplx-embed-v1-4b"),
		WithDimensions(256),
		WithBaseURL("http://perplexity.local/v1/embeddings"),
		WithInsecure(),
	)
	require.NoError(t, err)

	cfg := ef.GetConfig()
	assert.Equal(t, "MY_PERPLEXITY_KEY", cfg["api_key_env_var"])
	assert.Equal(t, "pplx-embed-v1-4b", cfg["model_name"])
	assert.Equal(t, 256, cfg["dimensions"])
	assert.Equal(t, "http://perplexity.local/v1/embeddings", cfg["base_url"])
	assert.Equal(t, true, cfg["insecure"])

	rebuilt, err := embeddings.BuildDense(ef.Name(), cfg)
	require.NoError(t, err)
	assert.Equal(t, "perplexity", rebuilt.Name())

	rebuiltCfg := rebuilt.GetConfig()
	assert.Equal(t, cfg["api_key_env_var"], rebuiltCfg["api_key_env_var"])
	assert.Equal(t, cfg["model_name"], rebuiltCfg["model_name"])
	assert.Equal(t, cfg["dimensions"], rebuiltCfg["dimensions"])
	assert.Equal(t, cfg["base_url"], rebuiltCfg["base_url"])
	assert.Equal(t, cfg["insecure"], rebuiltCfg["insecure"])
}

func TestPerplexityEmbeddingFunction_GetConfig_DefaultBaseURLNotPersisted(t *testing.T) {
	ef, err := NewPerplexityEmbeddingFunction(WithAPIKey("test-key"))
	require.NoError(t, err)

	cfg := ef.GetConfig()
	_, hasBaseURL := cfg["base_url"]
	assert.False(t, hasBaseURL, "default base_url should not be persisted")
}

func TestPerplexityEmbeddingFunction_RegisteredWithPerplexityName(t *testing.T) {
	require.True(t, embeddings.HasDense("perplexity"))

	t.Setenv(APIKeyEnvVar, "test-api-key")
	ef, err := embeddings.BuildDense("perplexity", embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": APIKeyEnvVar,
		"model_name":      "pplx-embed-v1-0.6b",
	})
	require.NoError(t, err)
	assert.Equal(t, "perplexity", ef.Name())
}

func TestPerplexityEmbeddingFunction_DefaultSpaceAndSupportedSpaces(t *testing.T) {
	ef, err := NewPerplexityEmbeddingFunction(WithAPIKey("test-key"))
	require.NoError(t, err)
	assert.Equal(t, embeddings.COSINE, ef.DefaultSpace())
	assert.Contains(t, ef.SupportedSpaces(), embeddings.COSINE)
	assert.Contains(t, ef.SupportedSpaces(), embeddings.L2)
	assert.Contains(t, ef.SupportedSpaces(), embeddings.IP)
}

func TestPerplexityEmbeddingFunction_EmbedQueryEmptyDocument(t *testing.T) {
	called := false
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return newResponse(http.StatusOK, `{"data":[]}`), nil
		}),
	}
	ef, err := NewPerplexityEmbeddingFunction(
		WithAPIKey("test-key"),
		WithHTTPClient(httpClient),
	)
	require.NoError(t, err)

	_, err = ef.EmbedQuery(context.Background(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "query document must not be empty")
	assert.False(t, called, "HTTP request should not be made for empty query")
}

func TestPerplexityEmbeddingFunction_TransportSecurityValidation(t *testing.T) {
	t.Run("HTTP URL rejected without WithInsecure", func(t *testing.T) {
		_, err := NewPerplexityEmbeddingFunction(
			WithAPIKey("test-key"),
			WithBaseURL("http://example.com/v1/embeddings"),
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "base URL must use HTTPS")
	})

	t.Run("HTTP URL accepted with WithInsecure", func(t *testing.T) {
		_, err := NewPerplexityEmbeddingFunction(
			WithAPIKey("test-key"),
			WithBaseURL("http://example.com/v1/embeddings"),
			WithInsecure(),
		)
		require.NoError(t, err)
	})
}

func TestNewPerplexityEmbeddingFunctionFromConfig_Validation(t *testing.T) {
	t.Run("missing api_key_env_var", func(t *testing.T) {
		_, err := NewPerplexityEmbeddingFunctionFromConfig(embeddings.EmbeddingFunctionConfig{
			"model_name": "pplx-embed-v1-0.6b",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "api_key_env_var is required")
	})

	t.Run("missing model_name", func(t *testing.T) {
		t.Setenv(APIKeyEnvVar, "test-key")
		_, err := NewPerplexityEmbeddingFunctionFromConfig(embeddings.EmbeddingFunctionConfig{
			"api_key_env_var": APIKeyEnvVar,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "model_name is required")
	})

	t.Run("non-positive dimensions", func(t *testing.T) {
		t.Setenv(APIKeyEnvVar, "test-key")
		_, err := NewPerplexityEmbeddingFunctionFromConfig(embeddings.EmbeddingFunctionConfig{
			"api_key_env_var": APIKeyEnvVar,
			"model_name":      "pplx-embed-v1-0.6b",
			"dimensions":      0,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "dimensions must be greater than 0")
	})
}

func TestPerplexityOptionsValidation(t *testing.T) {
	t.Run("empty API key rejected", func(t *testing.T) {
		_, err := NewPerplexityEmbeddingFunction(WithAPIKey(""))
		require.Error(t, err)
		require.Contains(t, err.Error(), "API key cannot be empty")
	})

	t.Run("empty model rejected", func(t *testing.T) {
		_, err := NewPerplexityEmbeddingFunction(WithModel(""))
		require.Error(t, err)
		require.Contains(t, err.Error(), "model cannot be empty")
	})

	t.Run("non-positive dimensions rejected", func(t *testing.T) {
		_, err := NewPerplexityEmbeddingFunction(WithDimensions(0))
		require.Error(t, err)
		require.Contains(t, err.Error(), "dimensions must be greater than 0")
	})

	t.Run("empty base URL rejected", func(t *testing.T) {
		_, err := NewPerplexityEmbeddingFunction(WithBaseURL(""))
		require.Error(t, err)
		require.Contains(t, err.Error(), "base URL cannot be empty")
	})

	t.Run("invalid base URL rejected", func(t *testing.T) {
		_, err := NewPerplexityEmbeddingFunction(WithBaseURL("://bad-url"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid base URL")
	})

	t.Run("nil HTTP client rejected", func(t *testing.T) {
		_, err := NewPerplexityEmbeddingFunction(WithHTTPClient(nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "HTTP client cannot be nil")
	})
}
