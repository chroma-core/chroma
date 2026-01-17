//go:build ef

package hf

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Test_huggingface_client(t *testing.T) {
	apiKey := os.Getenv("HF_API_KEY")
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
		apiKey = os.Getenv("HF_API_KEY")
	}
	ef := NewHuggingFaceEmbeddingFunction(apiKey, "sentence-transformers/all-MiniLM-L6-v2")

	t.Run("Test Create Embed", func(t *testing.T) {
		documents := []string{
			"Document 1 content here",
			"Document 2 content here",
			// Add more documents as needed
		}
		resp, rerr := ef.EmbedDocuments(context.Background(), documents)
		if rerr != nil && strings.Contains(rerr.Error(), "429 Too Many Requests") {
			t.Skipf("Skipping test due to rate limiting")
		}
		require.Nil(t, rerr)
		require.NotNil(t, resp)
		assert.Equal(t, 2, len(resp))
		// assert.Equal(t, 201, httpRes.StatusCode)
	})
}

func Test_Huggingface_client_with_options(t *testing.T) {
	apiKey := os.Getenv("HF_API_KEY")
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
		apiKey = os.Getenv("HF_API_KEY")
	}

	t.Run("Test with default huggingface endpoint", func(t *testing.T) {
		ef, err := NewHuggingFaceEmbeddingFunctionFromOptions(WithAPIKey(apiKey), WithModel("sentence-transformers/all-MiniLM-L6-v2"))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
			"Document 2 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("Test with huggingface endpoint", func(t *testing.T) {
		ef, err := NewHuggingFaceEmbeddingFunctionFromOptions(WithAPIKey(apiKey), WithModel("sentence-transformers/all-MiniLM-L6-v2"))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
			"Document 2 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("Test with huggingface endpoint", func(t *testing.T) {
		ef, err := NewHuggingFaceEmbeddingFunctionFromOptions(WithEnvAPIKey(), WithModel("sentence-transformers/all-MiniLM-L6-v2"))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
			"Document 2 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("Test client with default headers", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`[[1, 2, 3], [4, 5, 6]]`))
			w.WriteHeader(http.StatusOK)
			require.Equal(t, r.Header.Get("Custom-Token"), "Bearer my-custom-token")
		}))
		defer server.Close()
		var defaultHeaders = map[string]string{"Custom-Token": "Bearer my-custom-token"}
		ef, err := NewHuggingFaceEmbeddingInferenceFunction("http://"+server.Listener.Addr().String(), WithDefaultHeaders(defaultHeaders))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
			"Document 2 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 2, len(resp))
	})

	t.Run("Test with huggingface embedding inference", func(t *testing.T) {
		teiVersion := "1.8.3"
		teiImage := "ghcr.io/huggingface/text-embeddings-inference"
		if v := os.Getenv("TEI_VERSION"); v != "" {
			teiVersion = v
		}
		if img := os.Getenv("TEI_IMAGE"); img != "" {
			teiImage = img
		}
		ctx := context.Background()
		req := testcontainers.ContainerRequest{
			Image:         fmt.Sprintf("%s:cpu-%s", teiImage, teiVersion),
			ExposedPorts:  []string{"80/tcp"},
			WaitingFor:    wait.ForLog("Ready"),
			ImagePlatform: "linux/amd64",
			Cmd:           []string{"--model-id", "sentence-transformers/all-MiniLM-L6-v2"},
		}
		hfei, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, hfei.Terminate(ctx))
		})
		ip, err := hfei.Host(ctx)
		require.NoError(t, err)
		port, err := hfei.MappedPort(ctx, "80")
		require.NoError(t, err)
		ef, err := NewHuggingFaceEmbeddingInferenceFunction("http://" + ip + ":" + port.Port())

		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
			"Document 2 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("Test HTTP URL rejected with API key", func(t *testing.T) {
		_, err := NewHuggingFaceEmbeddingFunctionFromOptions(WithAPIKey(apiKey), WithBaseURL("http://example.com"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "base URL must use HTTPS")
	})

	t.Run("Test HTTP URL accepted with WithInsecure", func(t *testing.T) {
		_, err := NewHuggingFaceEmbeddingFunctionFromOptions(WithAPIKey(apiKey), WithBaseURL("http://example.com"), WithInsecure())
		require.NoError(t, err)
	})

	t.Run("Test HTTPS URL accepted", func(t *testing.T) {
		_, err := NewHuggingFaceEmbeddingFunctionFromOptions(WithAPIKey(apiKey), WithBaseURL("https://example.com"))
		require.NoError(t, err)
	})

	t.Run("Test HTTP URL allowed for TEI endpoint without API key", func(t *testing.T) {
		// TEI endpoints without API key should allow HTTP since there's no secret to protect
		_, err := NewHuggingFaceEmbeddingInferenceFunction("http://localhost:8080")
		require.NoError(t, err)
	})
}
