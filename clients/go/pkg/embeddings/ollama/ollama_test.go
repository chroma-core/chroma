//go:build ef

package ollama

import (
	"context"
	"fmt"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
	"github.com/stretchr/testify/require"
	tcollama "github.com/testcontainers/testcontainers-go/modules/ollama"
	"io"
	"net/http"
	"strings"
	"testing"
)

func Test_ollama(t *testing.T) {
	ctx := context.Background()
	ollamaContainer, err := tcollama.Run(ctx, "ollama/ollama:latest")
	require.NoError(t, err)
	// Clean up the container
	defer func() {
		if err := ollamaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s\n", err)
		}
	}()

	model := "nomic-embed-text"
	connectionStr, err := ollamaContainer.ConnectionString(ctx)
	require.NoError(t, err)
	pullURL := fmt.Sprintf("%s/api/pull", connectionStr)
	pullPayload := fmt.Sprintf(`{"name": "%s"}`, model)

	resp, err := http.Post(
		pullURL,
		"application/json",
		strings.NewReader(pullPayload),
	)
	require.NoError(t, err)
	respStr, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Contains(t, string(respStr), "success")

	// Ensure successful response
	require.Equal(t, http.StatusOK, resp.StatusCode)
	client, err := NewOllamaClient(WithBaseURL(connectionStr), WithModel(embeddings.EmbeddingModel(model)))
	require.NoError(t, err)
	t.Run("Test Create Embed Single document", func(t *testing.T) {
		resp, rerr := client.createEmbedding(context.Background(), &CreateEmbeddingRequest{Model: "nomic-embed-text", Input: &EmbeddingInput{Input: "Document 1 content here"}})
		require.Nil(t, rerr)
		require.NotNil(t, resp)
	})
	t.Run("Test Create Embed multi-document", func(t *testing.T) {
		documents := []string{
			"Document 1 content here",
			"Document 2 content here",
		}
		ef, err := NewOllamaEmbeddingFunction(WithBaseURL(connectionStr), WithModel(embeddings.EmbeddingModel(model)))
		require.NoError(t, err)
		resp, rerr := ef.EmbedDocuments(context.Background(), documents)
		require.Nil(t, rerr)
		require.NotNil(t, resp)
		require.Equal(t, 2, len(resp))
	})
}
