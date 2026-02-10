//go:build ef

package embeddings_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	v2 "github.com/chroma-core/chroma/clients/go"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/chromacloud"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/cohere"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/jina"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/mistral"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/morph"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/nomic"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/ollama"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/openai"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/together"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/voyage"
)

// TestEFPersistenceIntegration tests the full round-trip of EF persistence:
// 1. Create an EF
// 2. Create a collection with that EF
// 3. Get the collection WITHOUT specifying an EF
// 4. Verify the EF is auto-wired by performing embedding operations

func setupChromaContainer(t *testing.T) (string, func()) {
	ctx := context.Background()

	chromaImage := "ghcr.io/chroma-core/chroma:1.5.0"
	if img := os.Getenv("CHROMA_IMAGE"); img != "" {
		chromaImage = img
	}

	req := testcontainers.ContainerRequest{
		Image:        chromaImage,
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(nat.Port("8000/tcp")),
			wait.ForHTTP("/api/v2/heartbeat").WithPort("8000/tcp"),
		).WithDeadline(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, "8000")
	require.NoError(t, err)

	baseURL := fmt.Sprintf("http://%s:%s", host, port.Port())

	cleanup := func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return baseURL, cleanup
}

// TestEFPersistence_ConsistentHash_Integration tests consistent_hash EF persistence
// This test always runs as it doesn't require external API keys
func TestEFPersistence_ConsistentHash_Integration(t *testing.T) {
	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	// Create EF
	ef := embeddings.NewConsistentHashEmbeddingFunction()

	// Create collection with EF
	collectionName := "test_consistent_hash_persistence"
	col, err := client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)
	require.NotNil(t, col)

	// Get collection WITHOUT specifying EF - should auto-wire
	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)
	require.NotNil(t, retrievedCol)

	// Verify EF is auto-wired by performing embedding operations
	err = retrievedCol.Add(ctx, v2.WithIDs("doc1", "doc2"), v2.WithTexts("hello world", "goodbye world"))
	require.NoError(t, err)

	// Query using text (requires EF to be wired)
	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, results.GetDocumentsGroups())

	// Verify via ListCollections too
	collections, err := client.ListCollections(ctx)
	require.NoError(t, err)
	var foundCol v2.Collection
	for _, c := range collections {
		if c.Name() == collectionName {
			foundCol = c
			break
		}
	}
	require.NotNil(t, foundCol)

	// Verify ListCollections also auto-wires EF
	err = foundCol.Add(ctx, v2.WithIDs("doc3"), v2.WithTexts("another document"))
	require.NoError(t, err)

	count, err := foundCol.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

// TestEFPersistence_Ollama_Integration tests ollama EF persistence
// Requires OLLAMA_HOST env var or skips
func TestEFPersistence_Ollama_Integration(t *testing.T) {
	ollamaHost := os.Getenv("OLLAMA_HOST")
	if ollamaHost == "" {
		t.Skip("OLLAMA_HOST not set, skipping Ollama integration test")
	}

	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	// Create EF
	ef, err := ollama.NewOllamaEmbeddingFunction(
		ollama.WithBaseURL(ollamaHost),
		ollama.WithModel("nomic-embed-text"),
	)
	require.NoError(t, err)

	// Create collection with EF
	collectionName := "test_ollama_persistence"
	col, err := client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)
	require.NotNil(t, col)

	// Get collection WITHOUT specifying EF - should auto-wire
	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)
	require.NotNil(t, retrievedCol)

	// Verify EF is auto-wired by performing embedding operations
	err = retrievedCol.Add(ctx, v2.WithIDs("doc1", "doc2"), v2.WithTexts("hello world", "goodbye world"))
	require.NoError(t, err)

	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, results.GetDocumentsGroups())
}

// TestEFPersistence_OpenAI_Integration tests OpenAI EF persistence
// Requires OPENAI_API_KEY env var or skips
func TestEFPersistence_OpenAI_Integration(t *testing.T) {
	if os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("OPENAI_API_KEY not set, skipping OpenAI integration test")
	}

	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	// Create EF
	ef, err := openai.NewOpenAIEmbeddingFunction("", openai.WithEnvAPIKey())
	require.NoError(t, err)

	// Create collection with EF
	collectionName := "test_openai_persistence"
	col, err := client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)
	require.NotNil(t, col)

	// Get collection WITHOUT specifying EF - should auto-wire
	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)
	require.NotNil(t, retrievedCol)

	// Verify EF is auto-wired
	err = retrievedCol.Add(ctx, v2.WithIDs("doc1", "doc2"), v2.WithTexts("hello world", "goodbye world"))
	require.NoError(t, err)

	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotNil(t, results)
	require.NotEmpty(t, results.GetDocumentsGroups())
}

// TestEFPersistence_Cohere_Integration tests Cohere EF persistence
func TestEFPersistence_Cohere_Integration(t *testing.T) {
	if os.Getenv("COHERE_API_KEY") == "" {
		t.Skip("COHERE_API_KEY not set, skipping Cohere integration test")
	}

	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	ef, err := cohere.NewCohereEmbeddingFunction(cohere.WithEnvAPIKey())
	require.NoError(t, err)

	collectionName := "test_cohere_persistence"
	_, err = client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)

	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)

	err = retrievedCol.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("hello world"))
	require.NoError(t, err)

	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results.GetDocumentsGroups())
}

// TestEFPersistence_Jina_Integration tests Jina EF persistence
func TestEFPersistence_Jina_Integration(t *testing.T) {
	if os.Getenv("JINA_API_KEY") == "" {
		t.Skip("JINA_API_KEY not set, skipping Jina integration test")
	}

	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	ef, err := jina.NewJinaEmbeddingFunction(jina.WithEnvAPIKey())
	require.NoError(t, err)

	collectionName := "test_jina_persistence"
	_, err = client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)

	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)

	err = retrievedCol.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("hello world"))
	require.NoError(t, err)

	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results.GetDocumentsGroups())
}

// TestEFPersistence_Mistral_Integration tests Mistral EF persistence
func TestEFPersistence_Mistral_Integration(t *testing.T) {
	if os.Getenv("MISTRAL_API_KEY") == "" {
		t.Skip("MISTRAL_API_KEY not set, skipping Mistral integration test")
	}

	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	ef, err := mistral.NewMistralEmbeddingFunction(mistral.WithEnvAPIKey())
	require.NoError(t, err)

	collectionName := "test_mistral_persistence"
	_, err = client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)

	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)

	err = retrievedCol.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("hello world"))
	require.NoError(t, err)

	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results.GetDocumentsGroups())
}

// TestEFPersistence_Morph_Integration tests Morph EF persistence
func TestEFPersistence_Morph_Integration(t *testing.T) {
	if os.Getenv("MORPH_API_KEY") == "" {
		t.Skip("MORPH_API_KEY not set, skipping Morph integration test")
	}

	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	ef, err := morph.NewMorphEmbeddingFunction(morph.WithEnvAPIKey())
	require.NoError(t, err)

	collectionName := "test_morph_persistence"
	_, err = client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)

	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)

	err = retrievedCol.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("hello world"))
	require.NoError(t, err)

	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results.GetDocumentsGroups())
}

// TestEFPersistence_Voyage_Integration tests Voyage EF persistence
func TestEFPersistence_Voyage_Integration(t *testing.T) {
	if os.Getenv("VOYAGE_API_KEY") == "" {
		t.Skip("VOYAGE_API_KEY not set, skipping Voyage integration test")
	}

	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	ef, err := voyage.NewVoyageAIEmbeddingFunction(voyage.WithEnvAPIKey())
	require.NoError(t, err)

	collectionName := "test_voyage_persistence"
	_, err = client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)

	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)

	err = retrievedCol.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("hello world"))
	require.NoError(t, err)

	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results.GetDocumentsGroups())
}

// TestEFPersistence_Together_Integration tests Together EF persistence
func TestEFPersistence_Together_Integration(t *testing.T) {
	if os.Getenv("TOGETHER_API_KEY") == "" {
		t.Skip("TOGETHER_API_KEY not set, skipping Together integration test")
	}

	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	ef, err := together.NewTogetherEmbeddingFunction(together.WithEnvAPIToken())
	require.NoError(t, err)

	collectionName := "test_together_persistence"
	_, err = client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)

	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)

	err = retrievedCol.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("hello world"))
	require.NoError(t, err)

	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results.GetDocumentsGroups())
}

// TestEFPersistence_Nomic_Integration tests Nomic EF persistence
func TestEFPersistence_Nomic_Integration(t *testing.T) {
	if os.Getenv("NOMIC_API_KEY") == "" {
		t.Skip("NOMIC_API_KEY not set, skipping Nomic integration test")
	}

	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	ef, err := nomic.NewNomicEmbeddingFunction(nomic.WithEnvAPIKey())
	require.NoError(t, err)

	collectionName := "test_nomic_persistence"
	_, err = client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)

	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)

	err = retrievedCol.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("hello world"))
	require.NoError(t, err)

	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results.GetDocumentsGroups())
}

// TestEFPersistence_ChromaCloud_Integration tests ChromaCloud EF persistence
func TestEFPersistence_ChromaCloud_Integration(t *testing.T) {
	if os.Getenv("CHROMA_API_KEY") == "" {
		t.Skip("CHROMA_API_KEY not set, skipping ChromaCloud integration test")
	}

	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	ef, err := chromacloud.NewEmbeddingFunction(chromacloud.WithEnvAPIKey())
	require.NoError(t, err)

	collectionName := "test_chromacloud_persistence"
	_, err = client.CreateCollection(ctx, collectionName, v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)

	retrievedCol, err := client.GetCollection(ctx, collectionName)
	require.NoError(t, err)

	err = retrievedCol.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("hello world"))
	require.NoError(t, err)

	results, err := retrievedCol.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results.GetDocumentsGroups())
}

// TestEFPersistence_MultipleEFs_Integration tests that multiple collections with different EFs
// can coexist and each auto-wires correctly
func TestEFPersistence_MultipleEFs_Integration(t *testing.T) {
	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	// Create two collections with different EF configurations
	ef1 := embeddings.NewConsistentHashEmbeddingFunction()

	col1, err := client.CreateCollection(ctx, "collection_ef1", v2.WithEmbeddingFunctionCreate(ef1))
	require.NoError(t, err)

	col2, err := client.CreateCollection(ctx, "collection_ef2", v2.WithEmbeddingFunctionCreate(ef1))
	require.NoError(t, err)

	// Add different data to each
	err = col1.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("cats are fluffy"))
	require.NoError(t, err)

	err = col2.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("dogs are loyal"))
	require.NoError(t, err)

	// Get collections without EF - should auto-wire
	retrievedCol1, err := client.GetCollection(ctx, "collection_ef1")
	require.NoError(t, err)

	retrievedCol2, err := client.GetCollection(ctx, "collection_ef2")
	require.NoError(t, err)

	// Query each and verify correct results
	results1, err := retrievedCol1.Query(ctx, v2.WithQueryTexts("fluffy"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results1.GetDocumentsGroups())
	require.Contains(t, results1.GetDocumentsGroups()[0][0].ContentString(), "cats")

	results2, err := retrievedCol2.Query(ctx, v2.WithQueryTexts("loyal"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results2.GetDocumentsGroups())
	require.Contains(t, results2.GetDocumentsGroups()[0][0].ContentString(), "dogs")

	// Also verify via ListCollections
	collections, err := client.ListCollections(ctx)
	require.NoError(t, err)
	require.Len(t, collections, 2)

	for _, col := range collections {
		// Each collection should be able to perform embedding ops
		err = col.Add(ctx, v2.WithIDs("doc2"), v2.WithTexts("additional doc"))
		require.NoError(t, err)

		count, err := col.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, count)
	}
}

// TestEFPersistence_GetOrCreateCollection_Integration tests auto-wiring with GetOrCreateCollection
func TestEFPersistence_GetOrCreateCollection_Integration(t *testing.T) {
	baseURL, cleanup := setupChromaContainer(t)
	defer cleanup()

	ctx := context.Background()
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDatabaseAndTenant(v2.DefaultDatabase, v2.DefaultTenant),
	)
	require.NoError(t, err)
	defer client.Close()

	ef := embeddings.NewConsistentHashEmbeddingFunction()

	// First call creates the collection
	col1, err := client.GetOrCreateCollection(ctx, "getorcreate_test", v2.WithEmbeddingFunctionCreate(ef))
	require.NoError(t, err)

	err = col1.Add(ctx, v2.WithIDs("doc1"), v2.WithTexts("hello world"))
	require.NoError(t, err)

	// Second call gets existing collection - should auto-wire EF
	col2, err := client.GetOrCreateCollection(ctx, "getorcreate_test")
	require.NoError(t, err)

	// Verify auto-wired EF works
	err = col2.Add(ctx, v2.WithIDs("doc2"), v2.WithTexts("goodbye world"))
	require.NoError(t, err)

	results, err := col2.Query(ctx, v2.WithQueryTexts("hello"), v2.WithNResults(1))
	require.NoError(t, err)
	require.NotEmpty(t, results.GetDocumentsGroups())

	count, err := col2.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}
