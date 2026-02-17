//go:build !cloud

package chroma

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Masterminds/semver"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// NOTE: Running this test with -race flag may cause crashes during runtime cleanup
// (e.g., "fault 0x19bd96388" after all tests pass). This is a known limitation of
// Go's race detector with native code libraries (purego, CGO) on macOS ARM64.
// See: https://github.com/golang/go/issues/49138, https://github.com/golang/go/issues/17190
// The tests themselves pass correctly; the crash occurs during GC/runtime shutdown
// when ThreadSanitizer interacts with native library cleanup. This is not a bug in the code.
func TestCollectionAddIntegration(t *testing.T) {
	ctx := context.Background()
	var chromaVersion = "1.5.0"
	var chromaImage = "ghcr.io/chroma-core/chroma"
	if os.Getenv("CHROMA_VERSION") != "" {
		chromaVersion = os.Getenv("CHROMA_VERSION")
	}
	if os.Getenv("CHROMA_IMAGE") != "" {
		chromaImage = os.Getenv("CHROMA_IMAGE")
	}
	cwd, err := os.Getwd()
	require.NoError(t, err)
	mounts := []HostMount{
		{
			Source: filepath.Join(cwd, "v1-config.yaml"),
			Target: "/config.yaml",
		},
	}
	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("%s:%s", chromaImage, chromaVersion),
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("8000/tcp"),
			wait.ForHTTP("/api/v2/heartbeat").WithStatusCodeMatcher(func(status int) bool {
				return status == 200
			}),
		),
		Env: map[string]string{
			"ALLOW_RESET": "true", // does not work with Chroma v1.0.x
		},
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			dockerMounts := make([]mount.Mount, 0)
			for _, mnt := range mounts {
				dockerMounts = append(dockerMounts, mount.Mount{
					Type:   mount.TypeBind,
					Source: mnt.Source,
					Target: mnt.Target,
				})
			}
			hostConfig.Mounts = dockerMounts
		},
	}
	chromaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, chromaContainer.Terminate(ctx))
	})

	ip, err := chromaContainer.Host(ctx)
	require.NoError(t, err)
	port, err := chromaContainer.MappedPort(ctx, "8000")
	require.NoError(t, err)
	endpoint := fmt.Sprintf("http://%s:%s", ip, port.Port())

	chromaURL := os.Getenv("CHROMA_URL")
	if chromaURL == "" {
		chromaURL = endpoint
	}
	c, err := NewHTTPClient(WithBaseURL(chromaURL), WithLogger(testLogger()))
	require.NoError(t, err)

	// For Chroma versions < 1.0.0, disable EF config storage as they don't support it
	supportsEFConfig := supportsEFConfigPersistence(chromaVersion)

	// Helper to create collection with proper legacy support
	createCollection := func(name string, opts ...CreateCollectionOption) (Collection, error) {
		if !supportsEFConfig {
			opts = append(opts, WithDisableEFConfigStorage())
		}
		return c.CreateCollection(ctx, name, opts...)
	}

	t.Cleanup(func() {
		err := c.Close()
		require.NoError(t, err)
	})

	t.Run("add documents", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		count, err := collection.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, count)

		err = collection.Add(ctx, WithIDs("4", "5", "6"), WithTexts("test_document_4", "test_document_5", "test_document_6"))
		require.NoError(t, err)

		err = collection.Add(ctx, WithIDGenerator(NewSHA256Generator()), WithTexts("test_document_7", "test_document_8", "test_document_9"))
		require.NoError(t, err)

		err = collection.Add(ctx, WithIDGenerator(NewULIDGenerator()), WithTexts("test_document_10", "test_document_11", "test_document_12"))
		require.NoError(t, err)
	})

	t.Run("add documents with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		// no ids or id generator
		err = collection.Add(ctx, WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one ID or record is required. Alternatively, an ID generator can be provided")

		err = collection.Add(ctx, WithEmbeddings(embeddings.NewEmbeddingFromFloat32([]float32{1.0, 2.0, 3.0})))
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one ID or record is required. Alternatively, an ID generator can be provided")

		// no documents or embeddings
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()))
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one document or embedding is required")

	})

	t.Run("get documents", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDs("1", "2", "3"), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Get(ctx, WithIDs("1", "2", "3"))
		require.NoError(t, err)
		require.Equal(t, 3, len(res.GetIDs()))

		res, err = collection.Get(ctx, WithIDs("1_1", "2_3", "3_0"))
		require.NoError(t, err)
		require.Equal(t, 0, len(res.GetIDs()))

		res, err = collection.Get(ctx, WithInclude(IncludeEmbeddings))
		require.NoError(t, err)
		require.Equal(t, 3, len(res.GetIDs()))

	})

	t.Run("get documents with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)

		// wrong limit
		_, err = collection.Get(ctx, WithLimit(-1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "limit must be greater than 0")

		_, err = collection.Get(ctx, WithLimit(0))
		require.Error(t, err)
		require.Contains(t, err.Error(), "limit must be greater than 0")

		// wrong offset
		_, err = collection.Get(ctx, WithOffset(-1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "offset must be greater than or equal to 0")
	})

	t.Run("get documents with limit and offset", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Get(ctx, WithLimit(1), WithOffset(0))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDs()))
	})

	t.Run("get documents where_document regex", func(t *testing.T) {
		if chromaVersion != "latest" {
			cVersion, err := semver.NewVersion(chromaVersion)
			require.NoError(t, err)
			if !semver.MustParse("1.0.8").LessThan(cVersion) {
				t.Skipf("skipping for chroma version %s", cVersion)
			}
		}
		err = c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("this is document 1", "another document", "384km is the distance between the earth and the moon"))
		require.NoError(t, err)
		res, err := collection.Get(ctx, WithWhereDocument(Regex("[0-9]+km")))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDs()))
		require.Equal(t, "384km is the distance between the earth and the moon", res.GetDocuments()[0].ContentString())
	})

	t.Run("get documents with where", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"),
			WithMetadatas(
				NewDocumentMetadata(NewStringAttribute("test_key", "doc1")),
				NewDocumentMetadata(NewStringAttribute("test_key", "doc2")),
				NewDocumentMetadata(NewStringAttribute("test_key", "doc3")),
			),
		)
		require.NoError(t, err)
		res, err := collection.Get(ctx, WithWhere(EqString(K("test_key"), "doc1")))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDs()))
		require.Equal(t, "test_document_1", res.GetDocuments()[0].ContentString())
	})
	t.Run("count documents", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		count, err := collection.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, count)
	})

	t.Run("delete documents", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDs("1", "2", "3"), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		err = collection.Delete(ctx, WithIDs("1", "2", "3"))
		require.NoError(t, err)
		count, err := collection.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})

	t.Run("delete documents with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)

		// No Filters
		err = collection.Delete(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one filter is required, ids, where or whereDocument")
	})

	t.Run("upsert documents", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)

		err = collection.Add(ctx, WithIDs("1", "2", "3"), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		err = collection.Upsert(ctx, WithIDs("1", "2", "3"), WithTexts("test_document_1_updated", "test_document_2_updated", "test_document_3_updated"))
		require.NoError(t, err)
		count, err := collection.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, count)
		res, err := collection.Get(ctx, WithIDs("1", "2", "3"))
		require.NoError(t, err)
		require.Equal(t, 3, len(res.GetIDs()))
		require.Equal(t, "test_document_1_updated", res.GetDocuments()[0].ContentString())
		require.Equal(t, "test_document_2_updated", res.GetDocuments()[1].ContentString())
		require.Equal(t, "test_document_3_updated", res.GetDocuments()[2].ContentString())
	})

	t.Run("upsert with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		// no ids or id generator
		err = collection.Upsert(ctx, WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one ID or record is required. Alternatively, an ID generator can be provided")

		err = collection.Upsert(ctx, WithEmbeddings(embeddings.NewEmbeddingFromFloat32([]float32{1.0, 2.0, 3.0})))
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one ID or record is required. Alternatively, an ID generator can be provided")

		// no documents or embeddings
		err = collection.Upsert(ctx, WithIDGenerator(NewUUIDGenerator()))
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one document or embedding is required")
	})

	t.Run("update documents", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection",
			WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()),
		)
		require.NoError(t, err)
		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("test_document_1", "test_document_2", "test_document_3"),
			WithMetadatas(
				NewMetadata(NewStringAttribute("test_key_1", "original")),
				NewMetadata(NewStringAttribute("test_key_2", "original")),
				NewMetadata(NewStringAttribute("test_key_3", "original")),
			),
		)
		require.NoError(t, err)
		err = collection.Update(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("test_document_1_updated", "test_document_2_updated", "test_document_3_updated"),
			WithMetadatas(
				NewMetadata(NewIntAttribute("test_key_1", 1)),
				NewMetadata(RemoveAttribute("test_key_2"), NewStringAttribute("test_key_3", "updated")),
				NewMetadata(NewFloatAttribute("test_key_3", 2.0)),
			),
		)
		require.NoError(t, err)
		count, err := collection.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, count)
		res, err := collection.Get(ctx, WithIDs("1", "2", "3"))
		require.NoError(t, err)
		require.Equal(t, 3, len(res.GetIDs()))
		require.Equal(t, "test_document_1_updated", res.GetDocuments()[0].ContentString())
		require.Equal(t, "test_document_2_updated", res.GetDocuments()[1].ContentString())
		require.Equal(t, "test_document_3_updated", res.GetDocuments()[2].ContentString())
		mv1, ok := res.GetMetadatas()[0].GetInt("test_key_1")
		require.True(t, ok)
		require.Equal(t, int64(1), mv1)
		mv2, ok := res.GetMetadatas()[1].GetString("test_key_3")
		require.True(t, ok)
		require.Equal(t, "updated", mv2)
		_, nok := res.GetMetadatas()[1].GetString("test_key_2")
		require.False(t, nok, "test_key_2 should be removed")
		mv3, ok := res.GetMetadatas()[2].GetFloat("test_key_3")
		require.True(t, ok)
		require.Equal(t, 2.0, mv3)
	})

	t.Run("update documents with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		// silent ignore of update
		err = collection.Update(ctx, WithIDs("1", "2", "3"), WithTexts("test_document_1_updated", "test_document_2_updated", "test_document_3_updated"))
		require.NoError(t, err)
		count, err := collection.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, count)

		// no ids
		err = collection.Update(ctx, WithTexts("test_document_1_updated", "test_document_2_updated", "test_document_3_updated"))
		require.Error(t, err)
		fmt.Println("error", err)
		require.Contains(t, err.Error(), "at least one ID or record is required.")

	})

	t.Run("query documents", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("test_document_1"))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 3, len(res.GetIDGroups()[0]))
		require.Equal(t, "test_document_1", res.GetDocumentsGroups()[0][0].ContentString())
	})
	t.Run("query documents with where", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(
			NewUUIDGenerator()),
			WithTexts("test_document_1", "test_document_2", "test_document_3"),
			WithMetadatas(
				NewDocumentMetadata(NewStringAttribute("test_key", "doc1")),
				NewDocumentMetadata(NewStringAttribute("test_key", "doc2")),
				NewDocumentMetadata(NewStringAttribute("test_key", "doc3")),
			),
		)
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("test_document_1"), WithWhere(EqString(K("test_key"), "doc1")))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 1, len(res.GetIDGroups()[0]))
		require.Equal(t, "test_document_1", res.GetDocumentsGroups()[0][0].ContentString())
	})
	t.Run("query documents with where document", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("test_document_1"), WithWhereDocument(Contains("test_document_1")))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 1, len(res.GetIDGroups()[0]))
		require.Equal(t, "test_document_1", res.GetDocumentsGroups()[0][0].ContentString())
	})

	t.Run("query documents with where document - regex", func(t *testing.T) {
		if chromaVersion != "latest" {
			cVersion, err := semver.NewVersion(chromaVersion)
			require.NoError(t, err)
			if !semver.MustParse("1.0.8").LessThan(cVersion) {
				t.Skipf("skipping for chroma version %s", cVersion)
			}
		}
		err = c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("this is document about cats", "123141231", "$@!123115"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("123"), WithWhereDocument(Regex("^\\d+$")))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 1, len(res.GetIDGroups()[0]))
		require.Equal(t, "123141231", res.GetDocumentsGroups()[0][0].ContentString())
	})

	t.Run("query documents with include", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("test_document_1"), WithWhereDocument(Contains("test_document_1")), WithInclude(IncludeMetadatas))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 1, len(res.GetIDGroups()[0]))
		require.Equal(t, 1, len(res.GetMetadatasGroups()))
		require.Equal(t, 0, len(res.GetDocumentsGroups()))
		require.Equal(t, 0, len(res.GetDistancesGroups()))
	})

	t.Run("query with n_results", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("test_document_1"), WithNResults(2))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 2, len(res.GetIDGroups()[0]))
		require.Equal(t, 1, len(res.GetMetadatasGroups()))
		require.Equal(t, 2, len(res.GetMetadatasGroups()[0]))
		require.Equal(t, 1, len(res.GetDocumentsGroups()))
		require.Equal(t, 2, len(res.GetDocumentsGroups()[0]))
		require.Equal(t, 1, len(res.GetDistancesGroups()))
		require.Equal(t, 2, len(res.GetDistancesGroups()[0]))
	})

	t.Run("query with query embeddings", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		ef := embeddings.NewConsistentHashEmbeddingFunction()
		embedding, err := ef.EmbedQuery(ctx, "test_document_1")
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryEmbeddings(embedding))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 3, len(res.GetIDGroups()[0]))
		require.Equal(t, 1, len(res.GetDocumentsGroups()))
		require.Equal(t, 3, len(res.GetDocumentsGroups()[0]))
		require.Equal(t, "test_document_1", res.GetDocumentsGroups()[0][0].ContentString())
	})

	t.Run("query with query IDs", func(t *testing.T) {
		v, err := c.GetVersion(ctx)
		require.NoError(t, err)
		if !strings.HasPrefix(v, "1.") {
			t.Skipf("skipping for chroma version %s", v)
		}
		err = c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDs("1", "2", "3"), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("test_document_1"), WithIDs("1", "3"))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 2, len(res.GetIDGroups()[0]))
		require.Equal(t, 1, len(res.GetDocumentsGroups()))
		require.Equal(t, 2, len(res.GetDocumentsGroups()[0]))
		require.Equal(t, "test_document_1", res.GetDocumentsGroups()[0][0].ContentString())
	})

	t.Run("query with errors ", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		// no options
		_, err = collection.Query(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one query embedding or query text is required")

		// empty query texts

		_, err = collection.Query(ctx, WithQueryTexts())
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one query text is required")
		// empty query embeddings
		_, err = collection.Query(ctx, WithQueryEmbeddings())
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one query embedding is required")
		// empty query IDs
		_, err = collection.Query(ctx, WithIDs(), WithQueryTexts("test"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one id is required")

		// empty where
		_, err = collection.Query(ctx, WithWhere(EqString(K(""), "")), WithQueryTexts("test"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid key for $eq, expected non-empty")
	})

	t.Run("query with explicit include distances", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("test_document_1"), WithInclude(IncludeDistances))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 3, len(res.GetIDGroups()[0]))
		require.Equal(t, 1, len(res.GetDistancesGroups()))
		require.Equal(t, 3, len(res.GetDistancesGroups()[0]))
		require.Equal(t, 0, len(res.GetDocumentsGroups()))
		require.Equal(t, 0, len(res.GetMetadatasGroups()))
	})

	t.Run("query with multiple includes including distances", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("test_document_1"), WithInclude(IncludeDistances, IncludeDocuments, IncludeMetadatas))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 3, len(res.GetIDGroups()[0]))
		require.Equal(t, 1, len(res.GetDistancesGroups()))
		require.Equal(t, 3, len(res.GetDistancesGroups()[0]))
		require.Equal(t, 1, len(res.GetDocumentsGroups()))
		require.Equal(t, 3, len(res.GetDocumentsGroups()[0]))
		require.Equal(t, 1, len(res.GetMetadatasGroups()))
		require.Equal(t, 3, len(res.GetMetadatasGroups()[0]))
	})

	t.Run("query distances are in ascending order", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("apple", "banana", "cherry", "date"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("apple"), WithInclude(IncludeDistances), WithNResults(4))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetDistancesGroups()))
		distances := res.GetDistancesGroups()[0]
		require.Equal(t, 4, len(distances))
		for i := 0; i < len(distances)-1; i++ {
			require.LessOrEqual(t, distances[i], distances[i+1], "distances should be in ascending order")
		}
	})

	t.Run("query with multiple query texts returns distance groups", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("test_document_1", "test_document_2"), WithInclude(IncludeDistances), WithNResults(2))
		require.NoError(t, err)
		require.Equal(t, 2, len(res.GetIDGroups()))
		require.Equal(t, 2, len(res.GetDistancesGroups()))
		require.Equal(t, 2, len(res.GetDistancesGroups()[0]))
		require.Equal(t, 2, len(res.GetDistancesGroups()[1]))
	})

	t.Run("query with multiple query texts returns all field groups", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx,
			WithIDs("id1", "id2", "id3"),
			WithTexts("apple pie recipe", "banana smoothie recipe", "cherry tart recipe"),
			WithMetadatas(
				NewDocumentMetadata(NewStringAttribute("fruit", "apple")),
				NewDocumentMetadata(NewStringAttribute("fruit", "banana")),
				NewDocumentMetadata(NewStringAttribute("fruit", "cherry")),
			),
		)
		require.NoError(t, err)
		res, err := collection.Query(ctx,
			WithQueryTexts("apple pie recipe", "banana smoothie recipe", "cherry tart recipe"),
			WithInclude(IncludeDistances, IncludeDocuments, IncludeMetadatas, IncludeEmbeddings),
			WithNResults(2),
		)
		require.NoError(t, err)

		require.Equal(t, 3, len(res.GetIDGroups()), "expected 3 ID groups for 3 query texts")
		require.Equal(t, 3, len(res.GetDocumentsGroups()), "expected 3 document groups for 3 query texts")
		require.Equal(t, 3, len(res.GetDistancesGroups()), "expected 3 distance groups for 3 query texts")
		require.Equal(t, 3, len(res.GetMetadatasGroups()), "expected 3 metadata groups for 3 query texts")
		require.Equal(t, 3, len(res.GetEmbeddingsGroups()), "expected 3 embedding groups for 3 query texts")

		queryTexts := []string{"apple pie recipe", "banana smoothie recipe", "cherry tart recipe"}
		expectedIDs := []DocumentID{"id1", "id2", "id3"}
		expectedFruits := []string{"apple", "banana", "cherry"}
		for i := 0; i < 3; i++ {
			require.Equal(t, 2, len(res.GetIDGroups()[i]), "expected 2 IDs in group %d", i)
			require.Equal(t, 2, len(res.GetDocumentsGroups()[i]), "expected 2 documents in group %d", i)
			require.Equal(t, 2, len(res.GetDistancesGroups()[i]), "expected 2 distances in group %d", i)
			require.Equal(t, 2, len(res.GetMetadatasGroups()[i]), "expected 2 metadatas in group %d", i)
			require.Equal(t, 2, len(res.GetEmbeddingsGroups()[i]), "expected 2 embeddings in group %d", i)

			require.Equal(t, expectedIDs[i], res.GetIDGroups()[i][0], "closest ID for query %q", queryTexts[i])
			require.Equal(t, NewTextDocument(queryTexts[i]), res.GetDocumentsGroups()[i][0], "closest document for query %q", queryTexts[i])
			require.InDelta(t, 0, float64(res.GetDistancesGroups()[i][0]), 1e-6, "distance should be ~0 for exact match on query %q", queryTexts[i])
			fruit, ok := res.GetMetadatasGroups()[i][0].GetString("fruit")
			require.True(t, ok, "metadata 'fruit' should exist for query %q", queryTexts[i])
			require.Equal(t, expectedFruits[i], fruit, "metadata fruit for query %q", queryTexts[i])
			require.NotNil(t, res.GetEmbeddingsGroups()[i][0], "embedding should not be nil for query %q", queryTexts[i])
		}
	})

	t.Run("query without distances include returns no distances", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("test_document_1", "test_document_2", "test_document_3"))
		require.NoError(t, err)
		res, err := collection.Query(ctx, WithQueryTexts("test_document_1"), WithInclude(IncludeDocuments))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDGroups()))
		require.Equal(t, 3, len(res.GetIDGroups()[0]))
		require.Equal(t, 1, len(res.GetDocumentsGroups()))
		require.Equal(t, 3, len(res.GetDocumentsGroups()[0]))
		require.Equal(t, 0, len(res.GetDistancesGroups()))
	})

	t.Run("search with IDIn filter", func(t *testing.T) {
		// Search API is Cloud-only - skip for local Chroma
		t.Skip("Search API with ID filtering is Cloud-only")
		err = c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDs("1", "2", "3", "4", "5"), WithTexts("cats are fluffy", "dogs are loyal", "lions are big cats", "tigers are striped", "birds can fly"))
		require.NoError(t, err)

		// Search with IDIn - only include specific IDs
		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("cats"), WithKnnLimit(10)),
				WithFilter(IDIn("1", "3")),
				WithLimit(5),
				WithSelect(KID, KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		searchResult, ok := results.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, searchResult.IDs)
		// Should only return docs with ID 1 or 3
		require.LessOrEqual(t, len(searchResult.IDs[0]), 2)
		for _, id := range searchResult.IDs[0] {
			require.True(t, id == "1" || id == "3", "expected ID 1 or 3, got %s", id)
		}
	})

	t.Run("search with IDNotIn filter", func(t *testing.T) {
		// Search API is Cloud-only - skip for local Chroma
		t.Skip("Search API with ID filtering is Cloud-only")
		err = c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx, WithIDs("1", "2", "3", "4", "5"), WithTexts("cats are fluffy", "dogs are loyal", "lions are big cats", "tigers are striped", "birds can fly"))
		require.NoError(t, err)

		// Search with IDNotIn - exclude specific IDs
		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("cats"), WithKnnLimit(10)),
				WithFilter(IDNotIn("1", "3")),
				WithLimit(5),
				WithSelect(KID, KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		searchResult, ok := results.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, searchResult.IDs)
		// Should NOT return docs with ID 1 or 3
		for _, id := range searchResult.IDs[0] {
			require.True(t, id != "1" && id != "3", "expected ID to not be 1 or 3, got %s", id)
		}
	})

	t.Run("search with IDNotIn combined with metadata filter", func(t *testing.T) {
		// Search API is Cloud-only - skip for local Chroma
		t.Skip("Search API with ID filtering is Cloud-only")
		err = c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = collection.Add(ctx,
			WithIDs("1", "2", "3", "4", "5"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions", "lions are big cats", "tigers are striped cats", "birds can fly high"),
			WithMetadatas(
				NewDocumentMetadata(NewStringAttribute("category", "pets")),
				NewDocumentMetadata(NewStringAttribute("category", "pets")),
				NewDocumentMetadata(NewStringAttribute("category", "wildlife")),
				NewDocumentMetadata(NewStringAttribute("category", "wildlife")),
				NewDocumentMetadata(NewStringAttribute("category", "wildlife")),
			),
		)
		require.NoError(t, err)

		// Search with combined filters: exclude seen IDs AND filter by category
		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("cats"), WithKnnLimit(10)),
				WithFilter(And(
					EqString(K("category"), "wildlife"),
					IDNotIn("3"), // Exclude lions
				)),
				WithLimit(5),
				WithSelect(KID, KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		searchResult, ok := results.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, searchResult.IDs)
		// Should only return wildlife docs (4, 5) but not ID 3
		for _, id := range searchResult.IDs[0] {
			require.True(t, id == "4" || id == "5", "expected ID 4 or 5, got %s", id)
		}
	})

	t.Run("array metadata round-trip and filtering", func(t *testing.T) {
		// Requires Chroma >= 1.5.0
		if chromaVersion != "latest" {
			v, err := semver.NewVersion(chromaVersion)
			require.NoError(t, err)
			constraint, _ := semver.NewConstraint(">= 1.5.0")
			if !constraint.Check(v) {
				t.Skip("array metadata requires Chroma >= 1.5.0")
			}
		}

		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_array_metadata",
			WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()),
		)
		require.NoError(t, err)

		meta1 := NewDocumentMetadata(
			NewStringArrayAttribute("tags", []string{"science", "physics"}),
			NewIntArrayAttribute("scores", []int64{100, 200}),
			NewFloatArrayAttribute("ratios", []float64{0.5, 1.5}),
			NewBoolArrayAttribute("flags", []bool{true, false}),
		)
		meta2 := NewDocumentMetadata(
			NewStringArrayAttribute("tags", []string{"math", "algebra"}),
			NewIntArrayAttribute("scores", []int64{300, 400}),
			NewFloatArrayAttribute("ratios", []float64{2.0, 3.0}),
			NewBoolArrayAttribute("flags", []bool{false}),
		)
		meta3 := NewDocumentMetadata(
			NewStringArrayAttribute("tags", []string{"science", "biology"}),
			NewIntArrayAttribute("scores", []int64{500}),
			NewFloatArrayAttribute("ratios", []float64{4.0}),
			NewBoolArrayAttribute("flags", []bool{true}),
		)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("doc about physics", "doc about algebra", "doc about biology"),
			WithMetadatas(meta1, meta2, meta3),
		)
		require.NoError(t, err)

		// Verify round-trip: get docs back and check array metadata
		res, err := collection.Get(ctx, WithIDs("1"), WithInclude(IncludeMetadatas))
		require.NoError(t, err)
		require.Equal(t, 1, len(res.GetIDs()))
		md := res.GetMetadatas()
		require.Equal(t, 1, len(md))
		tags, ok := md[0].GetStringArray("tags")
		require.True(t, ok)
		require.Equal(t, []string{"science", "physics"}, tags)

		scores, ok := md[0].GetIntArray("scores")
		require.True(t, ok)
		require.Equal(t, []int64{100, 200}, scores)

		ratios, ok := md[0].GetFloatArray("ratios")
		require.True(t, ok)
		require.Equal(t, []float64{0.5, 1.5}, ratios)

		flags, ok := md[0].GetBoolArray("flags")
		require.True(t, ok)
		require.Equal(t, []bool{true, false}, flags)

		// Query with MetadataContainsString - should match doc 1 and 3
		qr, err := collection.Query(ctx,
			WithQueryTexts("science"),
			WithNResults(10),
			WithWhere(MetadataContainsString(K("tags"), "science")),
			WithInclude(IncludeMetadatas, IncludeDocuments),
		)
		require.NoError(t, err)
		idGroups := qr.GetIDGroups()
		require.Equal(t, 1, len(idGroups))
		require.Equal(t, 2, len(idGroups[0]))
		idSet := map[DocumentID]bool{}
		for _, id := range idGroups[0] {
			idSet[id] = true
		}
		require.True(t, idSet["1"], "expected doc 1 in results")
		require.True(t, idSet["3"], "expected doc 3 in results")

		// Query with MetadataNotContainsString - exclude "science", should return doc 2
		qr, err = collection.Query(ctx,
			WithQueryTexts("math"),
			WithNResults(10),
			WithWhere(MetadataNotContainsString(K("tags"), "science")),
			WithInclude(IncludeMetadatas),
		)
		require.NoError(t, err)
		idGroups = qr.GetIDGroups()
		require.Equal(t, 1, len(idGroups))
		require.Equal(t, 1, len(idGroups[0]))
		require.Equal(t, DocumentID("2"), idGroups[0][0])

		// Query with MetadataContainsInt
		qr, err = collection.Query(ctx,
			WithQueryTexts("test"),
			WithNResults(10),
			WithWhere(MetadataContainsInt(K("scores"), 500)),
		)
		require.NoError(t, err)
		idGroups = qr.GetIDGroups()
		require.Equal(t, 1, len(idGroups))
		require.Equal(t, 1, len(idGroups[0]))
		require.Equal(t, DocumentID("3"), idGroups[0][0])

		// And composition: array filter + scalar filter via contains
		qr, err = collection.Query(ctx,
			WithQueryTexts("test"),
			WithNResults(10),
			WithWhere(And(
				MetadataContainsString(K("tags"), "science"),
				MetadataContainsInt(K("scores"), 100),
			)),
		)
		require.NoError(t, err)
		idGroups = qr.GetIDGroups()
		require.Equal(t, 1, len(idGroups))
		require.Equal(t, 1, len(idGroups[0]))
		require.Equal(t, DocumentID("1"), idGroups[0][0])
	})
}
