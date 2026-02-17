//go:build cloud

package chroma

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/chromacloud"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings/chromacloudsplade"
)

// setupCloudClient creates a cloud client for testing with proper cleanup
func setupCloudClient(t *testing.T) Client {
	t.Helper()

	if os.Getenv("CHROMA_API_KEY") == "" && os.Getenv("CHROMA_DATABASE") == "" && os.Getenv("CHROMA_TENANT") == "" {
		err := godotenv.Load(".env")
		require.NoError(t, err)
	}

	client, err := NewCloudClient(
		WithLogger(testLogger()),
		WithDatabaseAndTenant(os.Getenv("CHROMA_DATABASE"), os.Getenv("CHROMA_TENANT")),
		WithCloudAPIKey(os.Getenv("CHROMA_API_KEY")),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Setenv("CHROMA_TENANT", "")
		t.Setenv("CHROMA_DATABASE", "")
		t.Setenv("CHROMA_API_KEY", "")
	})

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

// TestCloudCleanup is a dedicated cleanup test that runs after all other cloud tests
// It removes all test collections to clean up the shared cloud database
func TestCloudCleanup(t *testing.T) {
	client := setupCloudClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	collections, err := client.ListCollections(ctx)
	if err != nil {
		t.Fatalf("Failed to list collections for cleanup: %v", err)
	}

	var toDelete []string
	for _, collection := range collections {
		name := collection.Name()
		if strings.HasPrefix(name, "test_") {
			toDelete = append(toDelete, name)
		}
	}

	if len(toDelete) == 0 {
		t.Log("No test collections to clean up")
		return
	}

	t.Logf("Cleaning up %d test collections...", len(toDelete))

	var deleted, failed int
	for _, name := range toDelete {
		deleteCtx, deleteCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := client.DeleteCollection(deleteCtx, name); err != nil {
			t.Logf("Warning: failed to delete collection '%s': %v", name, err)
			failed++
		} else {
			deleted++
		}
		deleteCancel()
	}

	t.Logf("Cleanup completed: deleted %d, failed %d", deleted, failed)
}

// TestCloudClientCRUD tests basic CRUD operations
func TestCloudClientCRUD(t *testing.T) {
	client := setupCloudClient(t)

	t.Run("Get Version", func(t *testing.T) {
		ctx := context.Background()
		v, err := client.GetVersion(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, v)
		require.Contains(t, v, "1.0")
	})

	t.Run("List collections", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		collections, err := client.ListCollections(ctx)
		require.NoError(t, err)
		t.Logf("Found %d collections", len(collections))
	})

	t.Run("Count collections", func(t *testing.T) {
		ctx := context.Background()
		collectionCount, err := client.CountCollections(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, collectionCount, 0)
	})

	t.Run("Create collection", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_collection-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)
		require.Equal(t, collectionName, collection.Name())
	})

	t.Run("Delete collection", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_collection-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)
		require.Equal(t, collectionName, collection.Name())

		err = client.DeleteCollection(ctx, collectionName)
		require.NoError(t, err)

		collections, err := client.ListCollections(ctx)
		require.NoError(t, err)
		for _, c := range collections {
			require.NotEqual(t, collectionName, c.Name())
		}
	})

	t.Run("Add data to collection", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_collection-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)
		require.Equal(t, collectionName, collection.Name())

		err = collection.Add(ctx, WithIDGenerator(NewUUIDGenerator()), WithTexts("this is document about cats", "123141231", "$@!123115"))
		require.NoError(t, err)
	})

	t.Run("Delete data from collection", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_collection-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)
		require.Equal(t, collectionName, collection.Name())

		err = collection.Add(ctx, WithIDs("1", "2", "3"), WithTexts("this is document about cats", "123141231", "$@!123115"))
		require.NoError(t, err)

		err = collection.Delete(ctx, WithIDs("1", "2"))
		require.NoError(t, err)

		count, err := collection.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("Update and get data in collection", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_collection-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)
		require.Equal(t, collectionName, collection.Name())

		err = collection.Add(ctx, WithIDs("1", "2", "3"), WithTexts("this is document about cats", "123141231", "$@!123115"))
		require.NoError(t, err)

		err = collection.Update(ctx, WithIDs("1", "2"), WithTexts("updated text for 1", "updated text for 2"))
		require.NoError(t, err)

		results, err := collection.Get(ctx, WithIDs("1", "2"))
		require.NoError(t, err)
		require.Equal(t, results.Count(), 2)
		require.Equal(t, "updated text for 1", results.GetDocuments()[0].ContentString())
		require.Equal(t, "updated text for 2", results.GetDocuments()[1].ContentString())
	})

	t.Run("Query data in collection", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_collection-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)
		require.Equal(t, collectionName, collection.Name())

		err = collection.Add(ctx, WithIDs("1", "2", "3"), WithTexts("this is document about cats", "dogs are man's best friends", "lions are big cats"))
		require.NoError(t, err)

		results, err := collection.Query(ctx, WithQueryTexts("tell me about cats"), WithNResults(2))
		require.NoError(t, err)
		require.Contains(t, results.GetDocumentsGroups()[0][0].ContentString(), "cats")
		require.Contains(t, results.GetDocumentsGroups()[0][1].ContentString(), "cats")
	})
}

// TestCloudClientAutoWire tests embedding function auto-wiring
func TestCloudClientAutoWire(t *testing.T) {
	client := setupCloudClient(t)

	t.Run("auto-wire chroma cloud embedding function on GetCollection", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_autowire_cloud_get-" + uuid.New().String()

		ef, err := chromacloud.NewEmbeddingFunction(chromacloud.WithEnvAPIKey())
		require.NoError(t, err)
		createdCol, err := client.CreateCollection(ctx, collectionName, WithEmbeddingFunctionCreate(ef))
		require.NoError(t, err)
		require.NotNil(t, createdCol)
		require.Equal(t, collectionName, createdCol.Name())

		retrievedCol, err := client.GetCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, retrievedCol)

		err = retrievedCol.Add(ctx, WithIDs("doc1", "doc2"), WithTexts("hello world", "goodbye world"))
		require.NoError(t, err)

		time.Sleep(2 * time.Second)

		results, err := retrievedCol.Query(ctx, WithQueryTexts("hello"), WithNResults(1))
		require.NoError(t, err)
		require.NotNil(t, results)
		require.NotEmpty(t, results.GetDocumentsGroups())
	})

	t.Run("auto-wire custom embedding function on GetCollection", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_autowire_custom_get-" + uuid.New().String()

		ef := embeddings.NewConsistentHashEmbeddingFunction()
		createdCol, err := client.CreateCollection(ctx, collectionName, WithEmbeddingFunctionCreate(ef))
		require.NoError(t, err)
		require.NotNil(t, createdCol)
		require.Equal(t, collectionName, createdCol.Name())

		retrievedCol, err := client.GetCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, retrievedCol)

		err = retrievedCol.Add(ctx, WithIDs("doc1", "doc2"), WithTexts("hello world", "goodbye world"))
		require.NoError(t, err)

		time.Sleep(2 * time.Second)

		results, err := retrievedCol.Query(ctx, WithQueryTexts("hello"), WithNResults(1))
		require.NoError(t, err)
		require.NotNil(t, results)
		require.NotEmpty(t, results.GetDocumentsGroups())
	})

	t.Run("auto-wire embedding function on ListCollections", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_autowire_list-" + uuid.New().String()

		ef := embeddings.NewConsistentHashEmbeddingFunction()
		_, err := client.CreateCollection(ctx, collectionName, WithEmbeddingFunctionCreate(ef))
		require.NoError(t, err)

		collections, err := client.ListCollections(ctx)
		require.NoError(t, err)

		var foundCol Collection
		for _, col := range collections {
			if col.Name() == collectionName {
				foundCol = col
				break
			}
		}
		require.NotNil(t, foundCol, "collection should be found in list")

		err = foundCol.Add(ctx, WithIDs("doc1"), WithTexts("test document"))
		require.NoError(t, err)

		time.Sleep(2 * time.Second)

		count, err := foundCol.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("explicit EF overrides auto-wire", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_autowire_override-" + uuid.New().String()

		ef1 := embeddings.NewConsistentHashEmbeddingFunction()
		_, err := client.CreateCollection(ctx, collectionName, WithEmbeddingFunctionCreate(ef1))
		require.NoError(t, err)

		ef2 := embeddings.NewConsistentHashEmbeddingFunction()
		col, err := client.GetCollection(ctx, collectionName, WithEmbeddingFunctionGet(ef2))
		require.NoError(t, err)
		require.NotNil(t, col)

		err = col.Add(ctx, WithIDs("doc1"), WithTexts("test"))
		require.NoError(t, err)
	})

	t.Run("Collection fork", func(t *testing.T) {
		t.Skipf("Skipping fork")
		ctx := context.Background()
		collectionName := "test_collection-" + uuid.New().String()
		forkedCollectionName := "forked_collection-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)
		require.Equal(t, collectionName, collection.Name())

		err = collection.Add(ctx, WithIDs("1", "2", "3"), WithTexts("this is document about cats", "dogs are man's best friends", "lions are big cats"))
		require.NoError(t, err)
		time.Sleep(5 * time.Second)
		forkedCollection, err := collection.Fork(ctx, forkedCollectionName)
		require.NoError(t, err)

		results, err := forkedCollection.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 3, results)
	})

	t.Run("auto-wire sparse embedding function from schema", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_sparse_ef_autowire-" + uuid.New().String()

		sparseEF, err := chromacloudsplade.NewEmbeddingFunction(chromacloudsplade.WithEnvAPIKey())
		require.NoError(t, err)

		schema, err := NewSchema(
			WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
			WithSparseVectorIndex("sparse_embedding", NewSparseVectorIndexConfig(
				WithSparseEmbeddingFunction(sparseEF),
				WithSparseSourceKey("#document"),
			)),
		)
		require.NoError(t, err)

		createdCol, err := client.CreateCollection(ctx, collectionName, WithSchemaCreate(schema))
		require.NoError(t, err)
		require.NotNil(t, createdCol)

		retrievedCol, err := client.GetCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, retrievedCol)

		retrievedSchema := retrievedCol.Schema()
		require.NotNil(t, retrievedSchema, "Schema should be present")

		allSparseEFs := retrievedSchema.GetAllSparseEmbeddingFunctions()
		require.Len(t, allSparseEFs, 1, "Should have exactly one sparse EF")
		require.NotNil(t, allSparseEFs["sparse_embedding"], "Sparse EF should be auto-wired from Cloud schema")
		require.Equal(t, "chroma-cloud-splade", allSparseEFs["sparse_embedding"].Name())

		sparseEFByKey := retrievedSchema.GetSparseEmbeddingFunction("sparse_embedding")
		require.NotNil(t, sparseEFByKey, "Should find sparse EF by key")
		require.Equal(t, "chroma-cloud-splade", sparseEFByKey.Name())
	})
}

// TestCloudClientSearch tests Search API functionality
func TestCloudClientSearch(t *testing.T) {
	client := setupCloudClient(t)

	t.Run("Search data in collection", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_collection-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)
		require.Equal(t, collectionName, collection.Name())

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("this is document about cats", "dogs are man's best friends", "lions are big cats"),
			WithMetadatas(
				NewDocumentMetadata(NewStringAttribute("category", "pets")),
				NewDocumentMetadata(NewStringAttribute("category", "pets")),
				NewDocumentMetadata(NewStringAttribute("category", "wildlife")),
			),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("tell me about cats"), WithKnnLimit(10)),
				WithLimit(2),
				WithSelect(KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		searchResult, ok := results.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, searchResult.IDs)
		require.NotEmpty(t, searchResult.Documents)
		require.NotEmpty(t, searchResult.Scores)
	})

	t.Run("Search with pagination using NewPage", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_collection-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3", "4", "5"),
			WithTexts(
				"cats are fluffy pets",
				"dogs are loyal companions",
				"lions are wild cats",
				"tigers are striped cats",
				"birds can fly high",
			),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		page := NewPage(Limit(2))
		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("cats"), WithKnnLimit(10)),
				page,
				WithSelect(KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		searchResult, ok := results.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, searchResult.IDs)
		require.LessOrEqual(t, len(searchResult.IDs[0]), 2)

		require.Equal(t, 0, page.Number())
		require.Equal(t, 2, page.Size())
		nextPage := page.Next()
		require.Equal(t, 1, nextPage.Number())
		require.Equal(t, 2, nextPage.GetOffset())
	})

	t.Run("Search with IDIn filter", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_search_id_in-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("cats are fluffy", "dogs are loyal", "lions are big cats"),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

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

		sr := results.(*SearchResultImpl)
		require.NotEmpty(t, sr.IDs)
		require.LessOrEqual(t, len(sr.IDs[0]), 2)

		for _, id := range sr.IDs[0] {
			require.True(t, id == "1" || id == "3", "Expected ID 1 or 3, got %s", id)
		}
	})

	t.Run("Search with IDNotIn filter", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_search_id_not_in-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("cats are fluffy", "dogs are loyal", "lions are big cats"),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("cats"), WithKnnLimit(10)),
				WithFilter(IDNotIn("1")),
				WithLimit(5),
				WithSelect(KID, KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		sr := results.(*SearchResultImpl)
		require.NotEmpty(t, sr.IDs)

		for _, id := range sr.IDs[0] {
			require.NotEqual(t, DocumentID("1"), id, "ID 1 should be excluded")
		}
	})

	t.Run("Search with IDNotIn combined with metadata filter", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_search_id_not_in_combo-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3", "4"),
			WithTexts("cats are fluffy", "dogs are loyal", "lions are big cats", "tigers are striped"),
			WithMetadatas(
				NewDocumentMetadata(NewStringAttribute("category", "pets")),
				NewDocumentMetadata(NewStringAttribute("category", "pets")),
				NewDocumentMetadata(NewStringAttribute("category", "wildlife")),
				NewDocumentMetadata(NewStringAttribute("category", "wildlife")),
			),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("cats"), WithKnnLimit(10)),
				WithFilter(And(
					EqString(K("category"), "wildlife"),
					IDNotIn("3"),
				)),
				WithLimit(5),
				WithSelect(KID, KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		sr := results.(*SearchResultImpl)
		require.NotEmpty(t, sr.IDs)

		for _, id := range sr.IDs[0] {
			require.NotEqual(t, DocumentID("3"), id, "ID 3 should be excluded")
			require.True(t, id == "4", "Expected ID 4, got %s", id)
		}
	})

	t.Run("Search with DocumentContains filter", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_search_doc_contains-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts(
				"cats are fluffy pets that purr",
				"dogs are loyal companions that bark",
				"lions are big wild cats in Africa",
			),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("pets"), WithKnnLimit(10)),
				WithFilter(DocumentContains("fluffy")),
				WithLimit(5),
				WithSelect(KID, KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		sr := results.(*SearchResultImpl)
		require.NotEmpty(t, sr.IDs)
		require.Len(t, sr.IDs[0], 1, "Should only return 1 document containing 'fluffy'")
		require.Equal(t, DocumentID("1"), sr.IDs[0][0])
	})

	t.Run("Search with DocumentNotContains filter", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_search_doc_not_contains-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts(
				"cats are fluffy pets that purr",
				"dogs are loyal companions that bark",
				"lions are big wild cats in Africa",
			),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("animals"), WithKnnLimit(10)),
				WithFilter(DocumentNotContains("cats")),
				WithLimit(5),
				WithSelect(KID, KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		sr := results.(*SearchResultImpl)
		require.NotEmpty(t, sr.IDs)
		require.Len(t, sr.IDs[0], 1, "Should only return 1 document not containing 'cats'")
		require.Equal(t, DocumentID("2"), sr.IDs[0][0])
	})

	t.Run("Search with metadata projection and Rows iteration", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_search_metadata_rows-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions", "lions are big cats"),
			WithMetadatas(
				NewDocumentMetadata(
					NewStringAttribute("category", "pets"),
					NewIntAttribute("year", 2020),
					NewFloatAttribute("rating", 4.5),
				),
				NewDocumentMetadata(
					NewStringAttribute("category", "pets"),
					NewIntAttribute("year", 2021),
					NewFloatAttribute("rating", 4.8),
				),
				NewDocumentMetadata(
					NewStringAttribute("category", "wildlife"),
					NewIntAttribute("year", 2019),
					NewFloatAttribute("rating", 4.2),
				),
			),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("cats"), WithKnnLimit(10)),
				WithPage(PageLimit(3)),
				WithSelect(KID, KDocument, KScore, KMetadata),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		sr, ok := results.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, sr.IDs)
		require.NotEmpty(t, sr.Metadatas)
		require.NotNil(t, sr.Metadatas[0], "First group of metadatas should not be nil")

		rows := sr.Rows()
		require.NotEmpty(t, rows, "Rows should not be empty")

		for _, row := range rows {
			require.NotEmpty(t, row.ID, "Row ID should not be empty")
			require.NotEmpty(t, row.Document, "Row Document should not be empty")
			require.NotNil(t, row.Metadata, "Row Metadata should not be nil")

			category, ok := row.Metadata.GetString("category")
			require.True(t, ok, "Should be able to get category")
			require.NotEmpty(t, category)

			year, ok := row.Metadata.GetInt("year")
			require.True(t, ok, "Should be able to get year")
			require.Greater(t, year, int64(2000))

			rating, ok := row.Metadata.GetFloat("rating")
			require.True(t, ok, "Should be able to get rating")
			require.Greater(t, rating, float64(0))

			require.NotZero(t, row.Score, "Score should not be zero")
		}

		row, ok := sr.At(0, 0)
		require.True(t, ok, "At(0, 0) should succeed")
		require.NotEmpty(t, row.ID)
		require.NotNil(t, row.Metadata)

		_, ok = sr.At(0, 100)
		require.False(t, ok, "At(0, 100) should return false")
		_, ok = sr.At(100, 0)
		require.False(t, ok, "At(100, 0) should return false")
	})

	t.Run("Search with ReadLevelIndexAndWAL", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_search_read_level_wal-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions", "lions are big cats"),
		)
		require.NoError(t, err)

		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("animals"), WithKnnLimit(10)),
				WithPage(PageLimit(10)),
				WithSelect(KID, KDocument, KScore),
			),
			WithReadLevel(ReadLevelIndexAndWAL),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		sr, ok := results.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, sr.IDs)
		require.Len(t, sr.IDs[0], 3, "ReadLevelIndexAndWAL should return all 3 documents from WAL")
	})

	t.Run("Search with ReadLevelIndexOnly", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_search_read_level_index-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions", "lions are big cats"),
		)
		require.NoError(t, err)

		results, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("animals"), WithKnnLimit(10)),
				WithPage(PageLimit(10)),
				WithSelect(KID, KDocument, KScore),
			),
			WithReadLevel(ReadLevelIndexOnly),
		)
		require.NoError(t, err)
		require.NotNil(t, results)

		sr, ok := results.(*SearchResultImpl)
		require.True(t, ok)
		require.Len(t, sr.IDs, 1)
		require.LessOrEqual(t, len(sr.IDs[0]), 3, "ReadLevelIndexOnly should return 0-3 documents if index may not yet compacted")
	})
}

// TestCloudClientArrayMetadata tests array metadata with $contains/$not_contains queries
func TestCloudClientArrayMetadata(t *testing.T) {
	client := setupCloudClient(t)

	t.Run("array metadata round-trip and contains queries", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_array_meta-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		meta1 := NewDocumentMetadata(
			NewStringArrayAttribute("tags", []string{"science", "physics"}),
			NewIntArrayAttribute("scores", []int64{100, 200}),
		)
		meta2 := NewDocumentMetadata(
			NewStringArrayAttribute("tags", []string{"math", "algebra"}),
			NewIntArrayAttribute("scores", []int64{300}),
		)
		meta3 := NewDocumentMetadata(
			NewStringArrayAttribute("tags", []string{"science", "biology"}),
			NewIntArrayAttribute("scores", []int64{400, 500}),
		)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("doc about physics", "doc about algebra", "doc about biology"),
			WithMetadatas(meta1, meta2, meta3),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		// MetadataContainsString: "science" should match docs 1 and 3
		qr, err := collection.Query(ctx,
			WithQueryTexts("science"),
			WithNResults(10),
			WithWhere(MetadataContainsString(K("tags"), "science")),
		)
		require.NoError(t, err)
		idGroups := qr.GetIDGroups()
		require.Equal(t, 1, len(idGroups))
		require.Equal(t, 2, len(idGroups[0]))
		idSet := map[DocumentID]bool{}
		for _, id := range idGroups[0] {
			idSet[id] = true
		}
		require.True(t, idSet["1"])
		require.True(t, idSet["3"])

		// MetadataNotContainsString: exclude "science" should return doc 2
		qr, err = collection.Query(ctx,
			WithQueryTexts("math"),
			WithNResults(10),
			WithWhere(MetadataNotContainsString(K("tags"), "science")),
		)
		require.NoError(t, err)
		idGroups = qr.GetIDGroups()
		require.Equal(t, 1, len(idGroups))
		require.Equal(t, 1, len(idGroups[0]))
		require.Equal(t, DocumentID("2"), idGroups[0][0])
	})
}

// TestCloudClientSchema tests Schema configuration
func TestCloudClientSchema(t *testing.T) {
	client := setupCloudClient(t)

	t.Run("Create collection with default schema", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_schema_default-" + uuid.New().String()

		schema, err := NewSchemaWithDefaults()
		require.NoError(t, err)

		collection, err := client.CreateCollection(ctx, collectionName, WithSchemaCreate(schema))
		require.NoError(t, err)
		require.NotNil(t, collection)
		require.Equal(t, collectionName, collection.Name())

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions", "lions are big cats"),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Query(ctx, WithQueryTexts("tell me about cats"), WithNResults(2))
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())
		require.Contains(t, results.GetDocumentsGroups()[0][0].ContentString(), "cats")

		searchResults, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("cats"), WithKnnLimit(10)),
				WithLimit(2),
				WithSelect(KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, searchResults)
		sr, ok := searchResults.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, sr.IDs)
	})

	t.Run("Create collection with cosine space", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_schema_cosine-" + uuid.New().String()

		schema, err := NewSchema(
			WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceCosine))),
		)
		require.NoError(t, err)

		collection, err := client.CreateCollection(ctx, collectionName, WithSchemaCreate(schema))
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions", "lions are big cats"),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Query(ctx, WithQueryTexts("fluffy pets"), WithNResults(2))
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())
	})

	t.Run("Create collection with inner product space", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_schema_ip-" + uuid.New().String()

		schema, err := NewSchema(
			WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceIP))),
		)
		require.NoError(t, err)

		collection, err := client.CreateCollection(ctx, collectionName, WithSchemaCreate(schema))
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions", "lions are big cats"),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Query(ctx, WithQueryTexts("fluffy pets"), WithNResults(2))
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())
	})

	t.Run("Create collection with custom HNSW config", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_schema_hnsw-" + uuid.New().String()

		schema, err := NewSchema(
			WithDefaultVectorIndex(NewVectorIndexConfig(
				WithSpace(SpaceL2),
				WithHnsw(NewHnswConfig(
					WithEfConstruction(200),
					WithMaxNeighbors(32),
					WithEfSearch(50),
				)),
			)),
		)
		require.NoError(t, err)

		collection, err := client.CreateCollection(ctx, collectionName, WithSchemaCreate(schema))
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions", "lions are big cats"),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Query(ctx, WithQueryTexts("cats"), WithNResults(2))
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())

		searchResults, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("cats"), WithKnnLimit(10)),
				WithLimit(2),
				WithSelect(KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, searchResults)
	})

	t.Run("Create collection with WithVectorIndexCreate", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_schema_convenience-" + uuid.New().String()

		collection, err := client.CreateCollection(ctx, collectionName,
			WithVectorIndexCreate(NewVectorIndexConfig(WithSpace(SpaceCosine))),
		)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions", "lions are big cats"),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Query(ctx, WithQueryTexts("fluffy"), WithNResults(2))
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())
	})

	t.Run("Create collection with FTS index", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_schema_fts-" + uuid.New().String()

		schema, err := NewSchema(
			WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
			WithDefaultFtsIndex(&FtsIndexConfig{}),
		)
		require.NoError(t, err)

		collection, err := client.CreateCollection(ctx, collectionName, WithSchemaCreate(schema))
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts(
				"The quick brown fox jumps over the lazy dog",
				"A journey of a thousand miles begins with a single step",
				"To be or not to be that is the question",
			),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		searchResults, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("quick fox"), WithKnnLimit(10)),
				WithLimit(2),
				WithSelect(KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, searchResults)
		sr, ok := searchResults.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, sr.IDs)
	})

	t.Run("Create collection with metadata indexes", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_schema_metadata-" + uuid.New().String()

		schema, err := NewSchema(
			WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
			WithStringIndex("category"),
			WithIntIndex("year"),
			WithFloatIndex("rating"),
			WithBoolIndex("available"),
		)
		require.NoError(t, err)

		collection, err := client.CreateCollection(ctx, collectionName, WithSchemaCreate(schema))
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3", "4"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions", "lions are big cats", "birds can fly"),
			WithMetadatas(
				NewDocumentMetadata(
					NewStringAttribute("category", "pets"),
					NewIntAttribute("year", 2020),
					NewFloatAttribute("rating", 4.5),
					NewBoolAttribute("available", true),
				),
				NewDocumentMetadata(
					NewStringAttribute("category", "pets"),
					NewIntAttribute("year", 2021),
					NewFloatAttribute("rating", 4.8),
					NewBoolAttribute("available", true),
				),
				NewDocumentMetadata(
					NewStringAttribute("category", "wildlife"),
					NewIntAttribute("year", 2019),
					NewFloatAttribute("rating", 4.2),
					NewBoolAttribute("available", false),
				),
				NewDocumentMetadata(
					NewStringAttribute("category", "wildlife"),
					NewIntAttribute("year", 2022),
					NewFloatAttribute("rating", 3.9),
					NewBoolAttribute("available", true),
				),
			),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Query(ctx,
			WithQueryTexts("animals"),
			WithNResults(10),
			WithWhere(EqString(K("category"), "pets")),
		)
		require.NoError(t, err)
		require.LessOrEqual(t, len(results.GetDocumentsGroups()[0]), 2)

		results, err = collection.Query(ctx,
			WithQueryTexts("animals"),
			WithNResults(10),
			WithWhere(GteInt("year", 2020)),
		)
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())

		results, err = collection.Query(ctx,
			WithQueryTexts("animals"),
			WithNResults(10),
			WithWhere(GtFloat("rating", 4.0)),
		)
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())

		results, err = collection.Query(ctx,
			WithQueryTexts("animals"),
			WithNResults(10),
			WithWhere(EqBool("available", true)),
		)
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())

		searchResults, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("animals"), WithKnnLimit(10)),
				WithLimit(5),
				WithSelect(KDocument, KScore, KMetadata),
			),
		)
		require.NoError(t, err)
		sr, ok := searchResults.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, sr.IDs)
	})

	t.Run("Create collection with disabled indexes", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_schema_disabled-" + uuid.New().String()

		schema, err := NewSchema(
			WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
			DisableStringIndex("large_text"),
		)
		require.NoError(t, err)

		collection, err := client.CreateCollection(ctx, collectionName, WithSchemaCreate(schema))
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2"),
			WithTexts("cats are fluffy pets", "dogs are loyal companions"),
			WithMetadatas(
				NewDocumentMetadata(NewStringAttribute("large_text", "some long text that should not be indexed")),
				NewDocumentMetadata(NewStringAttribute("large_text", "another long text")),
			),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		results, err := collection.Query(ctx, WithQueryTexts("pets"), WithNResults(2))
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())
	})

	t.Run("Comprehensive schema test", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_schema_comprehensive-" + uuid.New().String()

		schema, err := NewSchema(
			WithDefaultVectorIndex(NewVectorIndexConfig(
				WithSpace(SpaceCosine),
				WithHnsw(NewHnswConfig(WithEfConstruction(150))),
			)),
			WithDefaultFtsIndex(&FtsIndexConfig{}),
			WithStringIndex("category"),
			WithIntIndex("year"),
		)
		require.NoError(t, err)

		collection, err := client.CreateCollection(ctx, collectionName, WithSchemaCreate(schema))
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx,
			WithIDs("1", "2", "3"),
			WithTexts(
				"Machine learning is transforming industries",
				"Deep learning neural networks are powerful",
				"Natural language processing enables chatbots",
			),
			WithMetadatas(
				NewDocumentMetadata(NewStringAttribute("category", "AI"), NewIntAttribute("year", 2023)),
				NewDocumentMetadata(NewStringAttribute("category", "AI"), NewIntAttribute("year", 2022)),
				NewDocumentMetadata(NewStringAttribute("category", "NLP"), NewIntAttribute("year", 2023)),
			),
		)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		searchResults, err := collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("machine learning AI"), WithKnnLimit(10)),
				WithLimit(3),
				WithSelect(KDocument, KScore),
			),
		)
		require.NoError(t, err)
		require.NotNil(t, searchResults)

		searchResults, err = collection.Search(ctx,
			NewSearchRequest(
				WithKnnRank(KnnQueryText("learning"), WithKnnLimit(10)),
				WithLimit(3),
				WithSelect(KDocument, KScore, KMetadata),
			),
		)
		require.NoError(t, err)
		sr, ok := searchResults.(*SearchResultImpl)
		require.True(t, ok)
		require.NotEmpty(t, sr.IDs)

		results, err := collection.Query(ctx,
			WithQueryTexts("neural networks"),
			WithNResults(2),
		)
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())

		results, err = collection.Query(ctx,
			WithQueryTexts("learning"),
			WithNResults(10),
			WithWhere(EqInt("year", 2023)),
		)
		require.NoError(t, err)
		require.NotEmpty(t, results.GetDocumentsGroups())
	})
}

// TestCloudClientConfig tests client configuration and validation
func TestCloudClientConfig(t *testing.T) {
	if os.Getenv("CHROMA_API_KEY") == "" && os.Getenv("CHROMA_DATABASE") == "" && os.Getenv("CHROMA_TENANT") == "" {
		err := godotenv.Load(".env")
		require.NoError(t, err)
	}

	client, err := NewCloudClient(
		WithLogger(testLogger()),
		WithDatabaseAndTenant(os.Getenv("CHROMA_DATABASE"), os.Getenv("CHROMA_TENANT")),
		WithCloudAPIKey(os.Getenv("CHROMA_API_KEY")),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = client.Close()
	})

	t.Run("indexing status", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_indexing_status-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		err = collection.Add(ctx, WithIDs("1", "2", "3"), WithTexts("doc1", "doc2", "doc3"))
		require.NoError(t, err)
		time.Sleep(2 * time.Second)

		status, err := collection.IndexingStatus(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, status.TotalOps, uint64(3))
		require.GreaterOrEqual(t, status.OpIndexingProgress, 0.0)
		require.LessOrEqual(t, status.OpIndexingProgress, 1.0)
	})

	t.Run("Without API Key", func(t *testing.T) {
		t.Setenv("CHROMA_API_KEY", "")
		testClient, err := NewCloudClient(
			WithLogger(testLogger()),
			WithDatabaseAndTenant("test_database", "test_tenant"),
		)
		require.Error(t, err)
		require.Nil(t, testClient)
		require.Contains(t, err.Error(), "api key")
	})

	t.Run("Without Tenant and DB", func(t *testing.T) {
		t.Setenv("CHROMA_TENANT", "")
		t.Setenv("CHROMA_DATABASE", "")
		testClient, err := NewCloudClient(
			WithLogger(testLogger()),
			WithCloudAPIKey("test"),
		)
		require.Error(t, err)
		require.Nil(t, testClient)
		require.Contains(t, err.Error(), "tenant and database must be set for cloud client")
	})

	t.Run("With env tenant and DB", func(t *testing.T) {
		t.Setenv("CHROMA_TENANT", "test_tenant")
		t.Setenv("CHROMA_DATABASE", "test_database")
		testClient, err := NewCloudClient(
			WithLogger(testLogger()),
			WithCloudAPIKey("test"),
		)
		require.NoError(t, err)
		require.NotNil(t, testClient)
		require.Equal(t, NewTenant("test_tenant"), testClient.Tenant())
		require.Equal(t, NewDatabase("test_database", NewTenant("test_tenant")), testClient.Database())
	})

	t.Run("With env API key, tenant and DB", func(t *testing.T) {
		t.Setenv("CHROMA_TENANT", "test_tenant")
		t.Setenv("CHROMA_DATABASE", "test_database")
		t.Setenv("CHROMA_API_KEY", "test")
		testClient, err := NewCloudClient(
			WithLogger(testLogger()),
		)
		require.NoError(t, err)
		require.NotNil(t, testClient)
		require.NotNil(t, testClient.authProvider)
		require.IsType(t, &TokenAuthCredentialsProvider{}, testClient.authProvider)
		p, ok := testClient.authProvider.(*TokenAuthCredentialsProvider)
		require.True(t, ok)
		require.Equal(t, "test", p.Token)
		require.Equal(t, NewTenant("test_tenant"), testClient.Tenant())
		require.Equal(t, NewDatabase("test_database", NewTenant("test_tenant")), testClient.Database())
	})

	t.Run("With options overrides (precedence)", func(t *testing.T) {
		t.Setenv("CHROMA_TENANT", "test_tenant")
		t.Setenv("CHROMA_DATABASE", "test_database")
		t.Setenv("CHROMA_API_KEY", "test")
		testClient, err := NewCloudClient(
			WithLogger(testLogger()),
			WithCloudAPIKey("different_test_key"),
			WithDatabaseAndTenant("other_db", "other_tenant"),
		)
		require.NoError(t, err)
		require.NotNil(t, testClient)
		require.NotNil(t, testClient.authProvider)
		require.IsType(t, &TokenAuthCredentialsProvider{}, testClient.authProvider)
		p, ok := testClient.authProvider.(*TokenAuthCredentialsProvider)
		require.True(t, ok)
		require.Equal(t, "different_test_key", p.Token)
		require.Equal(t, NewTenant("other_tenant"), testClient.Tenant())
		require.Equal(t, NewDatabase("other_db", NewTenant("other_tenant")), testClient.Database())
	})
}

// TestCloudModifyConfiguration tests ModifyConfiguration with HNSW and SPANN parameters
func TestCloudModifyConfiguration(t *testing.T) {
	client := setupCloudClient(t)

	t.Run("modify SPANN configuration", func(t *testing.T) {
		ctx := context.Background()
		collectionName := "test_modify_spann_cfg-" + uuid.New().String()
		collection, err := client.CreateCollection(ctx, collectionName)
		require.NoError(t, err)
		require.NotNil(t, collection)

		cfg := NewUpdateCollectionConfiguration(
			WithSpannSearchNprobeModify(32),
			WithSpannEfSearchModify(64),
		)
		err = collection.ModifyConfiguration(ctx, cfg)
		require.NoError(t, err)

		updated, err := client.GetCollection(ctx, collectionName)
		require.NoError(t, err)
		spannCfg, ok := updated.Configuration().GetRaw("spann")
		require.True(t, ok)
		spannMap, ok := spannCfg.(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, float64(32), spannMap["search_nprobe"])
		assert.Equal(t, float64(64), spannMap["ef_search"])
	})
}
