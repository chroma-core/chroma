//go:build !cloud

package chroma

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcchroma "github.com/testcontainers/testcontainers-go/modules/chroma"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// isChromaVersion1x checks if the Chroma version is specifically 1.x (not 2.x+)
// Used for:
// - Skipping tests incompatible with Chroma 1.x (auth, SSL)
// - Version API checks where 1.x returns API version instead of server version
func isChromaVersion1x(version string) bool {
	if version == "latest" {
		return true
	}
	v, err := semver.NewVersion(version)
	if err != nil {
		return false
	}
	return v.Major() == 1
}

// supportsEFConfigPersistence checks if the Chroma version supports storing
// embedding function configuration in collection configuration.
// This feature was introduced in Chroma 1.0.0.
func supportsEFConfigPersistence(version string) bool {
	if version == "latest" {
		return true
	}
	v, err := semver.NewVersion(version)
	if err != nil {
		return false
	}
	constraint, _ := semver.NewConstraint(">= 1.0.0")
	return constraint.Check(v)
}

func TestClientHTTPIntegration(t *testing.T) {
	ctx := context.Background()
	var chromaVersion = "1.3.3"
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
			"ALLOW_RESET": "true", // this does not work with 1.0.x
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
	c, err := NewHTTPClient(WithBaseURL(chromaURL), WithDebug())
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

	t.Run("get version", func(t *testing.T) {
		v, err := c.GetVersion(ctx)
		require.NoError(t, err)
		if isChromaVersion1x(chromaVersion) {
			// Chroma 1.x returns API version "1.0.0" instead of server version
			require.Contains(t, v, "1.")
		} else {
			require.Equal(t, chromaVersion, v)
		}
	})
	t.Run("heartbeat", func(t *testing.T) {
		err := c.Heartbeat(ctx)
		require.NoError(t, err)
	})
	t.Run("get identity", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		id, err := c.GetIdentity(ctx)
		require.NoError(t, err)
		require.Equal(t, NewDefaultTenant().Name(), id.Tenant)
		require.Equal(t, 1, len(id.Databases))
		require.Equal(t, NewDefaultDatabase().Name(), id.Databases[0])
	})

	t.Run("get tenant", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		tenant, err := c.GetTenant(ctx, NewDefaultTenant())
		require.NoError(t, err)
		require.Equal(t, NewDefaultTenant().Name(), tenant.Name())
	})

	t.Run("get tenant with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		_, err = c.GetTenant(ctx, NewTenant("dummy"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("create tenant", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		tenant, err := c.CreateTenant(ctx, NewTenant("test"))
		require.NoError(t, err)
		require.Equal(t, "test", tenant.Name())
	})

	t.Run("create tenant with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		_, err = c.CreateTenant(ctx, NewTenant("test"))
		require.NoError(t, err)

		_, err = c.CreateTenant(ctx, NewTenant("test"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")

		_, err = c.CreateTenant(ctx, NewTenant(""))
		require.Error(t, err)
		require.Contains(t, err.Error(), "tenant name cannot be empty")

		_, err = c.CreateTenant(ctx, NewTenant("l"))
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "Validation error: length") || strings.Contains(err.Error(), "Tenant name must be at least 3 characters long"))
	})

	t.Run("list databases", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		databases, err := c.ListDatabases(ctx, NewDefaultTenant())
		require.NoError(t, err)
		require.Equal(t, 1, len(databases))
		require.Equal(t, NewDefaultDatabase().Name(), databases[0].Name())
	})

	t.Run("list databases with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		// this is "weird" but perhaps intentional
		databases, err := c.ListDatabases(ctx, NewTenant("test"))
		require.NoError(t, err)
		require.Equal(t, 0, len(databases))
	})

	t.Run("get database", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		db, err := c.GetDatabase(ctx, NewDefaultDatabase())
		require.NoError(t, err)
		require.Equal(t, NewDefaultDatabase().Name(), db.Name())
	})

	t.Run("get database with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		_, err = c.GetDatabase(ctx, NewDatabase("testdb", NewDefaultTenant()))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")

		_, err = c.GetDatabase(ctx, NewDatabase("testdb", NewTenant("test")))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("create database", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		db, err := c.CreateDatabase(ctx, NewDefaultTenant().Database("test_database"))
		require.NoError(t, err)
		require.Equal(t, "test_database", db.Name())
	})

	t.Run("create tenant with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		_, err = c.CreateDatabase(ctx, NewDefaultTenant().Database("test_database"))
		require.NoError(t, err)

		_, err = c.CreateDatabase(ctx, NewDefaultTenant().Database("test_database"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")

		_, err = c.CreateDatabase(ctx, NewDefaultTenant().Database(""))
		require.Error(t, err)
		require.Contains(t, err.Error(), "database name cannot be empty")

		_, err = c.CreateDatabase(ctx, NewDefaultTenant().Database("l"))
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "Validation error: length") || strings.Contains(err.Error(), "Database name must be at least 3 characters long"))
	})

	t.Run("delete database", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		_, err = c.CreateDatabase(ctx, NewDefaultTenant().Database("testdb_to_delete"))
		require.NoError(t, err)
		err = c.DeleteDatabase(ctx, NewDefaultTenant().Database("testdb_to_delete"))
		require.NoError(t, err)
	})

	t.Run("delete database with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		err = c.DeleteDatabase(ctx, NewDefaultTenant().Database("testdb_to_delete"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("create collection", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		require.Equal(t, "test_collection", collection.Name())

		db, err := c.CreateDatabase(ctx, NewDefaultTenant().Database("test"))
		require.NoError(t, err)
		newCWithtenant, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()), WithDatabaseCreate(db))
		require.NoError(t, err)
		require.Equal(t, "test_collection", newCWithtenant.Name())
		require.Equal(t, "test", newCWithtenant.Database().Name())
	})

	t.Run("create collection with errors", func(t *testing.T) {
		ver, err := c.GetVersion(ctx)
		require.NoError(t, err)
		err = c.Reset(ctx)
		require.NoError(t, err)
		_, err = createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		_, err = createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
		if strings.HasPrefix(ver, "1.0") {
			_, err = createCollection("test_collection1", WithDatabaseCreate(NewDatabase("test", NewDefaultTenant())))
			require.Error(t, err)
			require.Contains(t, err.Error(), "does not exist")
		}
		_, err = createCollection("")
		require.Error(t, err)
		require.Contains(t, err.Error(), "collection name cannot be empty")

		_, err = createCollection("1")
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "Expected a name containing 3-512 characters") || strings.Contains(err.Error(), "Expected collection name that (1) contains 3-63 characters"))

		_, err = createCollection("11111$$")
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "Expected a name containing 3-512 characters") || strings.Contains(err.Error(), "Expected collection name that (1) contains 3-63 characters"))

		_, err = createCollection("_1abc2")
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "Expected a name containing 3-512 characters") || strings.Contains(err.Error(), "Expected collection name that (1) contains 3-63 characters"))
	})

	t.Run("get collection", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		newCollection, err := createCollection("test_collection_2", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		collection, err := c.GetCollection(ctx, newCollection.Name(), WithEmbeddingFunctionGet(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		require.Equal(t, newCollection.Name(), collection.Name())

		db, err := c.CreateDatabase(ctx, NewDefaultTenant().Database("test_database"))
		require.NoError(t, err)
		newCollection, err = createCollection("test_collection_2", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()), WithDatabaseCreate(NewDatabase("test_database", NewDefaultTenant())))
		require.NoError(t, err)
		_, err = c.GetCollection(ctx, newCollection.Name(), WithDatabaseGet(db))
		require.NoError(t, err)
	})

	t.Run("get collection with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)

		// Create a collection first so we can test getting non-existent ones
		_, err = createCollection("existing_col", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)

		_, err = c.GetCollection(ctx, "non_existing_collection")
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist")

		_, err = c.GetCollection(ctx, "", WithEmbeddingFunctionGet(embeddings.NewConsistentHashEmbeddingFunction()))
		require.Error(t, err)
		require.Contains(t, err.Error(), "collection name cannot be empty")

		_, err = c.GetCollection(ctx, "l", WithEmbeddingFunctionGet(embeddings.NewConsistentHashEmbeddingFunction()))
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist")

		_, err = c.GetCollection(ctx, "_1111", WithEmbeddingFunctionGet(embeddings.NewConsistentHashEmbeddingFunction()))
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("auto-wire embedding function on GetCollection", func(t *testing.T) {
		if !supportsEFConfig {
			t.Skip("Skipping auto-wire test: Chroma version doesn't support EF config persistence")
		}
		err := c.Reset(ctx)
		require.NoError(t, err)

		// Create collection WITH embedding function
		ef := embeddings.NewConsistentHashEmbeddingFunction()
		createdCol, err := createCollection("auto_wire_test", WithEmbeddingFunctionCreate(ef))
		require.NoError(t, err)
		require.NotNil(t, createdCol)

		// Get collection WITHOUT specifying embedding function - should auto-wire
		retrievedCol, err := c.GetCollection(ctx, "auto_wire_test")
		require.NoError(t, err)
		require.NotNil(t, retrievedCol)

		// Verify the collection can be used for embedding operations
		// Add documents using text (requires EF to be wired)
		err = retrievedCol.Add(ctx, WithIDs("doc1", "doc2"), WithTexts("hello world", "goodbye world"))
		require.NoError(t, err)

		// Query using text (requires EF to be wired)
		results, err := retrievedCol.Query(ctx, WithQueryTexts("hello"), WithNResults(1))
		require.NoError(t, err)
		require.NotNil(t, results)
		require.NotEmpty(t, results.GetDocumentsGroups())
	})

	t.Run("auto-wire embedding function on ListCollections", func(t *testing.T) {
		if !supportsEFConfig {
			t.Skip("Skipping auto-wire test: Chroma version doesn't support EF config persistence")
		}
		err := c.Reset(ctx)
		require.NoError(t, err)

		// Create collection with EF
		ef := embeddings.NewConsistentHashEmbeddingFunction()
		_, err = createCollection("list_test_col", WithEmbeddingFunctionCreate(ef))
		require.NoError(t, err)

		// List collections - should auto-wire EF
		collections, err := c.ListCollections(ctx)
		require.NoError(t, err)
		require.Len(t, collections, 1)

		col := collections[0]
		require.Equal(t, "list_test_col", col.Name())

		// Verify the collection can be used for embedding operations
		err = col.Add(ctx, WithIDs("doc1"), WithTexts("test document"))
		require.NoError(t, err)

		count, err := col.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("explicit EF overrides auto-wire", func(t *testing.T) {
		if !supportsEFConfig {
			t.Skip("Skipping auto-wire test: Chroma version doesn't support EF config persistence")
		}
		err := c.Reset(ctx)
		require.NoError(t, err)

		// Create collection with one EF
		ef1 := embeddings.NewConsistentHashEmbeddingFunction()
		_, err = createCollection("override_test", WithEmbeddingFunctionCreate(ef1))
		require.NoError(t, err)

		// Get with explicit EF - should use the explicit one
		ef2 := embeddings.NewConsistentHashEmbeddingFunction()
		col, err := c.GetCollection(ctx, "override_test", WithEmbeddingFunctionGet(ef2))
		require.NoError(t, err)
		require.NotNil(t, col)

		// Verify it works
		err = col.Add(ctx, WithIDs("doc1"), WithTexts("test"))
		require.NoError(t, err)
	})

	t.Run("list collections", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		_, err = createCollection("test_collection_3", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		collections, err := c.ListCollections(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(collections), 1)
		collectionNames := make([]string, 0)
		for _, collection := range collections {
			collectionNames = append(collectionNames, collection.Name())
		}
		require.Contains(t, collectionNames, "test_collection_3")
	})

	t.Run("list collections with limit and offset", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			_, err := createCollection(fmt.Sprintf("collection-%s", uuid.New().String()), WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
			require.NoError(t, err)
		}
		collections, err := c.ListCollections(ctx, ListWithLimit(5), ListWithOffset(0))
		require.NoError(t, err)
		require.Equal(t, len(collections), 5)
		collections, err = c.ListCollections(ctx, ListWithLimit(5), ListWithOffset(1))
		require.NoError(t, err)
		require.Equal(t, len(collections), 5)
		_, err = c.ListCollections(ctx, ListWithOffset(10000000))
		require.NoError(t, err)
		_, err = c.ListCollections(ctx, ListWithLimit(10000000))
		require.NoError(t, err)
		_, err = c.ListCollections(ctx, ListWithOffset(10000000), ListWithLimit(10000000))
		require.NoError(t, err)
	})

	t.Run("list collections with invalid limit and offset", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			_, err := createCollection(fmt.Sprintf("collection-%s", uuid.New().String()), WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
			require.NoError(t, err)
		}
		_, err = c.ListCollections(ctx, ListWithLimit(-1), ListWithOffset(1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "limit cannot be less than 1")
		_, err = c.ListCollections(ctx, ListWithOffset(-1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "offset cannot be negative")

	})

	t.Run("delete collection", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		newCollection, err := createCollection("test_collection_4", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		err = c.DeleteCollection(ctx, newCollection.Name())
		require.NoError(t, err)
	})

	t.Run("delete collection with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		err = c.DeleteCollection(ctx, "non_existing_collection")
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("count collections", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		_, err = createCollection("test_collection_5", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		count, err := c.CountCollections(ctx)
		require.NoError(t, err)
		require.Equal(t, count, 1)

		db, err := c.CreateDatabase(ctx, NewDefaultTenant().Database("test"))
		require.NoError(t, err)
		_, err = createCollection("test_collection_5", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()), WithDatabaseCreate(db))
		require.NoError(t, err)
		count, err = c.CountCollections(ctx, WithDatabaseCount(db))
		require.NoError(t, err)
		require.Equal(t, count, 1)
	})

	t.Run("count collections with errors", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		// TODO this is odd behaviour
		count, err := c.CountCollections(ctx, WithDatabaseCount(NewDefaultTenant().Database("test_count_error")))
		require.NoError(t, err)
		require.Equal(t, count, 0)
	})

	t.Run("reset", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
	})

	t.Run("create tenant, db and collection", func(t *testing.T) {
		err := c.Reset(ctx)
		require.NoError(t, err)
		tenant, err := c.CreateTenant(ctx, NewTenant("test"))
		require.NoError(t, err)
		require.Equal(t, "test", tenant.Name())
		db, err := c.CreateDatabase(ctx, tenant.Database("test_db"))
		require.NoError(t, err)
		require.Equal(t, "test_db", db.Name())
		err = c.UseDatabase(ctx, db)
		require.NoError(t, err)
		collection, err := createCollection("test_collection", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		require.Equal(t, "test_collection", collection.Name())
		require.Equal(t, tenant.Name(), collection.Tenant().Name())
		require.Equal(t, db.Name(), collection.Database().Name())
		count, err := collection.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})
}

type HostMount struct {
	Source string
	Target string
}

func TestClientHTTPIntegrationWithBasicAuth(t *testing.T) {
	ctx := context.Background()
	var chromaVersion = "0.6.3"
	var chromaImage = "ghcr.io/chroma-core/chroma"
	if os.Getenv("CHROMA_VERSION") != "" {
		chromaVersion = os.Getenv("CHROMA_VERSION")
	}
	if isChromaVersion1x(chromaVersion) {
		t.Skip("Not supported by Chroma 1.x")
	}
	if os.Getenv("CHROMA_IMAGE") != "" {
		chromaImage = os.Getenv("CHROMA_IMAGE")
	}
	cwd, err := os.Getwd()
	require.NoError(t, err)
	mounts := []HostMount{
		{
			Source: filepath.Join(cwd, "server.htpasswd"),
			Target: "/chroma/chroma/server.htpasswd",
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
			"ALLOW_RESET":                          "true",
			"CHROMA_SERVER_AUTHN_CREDENTIALS_FILE": "/chroma/chroma/server.htpasswd",
			"CHROMA_SERVER_AUTHN_PROVIDER":         "chromadb.auth.basic_authn.BasicAuthenticationServerProvider",
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
	t.Run("success auth", func(t *testing.T) {
		c, err := NewHTTPClient(WithBaseURL(chromaURL), WithDebug(), WithAuth(NewBasicAuthCredentialsProvider("admin", "password123")))
		require.NoError(t, err)
		require.NotNil(t, c)
		collections, err := c.ListCollections(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, len(collections))
	})
	t.Run("wrong auth", func(t *testing.T) {
		wrongAuthClient, err := NewHTTPClient(WithBaseURL(chromaURL), WithDebug(), WithAuth(NewBasicAuthCredentialsProvider("admin", "wrong_password")))
		require.NoError(t, err)
		_, err = wrongAuthClient.ListCollections(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "403")
	})
}

func TestClientHTTPIntegrationWithBearerAuthorizationHeaderAuth(t *testing.T) {
	ctx := context.Background()
	var chromaVersion = "0.6.3"
	var chromaImage = "ghcr.io/chroma-core/chroma"
	if os.Getenv("CHROMA_VERSION") != "" {
		chromaVersion = os.Getenv("CHROMA_VERSION")
	}
	if isChromaVersion1x(chromaVersion) {
		t.Skip("Not supported by Chroma 1.x")
	}
	if os.Getenv("CHROMA_IMAGE") != "" {
		chromaImage = os.Getenv("CHROMA_IMAGE")
	}
	token := "chr0ma-t0k3n"
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
			"ALLOW_RESET":                        "true",
			"CHROMA_SERVER_AUTHN_CREDENTIALS":    token,
			"CHROMA_SERVER_AUTHN_PROVIDER":       "chromadb.auth.token_authn.TokenAuthenticationServerProvider",
			"CHROMA_AUTH_TOKEN_TRANSPORT_HEADER": "Authorization",
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
	t.Run("success auth", func(t *testing.T) {
		c, err := NewHTTPClient(WithBaseURL(chromaURL), WithDebug(), WithAuth(NewTokenAuthCredentialsProvider(token, AuthorizationTokenHeader)))
		require.NoError(t, err)
		require.NotNil(t, c)
		collections, err := c.ListCollections(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, len(collections))
	})
	t.Run("wrong auth", func(t *testing.T) {
		wrongAuthClient, err := NewHTTPClient(WithBaseURL(chromaURL), WithDebug(), WithAuth(NewTokenAuthCredentialsProvider("wrong_token", AuthorizationTokenHeader)))
		require.NoError(t, err)
		_, err = wrongAuthClient.ListCollections(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "403")
	})
}

func TestClientHTTPIntegrationWithBearerXChromaTokenHeaderAuth(t *testing.T) {
	ctx := context.Background()
	var chromaVersion = "0.6.3"
	var chromaImage = "ghcr.io/chroma-core/chroma"
	if os.Getenv("CHROMA_VERSION") != "" {
		chromaVersion = os.Getenv("CHROMA_VERSION")
	}
	if isChromaVersion1x(chromaVersion) {
		t.Skip("Not supported by Chroma 1.x")
	}
	if os.Getenv("CHROMA_IMAGE") != "" {
		chromaImage = os.Getenv("CHROMA_IMAGE")
	}
	token := "chr0ma-t0k3n"

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
			"ALLOW_RESET":                        "true",
			"CHROMA_SERVER_AUTHN_CREDENTIALS":    token,
			"CHROMA_SERVER_AUTHN_PROVIDER":       "chromadb.auth.token_authn.TokenAuthenticationServerProvider",
			"CHROMA_AUTH_TOKEN_TRANSPORT_HEADER": "X-Chroma-Token",
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
	//
	// chromaContainer, err := tcchroma.Run(ctx,
	//	fmt.Sprintf("%s:%s", chromaImage, chromaVersion),
	//	testcontainers.WithEnv(map[string]string{"ALLOW_RESET": "true"}),
	//	testcontainers.WithEnv(map[string]string{"CHROMA_SERVER_AUTHN_CREDENTIALS": token}),
	//	testcontainers.WithEnv(map[string]string{"CHROMA_SERVER_AUTHN_PROVIDER": "chromadb.auth.token_authn.TokenAuthenticationServerProvider"}),
	//	testcontainers.WithEnv(map[string]string{"CHROMA_AUTH_TOKEN_TRANSPORT_HEADER": "X-Chroma-Token"}),
	//)
	// require.NoError(t, err)
	// t.Cleanup(func() {
	//	require.NoError(t, chromaContainer.Terminate(ctx))
	// })
	//endpoint, err := chromaContainer.RESTEndpoint(context.Background())
	//require.NoError(t, err)
	chromaURL := os.Getenv("CHROMA_URL")
	if chromaURL == "" {
		chromaURL = endpoint
	}
	t.Run("success auth", func(t *testing.T) {
		c, err := NewHTTPClient(WithBaseURL(chromaURL), WithDebug(), WithAuth(NewTokenAuthCredentialsProvider(token, XChromaTokenHeader)))
		require.NoError(t, err)
		require.NotNil(t, c)
		collections, err := c.ListCollections(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, len(collections))
	})
	t.Run("wrong auth", func(t *testing.T) {
		wrongAuthClient, err := NewHTTPClient(WithBaseURL(chromaURL), WithDebug(), WithAuth(NewTokenAuthCredentialsProvider("wrong_token", XChromaTokenHeader)))
		require.NoError(t, err)
		_, err = wrongAuthClient.ListCollections(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "403")
	})
}

func TestClientHTTPIntegrationWithSSL(t *testing.T) {

	ctx := context.Background()
	var chromaImage = "ghcr.io/chroma-core/chroma"
	var chromaVersion = "0.6.3"
	if os.Getenv("CHROMA_VERSION") != "" {
		chromaVersion = os.Getenv("CHROMA_VERSION")
	}
	if isChromaVersion1x(chromaVersion) {
		t.Skip("Not supported by Chroma 1.x")
	}

	if os.Getenv("CHROMA_IMAGE") != "" {
		chromaImage = os.Getenv("CHROMA_IMAGE")
	}
	tempDir := t.TempDir()
	certPath := fmt.Sprintf("%s/server.crt", tempDir)
	keyPath := fmt.Sprintf("%s/server.key", tempDir)
	containerCertPath := "/chroma/server.crt"
	containerKeyPath := "/chroma/server.key"

	cmd := []string{"--workers", "1",
		"--host", "0.0.0.0",
		"--port", "8000",
		"--proxy-headers",
		"--log-config", "/chroma/chromadb/log_config.yml",
		"--timeout-keep-alive", "30",
		"--ssl-certfile", containerCertPath,
		"--ssl-keyfile", containerKeyPath,
	}
	entrypoint := []string{}
	if chromaVersion != "latest" {
		cv := semver.MustParse(chromaVersion)
		if cv.LessThan(semver.MustParse("0.4.11")) {
			entrypoint = append(entrypoint, "/bin/bash", "-c")
			cmd = []string{fmt.Sprintf("pip install --force-reinstall --no-cache-dir chroma-hnswlib numpy==1.26.4 && ln -s /chroma/log_config.yml /chroma/chromadb/log_config.yml && uvicorn chromadb.app:app %s", strings.Join(cmd, " "))}
		} else if cv.LessThan(semver.MustParse("0.4.23")) {
			cmd = append([]string{"uvicorn", "chromadb.app:app"}, cmd...)
		}
	}

	CreateSelfSignedCert(certPath, keyPath)
	chromaContainer, err := tcchroma.Run(ctx,
		fmt.Sprintf("%s:%s", chromaImage, chromaVersion),
		testcontainers.WithEnv(map[string]string{"ALLOW_RESET": "true"}),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				WaitingFor: wait.ForAll(
					wait.ForListeningPort("8000/tcp"),
				),
				Entrypoint: entrypoint,
				HostConfigModifier: func(hostConfig *container.HostConfig) {
					hostConfig.Mounts = []mount.Mount{
						{
							Type:   mount.TypeBind,
							Source: certPath,
							Target: containerCertPath,
						},
						{
							Type:   mount.TypeBind,
							Source: keyPath,
							Target: containerKeyPath,
						},
					}
				},
				Cmd: cmd,
			},
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, chromaContainer.Terminate(ctx))
	})
	endpoint, err := chromaContainer.RESTEndpoint(context.Background())
	require.NoError(t, err)
	chromaURL := os.Getenv("CHROMA_URL")
	if chromaURL == "" {
		chromaURL = endpoint
	}
	chromaURL = strings.ReplaceAll(endpoint, "http://", "https://")
	time.Sleep(5 * time.Second)
	t.Run("Test with insecure client", func(t *testing.T) {
		client, err := NewHTTPClient(WithBaseURL(chromaURL), WithInsecure(), WithDebug())
		require.NoError(t, err)
		version, err := client.GetVersion(ctx)
		require.NoError(t, err)
		require.NotNil(t, version)
	})

	t.Run("Test without SSL", func(t *testing.T) {
		client, err := NewHTTPClient(WithBaseURL(chromaURL), WithDebug())
		require.NoError(t, err)
		_, err = client.GetVersion(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "x509: certificate signed by unknown authority")
	})

	t.Run("Test with SSL", func(t *testing.T) {
		client, err := NewHTTPClient(WithBaseURL(chromaURL), WithSSLCert(certPath), WithDebug())
		require.NoError(t, err)
		version, err := client.GetVersion(ctx)
		require.NoError(t, err)
		require.NotNil(t, version)
	})
}
