//go:build !cloud

package chroma

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"

	chhttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

func MetadataModel() gopter.Gen {
	return gen.SliceOf(
		gen.Struct(reflect.TypeOf(struct {
			Key   string
			Value interface{}
		}{}), map[string]gopter.Gen{
			"Key":   gen.Identifier(),
			"Value": gen.OneGenOf(gen.Int64(), gen.Float64(), gen.AlphaString(), gen.Bool()),
		}),
	).Map(func(entries *gopter.GenResult) CollectionMetadata {
		result := make(map[string]interface{})
		for _, entry := range entries.Result.([]struct {
			Key   string
			Value interface{}
		}) {
			result[entry.Key] = entry.Value
		}
		return NewMetadataFromMap(result)
	})
}

// CollectionIDStrategy generates random UUIDs as a gopter generator.
func CollectionIDStrategy() gopter.Gen {
	return func(params *gopter.GenParameters) *gopter.GenResult {
		id := uuid.New() // Generates a new random UUID
		return gopter.NewGenResult(id.String(), gopter.NoShrinker)
	}
}

func TenantStrategy() gopter.Gen {
	return gen.OneGenOf(func(params *gopter.GenParameters) *gopter.GenResult {
		id := uuid.New() // Generates a new random UUID
		return gopter.NewGenResult(id.String(), gopter.NoShrinker)
	}, func(params *gopter.GenParameters) *gopter.GenResult {
		return gopter.NewGenResult(DefaultTenant, gopter.NoShrinker)
	})
}

func DatabaseStrategy() gopter.Gen {
	return gen.OneGenOf(func(params *gopter.GenParameters) *gopter.GenResult {
		id := uuid.New() // Generates a new random UUID
		return gopter.NewGenResult(id.String(), gopter.NoShrinker)
	}, func(params *gopter.GenParameters) *gopter.GenResult {
		return gopter.NewGenResult(DefaultDatabase, gopter.NoShrinker)
	})
}

func CollectionModelStrategy() gopter.Gen {
	return gen.Struct(reflect.TypeOf(CollectionModel{}), map[string]gopter.Gen{
		"ID":       CollectionIDStrategy(),
		"Name":     gen.AlphaString(),
		"Tenant":   TenantStrategy(),
		"Database": DatabaseStrategy(),
		"Metadata": MetadataModel(),
	})
}

// Property-based test for creating collections
func TestCreateCollectionProperty(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	properties := gopter.NewProperties(parameters)

	properties.Property("CreateCollection handles different names and metadata", prop.ForAll(
		func(name string, col CollectionModel) bool {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				respBody, readErr := chhttp.ReadRespBody(r.Body)
				require.NoError(t, readErr)
				var op CreateCollectionOp
				err := json.Unmarshal([]byte(respBody), &op)
				require.NoError(t, err)
				require.Equal(t, name, op.Name)
				// Configuration is now included with EF info
				require.NotNil(t, op.Configuration)
				cm := CollectionModel{
					ID:       col.ID,
					Name:     col.Name,
					Tenant:   col.Tenant,
					Database: col.Database,
					Metadata: col.Metadata,
				}
				w.WriteHeader(http.StatusOK)
				err = json.NewEncoder(w).Encode(&cm)
				require.NoError(t, err)
			}))
			defer server.Close()

			client, err := NewHTTPClient(WithBaseURL(server.URL), WithDatabaseAndTenant(col.Database, col.Tenant))

			require.NoError(t, err)

			// Call API with random data
			c, err := client.CreateCollection(context.Background(), name, WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
			require.NoError(t, err)
			require.NotNil(t, c)
			require.Equal(t, col.ID, c.ID())
			require.Equal(t, col.Name, c.Name())
			require.Equal(t, col.Tenant, c.Tenant().Name())
			require.Equal(t, col.Database, c.Database().Name())
			require.ElementsMatch(t, col.Metadata.Keys(), c.Metadata().Keys())
			for _, k := range col.Metadata.Keys() {
				val1, ok1 := col.Metadata.GetRaw(k)
				require.True(t, ok1)
				metadataValue1, ok11 := val1.(MetadataValue)
				require.True(t, ok11)
				val2, ok2 := c.Metadata().GetRaw(k)
				require.True(t, ok2)
				metadataValue2, ok22 := val2.(MetadataValue)
				require.True(t, ok22)
				r1, _ := metadataValue1.GetRaw()
				r2, _ := metadataValue2.GetRaw()
				if !metadataValue1.Equal(&metadataValue2) {
					fmt.Println(col.Metadata.GetRaw(k))
					fmt.Println(c.Metadata().GetRaw(k))
					fmt.Printf("%T != %T\n", r1, r2)
					fmt.Println(k, r1, r2, metadataValue1.Equal(&metadataValue2))
				}
				require.Truef(t, metadataValue1.Equal(&metadataValue2), "metadata values are not equal: %v != %v", metadataValue1, metadataValue2)
			}
			return true
		},
		gen.AlphaString().SuchThat(func(v interface{}) bool {
			return len(v.(string)) > 0
		}), // Random collection name
		CollectionModelStrategy(), // Random collection
	))

	properties.TestingRun(t)
}

func TestAPIClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		respBody, readErr := chhttp.ReadRespBody(r.Body)
		require.NoError(t, readErr)
		t.Logf("Body: %s", respBody)
		switch {
		case r.URL.Path == "/api/v2/version" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`0.6.3`))
			require.NoError(t, err)
		case r.URL.Path == "/api/v2/heartbeat" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"nanosecond heartbeat":1732127707371421353}`))
			require.NoError(t, err)
		case r.URL.Path == "/api/v2/tenants/default_tenant" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"name":"default_tenant"}`))
			require.NoError(t, err)
		case r.URL.Path == "/api/v2/tenants" && r.Method == http.MethodPost:
			require.JSONEq(t, `{"name":"test_tenant"}`, respBody)
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{}`))
			require.NoError(t, err)
		// create database
		case r.URL.Path == "/api/v2/tenants/test_tenant/databases" && r.Method == http.MethodPost:
			require.JSONEq(t, `{"name":"test_db"}`, respBody)
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{}`))
			require.NoError(t, err)
		// get database
		case r.URL.Path == "/api/v2/tenants/test_tenant/databases/test_db" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "name": "test_db",
  "tenant": "test_tenant"
}`))
			require.NoError(t, err)
		case r.URL.Path == "/api/v2/tenants/test_tenant/databases" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`[
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "name": "test_db1",
  "tenant": "test_tenant"
},
{
  "id": "2fa85f64-5717-4562-b3fc-2c963f66afa6",
  "name": "test_db2",
  "tenant": "test_tenant"
}
]`))
			require.NoError(t, err)
		// Delete database
		case r.URL.Path == "/api/v2/tenants/test_tenant/databases/test_db" && r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{}`))
			require.NoError(t, err)
		case r.URL.Path == "/api/v2/tenants/default_tenant/databases/default_database/collections_count" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`100`))
			require.NoError(t, err)
		case r.URL.Path == "/api/v2/tenants/default_tenant/databases/default_database/collections" && r.Method == http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`[
  {
    "id": "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
    "configuration_json": {
      "hnsw_configuration": {
        "space": "l2",
        "ef_construction": 100,
        "ef_search": 10,
        "num_threads": 14,
        "M": 16,
        "resize_factor": 1.2,
        "batch_size": 100,
        "sync_threshold": 1000,
        "_type": "HNSWConfigurationInternal"
      },
      "_type": "CollectionConfigurationInternal"
    },
    "database": "default_database",
    "dimension": 384,
    "log_position": 0,
    "metadata": {
      "t": 1
    },
    "name": "testcoll",
    "tenant": "default_tenant",
    "version": 0
  }
]`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewHTTPClient(WithBaseURL(server.URL))
	require.NoError(t, err)

	t.Run("GetVersion", func(t *testing.T) {
		ver, err := client.GetVersion(context.Background())
		require.NoError(t, err)
		require.NotNil(t, ver)
		require.Equal(t, "0.6.3", ver)
	})
	t.Run("Hearbeat", func(t *testing.T) {
		err := client.Heartbeat(context.Background())
		require.NoError(t, err)
	})

	t.Run("GetTenant", func(t *testing.T) {
		tenant, err := client.GetTenant(context.Background(), NewDefaultTenant())
		require.NoError(t, err)
		require.NotNil(t, tenant)
		require.Equal(t, "default_tenant", tenant.Name())
	})

	t.Run("CreateTenant", func(t *testing.T) {
		tenant, err := client.CreateTenant(context.Background(), NewTenant("test_tenant"))
		require.NoError(t, err)
		require.NotNil(t, tenant)
		require.Equal(t, "test_tenant", tenant.Name())
	})

	t.Run("CreateDatabase", func(t *testing.T) {
		db, err := client.CreateDatabase(context.Background(), NewTenant("test_tenant").Database("test_db"))
		require.NoError(t, err)
		require.NotNil(t, db)
		require.Equal(t, "test_db", db.Name())
	})

	t.Run("ListDatabases", func(t *testing.T) {
		dbs, err := client.ListDatabases(context.Background(), NewTenant("test_tenant"))
		require.NoError(t, err)
		require.NotNil(t, dbs)
		require.Len(t, dbs, 2)
		require.Equal(t, "test_db1", dbs[0].Name())
		require.Equal(t, "test_tenant", dbs[0].Tenant().Name())
		require.Equal(t, "test_db2", dbs[1].Name())
		require.Equal(t, "test_tenant", dbs[1].Tenant().Name())
	})

	t.Run("GetDatabase", func(t *testing.T) {
		db, err := client.GetDatabase(context.Background(), NewTenant("test_tenant").Database("test_db"))
		require.NoError(t, err)
		require.NotNil(t, db)
		require.Equal(t, "test_db", db.Name())
		require.Equal(t, "test_tenant", db.Tenant().Name())
		require.Equal(t, "3fa85f64-5717-4562-b3fc-2c963f66afa6", db.ID())
	})

	t.Run("DeleteDatabase", func(t *testing.T) {
		err := client.DeleteDatabase(context.Background(), NewTenant("test_tenant").Database("test_db"))
		require.NoError(t, err)
	})

	t.Run("CountCollections", func(t *testing.T) {
		count, err := client.CountCollections(context.Background())
		require.NoError(t, err)
		require.Equal(t, 100, count)
	})

	t.Run("ListCollections", func(t *testing.T) {
		cols, err := client.ListCollections(context.Background())
		require.NoError(t, err)
		require.NotNil(t, cols)
		require.Len(t, cols, 1)
		c := cols[0]
		require.Equal(t, "8ecf0f7e-e806-47f8-96a1-4732ef42359e", c.ID())
		require.Equal(t, 384, c.Dimension())
		require.Equal(t, "testcoll", c.Name())
		require.Equal(t, NewDefaultTenant(), c.Tenant())
		require.Equal(t, NewDefaultDatabase(), c.Database())
		require.NotNil(t, c.Metadata())
		vi, ok := c.Metadata().GetInt("t")
		require.True(t, ok)
		require.Equal(t, int64(1), vi)
	})

	t.Run("CreateCollection", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			respBody, readErr := chhttp.ReadRespBody(r.Body)
			require.NoError(t, readErr)
			t.Logf("Body: %s", respBody)

			switch {
			case r.URL.Path == "/api/v2/tenants/default_tenant/databases/default_database/collections" && r.Method == http.MethodPost:
				w.WriteHeader(http.StatusOK)
				var op CreateCollectionOp
				err := json.Unmarshal([]byte(respBody), &op)
				require.NoError(t, err)
				require.Equal(t, "test", op.Name)
				require.NotNil(t, op.Configuration) // Configuration now includes EF info
				values, err := url.ParseQuery(r.URL.RawQuery)
				require.NoError(t, err)
				cm := CollectionModel{
					ID:       "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
					Name:     op.Name,
					Tenant:   values.Get("tenant"),
					Database: values.Get("database"),
					Metadata: op.Metadata,
				}
				err = json.NewEncoder(w).Encode(&cm)
				require.NoError(t, err)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()
		innerClient, err := NewHTTPClient(WithBaseURL(server.URL))
		require.NoError(t, err)
		c, err := innerClient.CreateCollection(context.Background(), "test", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		require.NotNil(t, c)
	})

	t.Run("GetOrCreateCollection", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			respBody, readErr := chhttp.ReadRespBody(r.Body)
			require.NoError(t, readErr)
			t.Logf("Body: %s", respBody)

			switch {
			case r.URL.Path == "/api/v2/tenants/default_tenant/databases/default_database/collections" && r.Method == http.MethodPost:
				w.WriteHeader(http.StatusOK)
				var reqBody map[string]interface{}
				require.NoError(t, json.Unmarshal([]byte(respBody), &reqBody))
				require.Equal(t, "test", reqBody["name"])
				require.Equal(t, true, reqBody["get_or_create"])
				values, err := url.ParseQuery(r.URL.RawQuery)
				require.NoError(t, err)
				var op CreateCollectionOp
				err = json.Unmarshal([]byte(respBody), &op)
				require.NoError(t, err)
				cm := CollectionModel{
					ID:        "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
					Name:      op.Name,
					Tenant:    values.Get("tenant"),
					Database:  values.Get("database"),
					Metadata:  op.Metadata,
					Dimension: 9001,
				}
				err = json.NewEncoder(w).Encode(&cm)
				require.NoError(t, err)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()
		innerClient, err := NewHTTPClient(WithBaseURL(server.URL))
		require.NoError(t, err)
		c, err := innerClient.GetOrCreateCollection(context.Background(), "test", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, "8ecf0f7e-e806-47f8-96a1-4732ef42359e", c.ID())
		require.Equal(t, "test", c.Name())
		require.Equal(t, 9001, c.Dimension())
	})

	t.Run("GetCollection", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)

			switch {
			case r.URL.Path == "/api/v2/tenants/default_tenant/databases/default_database/collections/test" && r.Method == http.MethodGet:
				w.WriteHeader(http.StatusOK)
				require.NoError(t, err)
				cm := CollectionModel{
					ID:        "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
					Name:      "test",
					Tenant:    "default_tenant",
					Database:  "default_database",
					Metadata:  NewMetadataFromMap(map[string]any{"t": 1}),
					Dimension: 9001,
				}
				err = json.NewEncoder(w).Encode(&cm)
				require.NoError(t, err)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()
		innerClient, err := NewHTTPClient(WithBaseURL(server.URL))
		require.NoError(t, err)
		c, err := innerClient.GetCollection(context.Background(), "test", WithEmbeddingFunctionGet(embeddings.NewConsistentHashEmbeddingFunction()))
		// TODO also test with tenant and database and EF
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, "8ecf0f7e-e806-47f8-96a1-4732ef42359e", c.ID())
		require.Equal(t, "test", c.Name())
		require.Equal(t, NewDefaultTenant(), c.Tenant())
		require.Equal(t, NewDefaultDatabase(), c.Database())
		require.NotNil(t, c.Metadata())
		require.Equal(t, 9001, c.Dimension())
		vi, ok := c.Metadata().GetInt("t")
		require.True(t, ok)
		require.Equal(t, int64(1), vi)
	})
}

func TestCreateCollection(t *testing.T) {
	var tests = []struct {
		name                        string
		validateRequestWithResponse func(w http.ResponseWriter, r *http.Request)
		sendRequest                 func(client Client)
	}{
		{
			name: "with name only",
			validateRequestWithResponse: func(w http.ResponseWriter, r *http.Request) {
				respBody, readErr := chhttp.ReadRespBody(r.Body)
				require.NoError(t, readErr)
				respMap := make(map[string]any)
				err := json.Unmarshal([]byte(respBody), &respMap)
				require.NoError(t, err)
				require.Equal(t, "test", respMap["name"])
				w.WriteHeader(http.StatusOK)
				_, err = w.Write([]byte(`{"id":"8ecf0f7e-e806-47f8-96a1-4732ef42359e","name":"test"}`))
				require.NoError(t, err)
			},
			sendRequest: func(client Client) {
				collection, err := client.CreateCollection(context.Background(), "test", WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()))
				require.NoError(t, err)
				require.NotNil(t, collection)
				require.Equal(t, "8ecf0f7e-e806-47f8-96a1-4732ef42359e", collection.ID())
				require.Equal(t, "test", collection.Name())
			},
		},
		{
			name: "with metadata",
			validateRequestWithResponse: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				respBody, readErr := chhttp.ReadRespBody(r.Body)
				require.NoError(t, readErr)
				var op CreateCollectionOp
				err := json.Unmarshal([]byte(respBody), &op)
				require.NoError(t, err)
				v, ok := op.Metadata.GetInt("int")
				require.True(t, ok)
				require.Equal(t, int64(1), v)
				vf, ok := op.Metadata.GetFloat("float")
				require.True(t, ok)
				require.Equal(t, 1.1, vf)
				vs, ok := op.Metadata.GetString("string")
				require.True(t, ok)
				require.Equal(t, "test", vs)
				vb, ok := op.Metadata.GetBool("bool")
				require.True(t, ok)
				require.True(t, vb)
				cm := CollectionModel{
					ID:       "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
					Name:     op.Name,
					Tenant:   "default_tenant",
					Database: "default_database",
					Metadata: op.Metadata,
				}
				err = json.NewEncoder(w).Encode(&cm)
				require.NoError(t, err)
			},
			sendRequest: func(client Client) {
				collection, err := client.CreateCollection(context.Background(), "test",
					WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()),
					WithCollectionMetadataCreate(
						NewMetadataFromMap(map[string]any{"int": 1, "float": 1.1, "string": "test", "bool": true})),
				)
				require.NoError(t, err)
				require.NotNil(t, collection)
				require.Equal(t, "8ecf0f7e-e806-47f8-96a1-4732ef42359e", collection.ID())
				require.Equal(t, "test", collection.Name())
				vf, ok := collection.Metadata().GetFloat("float")
				require.True(t, ok)
				require.Equal(t, 1.1, vf)
				vs, ok := collection.Metadata().GetString("string")
				require.True(t, ok)
				require.Equal(t, "test", vs)
				vb, ok := collection.Metadata().GetBool("bool")
				require.True(t, ok)
				require.True(t, vb)
				vi, ok := collection.Metadata().GetInt("int")
				require.True(t, ok)
				require.Equal(t, int64(1), vi)
				require.Equal(t, NewDefaultTenant(), collection.Tenant())
				require.Equal(t, NewDefaultDatabase(), collection.Database())
			},
		},
		{
			name: "with HNSW params",
			validateRequestWithResponse: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				respBody, readErr := chhttp.ReadRespBody(r.Body)
				require.NoError(t, readErr)
				var op CreateCollectionOp
				err := json.Unmarshal([]byte(respBody), &op)
				require.NoError(t, err)
				var vi int64
				var vs string
				var vf float64
				var ok bool
				vs, ok = op.Metadata.GetString(HNSWSpace)
				require.True(t, ok)
				require.Equal(t, string(embeddings.L2), vs)
				vi, ok = op.Metadata.GetInt(HNSWNumThreads)
				require.True(t, ok)
				require.Equal(t, int64(14), vi)
				vf, ok = op.Metadata.GetFloat(HNSWResizeFactor)
				require.True(t, ok)
				require.Equal(t, 1.2, vf)
				vi, ok = op.Metadata.GetInt(HNSWBatchSize)
				require.True(t, ok)
				require.Equal(t, int64(2000), vi)
				vi, ok = op.Metadata.GetInt(HNSWSyncThreshold)
				require.True(t, ok)
				require.Equal(t, int64(10000), vi)
				vi, ok = op.Metadata.GetInt(HNSWConstructionEF)
				require.True(t, ok)
				require.Equal(t, int64(100), vi)
				vi, ok = op.Metadata.GetInt(HNSWSearchEF)
				require.True(t, ok)
				require.Equal(t, int64(999), vi)
				cm := CollectionModel{
					ID:       "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
					Name:     op.Name,
					Tenant:   DefaultTenant,
					Database: DefaultDatabase,
					Metadata: op.Metadata,
				}
				err = json.NewEncoder(w).Encode(&cm)
				require.NoError(t, err)
			},
			sendRequest: func(client Client) {
				collection, err := client.CreateCollection(
					context.Background(),
					"test",
					WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()),
					WithHNSWSpaceCreate(embeddings.L2),
					WithHNSWMCreate(100),
					WithHNSWNumThreadsCreate(14),
					WithHNSWResizeFactorCreate(1.2),
					WithHNSWBatchSizeCreate(2000),
					WithHNSWSyncThresholdCreate(10000),
					WithHNSWConstructionEfCreate(100),
					WithHNSWSearchEfCreate(999),
				)
				require.NoError(t, err)
				require.NotNil(t, collection)
				require.Equal(t, "8ecf0f7e-e806-47f8-96a1-4732ef42359e", collection.ID())
				require.Equal(t, "test", collection.Name())
				hnswSpace, ok := collection.Metadata().GetString(HNSWSpace)
				require.True(t, ok)
				require.Equal(t, string(embeddings.L2), hnswSpace)
				hnswNumThreads, ok := collection.Metadata().GetInt(HNSWNumThreads)
				require.True(t, ok)
				require.Equal(t, int64(14), hnswNumThreads)
				hnswResizeFactor, ok := collection.Metadata().GetFloat(HNSWResizeFactor)
				require.True(t, ok)
				require.Equal(t, 1.2, hnswResizeFactor)
				hnswBatchSize, ok := collection.Metadata().GetInt(HNSWBatchSize)
				require.True(t, ok)
				require.Equal(t, int64(2000), hnswBatchSize)
				hnswSyncThreshold, ok := collection.Metadata().GetInt(HNSWSyncThreshold)
				require.True(t, ok)
				require.Equal(t, int64(10000), hnswSyncThreshold)
				hnswConstructionEf, ok := collection.Metadata().GetInt(HNSWConstructionEF)
				require.True(t, ok)
				require.Equal(t, int64(100), hnswConstructionEf)
				hnswSearchEf, ok := collection.Metadata().GetInt(HNSWSearchEF)
				require.True(t, ok)
				require.Equal(t, int64(999), hnswSearchEf)
			},
		},
		{
			name: "with tenant and database",
			validateRequestWithResponse: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				respBody, readErr := chhttp.ReadRespBody(r.Body)
				require.NoError(t, readErr)
				var op CreateCollectionOp
				err := json.Unmarshal([]byte(respBody), &op)
				require.NoError(t, err)
				require.Contains(t, "mytenant", r.URL.RawQuery)
				require.Contains(t, "mydb", r.URL.RawQuery)
				cm := CollectionModel{
					ID:       "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
					Name:     op.Name,
					Tenant:   "mytenant",
					Database: "mydb",
					Metadata: op.Metadata,
				}
				err = json.NewEncoder(w).Encode(&cm)
				require.NoError(t, err)
			},
			sendRequest: func(client Client) {
				collection, err := client.CreateCollection(
					context.Background(),
					"test",
					WithEmbeddingFunctionCreate(embeddings.NewConsistentHashEmbeddingFunction()),
					WithDatabaseCreate(NewTenant("mytenant").Database("mydb")),
				)
				require.NoError(t, err)
				require.NotNil(t, collection)
				require.Equal(t, "8ecf0f7e-e806-47f8-96a1-4732ef42359e", collection.ID())
				require.Equal(t, "test", collection.Name())
				require.Equal(t, NewTenant("mytenant"), collection.Tenant())
				require.Equal(t, NewDatabase("mydb", NewTenant("mytenant")), collection.Database())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
				matched, err := regexp.MatchString(`/api/v2/tenants/[^/]+/databases/[^/]+/collections`, r.URL.Path)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				switch {
				case matched && r.Method == http.MethodPost:
					tt.validateRequestWithResponse(w, r)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
			require.NoError(t, err)
			tt.sendRequest(client)
			err = client.Close()
			require.NoError(t, err)
		})
	}
}

func TestClientClose(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		matched, err := regexp.MatchString(`/api/v2/tenants/[^/]+/databases/[^/]+/collections`, r.URL.Path)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		switch {
		case matched && r.Method == http.MethodPost:
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
	require.NoError(t, err)
	err = client.Close()
	require.NoError(t, err)

}

func TestClientSetup(t *testing.T) {
	t.Run("With default tenant and database", func(t *testing.T) {
		client, err := NewHTTPClient(WithBaseURL("http://localhost:8080"), WithDebug())
		require.NoError(t, err)
		require.NotNil(t, client)
		require.Equal(t, NewDefaultTenant(), client.CurrentTenant())
		require.Equal(t, NewDefaultDatabase(), client.CurrentDatabase())
	})

	t.Run("With env tenant and database", func(t *testing.T) {
		t.Setenv("CHROMA_TENANT", "test_tenant")
		t.Setenv("CHROMA_DATABASE", "test_db")
		client, err := NewHTTPClient(WithBaseURL("http://localhost:8080"), WithDebug())
		require.NoError(t, err)
		require.NotNil(t, client)
		require.Equal(t, NewTenant("test_tenant"), client.CurrentTenant())
		require.Equal(t, NewDatabase("test_db", NewTenant("test_tenant")), client.CurrentDatabase())
	})
}

func TestPreFlightConcurrency(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v2/pre-flight-checks" && r.Method == http.MethodGet:
			atomic.AddInt32(&requestCount, 1)
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"max_batch_size": 100}`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(WithBaseURL(server.URL))
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	apiClient, ok := client.(*APIClientV2)
	require.True(t, ok)

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			err := apiClient.PreFlight(context.Background())
			if err != nil {
				errChan <- err
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		require.NoError(t, err)
	}

	require.Equal(t, int32(1), atomic.LoadInt32(&requestCount), "PreFlight should only make one HTTP request")
	require.True(t, apiClient.preflightCompleted)
	require.NotNil(t, apiClient.preflightLimits)
}
