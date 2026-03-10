//go:build !cloud

package chroma

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	chhttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type ChromaCollectionUpdateRequest struct {
	IDs        []string         `json:"ids"`
	Documents  []string         `json:"documents"`
	Embeddings [][]float64      `json:"embeddings"`
	Metadatas  []map[string]any `json:"metadatas"`
	Include    []string         `json:"include"`
	Limit      int              `json:"limit"`
	Offset     int              `json:"offset"`
	Where      map[string]any   `json:"where"`
	WhereDoc   map[string]any   `json:"where_document"`
}

func TestCollectionAdd(t *testing.T) {
	var tests = []struct {
		name                 string
		serverSideValidation func(resp string)
		addOptions           []CollectionAddOption
		limits               string
	}{
		{
			name: "with IDs and docs",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
			},
			addOptions: []CollectionAddOption{
				WithIDs("1", "2", "3"),
				WithTexts("doc1", "doc2", "doc3"),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with IDs and docs and embeddings",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
				require.Equal(t, [][]float64{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}, {7.0, 8.0, 9.0}}, req.Embeddings)
			},
			addOptions: []CollectionAddOption{
				WithIDs("1", "2", "3"),
				WithTexts("doc1", "doc2", "doc3"),
				WithEmbeddings(embeddings.NewEmbeddingFromFloat32([]float32{1.0, 2.0, 3.0}), embeddings.NewEmbeddingFromFloat32([]float32{4.0, 5.0, 6.0}), embeddings.NewEmbeddingFromFloat32([]float32{7.0, 8.0, 9.0})),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with IDs and docs and embeddings and metadatas",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
				require.Equal(t, [][]float64{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}, {7.0, 8.0, 9.0}}, req.Embeddings)
				fmt.Println(req.Metadatas)
				require.Equal(t, []map[string]any{
					{"metadata1": "metadata1", "metadata2": float64(2), "metadata3": true}, // ints if not handled will arrive as float64
					{"metadata1": "metadata1", "metadata2": float64(3), "metadata3": true},
					{"metadata1": "metadata1", "metadata2": float64(4), "metadata3": true},
				}, req.Metadatas)
			},
			addOptions: []CollectionAddOption{
				WithIDs("1", "2", "3"),
				WithTexts("doc1", "doc2", "doc3"),
				WithEmbeddings(
					embeddings.NewEmbeddingFromFloat32([]float32{1.0, 2.0, 3.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{4.0, 5.0, 6.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{7.0, 8.0, 9.0}),
				),
				WithMetadatas(
					NewDocumentMetadata(NewStringAttribute("metadata1", "metadata1"), NewIntAttribute("metadata2", 2), NewBoolAttribute("metadata3", true)),
					NewDocumentMetadata(NewStringAttribute("metadata1", "metadata1"), NewIntAttribute("metadata2", 3), NewBoolAttribute("metadata3", true)),
					NewDocumentMetadata(NewStringAttribute("metadata1", "metadata1"), NewIntAttribute("metadata2", 4), NewBoolAttribute("metadata3", true)),
				),
			},
			limits: `{"max_batch_size":100}`,
		},
	}

	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+/add`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
				respBody, readErr := chhttp.ReadRespBody(r.Body)
				require.NoError(t, readErr)
				t.Logf("Body: %s", respBody)
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
					w.WriteHeader(http.StatusOK)
					_, err := w.Write([]byte(tt.limits))
					require.NoError(t, err)
				case rx1.MatchString(r.URL.Path):
					w.WriteHeader(http.StatusOK)
					tt.serverSideValidation(respBody)
					_, err := w.Write([]byte(`true`))
					require.NoError(t, err)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
			require.NoError(t, err)
			collection := &CollectionImpl{
				name:              "test",
				id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
				tenant:            NewDefaultTenant(),
				database:          NewDefaultDatabase(),
				metadata:          NewMetadata(),
				client:            client.(*APIClientV2),
				embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
			}
			require.NotNil(t, collection)
			err = collection.Add(context.Background(), tt.addOptions...)
			require.NoError(t, err)
		})
	}
}

func TestCollectionUpdate(t *testing.T) {
	var tests = []struct {
		name                 string
		serverSideValidation func(resp string)
		updateOptions        []CollectionUpdateOption
		limits               string
	}{
		{
			name: "with IDs and docs",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
			},
			updateOptions: []CollectionUpdateOption{
				WithIDs("1", "2", "3"),
				WithTexts("doc1", "doc2", "doc3"),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with IDs and docs and embeddings",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
				require.Equal(t, [][]float64{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}, {7.0, 8.0, 9.0}}, req.Embeddings)
			},
			updateOptions: []CollectionUpdateOption{
				WithIDs("1", "2", "3"),
				WithTexts("doc1", "doc2", "doc3"),
				WithEmbeddings(
					embeddings.NewEmbeddingFromFloat32([]float32{1.0, 2.0, 3.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{4.0, 5.0, 6.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{7.0, 8.0, 9.0}),
				),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with IDs and docs and embeddings and metadatas",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
				require.Equal(t, [][]float64{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}, {7.0, 8.0, 9.0}}, req.Embeddings)
				fmt.Println(req.Metadatas)
				require.Equal(t, []map[string]any{
					{"metadata1": "metadata1", "metadata2": float64(2), "metadata3": true}, // ints if not handled will arrive as float64
					{"metadata1": "metadata1", "metadata2": float64(3), "metadata3": true},
					{"metadata1": "metadata1", "metadata2": float64(4), "metadata3": true},
				}, req.Metadatas)
			},
			updateOptions: []CollectionUpdateOption{
				WithIDs("1", "2", "3"),
				WithTexts("doc1", "doc2", "doc3"),
				WithEmbeddings(
					embeddings.NewEmbeddingFromFloat32([]float32{1.0, 2.0, 3.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{4.0, 5.0, 6.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{7.0, 8.0, 9.0}),
				),
				WithMetadatas(
					NewDocumentMetadata(NewStringAttribute("metadata1", "metadata1"), NewIntAttribute("metadata2", 2), NewBoolAttribute("metadata3", true)),
					NewDocumentMetadata(NewStringAttribute("metadata1", "metadata1"), NewIntAttribute("metadata2", 3), NewBoolAttribute("metadata3", true)),
					NewDocumentMetadata(NewStringAttribute("metadata1", "metadata1"), NewIntAttribute("metadata2", 4), NewBoolAttribute("metadata3", true)),
				),
			},
			limits: `{"max_batch_size":100}`,
		},
	}

	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+/update`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
				respBody, readErr := chhttp.ReadRespBody(r.Body)
				require.NoError(t, readErr)
				t.Logf("Body: %s", respBody)
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
					w.WriteHeader(http.StatusOK)
					_, err := w.Write([]byte(tt.limits))
					require.NoError(t, err)
				case rx1.MatchString(r.URL.Path):
					w.WriteHeader(http.StatusOK)
					tt.serverSideValidation(respBody)
					_, err := w.Write([]byte(`true`))
					require.NoError(t, err)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
			require.NoError(t, err)
			collection := &CollectionImpl{
				name:              "test",
				id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
				tenant:            NewDefaultTenant(),
				database:          NewDefaultDatabase(),
				metadata:          NewMetadata(),
				client:            client.(*APIClientV2),
				embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
			}
			require.NotNil(t, collection)
			err = collection.Update(context.Background(), tt.updateOptions...)
			require.NoError(t, err)
		})
	}
}

func TestCollectionUpsert(t *testing.T) {
	var tests = []struct {
		name                 string
		serverSideValidation func(resp string)
		updateOptions        []CollectionAddOption
		limits               string
	}{
		{
			name: "with IDs and docs",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
			},
			updateOptions: []CollectionAddOption{
				WithIDs("1", "2", "3"),
				WithTexts("doc1", "doc2", "doc3"),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with IDs and docs and embeddings",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
				require.Equal(t, [][]float64{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}, {7.0, 8.0, 9.0}}, req.Embeddings)
			},
			updateOptions: []CollectionAddOption{
				WithIDs("1", "2", "3"),
				WithTexts("doc1", "doc2", "doc3"),
				WithEmbeddings(
					embeddings.NewEmbeddingFromFloat32([]float32{1.0, 2.0, 3.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{4.0, 5.0, 6.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{7.0, 8.0, 9.0}),
				),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with IDs and docs and embeddings and metadatas",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Documents)
				require.Equal(t, [][]float64{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}, {7.0, 8.0, 9.0}}, req.Embeddings)
				fmt.Println(req.Metadatas)
				require.Equal(t, []map[string]any{
					{"metadata1": "metadata1", "metadata2": float64(2), "metadata3": true}, // ints if not handled will arrive as float64
					{"metadata1": "metadata1", "metadata2": float64(3), "metadata3": true},
					{"metadata1": "metadata1", "metadata2": float64(4), "metadata3": true},
				}, req.Metadatas)
			},
			updateOptions: []CollectionAddOption{
				WithIDs("1", "2", "3"),
				WithTexts("doc1", "doc2", "doc3"),
				WithEmbeddings(
					embeddings.NewEmbeddingFromFloat32([]float32{1.0, 2.0, 3.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{4.0, 5.0, 6.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{7.0, 8.0, 9.0}),
				),
				WithMetadatas(
					NewDocumentMetadata(NewStringAttribute("metadata1", "metadata1"), NewIntAttribute("metadata2", 2), NewBoolAttribute("metadata3", true)),
					NewDocumentMetadata(NewStringAttribute("metadata1", "metadata1"), NewIntAttribute("metadata2", 3), NewBoolAttribute("metadata3", true)),
					NewDocumentMetadata(NewStringAttribute("metadata1", "metadata1"), NewIntAttribute("metadata2", 4), NewBoolAttribute("metadata3", true)),
				),
			},
			limits: `{"max_batch_size":100}`,
		},
	}

	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+/upsert`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
				respBody, readErr := chhttp.ReadRespBody(r.Body)
				require.NoError(t, readErr)
				t.Logf("Body: %s", respBody)
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
					w.WriteHeader(http.StatusOK)
					_, err := w.Write([]byte(tt.limits))
					require.NoError(t, err)
				case rx1.MatchString(r.URL.Path):
					w.WriteHeader(http.StatusOK)
					tt.serverSideValidation(respBody)
					_, err := w.Write([]byte(`true`))
					require.NoError(t, err)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
			require.NoError(t, err)
			collection := &CollectionImpl{
				name:              "test",
				id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
				tenant:            NewDefaultTenant(),
				database:          NewDefaultDatabase(),
				metadata:          NewMetadata(),
				client:            client.(*APIClientV2),
				embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
			}
			require.NotNil(t, collection)
			err = collection.Upsert(context.Background(), tt.updateOptions...)
			require.NoError(t, err)
		})
	}
}

func TestCollectionDelete(t *testing.T) {
	var tests = []struct {
		name                 string
		serverSideValidation func(resp string)
		deleteOptions        []CollectionDeleteOption
		limits               string
	}{
		{
			name: "with IDs",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
			},
			deleteOptions: []CollectionDeleteOption{
				WithIDs("1", "2", "3"),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with where",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, map[string]any{"test": map[string]any{"$eq": "test"}}, req.Where)
			},
			deleteOptions: []CollectionDeleteOption{
				WithWhere(EqString(K("test"), "test")),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with where document",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, map[string]any{"$contains": "test"}, req.WhereDoc)
			},
			deleteOptions: []CollectionDeleteOption{
				WithWhereDocument(Contains("test")),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with where document and where",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, map[string]any{"test": map[string]any{"$eq": "test"}}, req.Where)
				require.Equal(t, map[string]any{"$contains": "test"}, req.WhereDoc)
			},
			deleteOptions: []CollectionDeleteOption{
				WithWhere(EqString(K("test"), "test")),
				WithWhereDocument(Contains("test")),
			},
			limits: `{"max_batch_size":100}`,
		},
	}

	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+/delete`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
				respBody, readErr := chhttp.ReadRespBody(r.Body)
				require.NoError(t, readErr)
				t.Logf("Body: %s", respBody)
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
					w.WriteHeader(http.StatusOK)
					_, err := w.Write([]byte(tt.limits))
					require.NoError(t, err)
				case rx1.MatchString(r.URL.Path):
					w.WriteHeader(http.StatusOK)
					tt.serverSideValidation(respBody)
					_, err := w.Write([]byte(`true`))
					require.NoError(t, err)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
			require.NoError(t, err)
			collection := &CollectionImpl{
				name:              "test",
				id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
				tenant:            NewDefaultTenant(),
				database:          NewDefaultDatabase(),
				metadata:          NewMetadata(),
				client:            client.(*APIClientV2),
				embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
			}
			require.NotNil(t, collection)
			err = collection.Delete(context.Background(), tt.deleteOptions...)
			require.NoError(t, err)
		})
	}
}

func TestCollectionCount(t *testing.T) {
	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+/count`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		respBody, readErr := chhttp.ReadRespBody(r.Body)
		require.NoError(t, readErr)
		t.Logf("Body: %s", respBody)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"max_batch_size":100}`))
			require.NoError(t, err)
		case r.Method == http.MethodGet && rx1.MatchString(r.URL.Path):
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`100`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
	require.NoError(t, err)
	collection := &CollectionImpl{
		name:              "test",
		id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:            NewDefaultTenant(),
		database:          NewDefaultDatabase(),
		metadata:          NewMetadata(),
		client:            client.(*APIClientV2),
		embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
	}
	require.NotNil(t, collection)
	r, err := collection.Count(context.Background())
	require.NoError(t, err)
	require.Equal(t, 100, r)
}

func TestCollectionIndexingStatus(t *testing.T) {
	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+/indexing_status`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		switch {
		case r.Method == http.MethodGet && rx1.MatchString(r.URL.Path):
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"num_indexed_ops":100,"num_unindexed_ops":10,"total_ops":110,"op_indexing_progress":0.909}`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewHTTPClient(WithBaseURL(server.URL))
	require.NoError(t, err)
	collection := &CollectionImpl{
		name:     "test",
		id:       "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:   NewDefaultTenant(),
		database: NewDefaultDatabase(),
		client:   client.(*APIClientV2),
	}
	status, err := collection.IndexingStatus(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(100), status.NumIndexedOps)
	require.Equal(t, uint64(10), status.NumUnindexedOps)
	require.Equal(t, uint64(110), status.TotalOps)
	require.InDelta(t, 0.909, status.OpIndexingProgress, 0.001)
}

func TestCollectionQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pathMatch := regexp.MustCompile("/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+/query")
		t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"max_batch_size":100}`))
			require.NoError(t, err)
		case r.Method == http.MethodPost && pathMatch.MatchString(r.URL.Path):
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{
  "distances": [
    [
      0.1
    ]
  ],
  "documents": [
    [
      "string"
    ]
  ],
  "embeddings": [
    [
      [
        0.1
      ]
    ]
  ],
  "ids": [
    [
      "id1"
    ]
  ],
  "include": [
    "distances"
  ],
  "metadatas": [
    [
      {
        "additionalProp1": true,
        "additionalProp2": true,
        "additionalProp3": true
      }
    ]
  ]
}`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
	require.NoError(t, err)

	collection := &CollectionImpl{
		name:              "test",
		id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:            NewDefaultTenant(),
		database:          NewDefaultDatabase(),
		metadata:          NewMetadata(),
		client:            client.(*APIClientV2),
		embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
	}

	require.NotNil(t, collection)
	r, err := collection.Query(context.Background(), WithQueryTexts("doc1", "doc2", "doc3"), WithWhere(Or(EqString(K("test"), "test"))))
	require.NoError(t, err)
	require.NotNil(t, r)
}

func TestCollectionModifyName(t *testing.T) {
	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		respBody, readErr := chhttp.ReadRespBody(r.Body)
		require.NoError(t, readErr)
		t.Logf("Body: %s", respBody)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"max_batch_size":100}`))
			require.NoError(t, err)
		case r.Method == http.MethodPut && rx1.MatchString(r.URL.Path):
			w.WriteHeader(http.StatusOK)
			require.Equal(t, `{"new_name":"test2"}`, respBody)
			_, err := w.Write([]byte(`true`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
	require.NoError(t, err)
	collection := &CollectionImpl{
		name:              "test",
		id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:            NewDefaultTenant(),
		database:          NewDefaultDatabase(),
		metadata:          NewMetadata(),
		client:            client.(*APIClientV2),
		embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
	}

	require.NotNil(t, collection)
	err = collection.ModifyName(context.Background(), "test2")
	require.NoError(t, err)
}

func TestCollectionModifyMetadata(t *testing.T) {
	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		respBody, readErr := chhttp.ReadRespBody(r.Body)
		require.NoError(t, readErr)
		t.Logf("Body: %s", respBody)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"max_batch_size":100}`))
			require.NoError(t, err)
		case r.Method == http.MethodPut && rx1.MatchString(r.URL.Path):
			w.WriteHeader(http.StatusOK)
			require.Equal(t, `{"new_metadata":{"test":"test"}}`, respBody)
			_, err := w.Write([]byte(`true`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
	require.NoError(t, err)
	collection := &CollectionImpl{
		name:              "test",
		id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:            NewDefaultTenant(),
		database:          NewDefaultDatabase(),
		metadata:          NewMetadata(),
		client:            client.(*APIClientV2),
		embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
	}

	require.NotNil(t, collection)
	err = collection.ModifyMetadata(context.Background(), NewMetadataFromMap(map[string]any{"test": "test"}))
	require.NoError(t, err)
}

func TestCollectionModifyConfiguration(t *testing.T) {
	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		respBody, readErr := chhttp.ReadRespBody(r.Body)
		require.NoError(t, readErr)
		t.Logf("Body: %s", respBody)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"max_batch_size":100}`))
			require.NoError(t, err)
		case r.Method == http.MethodPut && rx1.MatchString(r.URL.Path):
			w.WriteHeader(http.StatusOK)
			require.Equal(t, `{"new_configuration":{"hnsw":{"ef_search":200}}}`, respBody)
			_, err := w.Write([]byte(`true`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
	require.NoError(t, err)
	collection := &CollectionImpl{
		name:              "test",
		id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:            NewDefaultTenant(),
		database:          NewDefaultDatabase(),
		metadata:          NewMetadata(),
		client:            client.(*APIClientV2),
		embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
	}

	require.NotNil(t, collection)
	cfg := NewUpdateCollectionConfiguration(WithHNSWEfSearchModify(200))
	err = collection.ModifyConfiguration(context.Background(), cfg)
	require.NoError(t, err)
}

func TestCollectionModifyConfiguration_NilConfig(t *testing.T) {
	collection := &CollectionImpl{
		name:     "test",
		id:       "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:   NewDefaultTenant(),
		database: NewDefaultDatabase(),
	}
	err := collection.ModifyConfiguration(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "newConfig cannot be nil")
}

func TestCollectionModifyConfiguration_EmptyConfig(t *testing.T) {
	collection := &CollectionImpl{
		name:     "test",
		id:       "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:   NewDefaultTenant(),
		database: NewDefaultDatabase(),
	}
	cfg := NewUpdateCollectionConfiguration()
	err := collection.ModifyConfiguration(context.Background(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least one parameter")
}

func TestCollectionModifyConfiguration_MutualExclusivity(t *testing.T) {
	collection := &CollectionImpl{
		name:     "test",
		id:       "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:   NewDefaultTenant(),
		database: NewDefaultDatabase(),
	}
	cfg := NewUpdateCollectionConfiguration(
		WithHNSWEfSearchModify(200),
		WithSpannEfSearchModify(64),
	)
	err := collection.ModifyConfiguration(context.Background(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot update both")
}

func TestCollectionModifyConfiguration_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"max_batch_size":100}`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, err := w.Write([]byte(`{"error":"invalid configuration parameter"}`))
			require.NoError(t, err)
		}
	}))
	defer server.Close()
	client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
	require.NoError(t, err)
	collection := &CollectionImpl{
		name:              "test",
		id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:            NewDefaultTenant(),
		database:          NewDefaultDatabase(),
		metadata:          NewMetadata(),
		client:            client.(*APIClientV2),
		embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
	}
	cfg := NewUpdateCollectionConfiguration(WithHNSWEfSearchModify(200))
	err = collection.ModifyConfiguration(context.Background(), cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error modifying collection configuration")
}

func TestCollectionModifyConfiguration_SpannWireFormat(t *testing.T) {
	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respBody, readErr := chhttp.ReadRespBody(r.Body)
		require.NoError(t, readErr)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"max_batch_size":100}`))
			require.NoError(t, err)
		case r.Method == http.MethodPut && rx1.MatchString(r.URL.Path):
			w.WriteHeader(http.StatusOK)
			require.Equal(t, `{"new_configuration":{"spann":{"search_nprobe":32,"ef_search":64}}}`, respBody)
			_, err := w.Write([]byte(`true`))
			require.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
	require.NoError(t, err)
	collection := &CollectionImpl{
		name:              "test",
		id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
		tenant:            NewDefaultTenant(),
		database:          NewDefaultDatabase(),
		metadata:          NewMetadata(),
		client:            client.(*APIClientV2),
		embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
	}
	cfg := NewUpdateCollectionConfiguration(
		WithSpannSearchNprobeModify(32),
		WithSpannEfSearchModify(64),
	)
	err = collection.ModifyConfiguration(context.Background(), cfg)
	require.NoError(t, err)
}

func TestCollectionGet(t *testing.T) {
	var tests = []struct {
		name                 string
		serverSideValidation func(resp string)
		getOptions           []CollectionGetOption
		limits               string
	}{
		{
			name: "with IDs",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
			},
			getOptions: []CollectionGetOption{
				WithIDs("1", "2", "3"),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with IDs and include",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, []string{"documents"}, req.Include)
			},
			getOptions: []CollectionGetOption{
				WithIDs("1", "2", "3"),
				WithInclude(IncludeDocuments),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with IDs and include and limit",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, 10, req.Limit)
			},
			getOptions: []CollectionGetOption{
				WithIDs("1", "2", "3"),
				WithLimit(10),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with IDs and include and limit and offset",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, []string{"1", "2", "3"}, req.IDs)
				require.Equal(t, 10, req.Limit)
				require.Equal(t, 5, req.Offset)
			},
			getOptions: []CollectionGetOption{
				WithIDs("1", "2", "3"),
				WithLimit(10),
				WithOffset(5),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with where",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, map[string]any{"test": map[string]any{"$eq": "test"}}, req.Where)
			},
			getOptions: []CollectionGetOption{
				WithWhere(EqString(K("test"), "test")),
			},
			limits: `{"max_batch_size":100}`,
		},
		{
			name: "with where document",
			serverSideValidation: func(resp string) {
				var req ChromaCollectionUpdateRequest
				err := json.Unmarshal([]byte(resp), &req)
				require.NoError(t, err)
				require.Equal(t, map[string]any{"$contains": "test"}, req.WhereDoc)
			},
			getOptions: []CollectionGetOption{
				WithWhereDocument(Contains("test")),
			},
			limits: `{"max_batch_size":100}`,
		},
	}
	rx1 := regexp.MustCompile(`/api/v2/tenants/[^/]+/databases/[^/]+/collections/[^/]+/get`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("Request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
				respBody, readErr := chhttp.ReadRespBody(r.Body)
				require.NoError(t, readErr)
				t.Logf("Body: %s", respBody)
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v2/pre-flight-checks":
					w.WriteHeader(http.StatusOK)
					_, err := w.Write([]byte(tt.limits))
					require.NoError(t, err)
				case r.Method == http.MethodPost && rx1.MatchString(r.URL.Path):
					w.WriteHeader(http.StatusOK)
					tt.serverSideValidation(respBody)
					_, err := w.Write([]byte(`{
  "documents": [
    "document1",
	"document2"
  ],
  "embeddings": [
    [0.1,0.2],
	[0.3,0.4]
  ],
  "ids": [
    "id1",
	"id2"
  ],
  "include": [
    "distances"
  ],
  "metadatas": [
    {
      "additionalProp1": true,
      "additionalProp2": 1,
      "additionalProp3": "test"
    },
	{"additionalProp1": false}
  ]
}`))
					require.NoError(t, err)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithLogger(testLogger()))
			require.NoError(t, err)
			collection := &CollectionImpl{
				name:              "test",
				id:                "8ecf0f7e-e806-47f8-96a1-4732ef42359e",
				tenant:            NewDefaultTenant(),
				database:          NewDefaultDatabase(),
				metadata:          NewMetadata(),
				client:            client.(*APIClientV2),
				embeddingFunction: embeddings.NewConsistentHashEmbeddingFunction(),
			}
			require.NotNil(t, collection)
			r, err := collection.Get(context.Background(), tt.getOptions...)
			require.NoError(t, err)
			require.NotNil(t, r)
		})
	}
}

func TestCloneRank(t *testing.T) {
	t.Run("nil rank returns nil", func(t *testing.T) {
		require.Nil(t, cloneRank(nil))
	})

	t.Run("KnnRank clone is independent", func(t *testing.T) {
		original, err := NewKnnRank(KnnQueryText("test query"), WithKnnLimit(50))
		require.NoError(t, err)

		cloned := cloneRank(original).(*KnnRank)

		require.NotSame(t, original, cloned)
		require.Equal(t, original.Query, cloned.Query)
		require.Equal(t, original.Limit, cloned.Limit)

		// Mutate clone, verify original unchanged
		cloned.Query = []float32{1.0, 2.0, 3.0}
		cloned.Limit = 100

		require.Equal(t, "test query", original.Query)
		require.Equal(t, 50, original.Limit)
	})

	t.Run("nested rank tree is deep cloned", func(t *testing.T) {
		knn1, err := NewKnnRank(KnnQueryText("query1"), WithKnnReturnRank())
		require.NoError(t, err)
		knn2, err := NewKnnRank(KnnQueryText("query2"), WithKnnReturnRank())
		require.NoError(t, err)

		rrf, err := NewRrfRank(WithRffRanks(
			knn1.WithWeight(0.5),
			knn2.WithWeight(0.5),
		))
		require.NoError(t, err)

		cloned := cloneRank(rrf).(*RrfRank)

		require.NotSame(t, rrf, cloned)
		require.NotSame(t, rrf.Ranks[0].Rank, cloned.Ranks[0].Rank)
		require.NotSame(t, rrf.Ranks[1].Rank, cloned.Ranks[1].Rank)

		// Mutate nested clone
		clonedKnn := cloned.Ranks[0].Rank.(*KnnRank)
		clonedKnn.Query = []float32{1.0, 2.0}

		// Original unchanged
		require.Equal(t, "query1", knn1.Query)
	})

	t.Run("arithmetic expression tree is deep cloned", func(t *testing.T) {
		knn, err := NewKnnRank(KnnQueryText("test"))
		require.NoError(t, err)
		expr := knn.Multiply(FloatOperand(0.5)).Add(Val(1.0))

		cloned := cloneRank(expr)

		require.NotSame(t, expr, cloned)

		// Verify structure preserved via JSON
		origJSON, err := expr.MarshalJSON()
		require.NoError(t, err)
		clonedJSON, err := cloned.MarshalJSON()
		require.NoError(t, err)
		require.Equal(t, string(origJSON), string(clonedJSON))
	})
}
