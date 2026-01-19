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
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
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
				WithIDsUpdate("1", "2", "3"),
				WithTextsUpdate("doc1", "doc2", "doc3"),
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
				WithIDsUpdate("1", "2", "3"),
				WithTextsUpdate("doc1", "doc2", "doc3"),
				WithEmbeddingsUpdate(
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
				WithIDsUpdate("1", "2", "3"),
				WithTextsUpdate("doc1", "doc2", "doc3"),
				WithEmbeddingsUpdate(
					embeddings.NewEmbeddingFromFloat32([]float32{1.0, 2.0, 3.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{4.0, 5.0, 6.0}),
					embeddings.NewEmbeddingFromFloat32([]float32{7.0, 8.0, 9.0}),
				),
				WithMetadatasUpdate(
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
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
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
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
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
				WithIDsDelete("1", "2", "3"),
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
				WithWhereDelete(EqString(K("test"), "test")),
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
				WithWhereDocumentDelete(Contains("test")),
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
				WithWhereDelete(EqString(K("test"), "test")),
				WithWhereDocumentDelete(Contains("test")),
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
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
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
	client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
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
	require.Equal(t, 100, status.NumIndexedOps)
	require.Equal(t, 10, status.NumUnindexedOps)
	require.Equal(t, 110, status.TotalOps)
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

	client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
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
	r, err := collection.Query(context.Background(), WithQueryTexts("doc1", "doc2", "doc3"), WithWhereQuery(Or(EqString(K("test"), "test"))))
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
	client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
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
	client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
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
	t.Skip("not implemented")
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
				WithIDsGet("1", "2", "3"),
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
				WithIDsGet("1", "2", "3"),
				WithIncludeGet(IncludeDocuments),
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
				WithIDsGet("1", "2", "3"),
				WithLimitGet(10),
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
				WithIDsGet("1", "2", "3"),
				WithLimitGet(10),
				WithOffsetGet(5),
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
				WithWhereGet(EqString(K("test"), "test")),
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
				WithWhereDocumentGet(Contains("test")),
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
			client, err := NewHTTPClient(WithBaseURL(server.URL), WithDebug())
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
