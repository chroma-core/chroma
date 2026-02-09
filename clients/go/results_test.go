//go:build !cloud

package chroma

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

func TestGetResultDeserialization(t *testing.T) {
	var apiResponse = `{
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
}`

	var result GetResultImpl
	err := json.Unmarshal([]byte(apiResponse), &result)
	require.NoError(t, err)
	require.Len(t, result.GetDocuments(), 2)
	require.Len(t, result.GetIDs(), 2)
	require.Equal(t, result.GetIDs()[0], DocumentID("id1"))
	require.Equal(t, result.GetDocuments()[0], NewTextDocument("document1"))
	require.Equal(t, []float32{0.1, 0.2}, result.GetEmbeddings()[0].ContentAsFloat32())
	require.Len(t, result.GetEmbeddings(), 2)
	require.Len(t, result.GetMetadatas(), 2)
}

func TestQueryResultDeserialization(t *testing.T) {
	var apiResponse = `{
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
}`

	var result QueryResultImpl
	err := json.Unmarshal([]byte(apiResponse), &result)
	require.NoError(t, err)
	require.Len(t, result.GetIDGroups(), 1)
	require.Len(t, result.GetIDGroups()[0], 1)
	require.Equal(t, DocumentID("id1"), result.GetIDGroups()[0][0])

	require.Len(t, result.GetDocumentsGroups(), 1)
	require.Len(t, result.GetDocumentsGroups()[0], 1)
	require.Equal(t, NewTextDocument("string"), result.GetDocumentsGroups()[0][0])

	require.Len(t, result.GetEmbeddingsGroups(), 1)
	require.Len(t, result.GetEmbeddingsGroups()[0], 1)
	require.Equal(t, []float32{0.1}, result.GetEmbeddingsGroups()[0][0].ContentAsFloat32())

	require.Len(t, result.GetMetadatasGroups(), 1)
	require.Len(t, result.GetMetadatasGroups()[0], 1)
	metadata := NewDocumentMetadata(
		NewBoolAttribute("additionalProp1", true),
		NewBoolAttribute("additionalProp3", true),
		NewBoolAttribute("additionalProp2", true),
	)
	require.Equal(t, metadata, result.GetMetadatasGroups()[0][0])

	require.Len(t, result.GetDistancesGroups(), 1)
	require.Len(t, result.GetDistancesGroups()[0], 1)
	require.Equal(t, embeddings.Distance(0.1), result.GetDistancesGroups()[0][0])
}

func TestQueryResultDeserializationMultiGroup(t *testing.T) {
	var apiResponse = `{
  "distances": [
    [0.1, 0.2],
    [0.3, 0.4]
  ],
  "documents": [
    ["doc1", "doc2"],
    ["doc3", "doc4"]
  ],
  "embeddings": [
    [[0.1, 0.2], [0.3, 0.4]],
    [[0.5, 0.6], [0.7, 0.8]]
  ],
  "ids": [
    ["id1", "id2"],
    ["id3", "id4"]
  ],
  "include": ["distances", "documents", "embeddings", "metadatas"],
  "metadatas": [
    [{"key": "val1"}, {"key": "val2"}],
    [{"key": "val3"}, {"key": "val4"}]
  ]
}`

	var result QueryResultImpl
	err := json.Unmarshal([]byte(apiResponse), &result)
	require.NoError(t, err)

	require.Len(t, result.GetIDGroups(), 2)
	require.Len(t, result.GetIDGroups()[0], 2)
	require.Len(t, result.GetIDGroups()[1], 2)
	require.Equal(t, DocumentID("id1"), result.GetIDGroups()[0][0])
	require.Equal(t, DocumentID("id2"), result.GetIDGroups()[0][1])
	require.Equal(t, DocumentID("id3"), result.GetIDGroups()[1][0])
	require.Equal(t, DocumentID("id4"), result.GetIDGroups()[1][1])

	require.Len(t, result.GetDocumentsGroups(), 2)
	require.Len(t, result.GetDocumentsGroups()[0], 2)
	require.Len(t, result.GetDocumentsGroups()[1], 2)
	require.Equal(t, NewTextDocument("doc1"), result.GetDocumentsGroups()[0][0])
	require.Equal(t, NewTextDocument("doc2"), result.GetDocumentsGroups()[0][1])
	require.Equal(t, NewTextDocument("doc3"), result.GetDocumentsGroups()[1][0])
	require.Equal(t, NewTextDocument("doc4"), result.GetDocumentsGroups()[1][1])

	require.Len(t, result.GetDistancesGroups(), 2)
	require.Len(t, result.GetDistancesGroups()[0], 2)
	require.Len(t, result.GetDistancesGroups()[1], 2)
	require.Equal(t, embeddings.Distance(0.1), result.GetDistancesGroups()[0][0])
	require.Equal(t, embeddings.Distance(0.4), result.GetDistancesGroups()[1][1])

	require.Len(t, result.GetEmbeddingsGroups(), 2)
	require.Len(t, result.GetEmbeddingsGroups()[0], 2)
	require.Len(t, result.GetEmbeddingsGroups()[1], 2)
	require.Equal(t, []float32{0.1, 0.2}, result.GetEmbeddingsGroups()[0][0].ContentAsFloat32())
	require.Equal(t, []float32{0.7, 0.8}, result.GetEmbeddingsGroups()[1][1].ContentAsFloat32())

	require.Len(t, result.GetMetadatasGroups(), 2)
	require.Len(t, result.GetMetadatasGroups()[0], 2)
	require.Len(t, result.GetMetadatasGroups()[1], 2)
	val1, ok := result.GetMetadatasGroups()[0][0].GetString("key")
	require.True(t, ok)
	require.Equal(t, "val1", val1)
	val4, ok := result.GetMetadatasGroups()[1][1].GetString("key")
	require.True(t, ok)
	require.Equal(t, "val4", val4)
}

func TestGetResultImpl_Rows(t *testing.T) {
	result := &GetResultImpl{
		Ids:       []DocumentID{"id1", "id2", "id3"},
		Documents: Documents{NewTextDocument("doc1"), NewTextDocument("doc2"), NewTextDocument("doc3")},
		Metadatas: DocumentMetadatas{
			NewDocumentMetadata(NewStringAttribute("key", "val1")),
			NewDocumentMetadata(NewStringAttribute("key", "val2")),
			nil,
		},
		Embeddings: embeddings.Embeddings{
			embeddings.NewEmbeddingFromFloat32([]float32{0.1, 0.2}),
			embeddings.NewEmbeddingFromFloat32([]float32{0.3, 0.4}),
			embeddings.NewEmbeddingFromFloat32([]float32{0.5, 0.6}),
		},
	}

	rows := result.Rows()
	require.Len(t, rows, 3)

	require.Equal(t, DocumentID("id1"), rows[0].ID)
	require.Equal(t, "doc1", rows[0].Document)
	require.Equal(t, []float32{0.1, 0.2}, rows[0].Embedding)
	val, ok := rows[0].Metadata.GetString("key")
	require.True(t, ok)
	require.Equal(t, "val1", val)
	require.Equal(t, float64(0), rows[0].Score)

	require.Equal(t, DocumentID("id2"), rows[1].ID)
	require.Equal(t, DocumentID("id3"), rows[2].ID)
	require.Nil(t, rows[2].Metadata)
}

func TestGetResultImpl_Rows_Empty(t *testing.T) {
	result := &GetResultImpl{}
	rows := result.Rows()
	require.Nil(t, rows)
}

func TestGetResultImpl_At(t *testing.T) {
	result := &GetResultImpl{
		Ids:       []DocumentID{"id1", "id2"},
		Documents: Documents{NewTextDocument("doc1"), NewTextDocument("doc2")},
	}

	row, ok := result.At(0)
	require.True(t, ok)
	require.Equal(t, DocumentID("id1"), row.ID)
	require.Equal(t, "doc1", row.Document)

	row, ok = result.At(1)
	require.True(t, ok)
	require.Equal(t, DocumentID("id2"), row.ID)

	_, ok = result.At(-1)
	require.False(t, ok)

	_, ok = result.At(2)
	require.False(t, ok)
}

func TestQueryResultImpl_Rows(t *testing.T) {
	result := &QueryResultImpl{
		IDLists:        []DocumentIDs{{"id1", "id2"}, {"id3"}},
		DocumentsLists: []Documents{{NewTextDocument("doc1"), NewTextDocument("doc2")}, {NewTextDocument("doc3")}},
		DistancesLists: []embeddings.Distances{{0.1, 0.2}, {0.3}},
	}

	rows := result.Rows()
	require.Len(t, rows, 2)
	require.Equal(t, DocumentID("id1"), rows[0].ID)
	require.Equal(t, "doc1", rows[0].Document)
	require.InDelta(t, 0.1, rows[0].Score, 0.0001)
	require.Equal(t, DocumentID("id2"), rows[1].ID)
	require.InDelta(t, 0.2, rows[1].Score, 0.0001)
}

func TestQueryResultImpl_RowGroups(t *testing.T) {
	result := &QueryResultImpl{
		IDLists:        []DocumentIDs{{"id1", "id2"}, {"id3"}},
		DocumentsLists: []Documents{{NewTextDocument("doc1"), NewTextDocument("doc2")}, {NewTextDocument("doc3")}},
		DistancesLists: []embeddings.Distances{{0.1, 0.2}, {0.3}},
	}

	groups := result.RowGroups()
	require.Len(t, groups, 2)
	require.Len(t, groups[0], 2)
	require.Len(t, groups[1], 1)

	require.Equal(t, DocumentID("id1"), groups[0][0].ID)
	require.Equal(t, DocumentID("id2"), groups[0][1].ID)
	require.Equal(t, DocumentID("id3"), groups[1][0].ID)
	require.InDelta(t, 0.3, groups[1][0].Score, 0.0001)
}

func TestQueryResultImpl_At(t *testing.T) {
	result := &QueryResultImpl{
		IDLists:        []DocumentIDs{{"id1", "id2"}, {"id3"}},
		DocumentsLists: []Documents{{NewTextDocument("doc1"), NewTextDocument("doc2")}, {NewTextDocument("doc3")}},
	}

	row, ok := result.At(0, 0)
	require.True(t, ok)
	require.Equal(t, DocumentID("id1"), row.ID)

	row, ok = result.At(0, 1)
	require.True(t, ok)
	require.Equal(t, DocumentID("id2"), row.ID)

	row, ok = result.At(1, 0)
	require.True(t, ok)
	require.Equal(t, DocumentID("id3"), row.ID)

	_, ok = result.At(-1, 0)
	require.False(t, ok)

	_, ok = result.At(0, -1)
	require.False(t, ok)

	_, ok = result.At(2, 0)
	require.False(t, ok)

	_, ok = result.At(0, 2)
	require.False(t, ok)
}
