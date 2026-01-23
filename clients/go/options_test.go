package chroma

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

func TestWithIDsGet(t *testing.T) {
	opt := WithIDs("id1", "id2", "id3")

	op := &CollectionGetOp{}
	err := opt.ApplyToGet(op)
	require.NoError(t, err)
	require.Equal(t, []DocumentID{"id1", "id2", "id3"}, op.Ids)
}

func TestWithIDsQuery(t *testing.T) {
	opt := WithIDs("id1", "id2")

	op := &CollectionQueryOp{}
	err := opt.ApplyToQuery(op)
	require.NoError(t, err)
	require.Equal(t, []DocumentID{"id1", "id2"}, op.Ids)
}

func TestWithIDsDelete(t *testing.T) {
	opt := WithIDs("id1")

	op := &CollectionDeleteOp{}
	err := opt.ApplyToDelete(op)
	require.NoError(t, err)
	require.Equal(t, []DocumentID{"id1"}, op.Ids)
}

func TestWithIDsAdd(t *testing.T) {
	opt := WithIDs("id1", "id2")

	op := &CollectionAddOp{}
	err := opt.ApplyToAdd(op)
	require.NoError(t, err)
	require.Equal(t, []DocumentID{"id1", "id2"}, op.Ids)
}

func TestWithIDsUpdate(t *testing.T) {
	opt := WithIDs("id1", "id2", "id3", "id4")

	op := &CollectionUpdateOp{}
	err := opt.ApplyToUpdate(op)
	require.NoError(t, err)
	require.Equal(t, []DocumentID{"id1", "id2", "id3", "id4"}, op.Ids)
}

func TestWithIDsSearch(t *testing.T) {
	opt := WithIDs("id1", "id2")

	req := &SearchRequest{}
	err := opt.ApplyToSearchRequest(req)
	require.NoError(t, err)
	require.NotNil(t, req.Filter)
	require.Equal(t, []DocumentID{"id1", "id2"}, req.Filter.IDs)
}

func TestWithIDsAppends(t *testing.T) {
	opt1 := WithIDs("id1", "id2")
	opt2 := WithIDs("id3", "id4")

	op := &CollectionGetOp{}
	require.NoError(t, opt1.ApplyToGet(op))
	require.NoError(t, opt2.ApplyToGet(op))

	require.Equal(t, []DocumentID{"id1", "id2", "id3", "id4"}, op.Ids)
}

func TestWithWhereGet(t *testing.T) {
	filter := EqString("status", "active")
	opt := WithWhere(filter)

	op := &CollectionGetOp{}
	err := opt.ApplyToGet(op)
	require.NoError(t, err)
	require.Equal(t, filter, op.Where)
}

func TestWithWhereQuery(t *testing.T) {
	filter := GtInt("count", 10)
	opt := WithWhere(filter)

	op := &CollectionQueryOp{}
	err := opt.ApplyToQuery(op)
	require.NoError(t, err)
	require.Equal(t, filter, op.Where)
}

func TestWithWhereDelete(t *testing.T) {
	filter := EqString("status", "deleted")
	opt := WithWhere(filter)

	op := &CollectionDeleteOp{}
	err := opt.ApplyToDelete(op)
	require.NoError(t, err)
	require.Equal(t, filter, op.Where)
}

func TestWithWhereDocumentGet(t *testing.T) {
	filter := Contains("machine learning")
	opt := WithWhereDocument(filter)

	op := &CollectionGetOp{}
	err := opt.ApplyToGet(op)
	require.NoError(t, err)
	require.Equal(t, filter, op.WhereDocument)
}

func TestWithWhereDocumentQuery(t *testing.T) {
	filter := NotContains("deprecated")
	opt := WithWhereDocument(filter)

	op := &CollectionQueryOp{}
	err := opt.ApplyToQuery(op)
	require.NoError(t, err)
	require.Equal(t, filter, op.WhereDocument)
}

func TestWithWhereDocumentDelete(t *testing.T) {
	filter := Contains("old data")
	opt := WithWhereDocument(filter)

	op := &CollectionDeleteOp{}
	err := opt.ApplyToDelete(op)
	require.NoError(t, err)
	require.Equal(t, filter, op.WhereDocument)
}

func TestWithFilterSearch(t *testing.T) {
	filter := EqString(K("status"), "published")
	opt := WithFilter(filter)

	req := &SearchRequest{}
	err := opt.ApplyToSearchRequest(req)
	require.NoError(t, err)
	require.NotNil(t, req.Filter)
	require.Equal(t, filter, req.Filter.Where)
}

func TestCombinedOptionsGet(t *testing.T) {
	op := &CollectionGetOp{}
	require.NoError(t, WithIDs("id1", "id2").ApplyToGet(op))
	require.NoError(t, WithWhere(EqString("status", "active")).ApplyToGet(op))
	require.NoError(t, WithWhereDocument(Contains("test")).ApplyToGet(op))

	require.Equal(t, []DocumentID{"id1", "id2"}, op.Ids)
	require.NotNil(t, op.Where)
	require.NotNil(t, op.WhereDocument)
}

func TestCombinedOptionsQuery(t *testing.T) {
	op := &CollectionQueryOp{}
	require.NoError(t, WithIDs("id1").ApplyToQuery(op))
	require.NoError(t, WithWhere(GtFloat("score", 0.5)).ApplyToQuery(op))

	require.Equal(t, []DocumentID{"id1"}, op.Ids)
	require.NotNil(t, op.Where)
}

func TestCombinedOptionsSearch(t *testing.T) {
	req := &SearchRequest{}
	require.NoError(t, WithIDs("id1", "id2").ApplyToSearchRequest(req))
	require.NoError(t, WithFilter(EqString(K("category"), "tech")).ApplyToSearchRequest(req))

	require.NotNil(t, req.Filter)
	require.Equal(t, []DocumentID{"id1", "id2"}, req.Filter.IDs)
	require.NotNil(t, req.Filter.Where)
}

func TestUnifiedOptionsInNewCollectionGetOp(t *testing.T) {
	op, err := NewCollectionGetOp(
		WithIDs("id1", "id2"),
		WithWhere(EqString("status", "active")),
		WithWhereDocument(Contains("test")),
		WithInclude(IncludeDocuments, IncludeMetadatas),
		WithLimit(10),
		WithOffset(5),
	)
	require.NoError(t, err)
	require.Equal(t, []DocumentID{"id1", "id2"}, op.Ids)
	require.NotNil(t, op.Where)
	require.NotNil(t, op.WhereDocument)
	require.Equal(t, []Include{IncludeDocuments, IncludeMetadatas}, op.Include)
	require.Equal(t, 10, op.Limit)
	require.Equal(t, 5, op.Offset)
}

func TestUnifiedOptionsInNewCollectionQueryOp(t *testing.T) {
	op, err := NewCollectionQueryOp(
		WithIDs("id1"),
		WithWhere(GtInt("count", 5)),
		WithQueryTexts("hello world"),
		WithNResults(20),
		WithInclude(IncludeEmbeddings),
	)
	require.NoError(t, err)
	require.Equal(t, []DocumentID{"id1"}, op.Ids)
	require.NotNil(t, op.Where)
	require.Equal(t, []string{"hello world"}, op.QueryTexts)
	require.Equal(t, 20, op.NResults)
	require.Equal(t, []Include{IncludeEmbeddings}, op.Include)
}

func TestUnifiedOptionsInNewCollectionDeleteOp(t *testing.T) {
	op, err := NewCollectionDeleteOp(
		WithIDs("id1", "id2"),
		WithWhere(EqString("status", "deleted")),
	)
	require.NoError(t, err)
	require.Equal(t, []DocumentID{"id1", "id2"}, op.Ids)
	require.NotNil(t, op.Where)
}

func TestUnifiedOptionsInNewCollectionAddOp(t *testing.T) {
	op, err := NewCollectionAddOp(
		WithIDs("id1", "id2"),
		WithTexts("doc1", "doc2"),
	)
	require.NoError(t, err)
	require.Equal(t, []DocumentID{"id1", "id2"}, op.Ids)
	require.Len(t, op.Documents, 2)
}

func TestEarlyValidationEmptyIDs(t *testing.T) {
	t.Run("empty IDs for Query returns error", func(t *testing.T) {
		op := &CollectionQueryOp{}
		err := WithIDs().ApplyToQuery(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one id is required")
	})

	t.Run("empty IDs for Delete returns error", func(t *testing.T) {
		op := &CollectionDeleteOp{}
		err := WithIDs().ApplyToDelete(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one id is required")
	})

	t.Run("empty IDs for Add returns error", func(t *testing.T) {
		op := &CollectionAddOp{}
		err := WithIDs().ApplyToAdd(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one id is required")
	})

	t.Run("empty IDs for Update returns error", func(t *testing.T) {
		op := &CollectionUpdateOp{}
		err := WithIDs().ApplyToUpdate(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one id is required")
	})

	t.Run("empty IDs for Get returns error", func(t *testing.T) {
		op := &CollectionGetOp{}
		err := WithIDs().ApplyToGet(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one id is required")
	})

	t.Run("empty IDs for Search returns error", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithIDs().ApplyToSearchRequest(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one id is required")
	})
}

func TestEarlyValidationInvalidWhereFilter(t *testing.T) {
	t.Run("invalid where filter for Get returns error", func(t *testing.T) {
		invalidFilter := EqString("", "value")
		op := &CollectionGetOp{}
		err := WithWhere(invalidFilter).ApplyToGet(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid key")
	})

	t.Run("invalid where filter for Query returns error", func(t *testing.T) {
		invalidFilter := EqString("", "value")
		op := &CollectionQueryOp{}
		err := WithWhere(invalidFilter).ApplyToQuery(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid key")
	})

	t.Run("invalid where filter for Delete returns error", func(t *testing.T) {
		invalidFilter := EqString("", "value")
		op := &CollectionDeleteOp{}
		err := WithWhere(invalidFilter).ApplyToDelete(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid key")
	})

	t.Run("nil where filter is allowed", func(t *testing.T) {
		op := &CollectionGetOp{}
		err := WithWhere(nil).ApplyToGet(op)
		require.NoError(t, err)
	})
}

func TestEarlyValidationInvalidWhereDocumentFilter(t *testing.T) {
	t.Run("invalid where document filter for Get returns error", func(t *testing.T) {
		invalidFilter := OrDocument() // empty Or is invalid
		op := &CollectionGetOp{}
		err := WithWhereDocument(invalidFilter).ApplyToGet(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected at least one")
	})

	t.Run("invalid where document filter for Query returns error", func(t *testing.T) {
		invalidFilter := OrDocument() // empty Or is invalid
		op := &CollectionQueryOp{}
		err := WithWhereDocument(invalidFilter).ApplyToQuery(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected at least one")
	})

	t.Run("invalid where document filter for Delete returns error", func(t *testing.T) {
		invalidFilter := OrDocument() // empty Or is invalid
		op := &CollectionDeleteOp{}
		err := WithWhereDocument(invalidFilter).ApplyToDelete(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected at least one")
	})

	t.Run("nil where document filter is allowed", func(t *testing.T) {
		op := &CollectionGetOp{}
		err := WithWhereDocument(nil).ApplyToGet(op)
		require.NoError(t, err)
	})
}

func TestEarlyValidationInvalidFilterForSearch(t *testing.T) {
	t.Run("invalid filter for Search returns error", func(t *testing.T) {
		invalidFilter := EqString(K(""), "value")
		req := &SearchRequest{}
		err := WithFilter(invalidFilter).ApplyToSearchRequest(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid key")
	})

	t.Run("nil filter is allowed for Search", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithFilter(nil).ApplyToSearchRequest(req)
		require.NoError(t, err)
	})
}

func TestWithIDsDuplicateWithinSingleCall(t *testing.T) {
	t.Run("duplicate IDs within single call for Get returns error", func(t *testing.T) {
		opt := WithIDs("id1", "id1")
		op := &CollectionGetOp{}
		err := opt.ApplyToGet(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id1")
	})

	t.Run("duplicate IDs within single call for Query returns error", func(t *testing.T) {
		opt := WithIDs("id1", "id2", "id1")
		op := &CollectionQueryOp{}
		err := opt.ApplyToQuery(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id1")
	})

	t.Run("duplicate IDs within single call for Delete returns error", func(t *testing.T) {
		opt := WithIDs("id1", "id1")
		op := &CollectionDeleteOp{}
		err := opt.ApplyToDelete(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id1")
	})

	t.Run("duplicate IDs within single call for Add returns error", func(t *testing.T) {
		opt := WithIDs("id1", "id1")
		op := &CollectionAddOp{}
		err := opt.ApplyToAdd(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id1")
	})

	t.Run("duplicate IDs within single call for Update returns error", func(t *testing.T) {
		opt := WithIDs("id1", "id1")
		op := &CollectionUpdateOp{}
		err := opt.ApplyToUpdate(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id1")
	})

	t.Run("duplicate IDs within single call for Search returns error", func(t *testing.T) {
		opt := WithIDs("id1", "id1")
		req := &SearchRequest{}
		err := opt.ApplyToSearchRequest(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id1")
	})
}

func TestWithIDsDuplicateAcrossMultipleCalls(t *testing.T) {
	t.Run("duplicate IDs across calls for Get returns error", func(t *testing.T) {
		opt1 := WithIDs("id1", "id2")
		opt2 := WithIDs("id2", "id3")

		op := &CollectionGetOp{}
		require.NoError(t, opt1.ApplyToGet(op))
		err := opt2.ApplyToGet(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id2")
	})

	t.Run("duplicate IDs across calls for Query returns error", func(t *testing.T) {
		opt1 := WithIDs("id1", "id2")
		opt2 := WithIDs("id3", "id1")

		op := &CollectionQueryOp{}
		require.NoError(t, opt1.ApplyToQuery(op))
		err := opt2.ApplyToQuery(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id1")
	})

	t.Run("duplicate IDs across calls for Delete returns error", func(t *testing.T) {
		opt1 := WithIDs("id1")
		opt2 := WithIDs("id1")

		op := &CollectionDeleteOp{}
		require.NoError(t, opt1.ApplyToDelete(op))
		err := opt2.ApplyToDelete(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id1")
	})

	t.Run("duplicate IDs across calls for Add returns error", func(t *testing.T) {
		opt1 := WithIDs("id1", "id2")
		opt2 := WithIDs("id2")

		op := &CollectionAddOp{}
		require.NoError(t, opt1.ApplyToAdd(op))
		err := opt2.ApplyToAdd(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id2")
	})

	t.Run("duplicate IDs across calls for Update returns error", func(t *testing.T) {
		opt1 := WithIDs("id1", "id2", "id3")
		opt2 := WithIDs("id4", "id3")

		op := &CollectionUpdateOp{}
		require.NoError(t, opt1.ApplyToUpdate(op))
		err := opt2.ApplyToUpdate(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id3")
	})

	t.Run("duplicate IDs across calls for Search returns error", func(t *testing.T) {
		opt1 := WithIDs("id1", "id2")
		opt2 := WithIDs("id2", "id3")

		req := &SearchRequest{}
		require.NoError(t, opt1.ApplyToSearchRequest(req))
		err := opt2.ApplyToSearchRequest(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate id: id2")
	})
}

func TestWithIncludeGet(t *testing.T) {
	opt := WithInclude(IncludeDocuments, IncludeMetadatas)

	op := &CollectionGetOp{}
	err := opt.ApplyToGet(op)
	require.NoError(t, err)
	require.Equal(t, []Include{IncludeDocuments, IncludeMetadatas}, op.Include)
}

func TestWithIncludeQuery(t *testing.T) {
	opt := WithInclude(IncludeEmbeddings, IncludeDistances)

	op := &CollectionQueryOp{}
	err := opt.ApplyToQuery(op)
	require.NoError(t, err)
	require.Equal(t, []Include{IncludeEmbeddings, IncludeDistances}, op.Include)
}

func TestWithLimitGet(t *testing.T) {
	opt := WithLimit(100)

	op := &CollectionGetOp{}
	err := opt.ApplyToGet(op)
	require.NoError(t, err)
	require.Equal(t, 100, op.Limit)
}

func TestWithLimitSearch(t *testing.T) {
	opt := WithLimit(50)

	req := &SearchRequest{}
	err := opt.ApplyToSearchRequest(req)
	require.NoError(t, err)
	require.NotNil(t, req.Limit)
	require.Equal(t, 50, req.Limit.Limit)
}

func TestWithLimitInvalidGet(t *testing.T) {
	t.Run("zero limit returns error", func(t *testing.T) {
		op := &CollectionGetOp{}
		err := WithLimit(0).ApplyToGet(op)
		require.Error(t, err)
		require.Equal(t, ErrInvalidLimit, err)
	})

	t.Run("negative limit returns error", func(t *testing.T) {
		op := &CollectionGetOp{}
		err := WithLimit(-1).ApplyToGet(op)
		require.Error(t, err)
		require.Equal(t, ErrInvalidLimit, err)
	})
}

func TestWithLimitInvalidSearch(t *testing.T) {
	t.Run("zero limit returns error", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithLimit(0).ApplyToSearchRequest(req)
		require.Error(t, err)
		require.Equal(t, ErrInvalidLimit, err)
	})

	t.Run("negative limit returns error", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithLimit(-1).ApplyToSearchRequest(req)
		require.Error(t, err)
		require.Equal(t, ErrInvalidLimit, err)
	})
}

func TestWithOffsetGet(t *testing.T) {
	opt := WithOffset(50)

	op := &CollectionGetOp{}
	err := opt.ApplyToGet(op)
	require.NoError(t, err)
	require.Equal(t, 50, op.Offset)
}

func TestWithOffsetSearch(t *testing.T) {
	opt := WithOffset(100)

	req := &SearchRequest{}
	err := opt.ApplyToSearchRequest(req)
	require.NoError(t, err)
	require.NotNil(t, req.Limit)
	require.Equal(t, 100, req.Limit.Offset)
}

func TestWithOffsetInvalidGet(t *testing.T) {
	op := &CollectionGetOp{}
	err := WithOffset(-1).ApplyToGet(op)
	require.Error(t, err)
	require.Equal(t, ErrInvalidOffset, err)
}

func TestWithOffsetInvalidSearch(t *testing.T) {
	req := &SearchRequest{}
	err := WithOffset(-1).ApplyToSearchRequest(req)
	require.Error(t, err)
	require.Equal(t, ErrInvalidOffset, err)
}

func TestWithOffsetZeroIsAllowed(t *testing.T) {
	t.Run("zero offset for Get is allowed", func(t *testing.T) {
		op := &CollectionGetOp{}
		err := WithOffset(0).ApplyToGet(op)
		require.NoError(t, err)
		require.Equal(t, 0, op.Offset)
	})

	t.Run("zero offset for Search is allowed", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithOffset(0).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.Equal(t, 0, req.Limit.Offset)
	})
}

func TestWithNResultsQuery(t *testing.T) {
	opt := WithNResults(25)

	op := &CollectionQueryOp{}
	err := opt.ApplyToQuery(op)
	require.NoError(t, err)
	require.Equal(t, 25, op.NResults)
}

func TestWithNResultsInvalid(t *testing.T) {
	t.Run("zero nResults returns error", func(t *testing.T) {
		op := &CollectionQueryOp{}
		err := WithNResults(0).ApplyToQuery(op)
		require.Error(t, err)
		require.Equal(t, ErrInvalidNResults, err)
	})

	t.Run("negative nResults returns error", func(t *testing.T) {
		op := &CollectionQueryOp{}
		err := WithNResults(-1).ApplyToQuery(op)
		require.Error(t, err)
		require.Equal(t, ErrInvalidNResults, err)
	})
}

func TestWithQueryTexts(t *testing.T) {
	opt := WithQueryTexts("query1", "query2")

	op := &CollectionQueryOp{}
	err := opt.ApplyToQuery(op)
	require.NoError(t, err)
	require.Equal(t, []string{"query1", "query2"}, op.QueryTexts)
}

func TestWithQueryTextsEmpty(t *testing.T) {
	op := &CollectionQueryOp{}
	err := WithQueryTexts().ApplyToQuery(op)
	require.Error(t, err)
	require.Equal(t, ErrNoQueryTexts, err)
}

func TestWithQueryEmbeddings(t *testing.T) {
	emb1 := embeddings.NewEmbeddingFromFloat32([]float32{0.1, 0.2, 0.3})
	emb2 := embeddings.NewEmbeddingFromFloat32([]float32{0.4, 0.5, 0.6})
	opt := WithQueryEmbeddings(emb1, emb2)

	op := &CollectionQueryOp{}
	err := opt.ApplyToQuery(op)
	require.NoError(t, err)
	require.Len(t, op.QueryEmbeddings, 2)
}

func TestWithQueryEmbeddingsEmpty(t *testing.T) {
	op := &CollectionQueryOp{}
	err := WithQueryEmbeddings().ApplyToQuery(op)
	require.Error(t, err)
	require.Equal(t, ErrNoQueryEmbeddings, err)
}

func TestWithTextsAdd(t *testing.T) {
	opt := WithTexts("doc1", "doc2", "doc3")

	op := &CollectionAddOp{}
	err := opt.ApplyToAdd(op)
	require.NoError(t, err)
	require.Len(t, op.Documents, 3)
}

func TestWithTextsUpdate(t *testing.T) {
	opt := WithTexts("updated doc")

	op := &CollectionUpdateOp{}
	err := opt.ApplyToUpdate(op)
	require.NoError(t, err)
	require.Len(t, op.Documents, 1)
}

func TestWithTextsEmpty(t *testing.T) {
	t.Run("empty texts for Add returns error", func(t *testing.T) {
		op := &CollectionAddOp{}
		err := WithTexts().ApplyToAdd(op)
		require.Error(t, err)
		require.Equal(t, ErrNoTexts, err)
	})

	t.Run("empty texts for Update returns error", func(t *testing.T) {
		op := &CollectionUpdateOp{}
		err := WithTexts().ApplyToUpdate(op)
		require.Error(t, err)
		require.Equal(t, ErrNoTexts, err)
	})
}

func TestWithMetadatasAdd(t *testing.T) {
	meta1 := NewDocumentMetadata(NewStringAttribute("key", "value1"))
	meta2 := NewDocumentMetadata(NewIntAttribute("count", 42))
	opt := WithMetadatas(meta1, meta2)

	op := &CollectionAddOp{}
	err := opt.ApplyToAdd(op)
	require.NoError(t, err)
	require.Len(t, op.Metadatas, 2)
}

func TestWithMetadatasUpdate(t *testing.T) {
	meta := NewDocumentMetadata(NewStringAttribute("status", "updated"))
	opt := WithMetadatas(meta)

	op := &CollectionUpdateOp{}
	err := opt.ApplyToUpdate(op)
	require.NoError(t, err)
	require.Len(t, op.Metadatas, 1)
}

func TestWithEmbeddingsAdd(t *testing.T) {
	emb1 := embeddings.NewEmbeddingFromFloat32([]float32{0.1, 0.2, 0.3})
	emb2 := embeddings.NewEmbeddingFromFloat32([]float32{0.4, 0.5, 0.6})
	opt := WithEmbeddings(emb1, emb2)

	op := &CollectionAddOp{}
	err := opt.ApplyToAdd(op)
	require.NoError(t, err)
	require.Len(t, op.Embeddings, 2)
}

func TestWithEmbeddingsUpdate(t *testing.T) {
	emb := embeddings.NewEmbeddingFromFloat32([]float32{0.7, 0.8, 0.9})
	opt := WithEmbeddings(emb)

	op := &CollectionUpdateOp{}
	err := opt.ApplyToUpdate(op)
	require.NoError(t, err)
	require.Len(t, op.Embeddings, 1)
}

func TestWithEmbeddingsEmpty(t *testing.T) {
	t.Run("empty embeddings for Add returns error", func(t *testing.T) {
		op := &CollectionAddOp{}
		err := WithEmbeddings().ApplyToAdd(op)
		require.Error(t, err)
		require.Equal(t, ErrNoEmbeddings, err)
	})

	t.Run("empty embeddings for Update returns error", func(t *testing.T) {
		op := &CollectionUpdateOp{}
		err := WithEmbeddings().ApplyToUpdate(op)
		require.Error(t, err)
		require.Equal(t, ErrNoEmbeddings, err)
	})
}

func TestWithIDGenerator(t *testing.T) {
	gen := NewULIDGenerator()
	opt := WithIDGenerator(gen)

	op := &CollectionAddOp{}
	err := opt.ApplyToAdd(op)
	require.NoError(t, err)
	require.Equal(t, gen, op.IDGenerator)
}

func TestWithSelectSearch(t *testing.T) {
	opt := WithSelect(KDocument, KScore, K("title"))

	req := &SearchRequest{}
	err := opt.ApplyToSearchRequest(req)
	require.NoError(t, err)
	require.NotNil(t, req.Select)
	require.Equal(t, []Key{KDocument, KScore, "title"}, req.Select.Keys)
}

func TestWithSelectAllSearch(t *testing.T) {
	opt := WithSelectAll()

	req := &SearchRequest{}
	err := opt.ApplyToSearchRequest(req)
	require.NoError(t, err)
	require.NotNil(t, req.Select)
	require.Equal(t, []Key{KID, KDocument, KEmbedding, KMetadata, KScore}, req.Select.Keys)
}

func TestOptionFuncWrappers(t *testing.T) {
	t.Run("GetOptionFunc", func(t *testing.T) {
		var called bool
		fn := GetOptionFunc(func(op *CollectionGetOp) error {
			called = true
			op.Limit = 99
			return nil
		})

		op := &CollectionGetOp{}
		err := fn.ApplyToGet(op)
		require.NoError(t, err)
		require.True(t, called)
		require.Equal(t, 99, op.Limit)
	})

	t.Run("QueryOptionFunc", func(t *testing.T) {
		var called bool
		fn := QueryOptionFunc(func(op *CollectionQueryOp) error {
			called = true
			op.NResults = 42
			return nil
		})

		op := &CollectionQueryOp{}
		err := fn.ApplyToQuery(op)
		require.NoError(t, err)
		require.True(t, called)
		require.Equal(t, 42, op.NResults)
	})

	t.Run("DeleteOptionFunc", func(t *testing.T) {
		var called bool
		fn := DeleteOptionFunc(func(op *CollectionDeleteOp) error {
			called = true
			return nil
		})

		op := &CollectionDeleteOp{}
		err := fn.ApplyToDelete(op)
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("AddOptionFunc", func(t *testing.T) {
		var called bool
		fn := AddOptionFunc(func(op *CollectionAddOp) error {
			called = true
			return nil
		})

		op := &CollectionAddOp{}
		err := fn.ApplyToAdd(op)
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("UpdateOptionFunc", func(t *testing.T) {
		var called bool
		fn := UpdateOptionFunc(func(op *CollectionUpdateOp) error {
			called = true
			return nil
		})

		op := &CollectionUpdateOp{}
		err := fn.ApplyToUpdate(op)
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("SearchRequestOptionFunc", func(t *testing.T) {
		var called bool
		fn := SearchRequestOptionFunc(func(req *SearchRequest) error {
			called = true
			req.Limit = &SearchPage{Limit: 77}
			return nil
		})

		req := &SearchRequest{}
		err := fn.ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.True(t, called)
		require.Equal(t, 77, req.Limit.Limit)
	})
}
