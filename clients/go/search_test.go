//go:build !cloud

package chroma

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// mustKnnRank is a test helper that creates a KnnRank or fails the test
func mustKnnRank(t *testing.T, query KnnQueryOption, knnOptions ...KnnOption) *KnnRank {
	t.Helper()
	knn, err := NewKnnRank(query, knnOptions...)
	require.NoError(t, err)
	return knn
}

func TestSearchPage(t *testing.T) {
	t.Run("limit only", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithLimit(10).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.Equal(t, 10, req.Limit.Limit)
	})

	t.Run("offset only", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithOffset(5).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.Equal(t, 5, req.Limit.Offset)
	})

	t.Run("limit and offset", func(t *testing.T) {
		req := &SearchRequest{}
		_ = WithLimit(20).ApplyToSearchRequest(req)
		_ = WithOffset(10).ApplyToSearchRequest(req)
		require.Equal(t, 20, req.Limit.Limit)
		require.Equal(t, 10, req.Limit.Offset)
	})

	t.Run("invalid limit", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithLimit(0).ApplyToSearchRequest(req)
		require.Error(t, err)
	})

	t.Run("negative offset", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithOffset(-1).ApplyToSearchRequest(req)
		require.Error(t, err)
	})

	t.Run("with page helper", func(t *testing.T) {
		req := &SearchRequest{}
		err := NewPage(Limit(20), Offset(40)).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.Equal(t, 20, req.Limit.Limit)
		require.Equal(t, 40, req.Limit.Offset)
	})
}

func TestSearchSelect(t *testing.T) {
	t.Run("select standard keys", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithSelect(KDocument, KScore, KEmbedding).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.Len(t, req.Select.Keys, 3)
		require.Contains(t, req.Select.Keys, KDocument)
		require.Contains(t, req.Select.Keys, KScore)
		require.Contains(t, req.Select.Keys, KEmbedding)
	})

	t.Run("select custom keys", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithSelect(K("title"), K("author")).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.Len(t, req.Select.Keys, 2)
	})

	t.Run("select all", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithSelectAll().ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.Len(t, req.Select.Keys, 5)
		require.Contains(t, req.Select.Keys, KID)
		require.Contains(t, req.Select.Keys, KDocument)
		require.Contains(t, req.Select.Keys, KEmbedding)
		require.Contains(t, req.Select.Keys, KMetadata)
		require.Contains(t, req.Select.Keys, KScore)
	})

	t.Run("append to existing select", func(t *testing.T) {
		req := &SearchRequest{}
		_ = WithSelect(KDocument).ApplyToSearchRequest(req)
		_ = WithSelect(K("custom")).ApplyToSearchRequest(req)
		require.Len(t, req.Select.Keys, 2)
	})
}

func TestSearchFilter(t *testing.T) {
	t.Run("with where clause", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithFilter(EqString(K("status"), "active")).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.NotNil(t, req.Filter)
		require.NotNil(t, req.Filter.Where)
	})

	t.Run("with ids", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithIDs("id1", "id2", "id3").ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.NotNil(t, req.Filter)
		require.Len(t, req.Filter.IDs, 3)
	})

	t.Run("combine filter and ids", func(t *testing.T) {
		req := &SearchRequest{}
		_ = WithFilter(EqString(K("type"), "document")).ApplyToSearchRequest(req)
		_ = WithIDs("doc1", "doc2").ApplyToSearchRequest(req)
		require.NotNil(t, req.Filter.Where)
		require.Len(t, req.Filter.IDs, 2)
	})
}

func TestSearchRequestJSON(t *testing.T) {
	t.Run("basic request with knn rank", func(t *testing.T) {
		req := &SearchRequest{
			Rank: mustKnnRank(t, KnnQueryText("test query")),
			Limit: &SearchPage{
				Limit:  10,
				Offset: 0,
			},
		}

		data, err := req.MarshalJSON()
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		require.Contains(t, result, "rank")
		require.Contains(t, result, "limit")
	})

	t.Run("request with filter", func(t *testing.T) {
		req := &SearchRequest{}
		_ = WithFilter(EqString(K("category"), "tech")).ApplyToSearchRequest(req)
		_ = NewPage(Limit(20)).ApplyToSearchRequest(req)

		data, err := req.MarshalJSON()
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		require.Contains(t, result, "filter")
		require.Contains(t, result, "limit")
	})

	t.Run("request with select", func(t *testing.T) {
		req := &SearchRequest{}
		_ = WithSelect(KDocument, KScore, K("title")).ApplyToSearchRequest(req)

		data, err := req.MarshalJSON()
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		require.Contains(t, result, "select")
		selectObj := result["select"].(map[string]interface{})
		keys := selectObj["keys"].([]interface{})
		require.Len(t, keys, 3)
	})

	t.Run("empty request produces empty json", func(t *testing.T) {
		req := &SearchRequest{}
		data, err := req.MarshalJSON()
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(data))
	})
}

func TestSearchQuery(t *testing.T) {
	t.Run("single search request", func(t *testing.T) {
		sq := &SearchQuery{}
		opt := NewSearchRequest(
			WithKnnRank(KnnQueryText("test"), WithKnnLimit(50)),
			WithLimit(10),
		)
		err := opt(sq)
		require.NoError(t, err)
		require.Len(t, sq.Searches, 1)
	})

	t.Run("multiple search requests", func(t *testing.T) {
		sq := &SearchQuery{}

		opt1 := NewSearchRequest(
			WithKnnRank(KnnQueryText("query1")),
		)
		opt2 := NewSearchRequest(
			WithKnnRank(KnnQueryText("query2")),
		)

		_ = opt1(sq)
		_ = opt2(sq)

		require.Len(t, sq.Searches, 2)
	})
}

func TestWithKnnRank(t *testing.T) {
	t.Run("basic knn rank", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithKnnRank(KnnQueryText("machine learning")).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.NotNil(t, req.Rank)

		knn, ok := req.Rank.(*KnnRank)
		require.True(t, ok)
		require.Equal(t, "machine learning", knn.Query)
	})

	t.Run("knn rank with options", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithKnnRank(
			KnnQueryText("AI research"),
			WithKnnLimit(100),
			WithKnnDefault(10.0),
			WithKnnKey(K("custom_field")),
		).ApplyToSearchRequest(req)
		require.NoError(t, err)

		knn, ok := req.Rank.(*KnnRank)
		require.True(t, ok)
		require.Equal(t, 100, knn.Limit)
		require.NotNil(t, knn.DefaultScore)
		require.Equal(t, 10.0, *knn.DefaultScore)
		require.Equal(t, Key("custom_field"), knn.Key)
	})
}

func TestWithRffRank(t *testing.T) {
	t.Run("basic rff rank", func(t *testing.T) {
		req := &SearchRequest{}
		knn1 := mustKnnRank(t, KnnQueryText("query1"))
		knn2 := mustKnnRank(t, KnnQueryText("query2"))
		err := WithRffRank(
			WithRffRanks(
				knn1.WithWeight(0.5),
				knn2.WithWeight(0.5),
			),
		).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.NotNil(t, req.Rank)

		rrf, ok := req.Rank.(*RrfRank)
		require.True(t, ok)
		require.Len(t, rrf.Ranks, 2)
	})

	t.Run("rff with custom k", func(t *testing.T) {
		req := &SearchRequest{}
		knn := mustKnnRank(t, KnnQueryText("test"))
		err := WithRffRank(
			WithRffRanks(knn.WithWeight(1.0)),
			WithRffK(100),
		).ApplyToSearchRequest(req)
		require.NoError(t, err)

		rrf := req.Rank.(*RrfRank)
		require.Equal(t, 100, rrf.K)
	})

	t.Run("rff with invalid k returns error", func(t *testing.T) {
		req := &SearchRequest{}
		knn := mustKnnRank(t, KnnQueryText("test"))
		err := WithRffRank(
			WithRffRanks(knn.WithWeight(1.0)),
			WithRffK(-1),
		).ApplyToSearchRequest(req)
		require.Error(t, err)
		_ = req // avoid unused variable warning
	})
}

func TestKey(t *testing.T) {
	t.Run("standard keys", func(t *testing.T) {
		require.Equal(t, Key("#document"), KDocument)
		require.Equal(t, Key("#embedding"), KEmbedding)
		require.Equal(t, Key("#score"), KScore)
		require.Equal(t, Key("#metadata"), KMetadata)
		require.Equal(t, Key("#id"), KID)
	})

	t.Run("custom key", func(t *testing.T) {
		key := K("my_custom_field")
		require.Equal(t, Key("my_custom_field"), key)
	})
}

func TestSearchFilterJSON(t *testing.T) {
	t.Run("filter with where", func(t *testing.T) {
		filter := &SearchFilter{
			Where: EqString(K("status"), "active"),
		}

		data, err := filter.MarshalJSON()
		require.NoError(t, err)
		require.NotNil(t, data)

		// New format: where clause is serialized directly as the filter
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		require.Contains(t, result, "status") // Key is at top level
	})

	t.Run("filter with ids", func(t *testing.T) {
		filter := &SearchFilter{
			IDs: []DocumentID{"id1", "id2"},
		}

		data, err := filter.MarshalJSON()
		require.NoError(t, err)

		// New format: IDs are converted to #id $in clause
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		require.Contains(t, result, "#id") // Converted to #id
	})

	t.Run("empty filter returns empty object", func(t *testing.T) {
		filter := &SearchFilter{}
		data, err := filter.MarshalJSON()
		require.NoError(t, err)
		require.Equal(t, "{}", string(data))
	})

	t.Run("filter with ids and where combined", func(t *testing.T) {
		filter := &SearchFilter{
			IDs:   []DocumentID{"id1", "id2"},
			Where: EqString(K("status"), "active"),
		}

		data, err := filter.MarshalJSON()
		require.NoError(t, err)

		// Combined filters use $and
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		require.Contains(t, result, "$and")
	})

	t.Run("filter with empty IDIn fails on marshal", func(t *testing.T) {
		filter := &SearchFilter{
			Where: IDIn(), // Empty IDIn - lazy validation
		}

		// Construction is fine (lazy)
		require.NotNil(t, filter)

		// MarshalJSON should fail due to validation
		_, err := filter.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid search filter")
		require.Contains(t, err.Error(), "expected at least one value")
	})

	t.Run("filter with empty IDNotIn fails on marshal", func(t *testing.T) {
		filter := &SearchFilter{
			Where: IDNotIn(), // Empty IDNotIn - lazy validation
		}

		_, err := filter.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid search filter")
	})

	t.Run("filter with nested empty IDIn fails on marshal", func(t *testing.T) {
		filter := &SearchFilter{
			Where: And(EqString(K("status"), "active"), IDIn()),
		}

		_, err := filter.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid search filter")
	})
}

func TestCompleteSearchScenario(t *testing.T) {
	t.Run("full search with all options", func(t *testing.T) {
		sq := &SearchQuery{}

		opt := NewSearchRequest(
			WithFilter(
				And(
					EqString(K("status"), "published"),
					GtInt(K("views"), 100),
				),
			),
			WithKnnRank(
				KnnQueryText("machine learning tutorials"),
				WithKnnLimit(50),
				WithKnnDefault(1000.0),
			),
			NewPage(Limit(20)),
			WithSelect(KDocument, KScore, K("title"), K("author")),
		)

		err := opt(sq)
		require.NoError(t, err)
		require.Len(t, sq.Searches, 1)

		search := sq.Searches[0]
		require.NotNil(t, search.Filter)
		require.NotNil(t, search.Rank)
		require.NotNil(t, search.Limit)
		require.NotNil(t, search.Select)

		// Verify JSON serialization
		data, err := json.Marshal(sq)
		require.NoError(t, err)
		require.NotEmpty(t, data)
	})

	t.Run("full search with groupby", func(t *testing.T) {
		sq := &SearchQuery{}

		opt := NewSearchRequest(
			WithFilter(EqString(K("status"), "published")),
			WithKnnRank(
				KnnQueryText("machine learning"),
				WithKnnLimit(100),
			),
			WithGroupBy(NewGroupBy(NewMinK(3, KScore), K("category"))),
			WithLimit(30),
			WithSelect(KDocument, KScore, K("category")),
		)

		err := opt(sq)
		require.NoError(t, err)
		require.Len(t, sq.Searches, 1)

		search := sq.Searches[0]
		require.NotNil(t, search.Filter)
		require.NotNil(t, search.Rank)
		require.NotNil(t, search.GroupBy)
		require.NotNil(t, search.Limit)
		require.NotNil(t, search.Select)

		// Verify JSON serialization
		data, err := json.Marshal(sq)
		require.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		searches := result["searches"].([]any)
		searchObj := searches[0].(map[string]any)
		require.Contains(t, searchObj, "group_by")

		groupBy := searchObj["group_by"].(map[string]any)
		require.Contains(t, groupBy, "keys")
		require.Contains(t, groupBy, "aggregate")

		keys := groupBy["keys"].([]any)
		require.Equal(t, "category", keys[0])

		aggregate := groupBy["aggregate"].(map[string]any)
		require.Contains(t, aggregate, "$min_k")
	})

	t.Run("search with maxk groupby", func(t *testing.T) {
		sq := &SearchQuery{}

		opt := NewSearchRequest(
			WithKnnRank(KnnQueryText("top rated movies")),
			WithGroupBy(NewGroupBy(NewMaxK(5, K("rating")), K("genre"))),
			WithLimit(50),
		)

		err := opt(sq)
		require.NoError(t, err)

		search := sq.Searches[0]
		require.NotNil(t, search.GroupBy)

		data, err := search.GroupBy.MarshalJSON()
		require.NoError(t, err)

		var groupBy map[string]any
		err = json.Unmarshal(data, &groupBy)
		require.NoError(t, err)

		aggregate := groupBy["aggregate"].(map[string]any)
		require.Contains(t, aggregate, "$max_k")

		maxK := aggregate["$max_k"].(map[string]any)
		require.Equal(t, float64(5), maxK["k"])
	})
}

func TestSearchResultUnmarshal(t *testing.T) {
	t.Run("unmarshal with all fields", func(t *testing.T) {
		jsonData := `{
			"ids": [["id1", "id2"]],
			"documents": [["doc1", "doc2"]],
			"metadatas": [[{"key": "value", "num": 42}]],
			"embeddings": [[[0.1, 0.2], [0.3, 0.4]]],
			"scores": [[0.9, 0.8]]
		}`

		var result SearchResultImpl
		err := json.Unmarshal([]byte(jsonData), &result)
		require.NoError(t, err)

		require.Len(t, result.IDs, 1)
		require.Len(t, result.IDs[0], 2)
		require.Equal(t, DocumentID("id1"), result.IDs[0][0])

		require.Len(t, result.Documents, 1)
		require.Len(t, result.Documents[0], 2)
		require.Equal(t, "doc1", result.Documents[0][0])

		require.Len(t, result.Metadatas, 1)
		require.Len(t, result.Metadatas[0], 1)
		require.NotNil(t, result.Metadatas[0][0])
		val, ok := result.Metadatas[0][0].GetString("key")
		require.True(t, ok)
		require.Equal(t, "value", val)

		require.Len(t, result.Embeddings, 1)
		require.Len(t, result.Embeddings[0], 2)
		require.Equal(t, float32(0.1), result.Embeddings[0][0][0])

		require.Len(t, result.Scores, 1)
		require.Len(t, result.Scores[0], 2)
		require.Equal(t, 0.9, result.Scores[0][0])
	})

	t.Run("unmarshal with null fields", func(t *testing.T) {
		// Simulates actual Chroma Cloud response format
		jsonData := `{
			"ids": [["1", "3"]],
			"documents": [["cats are fluffy pets", "lions are big cats"]],
			"embeddings": [null],
			"metadatas": [null],
			"scores": [[0.6631017, 0.9698644]],
			"select": [["#document", "#score"]]
		}`

		var result SearchResultImpl
		err := json.Unmarshal([]byte(jsonData), &result)
		require.NoError(t, err)

		require.Len(t, result.IDs, 1)
		require.Len(t, result.IDs[0], 2)
		require.Equal(t, DocumentID("1"), result.IDs[0][0])

		require.Len(t, result.Documents, 1)
		require.Len(t, result.Documents[0], 2)

		require.Len(t, result.Embeddings, 1)
		require.Nil(t, result.Embeddings[0])

		require.Len(t, result.Metadatas, 1)
		require.Nil(t, result.Metadatas[0])

		require.Len(t, result.Scores, 1)
		require.Len(t, result.Scores[0], 2)
	})

	t.Run("unmarshal with metadata", func(t *testing.T) {
		jsonData := `{
			"ids": [["1", "2"]],
			"documents": [["doc1", "doc2"]],
			"metadatas": [[{"category": "AI", "year": 2023}, {"category": "ML", "year": 2022}]],
			"scores": [[0.95, 0.85]]
		}`

		var result SearchResultImpl
		err := json.Unmarshal([]byte(jsonData), &result)
		require.NoError(t, err)

		require.Len(t, result.Metadatas, 1)
		require.Len(t, result.Metadatas[0], 2)

		cat, ok := result.Metadatas[0][0].GetString("category")
		require.True(t, ok)
		require.Equal(t, "AI", cat)

		year, ok := result.Metadatas[0][0].GetInt("year")
		require.True(t, ok)
		require.Equal(t, int64(2023), year)
	})

	t.Run("unmarshal empty result", func(t *testing.T) {
		jsonData := `{}`

		var result SearchResultImpl
		err := json.Unmarshal([]byte(jsonData), &result)
		require.NoError(t, err)

		require.Empty(t, result.IDs)
		require.Empty(t, result.Documents)
		require.Empty(t, result.Metadatas)
		require.Empty(t, result.Embeddings)
		require.Empty(t, result.Scores)
	})

	t.Run("unmarshal with multiple groups", func(t *testing.T) {
		jsonData := `{
			"ids": [["id1", "id2"], ["id3", "id4"]],
			"documents": [["doc1", "doc2"], ["doc3", "doc4"]],
			"metadatas": [[{"k": "v1"}, {"k": "v2"}], [{"k": "v3"}, {"k": "v4"}]],
			"embeddings": [[[0.1, 0.2], [0.3, 0.4]], [[0.5, 0.6], [0.7, 0.8]]],
			"scores": [[0.9, 0.8], [0.7, 0.6]]
		}`

		var result SearchResultImpl
		err := json.Unmarshal([]byte(jsonData), &result)
		require.NoError(t, err)

		require.Len(t, result.IDs, 2)
		require.Len(t, result.IDs[0], 2)
		require.Len(t, result.IDs[1], 2)
		require.Equal(t, DocumentID("id1"), result.IDs[0][0])
		require.Equal(t, DocumentID("id4"), result.IDs[1][1])

		require.Len(t, result.Documents, 2)
		require.Len(t, result.Documents[0], 2)
		require.Len(t, result.Documents[1], 2)
		require.Equal(t, "doc1", result.Documents[0][0])
		require.Equal(t, "doc4", result.Documents[1][1])

		require.Len(t, result.Metadatas, 2)
		require.Len(t, result.Metadatas[0], 2)
		require.Len(t, result.Metadatas[1], 2)
		val, ok := result.Metadatas[0][0].GetString("k")
		require.True(t, ok)
		require.Equal(t, "v1", val)
		val, ok = result.Metadatas[1][1].GetString("k")
		require.True(t, ok)
		require.Equal(t, "v4", val)

		require.Len(t, result.Embeddings, 2)
		require.Len(t, result.Embeddings[0], 2)
		require.Len(t, result.Embeddings[1], 2)
		require.Equal(t, float32(0.1), result.Embeddings[0][0][0])
		require.Equal(t, float32(0.5), result.Embeddings[1][0][0])

		require.Len(t, result.Scores, 2)
		require.Len(t, result.Scores[0], 2)
		require.Len(t, result.Scores[1], 2)
		require.Equal(t, 0.9, result.Scores[0][0])
		require.Equal(t, 0.6, result.Scores[1][1])
	})
}

func TestSearchResultImpl_Rows(t *testing.T) {
	result := &SearchResultImpl{
		IDs:       [][]DocumentID{{"id1", "id2"}, {"id3"}},
		Documents: [][]string{{"doc1", "doc2"}, {"doc3"}},
		Metadatas: [][]DocumentMetadata{{NewDocumentMetadata(NewStringAttribute("k", "v1")), nil}, {nil}},
		Scores:    [][]float64{{0.9, 0.8}, {0.7}},
	}

	rows := result.Rows()
	require.Len(t, rows, 2)

	require.Equal(t, DocumentID("id1"), rows[0].ID)
	require.Equal(t, "doc1", rows[0].Document)
	require.Equal(t, 0.9, rows[0].Score)
	val, ok := rows[0].Metadata.GetString("k")
	require.True(t, ok)
	require.Equal(t, "v1", val)

	require.Equal(t, DocumentID("id2"), rows[1].ID)
	require.Equal(t, "doc2", rows[1].Document)
	require.Equal(t, 0.8, rows[1].Score)
	require.Nil(t, rows[1].Metadata)
}

func TestSearchResultImpl_Rows_Empty(t *testing.T) {
	result := &SearchResultImpl{}
	rows := result.Rows()
	require.Nil(t, rows)
}

func TestSearchResultImpl_RowGroups(t *testing.T) {
	result := &SearchResultImpl{
		IDs:       [][]DocumentID{{"id1", "id2"}, {"id3"}},
		Documents: [][]string{{"doc1", "doc2"}, {"doc3"}},
		Scores:    [][]float64{{0.9, 0.8}, {0.7}},
	}

	groups := result.RowGroups()
	require.Len(t, groups, 2)
	require.Len(t, groups[0], 2)
	require.Len(t, groups[1], 1)

	require.Equal(t, DocumentID("id1"), groups[0][0].ID)
	require.Equal(t, DocumentID("id2"), groups[0][1].ID)
	require.Equal(t, DocumentID("id3"), groups[1][0].ID)
	require.Equal(t, 0.7, groups[1][0].Score)
}

func TestSearchResultImpl_At(t *testing.T) {
	result := &SearchResultImpl{
		IDs:       [][]DocumentID{{"id1", "id2"}, {"id3"}},
		Documents: [][]string{{"doc1", "doc2"}, {"doc3"}},
		Scores:    [][]float64{{0.9, 0.8}, {0.7}},
	}

	row, ok := result.At(0, 0)
	require.True(t, ok)
	require.Equal(t, DocumentID("id1"), row.ID)
	require.Equal(t, "doc1", row.Document)
	require.Equal(t, 0.9, row.Score)

	row, ok = result.At(0, 1)
	require.True(t, ok)
	require.Equal(t, DocumentID("id2"), row.ID)

	row, ok = result.At(1, 0)
	require.True(t, ok)
	require.Equal(t, DocumentID("id3"), row.ID)
	require.Equal(t, 0.7, row.Score)

	_, ok = result.At(-1, 0)
	require.False(t, ok)

	_, ok = result.At(0, -1)
	require.False(t, ok)

	_, ok = result.At(2, 0)
	require.False(t, ok)

	_, ok = result.At(0, 2)
	require.False(t, ok)
}

func TestWithReadLevel(t *testing.T) {
	t.Run("set index_and_wal", func(t *testing.T) {
		sq := &SearchQuery{}
		err := WithReadLevel(ReadLevelIndexAndWAL)(sq)
		require.NoError(t, err)
		require.Equal(t, ReadLevelIndexAndWAL, sq.ReadLevel)
	})

	t.Run("set index_only", func(t *testing.T) {
		sq := &SearchQuery{}
		err := WithReadLevel(ReadLevelIndexOnly)(sq)
		require.NoError(t, err)
		require.Equal(t, ReadLevelIndexOnly, sq.ReadLevel)
	})

	t.Run("json serialization includes read_level", func(t *testing.T) {
		sq := &SearchQuery{
			ReadLevel: ReadLevelIndexOnly,
		}
		data, err := json.Marshal(sq)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		require.Equal(t, "index_only", result["read_level"])
	})

	t.Run("json serialization omits empty read_level", func(t *testing.T) {
		sq := &SearchQuery{}
		data, err := json.Marshal(sq)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		_, exists := result["read_level"]
		require.False(t, exists)
	})

	t.Run("combined with search request", func(t *testing.T) {
		sq := &SearchQuery{}
		opt1 := NewSearchRequest(
			WithKnnRank(KnnQueryText("test"), WithKnnLimit(50)),
			WithLimit(10),
		)
		opt2 := WithReadLevel(ReadLevelIndexOnly)

		err := opt1(sq)
		require.NoError(t, err)
		err = opt2(sq)
		require.NoError(t, err)

		require.Len(t, sq.Searches, 1)
		require.Equal(t, ReadLevelIndexOnly, sq.ReadLevel)

		data, err := json.Marshal(sq)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		require.Equal(t, "index_only", result["read_level"])
		require.Contains(t, result, "searches")
	})
	t.Run("invalid read level returns error", func(t *testing.T) {
		sq := &SearchQuery{}
		err := WithReadLevel(ReadLevel("invalid"))(sq)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid read level")
	})
}

func TestSearchFilterHelpers(t *testing.T) {
	t.Run("AppendIDs adds to filter", func(t *testing.T) {
		filter := &SearchFilter{}
		filter.AppendIDs("id1", "id2")
		require.Len(t, filter.IDs, 2)

		filter.AppendIDs("id3")
		require.Len(t, filter.IDs, 3)
	})
}

func TestSearchWithNewPage(t *testing.T) {
	t.Run("NewPage in search request", func(t *testing.T) {
		sq := &SearchQuery{}
		opt := NewSearchRequest(
			WithKnnRank(KnnQueryText("test")),
			NewPage(Limit(25), Offset(50)),
			WithSelect(KDocument),
		)
		err := opt(sq)
		require.NoError(t, err)
		require.Len(t, sq.Searches, 1)
		require.Equal(t, 25, sq.Searches[0].Limit.Limit)
		require.Equal(t, 50, sq.Searches[0].Limit.Offset)
	})

	t.Run("NewPage default limit", func(t *testing.T) {
		sq := &SearchQuery{}
		opt := NewSearchRequest(
			WithKnnRank(KnnQueryText("test")),
			NewPage(), // default limit is 10
		)
		err := opt(sq)
		require.NoError(t, err)
		require.Equal(t, 10, sq.Searches[0].Limit.Limit)
	})

	t.Run("invalid NewPage returns error", func(t *testing.T) {
		sq := &SearchQuery{}
		opt := NewSearchRequest(
			WithKnnRank(KnnQueryText("test")),
			NewPage(Limit(-1)), // invalid
		)
		err := opt(sq)
		require.Error(t, err)
		require.Contains(t, err.Error(), "limit must be greater than 0")
	})
}
