//go:build !cloud

package chroma

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMinK(t *testing.T) {
	t.Run("create with single key", func(t *testing.T) {
		minK := NewMinK(3, KScore)
		require.NotNil(t, minK)
		require.Equal(t, 3, minK.K)
		require.Len(t, minK.Keys, 1)
		require.Equal(t, KScore, minK.Keys[0])
		require.NoError(t, minK.Validate())
	})

	t.Run("create with multiple keys", func(t *testing.T) {
		minK := NewMinK(5, K("priority"), KScore)
		require.NotNil(t, minK)
		require.Equal(t, 5, minK.K)
		require.Len(t, minK.Keys, 2)
		require.NoError(t, minK.Validate())
	})

	t.Run("validate invalid k", func(t *testing.T) {
		minK := NewMinK(0, KScore)
		err := minK.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "k must be >= 1")
	})

	t.Run("validate no keys", func(t *testing.T) {
		minK := NewMinK(3)
		err := minK.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one key is required")
	})

	t.Run("json serialization", func(t *testing.T) {
		minK := NewMinK(3, KScore)
		require.NoError(t, minK.Validate())

		data, err := minK.MarshalJSON()
		require.NoError(t, err)

		expected := `{"$min_k":{"k":3,"keys":["#score"]}}`
		require.JSONEq(t, expected, string(data))
	})

	t.Run("json serialization with multiple keys", func(t *testing.T) {
		minK := NewMinK(2, K("priority"), KScore)
		require.NoError(t, minK.Validate())

		data, err := minK.MarshalJSON()
		require.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		minKObj := result["$min_k"].(map[string]any)
		require.Equal(t, float64(2), minKObj["k"])
		keys := minKObj["keys"].([]any)
		require.Len(t, keys, 2)
		require.Equal(t, "priority", keys[0])
		require.Equal(t, "#score", keys[1])
	})
}

func TestMaxK(t *testing.T) {
	t.Run("create with single key", func(t *testing.T) {
		maxK := NewMaxK(3, K("rating"))
		require.NotNil(t, maxK)
		require.Equal(t, 3, maxK.K)
		require.Len(t, maxK.Keys, 1)
		require.NoError(t, maxK.Validate())
	})

	t.Run("create with multiple keys", func(t *testing.T) {
		maxK := NewMaxK(5, K("year"), K("rating"))
		require.NotNil(t, maxK)
		require.Equal(t, 5, maxK.K)
		require.Len(t, maxK.Keys, 2)
		require.NoError(t, maxK.Validate())
	})

	t.Run("validate invalid k", func(t *testing.T) {
		maxK := NewMaxK(-1, KScore)
		err := maxK.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "k must be >= 1")
	})

	t.Run("validate no keys", func(t *testing.T) {
		maxK := NewMaxK(3)
		err := maxK.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one key is required")
	})

	t.Run("json serialization", func(t *testing.T) {
		maxK := NewMaxK(3, K("rating"))
		require.NoError(t, maxK.Validate())

		data, err := maxK.MarshalJSON()
		require.NoError(t, err)

		expected := `{"$max_k":{"k":3,"keys":["rating"]}}`
		require.JSONEq(t, expected, string(data))
	})
}

func TestGroupBy(t *testing.T) {
	t.Run("create with minK aggregate", func(t *testing.T) {
		groupBy := NewGroupBy(NewMinK(3, KScore), K("category"))
		require.NotNil(t, groupBy)
		require.Len(t, groupBy.Keys, 1)
		require.Equal(t, K("category"), groupBy.Keys[0])
		require.NotNil(t, groupBy.Aggregate)
		require.NoError(t, groupBy.Validate())
	})

	t.Run("create with multiple keys", func(t *testing.T) {
		groupBy := NewGroupBy(NewMaxK(2, K("rating")), K("category"), K("year"))
		require.Len(t, groupBy.Keys, 2)
		require.NoError(t, groupBy.Validate())
	})

	t.Run("validate nil aggregate", func(t *testing.T) {
		groupBy := NewGroupBy(nil, K("category"))
		err := groupBy.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "aggregate is required")
	})

	t.Run("validate no keys", func(t *testing.T) {
		groupBy := NewGroupBy(NewMinK(3, KScore))
		err := groupBy.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one key is required")
	})

	t.Run("validate invalid aggregate", func(t *testing.T) {
		groupBy := NewGroupBy(NewMinK(0, KScore), K("category"))
		err := groupBy.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid aggregate")
	})

	t.Run("json serialization", func(t *testing.T) {
		groupBy := NewGroupBy(NewMinK(3, KScore), K("category"))
		require.NoError(t, groupBy.Validate())

		data, err := groupBy.MarshalJSON()
		require.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		keys := result["keys"].([]any)
		require.Len(t, keys, 1)
		require.Equal(t, "category", keys[0])

		aggregate := result["aggregate"].(map[string]any)
		require.Contains(t, aggregate, "$min_k")
	})

	t.Run("marshal with nil aggregate returns error not panic", func(t *testing.T) {
		groupBy := NewGroupBy(nil, K("category"))
		_, err := groupBy.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "aggregate is required")
	})
}

func TestWithGroupBy(t *testing.T) {
	t.Run("apply valid groupby to search request", func(t *testing.T) {
		groupBy := NewGroupBy(NewMinK(3, KScore), K("category"))

		req := &SearchRequest{}
		err := WithGroupBy(groupBy).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.NotNil(t, req.GroupBy)
		require.Equal(t, groupBy, req.GroupBy)
	})

	t.Run("nil groupby", func(t *testing.T) {
		req := &SearchRequest{}
		err := WithGroupBy(nil).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.Nil(t, req.GroupBy)
	})

	t.Run("invalid groupby returns error", func(t *testing.T) {
		groupBy := NewGroupBy(NewMinK(0, KScore), K("category"))

		req := &SearchRequest{}
		err := WithGroupBy(groupBy).ApplyToSearchRequest(req)
		require.Error(t, err)
		require.Nil(t, req.GroupBy)
	})

	t.Run("groupby without keys returns error", func(t *testing.T) {
		groupBy := NewGroupBy(NewMinK(3, KScore))

		req := &SearchRequest{}
		err := WithGroupBy(groupBy).ApplyToSearchRequest(req)
		require.Error(t, err)
	})
}

func TestSearchRequestWithGroupBy(t *testing.T) {
	t.Run("json serialization with groupby", func(t *testing.T) {
		groupBy := NewGroupBy(NewMinK(3, KScore), K("category"))
		require.NoError(t, groupBy.Validate())

		req := &SearchRequest{
			GroupBy: groupBy,
		}

		data, err := req.MarshalJSON()
		require.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		require.Contains(t, result, "group_by")
		groupByObj := result["group_by"].(map[string]any)
		require.Contains(t, groupByObj, "keys")
		require.Contains(t, groupByObj, "aggregate")
	})

	t.Run("full search with groupby", func(t *testing.T) {
		sq := &SearchQuery{}

		opt := NewSearchRequest(
			WithKnnRank(KnnQueryText("machine learning"), WithKnnLimit(100)),
			WithGroupBy(NewGroupBy(NewMinK(3, KScore), K("category"))),
			WithLimit(30),
			WithSelect(KDocument, KScore, K("category")),
		)

		err := opt(sq)
		require.NoError(t, err)
		require.Len(t, sq.Searches, 1)

		search := sq.Searches[0]
		require.NotNil(t, search.GroupBy)
		require.NotNil(t, search.Rank)
		require.NotNil(t, search.Limit)
		require.NotNil(t, search.Select)

		data, err := json.Marshal(sq)
		require.NoError(t, err)
		require.NotEmpty(t, data)

		var result map[string]any
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		searches := result["searches"].([]any)
		require.Len(t, searches, 1)

		searchObj := searches[0].(map[string]any)
		require.Contains(t, searchObj, "group_by")
		require.Contains(t, searchObj, "rank")
		require.Contains(t, searchObj, "limit")
		require.Contains(t, searchObj, "select")
	})

	t.Run("search with invalid groupby fails", func(t *testing.T) {
		sq := &SearchQuery{}

		opt := NewSearchRequest(
			WithKnnRank(KnnQueryText("machine learning")),
			WithGroupBy(NewGroupBy(NewMinK(0, KScore), K("category"))),
		)

		err := opt(sq)
		require.Error(t, err)
	})
}
