//go:build !cloud

package chroma

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// mustNewKnnRank is a test helper that panics if NewKnnRank returns an error
func mustNewKnnRank(t *testing.T, query KnnQueryOption, knnOptions ...KnnOption) *KnnRank {
	t.Helper()
	knn, err := NewKnnRank(query, knnOptions...)
	require.NoError(t, err)
	return knn
}

func TestValRank(t *testing.T) {
	tests := []struct {
		name     string
		value    float64
		expected string
	}{
		{
			name:     "positive value",
			value:    0.5,
			expected: `{"$val":0.5}`,
		},
		{
			name:     "negative value",
			value:    -1.0,
			expected: `{"$val":-1}`,
		},
		{
			name:     "zero",
			value:    0,
			expected: `{"$val":0}`,
		},
		{
			name:     "large value",
			value:    1000.0,
			expected: `{"$val":1000}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := Val(tt.value)
			data, err := val.MarshalJSON()
			require.NoError(t, err)
			require.JSONEq(t, tt.expected, string(data))
		})
	}
}

func TestKnnRank(t *testing.T) {
	tests := []struct {
		name     string
		makeRank func(t *testing.T) *KnnRank
		expected string
	}{
		{
			name: "text query with defaults",
			makeRank: func(t *testing.T) *KnnRank {
				return mustNewKnnRank(t, KnnQueryText("machine learning"))
			},
			expected: `{"$knn":{"query":"machine learning","key":"#embedding","limit":16}}`,
		},
		{
			name: "text query with custom limit",
			makeRank: func(t *testing.T) *KnnRank {
				return mustNewKnnRank(t, KnnQueryText("deep learning"), WithKnnLimit(100))
			},
			expected: `{"$knn":{"query":"deep learning","key":"#embedding","limit":100}}`,
		},
		{
			name: "text query with custom key",
			makeRank: func(t *testing.T) *KnnRank {
				return mustNewKnnRank(t, KnnQueryText("neural networks"), WithKnnKey(K("sparse_embedding")))
			},
			expected: `{"$knn":{"query":"neural networks","key":"sparse_embedding","limit":16}}`,
		},
		{
			name: "text query with default score",
			makeRank: func(t *testing.T) *KnnRank {
				return mustNewKnnRank(t, KnnQueryText("AI research"), WithKnnDefault(10.0))
			},
			expected: `{"$knn":{"query":"AI research","key":"#embedding","limit":16,"default":10}}`,
		},
		{
			name: "text query with return_rank",
			makeRank: func(t *testing.T) *KnnRank {
				return mustNewKnnRank(t, KnnQueryText("papers"), WithKnnReturnRank())
			},
			expected: `{"$knn":{"query":"papers","key":"#embedding","limit":16,"return_rank":true}}`,
		},
		{
			name: "all options",
			makeRank: func(t *testing.T) *KnnRank {
				return mustNewKnnRank(t,
					KnnQueryText("complete example"),
					WithKnnLimit(50),
					WithKnnKey(K("custom_field")),
					WithKnnDefault(100.0),
					WithKnnReturnRank(),
				)
			},
			expected: `{"$knn":{"query":"complete example","key":"custom_field","limit":50,"default":100,"return_rank":true}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rank := tt.makeRank(t)
			data, err := rank.MarshalJSON()
			require.NoError(t, err)
			require.JSONEq(t, tt.expected, string(data))
		})
	}
}

func TestKnnRankWithVectors(t *testing.T) {
	t.Run("dense vector", func(t *testing.T) {
		// Create a KnnRank with a float32 slice directly
		knn := mustNewKnnRank(t, nil)
		knn.Query = []float32{0.1, 0.2, 0.3}

		data, err := knn.MarshalJSON()
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		knnData := result["$knn"].(map[string]interface{})
		query := knnData["query"].([]interface{})
		require.Len(t, query, 3)
	})

	t.Run("sparse vector", func(t *testing.T) {
		sparseVector, err := embeddings.NewSparseVector(
			[]int{1, 5, 10},
			[]float32{0.5, 0.3, 0.8},
		)
		require.NoError(t, err)
		rank := mustNewKnnRank(t,
			KnnQuerySparseVector(sparseVector),
			WithKnnKey(K("sparse_embedding")),
		)
		data, err := rank.MarshalJSON()
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)

		knn := result["$knn"].(map[string]interface{})
		query := knn["query"].(map[string]interface{})
		require.Contains(t, query, "indices")
		require.Contains(t, query, "values")
	})
}

func TestArithmeticOperations(t *testing.T) {
	tests := []struct {
		name     string
		makeRank func(t *testing.T) Rank
		expected string
	}{
		{
			name:     "addition with val",
			makeRank: func(_ *testing.T) Rank { return Val(1.0).Add(FloatOperand(2.0)) },
			expected: `{"$sum":[{"$val":1},{"$val":2}]}`,
		},
		{
			name:     "subtraction with val",
			makeRank: func(_ *testing.T) Rank { return Val(5.0).Sub(FloatOperand(3.0)) },
			expected: `{"$sub":{"left":{"$val":5},"right":{"$val":3}}}`,
		},
		{
			name:     "multiplication with val",
			makeRank: func(_ *testing.T) Rank { return Val(2.0).Multiply(FloatOperand(3.0)) },
			expected: `{"$mul":[{"$val":2},{"$val":3}]}`,
		},
		{
			name:     "division with val",
			makeRank: func(_ *testing.T) Rank { return Val(10.0).Div(FloatOperand(2.0)) },
			expected: `{"$div":{"left":{"$val":10},"right":{"$val":2}}}`,
		},
		{
			name:     "negation",
			makeRank: func(_ *testing.T) Rank { return Val(5.0).Negate() },
			expected: `{"$mul":[{"$val":-1},{"$val":5}]}`,
		},
		{
			name: "knn multiply by scalar",
			makeRank: func(t *testing.T) Rank {
				return mustNewKnnRank(t, KnnQueryText("test")).Multiply(FloatOperand(0.5))
			},
			expected: `{"$mul":[{"$knn":{"query":"test","key":"#embedding","limit":16}},{"$val":0.5}]}`,
		},
		{
			name: "knn add knn",
			makeRank: func(t *testing.T) Rank {
				return mustNewKnnRank(t, KnnQueryText("a")).Add(mustNewKnnRank(t, KnnQueryText("b")))
			},
			expected: `{"$sum":[{"$knn":{"query":"a","key":"#embedding","limit":16}},{"$knn":{"query":"b","key":"#embedding","limit":16}}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rank := tt.makeRank(t)
			data, err := rank.MarshalJSON()
			require.NoError(t, err)
			require.JSONEq(t, tt.expected, string(data))
		})
	}
}

func TestMathFunctions(t *testing.T) {
	tests := []struct {
		name     string
		makeRank func(t *testing.T) Rank
		expected string
	}{
		{
			name:     "abs",
			makeRank: func(_ *testing.T) Rank { return Val(-5.0).Abs() },
			expected: `{"$abs":{"$val":-5}}`,
		},
		{
			name:     "exp",
			makeRank: func(_ *testing.T) Rank { return Val(1.0).Exp() },
			expected: `{"$exp":{"$val":1}}`,
		},
		{
			name:     "log",
			makeRank: func(_ *testing.T) Rank { return Val(10.0).Log() },
			expected: `{"$log":{"$val":10}}`,
		},
		{
			name:     "max",
			makeRank: func(_ *testing.T) Rank { return Val(1.0).Max(FloatOperand(5.0)) },
			expected: `{"$max":[{"$val":1},{"$val":5}]}`,
		},
		{
			name:     "min",
			makeRank: func(_ *testing.T) Rank { return Val(10.0).Min(FloatOperand(5.0)) },
			expected: `{"$min":[{"$val":10},{"$val":5}]}`,
		},
		{
			name: "knn with exp",
			makeRank: func(t *testing.T) Rank {
				return mustNewKnnRank(t, KnnQueryText("test")).Exp()
			},
			expected: `{"$exp":{"$knn":{"query":"test","key":"#embedding","limit":16}}}`,
		},
		{
			name: "knn with min and max (clamping)",
			makeRank: func(t *testing.T) Rank {
				return mustNewKnnRank(t, KnnQueryText("test")).Min(FloatOperand(0.0)).Max(FloatOperand(1.0))
			},
			expected: `{"$max":[{"$min":[{"$knn":{"query":"test","key":"#embedding","limit":16}},{"$val":0}]},{"$val":1}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rank := tt.makeRank(t)
			data, err := rank.MarshalJSON()
			require.NoError(t, err)
			require.JSONEq(t, tt.expected, string(data))
		})
	}
}

func TestDivisionByZero(t *testing.T) {
	t.Run("literal zero denominator", func(t *testing.T) {
		rank := Val(10.0).Div(Val(0.0))
		_, err := rank.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "division by zero")
	})

	t.Run("float operand zero denominator", func(t *testing.T) {
		rank := Val(10.0).Div(FloatOperand(0.0))
		_, err := rank.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "division by zero")
	})

	t.Run("int operand zero denominator", func(t *testing.T) {
		rank := Val(10.0).Div(IntOperand(0))
		_, err := rank.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "division by zero")
	})

	t.Run("non-zero denominator succeeds", func(t *testing.T) {
		rank := Val(10.0).Div(Val(2.0))
		data, err := rank.MarshalJSON()
		require.NoError(t, err)
		require.JSONEq(t, `{"$div":{"left":{"$val":10},"right":{"$val":2}}}`, string(data))
	})
}

func TestUnknownRankError(t *testing.T) {
	t.Run("unknown rank errors on marshal", func(t *testing.T) {
		unknown := &UnknownRank{}
		_, err := unknown.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown operand type")
	})

	t.Run("unknown rank in expression errors on marshal", func(t *testing.T) {
		// UnknownRank embedded in an expression should still error
		rank := Val(10.0).Add(&UnknownRank{})
		_, err := rank.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown operand type")
	})
}

func TestComplexExpressions(t *testing.T) {
	t.Run("weighted combination", func(t *testing.T) {
		// weighted_combo = knn1 * 0.7 + knn2 * 0.3
		knn1 := mustNewKnnRank(t, KnnQueryText("machine learning"))
		knn2 := mustNewKnnRank(t, KnnQueryText("machine learning"), WithKnnKey(K("sparse_embedding")))
		rank := knn1.Multiply(FloatOperand(0.7)).Add(knn2.Multiply(FloatOperand(0.3)))

		data, err := rank.MarshalJSON()
		require.NoError(t, err)

		// Verify structure
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		require.Contains(t, result, "$sum")
	})

	t.Run("log compression", func(t *testing.T) {
		// (knn + 1).log()
		knn := mustNewKnnRank(t, KnnQueryText("deep learning"))
		rank := knn.Add(FloatOperand(1)).Log()

		data, err := rank.MarshalJSON()
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		require.Contains(t, result, "$log")
	})

	t.Run("exponential with clamping", func(t *testing.T) {
		// knn.exp().min(0.0)
		knn := mustNewKnnRank(t, KnnQueryText("AI"))
		rank := knn.Exp().Min(FloatOperand(0.0))

		data, err := rank.MarshalJSON()
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		require.Contains(t, result, "$min")
	})
}

func TestRrfRank(t *testing.T) {
	t.Run("basic rrf", func(t *testing.T) {
		knn1 := mustNewKnnRank(t, KnnQueryText("query1"), WithKnnReturnRank())
		knn2 := mustNewKnnRank(t, KnnQueryText("query2"), WithKnnReturnRank())
		rrf, err := NewRrfRank(
			WithRffRanks(
				knn1.WithWeight(1.0),
				knn2.WithWeight(1.0),
			),
			WithRffK(60),
		)
		require.NoError(t, err)

		data, err := rrf.MarshalJSON()
		require.NoError(t, err)

		// RRF produces: -sum(w/(k+rank))
		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		require.NoError(t, err)
		require.Contains(t, result, "$mul") // negation creates $mul with -1
	})

	t.Run("rrf with custom k", func(t *testing.T) {
		knn := mustNewKnnRank(t, KnnQueryText("test"))
		rrf, err := NewRrfRank(
			WithRffRanks(
				knn.WithWeight(1.0),
			),
			WithRffK(100),
		)
		require.NoError(t, err)
		require.Equal(t, 100, rrf.K)
	})

	t.Run("rrf with normalization", func(t *testing.T) {
		knnA := mustNewKnnRank(t, KnnQueryText("a"))
		knnB := mustNewKnnRank(t, KnnQueryText("b"))
		rrf, err := NewRrfRank(
			WithRffRanks(
				knnA.WithWeight(3.0),
				knnB.WithWeight(1.0),
			),
			WithRffNormalize(),
		)
		require.NoError(t, err)
		require.True(t, rrf.Normalize)

		// Should serialize without error
		_, err = rrf.MarshalJSON()
		require.NoError(t, err)
	})

	t.Run("rrf requires at least one rank", func(t *testing.T) {
		_, err := NewRrfRank()
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one rank")
	})

	t.Run("rrf k must be positive", func(t *testing.T) {
		_, err := NewRrfRank(WithRffK(0))
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be >= 1")
	})

	t.Run("rrf rejects negative weights", func(t *testing.T) {
		knn := mustNewKnnRank(t, KnnQueryText("test"))
		_, err := NewRrfRank(
			WithRffRanks(knn.WithWeight(-0.5)),
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "negative weight")
	})

	t.Run("rrf rejects NaN weights", func(t *testing.T) {
		knn := mustNewKnnRank(t, KnnQueryText("test"))
		_, err := NewRrfRank(
			WithRffRanks(knn.WithWeight(math.NaN())),
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid weight")
	})

	t.Run("rrf rejects Inf weights", func(t *testing.T) {
		knn := mustNewKnnRank(t, KnnQueryText("test"))
		_, err := NewRrfRank(
			WithRffRanks(knn.WithWeight(math.Inf(1))),
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid weight")
	})

	t.Run("rrf detects weight sum overflow on normalize", func(t *testing.T) {
		knn1 := mustNewKnnRank(t, KnnQueryText("a"))
		knn2 := mustNewKnnRank(t, KnnQueryText("b"))
		rrf, err := NewRrfRank(
			WithRffRanks(
				knn1.WithWeight(math.MaxFloat64),
				knn2.WithWeight(math.MaxFloat64),
			),
			WithRffNormalize(),
		)
		require.NoError(t, err)
		_, err = rrf.MarshalJSON()
		require.Error(t, err)
		require.Contains(t, err.Error(), "overflowed")
	})
}

func TestRankWithWeight(t *testing.T) {
	t.Run("knn with weight", func(t *testing.T) {
		knn := mustNewKnnRank(t, KnnQueryText("test"))
		rw := knn.WithWeight(0.5)

		require.Equal(t, knn, rw.Rank)
		require.Equal(t, 0.5, rw.Weight)
	})
}

func TestOperandConversion(t *testing.T) {
	t.Run("int operand", func(t *testing.T) {
		rank := Val(1.0).Add(IntOperand(5))
		data, err := rank.MarshalJSON()
		require.NoError(t, err)
		require.JSONEq(t, `{"$sum":[{"$val":1},{"$val":5}]}`, string(data))
	})

	t.Run("float operand", func(t *testing.T) {
		rank := Val(1.0).Multiply(FloatOperand(2.5))
		data, err := rank.MarshalJSON()
		require.NoError(t, err)
		require.JSONEq(t, `{"$mul":[{"$val":1},{"$val":2.5}]}`, string(data))
	})
}

func TestKnnOptionValidation(t *testing.T) {
	t.Run("limit must be >= 1", func(t *testing.T) {
		knn := &KnnRank{}
		err := WithKnnLimit(0)(knn)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be >= 1")
	})

	t.Run("valid limit", func(t *testing.T) {
		knn := &KnnRank{}
		err := WithKnnLimit(100)(knn)
		require.NoError(t, err)
		require.Equal(t, 100, knn.Limit)
	})
}

func TestMaxExpressionDepthConstant(t *testing.T) {
	// Verify the constant is defined and has a reasonable value
	require.Greater(t, MaxExpressionDepth, 0)
	require.LessOrEqual(t, MaxExpressionDepth, 1000)
}

func TestDeepExpressionChain(t *testing.T) {
	// Create a deeply nested Sub expression (which doesn't flatten)
	// This tests that such expressions can be built and serialized
	var rank Rank = Val(0.0)
	for i := 0; i < 50; i++ {
		rank = rank.Sub(Val(1.0))
	}

	// Should serialize without error (50 < MaxExpressionDepth)
	data, err := rank.MarshalJSON()
	require.NoError(t, err)
	require.NotEmpty(t, data)
}
