package embeddings

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshalEmbeddings(t *testing.T) {
	embed := NewEmbeddingFromFloat32([]float32{1.1234567891, 2.4, 3.5})

	bytes, err := json.Marshal(embed)
	require.NoError(t, err)
	require.JSONEq(t, `[1.1234568,2.4,3.5]`, string(bytes))
}

func TestUnmarshalEmbeddings(t *testing.T) {
	var embed Float32Embedding
	jsonStr := `[1.1234568,2.4,3.5]`

	err := json.Unmarshal([]byte(jsonStr), &embed)
	require.NoError(t, err)
	require.Equal(t, 3, embed.Len())
	require.Equal(t, float32(1.1234568), embed.ContentAsFloat32()[0])
	require.Equal(t, float32(2.4), embed.ContentAsFloat32()[1])
	require.Equal(t, float32(3.5), embed.ContentAsFloat32()[2])
}

func TestSparseVectorValidate(t *testing.T) {
	t.Run("valid sparse vector", func(t *testing.T) {
		sv, err := NewSparseVector([]int{1, 5, 10}, []float32{0.5, 0.3, 0.8})
		require.NoError(t, err)
		require.NoError(t, sv.Validate())
	})

	t.Run("mismatched lengths", func(t *testing.T) {
		_, err := NewSparseVector([]int{1, 5, 10}, []float32{0.5})
		require.Error(t, err)
		require.Contains(t, err.Error(), "same length")
	})

	t.Run("nil sparse vector", func(t *testing.T) {
		var sv *SparseVector
		err := sv.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "nil")
	})

	t.Run("empty sparse vector is valid", func(t *testing.T) {
		sv, err := NewSparseVector([]int{}, []float32{})
		require.NoError(t, err)
		require.NoError(t, sv.Validate())
	})

	t.Run("negative index at construction", func(t *testing.T) {
		_, err := NewSparseVector([]int{1, -5, 10}, []float32{0.5, 0.3, 0.8})
		require.Error(t, err)
		require.Contains(t, err.Error(), "negative")
		require.Contains(t, err.Error(), "position 1")
	})

	t.Run("negative index in validate", func(t *testing.T) {
		sv := &SparseVector{
			Indices: []int{0, -1, 2},
			Values:  []float32{0.1, 0.2, 0.3},
		}
		err := sv.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "negative")
	})

	t.Run("duplicate index at construction", func(t *testing.T) {
		_, err := NewSparseVector([]int{1, 5, 1}, []float32{0.1, 0.2, 0.3})
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate index")
	})

	t.Run("duplicate index in validate", func(t *testing.T) {
		sv := &SparseVector{
			Indices: []int{1, 5, 1},
			Values:  []float32{0.1, 0.2, 0.3},
		}
		err := sv.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "duplicate index")
	})

	t.Run("NaN value at construction", func(t *testing.T) {
		nan := float32(math.NaN())
		_, err := NewSparseVector([]int{1, 2}, []float32{0.5, nan})
		require.Error(t, err)
		require.Contains(t, err.Error(), "NaN")
	})

	t.Run("NaN value in validate", func(t *testing.T) {
		nan := float32(math.NaN())
		sv := &SparseVector{
			Indices: []int{1, 2},
			Values:  []float32{0.5, nan},
		}
		err := sv.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "NaN")
	})

	t.Run("positive infinity at construction", func(t *testing.T) {
		inf := float32(math.Inf(1))
		_, err := NewSparseVector([]int{1, 2}, []float32{0.5, inf})
		require.Error(t, err)
		require.Contains(t, err.Error(), "infinite")
	})

	t.Run("negative infinity at construction", func(t *testing.T) {
		inf := float32(math.Inf(-1))
		_, err := NewSparseVector([]int{1, 2}, []float32{inf, 0.5})
		require.Error(t, err)
		require.Contains(t, err.Error(), "infinite")
	})

	t.Run("infinity in validate", func(t *testing.T) {
		inf := float32(math.Inf(1))
		sv := &SparseVector{
			Indices: []int{1, 2},
			Values:  []float32{0.5, inf},
		}
		err := sv.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "infinite")
	})
}

func TestConfigInt(t *testing.T) {
	t.Run("direct int value", func(t *testing.T) {
		cfg := EmbeddingFunctionConfig{"value": 42}
		val, ok := ConfigInt(cfg, "value")
		require.True(t, ok)
		require.Equal(t, 42, val)
	})

	t.Run("float64 from JSON unmarshal", func(t *testing.T) {
		// Simulate JSON unmarshaling where numbers become float64
		jsonStr := `{"value": 42}`
		var cfg EmbeddingFunctionConfig
		err := json.Unmarshal([]byte(jsonStr), &cfg)
		require.NoError(t, err)

		val, ok := ConfigInt(cfg, "value")
		require.True(t, ok)
		require.Equal(t, 42, val)
	})

	t.Run("missing key", func(t *testing.T) {
		cfg := EmbeddingFunctionConfig{}
		_, ok := ConfigInt(cfg, "value")
		require.False(t, ok)
	})

	t.Run("wrong type", func(t *testing.T) {
		cfg := EmbeddingFunctionConfig{"value": "not a number"}
		_, ok := ConfigInt(cfg, "value")
		require.False(t, ok)
	})
}

func TestConfigFloat64(t *testing.T) {
	t.Run("direct float64 value", func(t *testing.T) {
		cfg := EmbeddingFunctionConfig{"value": 3.14}
		val, ok := ConfigFloat64(cfg, "value")
		require.True(t, ok)
		require.Equal(t, 3.14, val)
	})

	t.Run("float64 from JSON unmarshal", func(t *testing.T) {
		jsonStr := `{"value": 3.14}`
		var cfg EmbeddingFunctionConfig
		err := json.Unmarshal([]byte(jsonStr), &cfg)
		require.NoError(t, err)

		val, ok := ConfigFloat64(cfg, "value")
		require.True(t, ok)
		require.Equal(t, 3.14, val)
	})

	t.Run("int converted to float64", func(t *testing.T) {
		cfg := EmbeddingFunctionConfig{"value": 42}
		val, ok := ConfigFloat64(cfg, "value")
		require.True(t, ok)
		require.Equal(t, 42.0, val)
	})

	t.Run("missing key", func(t *testing.T) {
		cfg := EmbeddingFunctionConfig{}
		_, ok := ConfigFloat64(cfg, "value")
		require.False(t, ok)
	})
}

func TestConfigStringSlice(t *testing.T) {
	t.Run("direct string slice", func(t *testing.T) {
		cfg := EmbeddingFunctionConfig{"words": []string{"hello", "world"}}
		val, ok := ConfigStringSlice(cfg, "words")
		require.True(t, ok)
		require.Equal(t, []string{"hello", "world"}, val)
	})

	t.Run("[]any from JSON unmarshal", func(t *testing.T) {
		// Simulate JSON unmarshaling where arrays become []any
		jsonStr := `{"words": ["hello", "world"]}`
		var cfg EmbeddingFunctionConfig
		err := json.Unmarshal([]byte(jsonStr), &cfg)
		require.NoError(t, err)

		val, ok := ConfigStringSlice(cfg, "words")
		require.True(t, ok)
		require.Equal(t, []string{"hello", "world"}, val)
	})

	t.Run("missing key", func(t *testing.T) {
		cfg := EmbeddingFunctionConfig{}
		_, ok := ConfigStringSlice(cfg, "words")
		require.False(t, ok)
	})

	t.Run("mixed types in array", func(t *testing.T) {
		cfg := EmbeddingFunctionConfig{"words": []any{"hello", 42}}
		_, ok := ConfigStringSlice(cfg, "words")
		require.False(t, ok) // Should fail because not all elements are strings
	})
}

func TestConfigJSONRoundTrip(t *testing.T) {
	t.Run("full config round-trip", func(t *testing.T) {
		original := EmbeddingFunctionConfig{
			"int_value":    42,
			"float_value":  3.14,
			"string_value": "hello",
			"string_slice": []string{"a", "b", "c"},
		}

		// Marshal to JSON
		jsonBytes, err := json.Marshal(original)
		require.NoError(t, err)

		// Unmarshal from JSON
		var restored EmbeddingFunctionConfig
		err = json.Unmarshal(jsonBytes, &restored)
		require.NoError(t, err)

		// Verify values can be extracted with helpers
		intVal, ok := ConfigInt(restored, "int_value")
		require.True(t, ok)
		require.Equal(t, 42, intVal)

		floatVal, ok := ConfigFloat64(restored, "float_value")
		require.True(t, ok)
		require.Equal(t, 3.14, floatVal)

		strVal, ok := restored["string_value"].(string)
		require.True(t, ok)
		require.Equal(t, "hello", strVal)

		sliceVal, ok := ConfigStringSlice(restored, "string_slice")
		require.True(t, ok)
		require.Equal(t, []string{"a", "b", "c"}, sliceVal)
	})
}
