//go:build !cloud

package chroma

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

// Generators

func SpaceStrategy() gopter.Gen {
	return gen.OneConstOf(SpaceL2, SpaceCosine, SpaceIP)
}

func HnswConfigStrategy() gopter.Gen {
	return gen.Struct(reflect.TypeOf(HnswIndexConfig{}), map[string]gopter.Gen{
		"EfConstruction": gen.UIntRange(10, 500),
		"MaxNeighbors":   gen.UIntRange(4, 128),
		"EfSearch":       gen.UIntRange(10, 500),
		"NumThreads":     gen.UIntRange(1, 32),
		"BatchSize":      gen.UIntRange(2, 1000),
		"SyncThreshold":  gen.UIntRange(2, 10000),
		"ResizeFactor":   gen.Float64Range(1.0, 2.0),
	})
}

func SpannConfigStrategy() gopter.Gen {
	return gen.Struct(reflect.TypeOf(SpannIndexConfig{}), map[string]gopter.Gen{
		"SearchNprobe":          gen.UIntRange(1, 128),
		"SearchRngFactor":       gen.Const(float64(1.0)),
		"SearchRngEpsilon":      gen.Float64Range(5.0, 10.0),
		"NReplicaCount":         gen.UIntRange(1, 8),
		"WriteRngFactor":        gen.Const(float64(1.0)),
		"WriteRngEpsilon":       gen.Float64Range(5.0, 10.0),
		"SplitThreshold":        gen.UIntRange(50, 200),
		"NumSamplesKmeans":      gen.UIntRange(1, 1000),
		"InitialLambda":         gen.Const(float64(100.0)),
		"ReassignNeighborCount": gen.UIntRange(1, 64),
		"MergeThreshold":        gen.UIntRange(25, 100),
		"NumCentersToMergeTo":   gen.UIntRange(1, 8),
		"WriteNprobe":           gen.UIntRange(1, 64),
		"EfConstruction":        gen.UIntRange(1, 200),
		"EfSearch":              gen.UIntRange(1, 200),
		"MaxNeighbors":          gen.UIntRange(1, 64),
		"Quantize": gen.OneConstOf(
			SpannQuantization(""),
			SpannQuantizationNone,
			SpannQuantizationFourBitRabitQWithUSearch,
		),
	})
}

func MetadataKeyStrategy() gopter.Gen {
	return gen.Identifier().SuchThat(func(s string) bool {
		return len(s) > 0
	})
}

// Test Functions

func TestSchemaCreationProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	properties := gopter.NewProperties(parameters)

	properties.Property("schema always initialized", prop.ForAll(
		func() bool {
			schema, err := NewSchema()
			require.NoError(t, err)
			require.NotNil(t, schema)
			require.NotNil(t, schema.Defaults())
			require.NotNil(t, schema.Keys())
			return true
		},
	))

	properties.Property("schema with vector index option initializes correctly", prop.ForAll(
		func(space Space) bool {
			schema, err := NewSchema(
				WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(space))),
			)
			require.NoError(t, err)
			require.NotNil(t, schema)
			// Vector index is now on #embedding key (Chroma Cloud requirement)
			embeddingKey, ok := schema.GetKey(EmbeddingKey)
			require.True(t, ok)
			require.NotNil(t, embeddingKey)
			require.NotNil(t, embeddingKey.FloatList)
			require.NotNil(t, embeddingKey.FloatList.VectorIndex)
			require.True(t, embeddingKey.FloatList.VectorIndex.Enabled)
			require.Equal(t, space, embeddingKey.FloatList.VectorIndex.Config.Space)
			return true
		},
		SpaceStrategy(),
	))

	properties.Property("schema with hnsw config initializes correctly", prop.ForAll(
		func(cfg HnswIndexConfig) bool {
			schema, err := NewSchema(
				WithDefaultVectorIndex(NewVectorIndexConfig(
					WithSpace(SpaceL2),
					WithHnsw(&cfg),
				)),
			)
			require.NoError(t, err)
			require.NotNil(t, schema)
			// Vector index is now on #embedding key (Chroma Cloud requirement)
			embeddingKey, ok := schema.GetKey(EmbeddingKey)
			require.True(t, ok)
			require.NotNil(t, embeddingKey)
			hnsw := embeddingKey.FloatList.VectorIndex.Config.Hnsw
			require.NotNil(t, hnsw)
			require.Equal(t, cfg.EfConstruction, hnsw.EfConstruction)
			require.Equal(t, cfg.MaxNeighbors, hnsw.MaxNeighbors)
			require.Equal(t, cfg.EfSearch, hnsw.EfSearch)
			return true
		},
		HnswConfigStrategy(),
	))

	properties.Property("schema with spann config initializes correctly", prop.ForAll(
		func(cfg SpannIndexConfig) bool {
			schema, err := NewSchema(
				WithDefaultVectorIndex(NewVectorIndexConfig(
					WithSpace(SpaceCosine),
					WithSpann(&cfg),
				)),
			)
			require.NoError(t, err)
			require.NotNil(t, schema)
			// Vector index is now on #embedding key (Chroma Cloud requirement)
			embeddingKey, ok := schema.GetKey(EmbeddingKey)
			require.True(t, ok)
			require.NotNil(t, embeddingKey)
			spann := embeddingKey.FloatList.VectorIndex.Config.Spann
			require.NotNil(t, spann)
			require.Equal(t, cfg.SearchNprobe, spann.SearchNprobe)
			require.Equal(t, cfg.EfConstruction, spann.EfConstruction)
			require.Equal(t, cfg.MaxNeighbors, spann.MaxNeighbors)
			require.Equal(t, cfg.MergeThreshold, spann.MergeThreshold)
			return true
		},
		SpannConfigStrategy(),
	))

	properties.TestingRun(t)
}

func TestSchemaMarshalProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	properties := gopter.NewProperties(parameters)

	properties.Property("schema marshals to valid JSON", prop.ForAll(
		func(space Space) bool {
			schema, err := NewSchema(
				WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(space))),
			)
			require.NoError(t, err)

			data, err := json.Marshal(schema)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			var result map[string]interface{}
			err = json.Unmarshal(data, &result)
			require.NoError(t, err)
			require.Contains(t, result, "defaults")
			return true
		},
		SpaceStrategy(),
	))

	properties.Property("hnsw config marshals correctly", prop.ForAll(
		func(cfg HnswIndexConfig) bool {
			data, err := json.Marshal(cfg)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			var result map[string]interface{}
			err = json.Unmarshal(data, &result)
			require.NoError(t, err)
			return true
		},
		HnswConfigStrategy(),
	))

	properties.Property("spann config marshals correctly", prop.ForAll(
		func(cfg SpannIndexConfig) bool {
			data, err := json.Marshal(cfg)
			require.NoError(t, err)
			require.NotEmpty(t, data)

			var result map[string]interface{}
			err = json.Unmarshal(data, &result)
			require.NoError(t, err)
			return true
		},
		SpannConfigStrategy(),
	))

	properties.Property("vector index config marshals with space", prop.ForAll(
		func(space Space) bool {
			cfg := NewVectorIndexConfig(WithSpace(space))
			data, err := json.Marshal(cfg)
			require.NoError(t, err)

			var result map[string]interface{}
			err = json.Unmarshal(data, &result)
			require.NoError(t, err)
			require.Equal(t, string(space), result["space"])
			return true
		},
		SpaceStrategy(),
	))

	properties.Property("schema with keys marshals correctly", prop.ForAll(
		func(key string) bool {
			schema, err := NewSchema(WithStringIndex(key))
			require.NoError(t, err)

			data, err := json.Marshal(schema)
			require.NoError(t, err)

			var result map[string]interface{}
			err = json.Unmarshal(data, &result)
			require.NoError(t, err)
			require.Contains(t, result, "keys")

			keys, ok := result["keys"].(map[string]interface{})
			require.True(t, ok)
			require.Contains(t, keys, key)
			return true
		},
		MetadataKeyStrategy(),
	))

	properties.TestingRun(t)
}

func TestSchemaKeyIndexProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	properties := gopter.NewProperties(parameters)

	properties.Property("string index on random keys", prop.ForAll(
		func(key string) bool {
			schema, err := NewSchema(WithStringIndex(key))
			require.NoError(t, err)

			vt, ok := schema.GetKey(key)
			require.True(t, ok)
			require.NotNil(t, vt.String)
			require.NotNil(t, vt.String.StringInvertedIndex)
			require.True(t, vt.String.StringInvertedIndex.Enabled)
			return true
		},
		MetadataKeyStrategy(),
	))

	properties.Property("int index on random keys", prop.ForAll(
		func(key string) bool {
			schema, err := NewSchema(WithIntIndex(key))
			require.NoError(t, err)

			vt, ok := schema.GetKey(key)
			require.True(t, ok)
			require.NotNil(t, vt.Int)
			require.NotNil(t, vt.Int.IntInvertedIndex)
			require.True(t, vt.Int.IntInvertedIndex.Enabled)
			return true
		},
		MetadataKeyStrategy(),
	))

	properties.Property("float index on random keys", prop.ForAll(
		func(key string) bool {
			schema, err := NewSchema(WithFloatIndex(key))
			require.NoError(t, err)

			vt, ok := schema.GetKey(key)
			require.True(t, ok)
			require.NotNil(t, vt.Float)
			require.NotNil(t, vt.Float.FloatInvertedIndex)
			require.True(t, vt.Float.FloatInvertedIndex.Enabled)
			return true
		},
		MetadataKeyStrategy(),
	))

	properties.Property("bool index on random keys", prop.ForAll(
		func(key string) bool {
			schema, err := NewSchema(WithBoolIndex(key))
			require.NoError(t, err)

			vt, ok := schema.GetKey(key)
			require.True(t, ok)
			require.NotNil(t, vt.Bool)
			require.NotNil(t, vt.Bool.BoolInvertedIndex)
			require.True(t, vt.Bool.BoolInvertedIndex.Enabled)
			return true
		},
		MetadataKeyStrategy(),
	))

	properties.Property("fts index on random keys", prop.ForAll(
		func(key string) bool {
			schema, err := NewSchema(WithFtsIndex(key))
			require.NoError(t, err)

			vt, ok := schema.GetKey(key)
			require.True(t, ok)
			require.NotNil(t, vt.String)
			require.NotNil(t, vt.String.FtsIndex)
			require.True(t, vt.String.FtsIndex.Enabled)
			return true
		},
		MetadataKeyStrategy(),
	))

	properties.Property("vector index on random keys", prop.ForAll(
		func(key string, space Space) bool {
			cfg := NewVectorIndexConfig(WithSpace(space))
			schema, err := NewSchema(WithVectorIndex(key, cfg))
			require.NoError(t, err)

			vt, ok := schema.GetKey(key)
			require.True(t, ok)
			require.NotNil(t, vt.FloatList)
			require.NotNil(t, vt.FloatList.VectorIndex)
			require.True(t, vt.FloatList.VectorIndex.Enabled)
			require.Equal(t, space, vt.FloatList.VectorIndex.Config.Space)
			return true
		},
		MetadataKeyStrategy(),
		SpaceStrategy(),
	))

	properties.Property("sparse vector index on random keys", prop.ForAll(
		func(key string, bm25 bool) bool {
			cfg := NewSparseVectorIndexConfig(WithBM25(bm25))
			schema, err := NewSchema(WithSparseVectorIndex(key, cfg))
			require.NoError(t, err)

			vt, ok := schema.GetKey(key)
			require.True(t, ok)
			require.NotNil(t, vt.SparseVector)
			require.NotNil(t, vt.SparseVector.SparseVectorIndex)
			require.True(t, vt.SparseVector.SparseVectorIndex.Enabled)
			require.Equal(t, bm25, vt.SparseVector.SparseVectorIndex.Config.BM25)
			return true
		},
		MetadataKeyStrategy(),
		gen.Bool(),
	))

	properties.TestingRun(t)
}

func TestSchemaConstraintProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	properties := gopter.NewProperties(parameters)

	properties.Property("empty key rejected for string index", prop.ForAll(
		func() bool {
			_, err := NewSchema(WithStringIndex(""))
			require.Error(t, err)
			require.Contains(t, err.Error(), "key cannot be empty")
			return true
		},
	))

	properties.Property("empty key rejected for int index", prop.ForAll(
		func() bool {
			_, err := NewSchema(WithIntIndex(""))
			require.Error(t, err)
			require.Contains(t, err.Error(), "key cannot be empty")
			return true
		},
	))

	properties.Property("empty key rejected for float index", prop.ForAll(
		func() bool {
			_, err := NewSchema(WithFloatIndex(""))
			require.Error(t, err)
			require.Contains(t, err.Error(), "key cannot be empty")
			return true
		},
	))

	properties.Property("empty key rejected for bool index", prop.ForAll(
		func() bool {
			_, err := NewSchema(WithBoolIndex(""))
			require.Error(t, err)
			require.Contains(t, err.Error(), "key cannot be empty")
			return true
		},
	))

	properties.Property("empty key rejected for fts index", prop.ForAll(
		func() bool {
			_, err := NewSchema(WithFtsIndex(""))
			require.Error(t, err)
			require.Contains(t, err.Error(), "key cannot be empty")
			return true
		},
	))

	properties.Property("empty key rejected for vector index", prop.ForAll(
		func() bool {
			cfg := NewVectorIndexConfig(WithSpace(SpaceL2))
			_, err := NewSchema(WithVectorIndex("", cfg))
			require.Error(t, err)
			require.Contains(t, err.Error(), "key cannot be empty")
			return true
		},
	))

	properties.Property("document key fts disable allowed", prop.ForAll(
		func() bool {
			schema, err := NewSchema(DisableFtsIndex(DocumentKey))
			require.NoError(t, err)
			vt, ok := schema.GetKey(DocumentKey)
			require.True(t, ok)
			require.NotNil(t, vt.String)
			require.NotNil(t, vt.String.FtsIndex)
			require.False(t, vt.String.FtsIndex.Enabled)
			return true
		},
	))

	properties.Property("nil vector config rejected", prop.ForAll(
		func() bool {
			_, err := NewSchema(WithDefaultVectorIndex(nil))
			require.Error(t, err)
			require.Contains(t, err.Error(), "vector index config cannot be nil")
			return true
		},
	))

	properties.Property("nil sparse vector config rejected", prop.ForAll(
		func() bool {
			_, err := NewSchema(WithDefaultSparseVectorIndex(nil))
			require.Error(t, err)
			require.Contains(t, err.Error(), "sparse vector index config cannot be nil")
			return true
		},
	))

	properties.TestingRun(t)
}

func TestSchemaAccessorProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	properties := gopter.NewProperties(parameters)

	properties.Property("Keys() returns configured keys", prop.ForAll(
		func(key1, key2 string) bool {
			if key1 == key2 {
				return true // skip if keys are the same
			}
			schema, err := NewSchema(
				WithStringIndex(key1),
				WithIntIndex(key2),
			)
			require.NoError(t, err)

			keys := schema.Keys()
			require.Equal(t, 2, len(keys))
			require.Contains(t, keys, key1)
			require.Contains(t, keys, key2)
			return true
		},
		MetadataKeyStrategy(),
		MetadataKeyStrategy(),
	))

	properties.Property("GetKey() retrieves correct config", prop.ForAll(
		func(key string) bool {
			schema, err := NewSchema(WithStringIndex(key))
			require.NoError(t, err)

			vt, ok := schema.GetKey(key)
			require.True(t, ok)
			require.NotNil(t, vt)
			require.NotNil(t, vt.String)
			require.NotNil(t, vt.String.StringInvertedIndex)
			return true
		},
		MetadataKeyStrategy(),
	))

	properties.Property("GetKey() returns false for missing key", prop.ForAll(
		func(key string) bool {
			schema, err := NewSchema()
			require.NoError(t, err)

			_, ok := schema.GetKey(key)
			require.False(t, ok)
			return true
		},
		MetadataKeyStrategy(),
	))

	properties.Property("Defaults() and GetKey() return correct vector index location", prop.ForAll(
		func(space Space) bool {
			schema, err := NewSchema(
				WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(space))),
			)
			require.NoError(t, err)

			defaults := schema.Defaults()
			require.NotNil(t, defaults)
			// Vector index is now on #embedding key, not defaults (Chroma Cloud requirement)
			embeddingKey, ok := schema.GetKey(EmbeddingKey)
			require.True(t, ok)
			require.NotNil(t, embeddingKey.FloatList)
			require.NotNil(t, embeddingKey.FloatList.VectorIndex)
			require.Equal(t, space, embeddingKey.FloatList.VectorIndex.Config.Space)
			return true
		},
		SpaceStrategy(),
	))

	properties.Property("multiple index types on same key", prop.ForAll(
		func(key string) bool {
			schema, err := NewSchema(
				WithStringIndex(key),
				WithFtsIndex(key),
			)
			require.NoError(t, err)

			vt, ok := schema.GetKey(key)
			require.True(t, ok)
			require.NotNil(t, vt.String)
			require.NotNil(t, vt.String.StringInvertedIndex)
			require.True(t, vt.String.StringInvertedIndex.Enabled)
			require.NotNil(t, vt.String.FtsIndex)
			require.True(t, vt.String.FtsIndex.Enabled)
			return true
		},
		MetadataKeyStrategy(),
	))

	properties.TestingRun(t)
}
