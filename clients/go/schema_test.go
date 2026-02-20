package chroma

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"

	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/bm25"
)

func TestNewSchema(t *testing.T) {
	schema, err := NewSchema()
	require.NoError(t, err)
	assert.NotNil(t, schema)
	assert.NotNil(t, schema.Defaults())
	assert.Equal(t, 0, len(schema.Keys()))
}

func TestNewSchemaWithDefaults(t *testing.T) {
	schema, err := NewSchemaWithDefaults()
	require.NoError(t, err)
	assert.NotNil(t, schema)

	// Check vector index exists on #embedding key with L2 space
	embeddingVT, ok := schema.GetKey(EmbeddingKey)
	assert.True(t, ok)
	assert.NotNil(t, embeddingVT.FloatList)
	assert.NotNil(t, embeddingVT.FloatList.VectorIndex)
	assert.True(t, embeddingVT.FloatList.VectorIndex.Enabled)
	assert.Equal(t, SpaceL2, embeddingVT.FloatList.VectorIndex.Config.Space)

	// Other indexes (FTS, string, int, float, bool) are enabled by default
	// in Chroma, so they don't need to be explicitly set in the schema
}

func TestNewSchema_WithOptions(t *testing.T) {
	schema, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(
			WithSpace(SpaceCosine),
			WithHnsw(NewHnswConfig(
				WithEfConstruction(200),
				WithMaxNeighbors(32),
				WithEfSearch(20),
			)),
		)),
	)
	require.NoError(t, err)
	assert.NotNil(t, schema)

	// Verify vector config is on #embedding key
	embeddingVT, ok := schema.GetKey(EmbeddingKey)
	assert.True(t, ok)
	assert.NotNil(t, embeddingVT.FloatList)
	assert.NotNil(t, embeddingVT.FloatList.VectorIndex)
	assert.True(t, embeddingVT.FloatList.VectorIndex.Enabled)
	assert.Equal(t, SpaceCosine, embeddingVT.FloatList.VectorIndex.Config.Space)
	assert.Equal(t, uint(200), embeddingVT.FloatList.VectorIndex.Config.Hnsw.EfConstruction)
	assert.Equal(t, uint(32), embeddingVT.FloatList.VectorIndex.Config.Hnsw.MaxNeighbors)
	assert.Equal(t, uint(20), embeddingVT.FloatList.VectorIndex.Config.Hnsw.EfSearch)
}

func TestNewSchema_WithKeyOverrides(t *testing.T) {
	schema, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
		WithStringIndex("category"),
		WithIntIndex("price"),
	)
	require.NoError(t, err)
	assert.NotNil(t, schema)

	// Check keys were created (3 keys: #embedding, category, price)
	keys := schema.Keys()
	assert.Equal(t, 3, len(keys))
	assert.Contains(t, keys, EmbeddingKey)
	assert.Contains(t, keys, "category")
	assert.Contains(t, keys, "price")

	// Check category key has string inverted index
	categoryVT, ok := schema.GetKey("category")
	assert.True(t, ok)
	assert.NotNil(t, categoryVT.String)
	assert.NotNil(t, categoryVT.String.StringInvertedIndex)
	assert.True(t, categoryVT.String.StringInvertedIndex.Enabled)

	// Check price key has int inverted index
	priceVT, ok := schema.GetKey("price")
	assert.True(t, ok)
	assert.NotNil(t, priceVT.Int)
	assert.NotNil(t, priceVT.Int.IntInvertedIndex)
	assert.True(t, priceVT.Int.IntInvertedIndex.Enabled)
}

func TestNewSchema_ErrorHandling(t *testing.T) {
	// Test nil vector config
	_, err := NewSchema(WithDefaultVectorIndex(nil))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "vector index config cannot be nil")

	// Test nil sparse vector config
	_, err = NewSchema(WithDefaultSparseVectorIndex(nil))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sparse vector index config cannot be nil")

	// Test empty key
	_, err = NewSchema(WithStringIndex(""))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key cannot be empty")
}

func TestHnswConfig_Options(t *testing.T) {
	config := NewHnswConfig(
		WithEfConstruction(200),
		WithMaxNeighbors(32),
		WithEfSearch(50),
		WithNumThreads(8),
		WithBatchSize(1000),
		WithSyncThreshold(2000),
		WithResizeFactor(1.5),
	)

	assert.Equal(t, uint(200), config.EfConstruction)
	assert.Equal(t, uint(32), config.MaxNeighbors)
	assert.Equal(t, uint(50), config.EfSearch)
	assert.Equal(t, uint(8), config.NumThreads)
	assert.Equal(t, uint(1000), config.BatchSize)
	assert.Equal(t, uint(2000), config.SyncThreshold)
	assert.Equal(t, 1.5, config.ResizeFactor)
}

func TestHnswConfig_Defaults(t *testing.T) {
	config, err := NewHnswConfigWithDefaults()
	require.NoError(t, err)

	assert.Equal(t, uint(100), config.EfConstruction)
	assert.Equal(t, uint(16), config.MaxNeighbors)
	assert.Equal(t, uint(100), config.EfSearch)
	assert.Equal(t, uint(1), config.NumThreads)
	assert.Equal(t, uint(100), config.BatchSize)
	assert.Equal(t, uint(1000), config.SyncThreshold)
	assert.Equal(t, 1.2, config.ResizeFactor)
}

func TestHnswConfig_DefaultsWithOverride(t *testing.T) {
	config, err := NewHnswConfigWithDefaults(
		WithEfConstruction(200),
		WithMaxNeighbors(32),
	)
	require.NoError(t, err)

	assert.Equal(t, uint(200), config.EfConstruction)
	assert.Equal(t, uint(32), config.MaxNeighbors)
	// Other values should be defaults
	assert.Equal(t, uint(100), config.EfSearch)
	assert.Equal(t, uint(1000), config.SyncThreshold)
}

func TestHnswConfig_ValidationRejectsInvalid(t *testing.T) {
	// BatchSize < 2 should fail
	_, err := NewHnswConfigWithDefaults(WithBatchSize(1))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")

	// SyncThreshold < 2 should fail
	_, err = NewHnswConfigWithDefaults(WithSyncThreshold(1))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")

	// BatchSize = 0 should fail
	_, err = NewHnswConfigWithDefaults(WithBatchSize(0))
	assert.Error(t, err)

	// SyncThreshold = 0 should fail
	_, err = NewHnswConfigWithDefaults(WithSyncThreshold(0))
	assert.Error(t, err)
}

func TestSpannConfig_Options(t *testing.T) {
	config := NewSpannConfig(
		WithSpannSearchNprobe(64),
		WithSpannSearchRngFactor(1.0),
		WithSpannSearchRngEpsilon(10.0),
		WithSpannNReplicaCount(8),
		WithSpannWriteRngFactor(1.0),
		WithSpannWriteRngEpsilon(5.0),
		WithSpannSplitThreshold(50),
		WithSpannNumSamplesKmeans(1000),
		WithSpannInitialLambda(100.0),
		WithSpannReassignNeighborCount(64),
		WithSpannMergeThreshold(25),
		WithSpannNumCentersToMergeTo(8),
		WithSpannWriteNprobe(32),
		WithSpannEfConstruction(200),
		WithSpannEfSearch(200),
		WithSpannMaxNeighbors(64),
		WithSpannQuantize(SpannQuantizationNone),
	)

	assert.Equal(t, uint(64), config.SearchNprobe)
	assert.Equal(t, 1.0, config.SearchRngFactor)
	assert.Equal(t, 10.0, config.SearchRngEpsilon)
	assert.Equal(t, uint(8), config.NReplicaCount)
	assert.Equal(t, 1.0, config.WriteRngFactor)
	assert.Equal(t, 5.0, config.WriteRngEpsilon)
	assert.Equal(t, uint(50), config.SplitThreshold)
	assert.Equal(t, uint(1000), config.NumSamplesKmeans)
	assert.Equal(t, 100.0, config.InitialLambda)
	assert.Equal(t, uint(64), config.ReassignNeighborCount)
	assert.Equal(t, uint(25), config.MergeThreshold)
	assert.Equal(t, uint(8), config.NumCentersToMergeTo)
	assert.Equal(t, uint(32), config.WriteNprobe)
	assert.Equal(t, uint(200), config.EfConstruction)
	assert.Equal(t, uint(200), config.EfSearch)
	assert.Equal(t, uint(64), config.MaxNeighbors)
	assert.Equal(t, SpannQuantizationNone, config.Quantize)
}

func TestVectorIndexConfig_Options(t *testing.T) {
	hnswCfg := NewHnswConfig(WithEfConstruction(100))
	config := NewVectorIndexConfig(
		WithSpace(SpaceIP),
		WithSourceKey(DocumentKey),
		WithHnsw(hnswCfg),
	)

	assert.Equal(t, SpaceIP, config.Space)
	assert.Equal(t, DocumentKey, config.SourceKey)
	assert.NotNil(t, config.Hnsw)
	assert.Equal(t, uint(100), config.Hnsw.EfConstruction)
}

func TestVectorIndexConfig_WithSpann(t *testing.T) {
	spannCfg := NewSpannConfig(
		WithSpannSearchNprobe(64),
		WithSpannEfConstruction(200),
	)
	config := NewVectorIndexConfig(
		WithSpace(SpaceCosine),
		WithSpann(spannCfg),
	)

	assert.Equal(t, SpaceCosine, config.Space)
	assert.NotNil(t, config.Spann)
	assert.Equal(t, uint(64), config.Spann.SearchNprobe)
	assert.Equal(t, uint(200), config.Spann.EfConstruction)
}

func TestSpannConfig_Defaults(t *testing.T) {
	config, err := NewSpannConfigWithDefaults()
	require.NoError(t, err)

	assert.Equal(t, uint(64), config.SearchNprobe)
	assert.Equal(t, 1.0, config.SearchRngFactor)
	assert.Equal(t, 10.0, config.SearchRngEpsilon)
	assert.Equal(t, uint(8), config.NReplicaCount)
	assert.Equal(t, 1.0, config.WriteRngFactor)
	assert.Equal(t, 5.0, config.WriteRngEpsilon)
	assert.Equal(t, uint(50), config.SplitThreshold)
	assert.Equal(t, uint(1000), config.NumSamplesKmeans)
	assert.Equal(t, 100.0, config.InitialLambda)
	assert.Equal(t, uint(64), config.ReassignNeighborCount)
	assert.Equal(t, uint(25), config.MergeThreshold)
	assert.Equal(t, uint(8), config.NumCentersToMergeTo)
	assert.Equal(t, uint(32), config.WriteNprobe)
	assert.Equal(t, uint(200), config.EfConstruction)
	assert.Equal(t, uint(200), config.EfSearch)
	assert.Equal(t, uint(64), config.MaxNeighbors)
	assert.Empty(t, config.Quantize)
}

func TestSpannConfig_DefaultsWithOverride(t *testing.T) {
	config, err := NewSpannConfigWithDefaults(
		WithSpannSearchNprobe(100),
		WithSpannMergeThreshold(50),
	)
	require.NoError(t, err)

	assert.Equal(t, uint(100), config.SearchNprobe)
	assert.Equal(t, uint(50), config.MergeThreshold)
	// Other values should be defaults
	assert.Equal(t, uint(64), config.MaxNeighbors)
}

func TestSpannConfig_ValidationRejectsInvalid(t *testing.T) {
	// SearchNprobe > 128 should fail
	_, err := NewSpannConfigWithDefaults(WithSpannSearchNprobe(200))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")

	// MergeThreshold < 25 should fail
	_, err = NewSpannConfigWithDefaults(WithSpannMergeThreshold(10))
	assert.Error(t, err)

	// MergeThreshold > 100 should fail
	_, err = NewSpannConfigWithDefaults(WithSpannMergeThreshold(150))
	assert.Error(t, err)

	// SplitThreshold < 50 should fail
	_, err = NewSpannConfigWithDefaults(WithSpannSplitThreshold(25))
	assert.Error(t, err)

	// NReplicaCount > 8 should fail
	_, err = NewSpannConfigWithDefaults(WithSpannNReplicaCount(10))
	assert.Error(t, err)

	// Unknown quantization value should fail
	_, err = NewSpannConfigWithDefaults(WithSpannQuantize(SpannQuantization("invalid")))
	assert.Error(t, err)
}

func TestSparseVectorIndexConfig_Options(t *testing.T) {
	config := NewSparseVectorIndexConfig(
		WithSparseSourceKey(DocumentKey),
		WithBM25(true),
	)

	assert.Equal(t, DocumentKey, config.SourceKey)
	assert.True(t, config.BM25)
}

func TestSchema_MultipleKeyOptions(t *testing.T) {
	schema, err := NewSchema(
		WithStringIndex("field1"),
		WithIntIndex("field2"),
		WithFloatIndex("field3"),
		WithBoolIndex("field4"),
		WithFtsIndex("field5"),
	)
	require.NoError(t, err)

	// Verify all keys were created
	keys := schema.Keys()
	assert.Equal(t, 5, len(keys))

	// Verify each key has correct index type
	vt, ok := schema.GetKey("field1")
	assert.True(t, ok)
	assert.NotNil(t, vt.String.StringInvertedIndex)

	vt, ok = schema.GetKey("field2")
	assert.True(t, ok)
	assert.NotNil(t, vt.Int.IntInvertedIndex)
}

func TestSchema_WithVectorIndex(t *testing.T) {
	cfg := NewVectorIndexConfig(WithSpace(SpaceCosine))
	schema, err := NewSchema(
		WithVectorIndex(EmbeddingKey, cfg),
	)
	require.NoError(t, err)

	vt, ok := schema.GetKey(EmbeddingKey)
	assert.True(t, ok)
	assert.NotNil(t, vt.FloatList)
	assert.NotNil(t, vt.FloatList.VectorIndex)
	assert.True(t, vt.FloatList.VectorIndex.Enabled)
	assert.Equal(t, SpaceCosine, vt.FloatList.VectorIndex.Config.Space)
}

func TestSchema_MarshalJSON(t *testing.T) {
	schema, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
		WithStringIndex("my_field"),
	)
	require.NoError(t, err)

	data, err := json.Marshal(schema)
	require.NoError(t, err)
	assert.NotNil(t, data)

	// Verify JSON structure
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Contains(t, result, "defaults")
	assert.Contains(t, result, "keys")

	// Verify keys contains my_field
	keysMap, ok := result["keys"].(map[string]interface{})
	assert.True(t, ok)
	assert.Contains(t, keysMap, "my_field")
}

func TestSchema_UnmarshalJSON(t *testing.T) {
	// Create a schema, marshal it, then unmarshal
	original, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
		WithStringIndex("test_field"),
	)
	require.NoError(t, err)

	data, err := json.Marshal(original)
	require.NoError(t, err)

	// Unmarshal into new schema
	unmarshaled := &Schema{}
	err = json.Unmarshal(data, unmarshaled)
	require.NoError(t, err)

	// Verify structure preserved
	assert.NotNil(t, unmarshaled.Defaults())
	assert.Equal(t, len(original.Keys()), len(unmarshaled.Keys()))
}

func TestSpaceConstants(t *testing.T) {
	assert.Equal(t, Space("l2"), SpaceL2)
	assert.Equal(t, Space("cosine"), SpaceCosine)
	assert.Equal(t, Space("ip"), SpaceIP)
}

func TestSpannQuantizationConstantAliases(t *testing.T) {
	assert.Equal(t, SpannQuantization("four_bit_rabit_q_with_u_search"), SpannQuantizationFourBitRabitQWithUSearch)
	assert.Equal(t, SpannQuantizationFourBitRabitQWithUSearch, SpannQuantizationFourBitRabbitQWithUSearch)
}

func TestReservedKeyConstants(t *testing.T) {
	assert.Equal(t, "#document", DocumentKey)
	assert.Equal(t, "#embedding", EmbeddingKey)
}

// Disable options tests

func TestDisableStringIndex(t *testing.T) {
	schema, err := NewSchema(
		DisableStringIndex("excluded_field"),
	)
	require.NoError(t, err)

	vt, ok := schema.GetKey("excluded_field")
	assert.True(t, ok)
	assert.NotNil(t, vt.String)
	assert.NotNil(t, vt.String.StringInvertedIndex)
	assert.False(t, vt.String.StringInvertedIndex.Enabled)
}

func TestDisableIntIndex(t *testing.T) {
	schema, err := NewSchema(DisableIntIndex("temp_id"))
	require.NoError(t, err)

	vt, ok := schema.GetKey("temp_id")
	assert.True(t, ok)
	assert.False(t, vt.Int.IntInvertedIndex.Enabled)
}

func TestDisableFloatIndex(t *testing.T) {
	schema, err := NewSchema(DisableFloatIndex("temp_score"))
	require.NoError(t, err)

	vt, ok := schema.GetKey("temp_score")
	assert.True(t, ok)
	assert.False(t, vt.Float.FloatInvertedIndex.Enabled)
}

func TestDisableBoolIndex(t *testing.T) {
	schema, err := NewSchema(DisableBoolIndex("temp_flag"))
	require.NoError(t, err)

	vt, ok := schema.GetKey("temp_flag")
	assert.True(t, ok)
	assert.False(t, vt.Bool.BoolInvertedIndex.Enabled)
}

func TestDisableFtsIndex(t *testing.T) {
	schema, err := NewSchema(DisableFtsIndex("notes"))
	require.NoError(t, err)

	vt, ok := schema.GetKey("notes")
	assert.True(t, ok)
	assert.False(t, vt.String.FtsIndex.Enabled)
}

func TestDisableFtsIndex_DocumentKey(t *testing.T) {
	schema, err := NewSchema(DisableFtsIndex(DocumentKey))
	require.NoError(t, err)

	vt, ok := schema.GetKey(DocumentKey)
	assert.True(t, ok)
	require.NotNil(t, vt.String)
	require.NotNil(t, vt.String.FtsIndex)
	assert.False(t, vt.String.FtsIndex.Enabled)
}

func TestDisableFtsIndex_CannotDisableEmbedding(t *testing.T) {
	_, err := NewSchema(DisableFtsIndex(EmbeddingKey))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot disable FTS index on reserved key")
}

func TestDisableStringIndex_CannotDisableReservedKeys(t *testing.T) {
	_, err := NewSchema(DisableStringIndex(DocumentKey))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot disable string index on reserved key")

	_, err = NewSchema(DisableStringIndex(EmbeddingKey))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot disable string index on reserved key")
}

func TestDisableIntIndex_CannotDisableReservedKeys(t *testing.T) {
	_, err := NewSchema(DisableIntIndex(DocumentKey))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot disable int index on reserved key")

	_, err = NewSchema(DisableIntIndex(EmbeddingKey))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot disable int index on reserved key")
}

func TestDisableFloatIndex_CannotDisableReservedKeys(t *testing.T) {
	_, err := NewSchema(DisableFloatIndex(DocumentKey))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot disable float index on reserved key")

	_, err = NewSchema(DisableFloatIndex(EmbeddingKey))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot disable float index on reserved key")
}

func TestDisableBoolIndex_CannotDisableReservedKeys(t *testing.T) {
	_, err := NewSchema(DisableBoolIndex(DocumentKey))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot disable bool index on reserved key")

	_, err = NewSchema(DisableBoolIndex(EmbeddingKey))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot disable bool index on reserved key")
}

func TestWithDefaultFtsIndex_NilConfig(t *testing.T) {
	_, err := NewSchema(WithDefaultFtsIndex(nil))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "FTS index config cannot be nil")
}

func TestDisableDefaultStringIndex(t *testing.T) {
	schema, err := NewSchema(DisableDefaultStringIndex())
	require.NoError(t, err)
	assert.False(t, schema.Defaults().String.StringInvertedIndex.Enabled)
}

func TestDisableDefaultIntIndex(t *testing.T) {
	schema, err := NewSchema(DisableDefaultIntIndex())
	require.NoError(t, err)
	assert.False(t, schema.Defaults().Int.IntInvertedIndex.Enabled)
}

func TestDisableDefaultFloatIndex(t *testing.T) {
	schema, err := NewSchema(DisableDefaultFloatIndex())
	require.NoError(t, err)
	assert.False(t, schema.Defaults().Float.FloatInvertedIndex.Enabled)
}

func TestDisableDefaultBoolIndex(t *testing.T) {
	schema, err := NewSchema(DisableDefaultBoolIndex())
	require.NoError(t, err)
	assert.False(t, schema.Defaults().Bool.BoolInvertedIndex.Enabled)
}

func TestDisableDocumentFtsIndex(t *testing.T) {
	schema, err := NewSchema(DisableDocumentFtsIndex())
	require.NoError(t, err)

	vt, ok := schema.GetKey(DocumentKey)
	assert.True(t, ok)
	require.NotNil(t, vt.String)
	require.NotNil(t, vt.String.FtsIndex)
	assert.False(t, vt.String.FtsIndex.Enabled)
}

func TestDisableDefaultFtsIndex(t *testing.T) {
	schema, err := NewSchema(DisableDefaultFtsIndex())
	require.NoError(t, err)

	vt, ok := schema.GetKey(DocumentKey)
	assert.True(t, ok)
	require.NotNil(t, vt.String)
	require.NotNil(t, vt.String.FtsIndex)
	assert.False(t, vt.String.FtsIndex.Enabled)
	require.NotNil(t, schema.Defaults().String)
	require.NotNil(t, schema.Defaults().String.FtsIndex)
	assert.False(t, schema.Defaults().String.FtsIndex.Enabled)
}

func TestSchemaIsFtsEnabled_DefaultsToTrue(t *testing.T) {
	var nilSchema *Schema
	assert.True(t, nilSchema.IsFtsEnabled())

	schema, err := NewSchema()
	require.NoError(t, err)
	assert.True(t, schema.IsFtsEnabled())
}

func TestSchemaIsFtsEnabled_LegacyDefaultsFallback(t *testing.T) {
	schema := &Schema{
		defaults: &ValueTypes{
			String: &StringValueType{
				FtsIndex: &FtsIndexType{
					Enabled: false,
					Config:  &FtsIndexConfig{},
				},
			},
		},
		keys: map[string]*ValueTypes{},
	}
	assert.False(t, schema.IsFtsEnabled())
}

func TestSchemaIsFtsEnabled_DocumentKeyPrecedence(t *testing.T) {
	schema := &Schema{
		defaults: &ValueTypes{
			String: &StringValueType{
				FtsIndex: &FtsIndexType{
					Enabled: false,
					Config:  &FtsIndexConfig{},
				},
			},
		},
		keys: map[string]*ValueTypes{
			DocumentKey: {
				String: &StringValueType{
					FtsIndex: &FtsIndexType{
						Enabled: true,
						Config:  &FtsIndexConfig{},
					},
				},
			},
		},
	}
	assert.True(t, schema.IsFtsEnabled())
}

func TestDisableIndex_EmptyKey(t *testing.T) {
	_, err := NewSchema(DisableStringIndex(""))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key cannot be empty")

	_, err = NewSchema(DisableIntIndex(""))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key cannot be empty")

	_, err = NewSchema(DisableFloatIndex(""))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key cannot be empty")

	_, err = NewSchema(DisableBoolIndex(""))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key cannot be empty")

	_, err = NewSchema(DisableFtsIndex(""))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key cannot be empty")
}

func TestWithVectorIndexCreate_MergesWithExistingSchema(t *testing.T) {
	op, err := NewCreateCollectionOp("test",
		WithVectorIndexCreate(NewVectorIndexConfig(WithSpace(SpaceL2))),
		WithFtsIndexCreate(&FtsIndexConfig{}),
	)
	require.NoError(t, err)
	require.NotNil(t, op.Schema)

	embeddingVT, ok := op.Schema.GetKey(EmbeddingKey)
	require.True(t, ok, "should have #embedding key")
	require.NotNil(t, embeddingVT.FloatList)
	require.NotNil(t, embeddingVT.FloatList.VectorIndex)
	assert.True(t, embeddingVT.FloatList.VectorIndex.Enabled)

	documentVT, ok := op.Schema.GetKey(DocumentKey)
	require.True(t, ok, "should have #document key")
	require.NotNil(t, documentVT.String)
	require.NotNil(t, documentVT.String.FtsIndex)
	assert.True(t, documentVT.String.FtsIndex.Enabled)
}

// CMEK tests

func TestNewGCPCmek(t *testing.T) {
	resource := "projects/my-project/locations/us-central1/keyRings/my-keyring/cryptoKeys/my-key"
	cmek := NewGCPCmek(resource)

	assert.NotNil(t, cmek)
	assert.Equal(t, CmekProviderGCP, cmek.Provider)
	assert.Equal(t, resource, cmek.Resource)
}

func TestCmek_ValidatePattern_ValidGCP(t *testing.T) {
	validResources := []string{
		"projects/my-project/locations/us-central1/keyRings/my-keyring/cryptoKeys/my-key",
		"projects/project-123/locations/global/keyRings/ring/cryptoKeys/key",
		"projects/a/locations/b/keyRings/c/cryptoKeys/d",
	}

	for _, resource := range validResources {
		cmek := NewGCPCmek(resource)
		err := cmek.ValidatePattern()
		assert.NoError(t, err, "resource %q should be valid", resource)
	}
}

func TestCmek_ValidatePattern_InvalidGCP(t *testing.T) {
	invalidResources := []string{
		"",
		"my-key",
		"projects/my-project/keyRings/my-keyring/cryptoKeys/my-key",                 // missing locations
		"projects/my-project/locations/us-central1/cryptoKeys/my-key",               // missing keyRings
		"projects/my-project/locations/us-central1/keyRings/my-keyring",             // missing cryptoKeys
		"projects//locations/us-central1/keyRings/my-keyring/cryptoKeys/my-key",     // empty project
		"projects/my-project/locations//keyRings/my-keyring/cryptoKeys/my-key",      // empty location
		"projects/my-project/locations/us-central1/keyRings//cryptoKeys/my-key",     // empty keyring
		"projects/my-project/locations/us-central1/keyRings/my-keyring/cryptoKeys/", // empty key
		"projects/my/project/locations/us-central1/keyRings/ring/cryptoKeys/key",    // slash in project
		"projects/proj/locations/us/central/keyRings/ring/cryptoKeys/key",           // slash in location
		"projects/proj/locations/loc/keyRings/key/ring/cryptoKeys/key",              // slash in keyring
		"projects/proj/locations/loc/keyRings/ring/cryptoKeys/my/key",               // slash in key
	}

	for _, resource := range invalidResources {
		cmek := NewGCPCmek(resource)
		err := cmek.ValidatePattern()
		assert.Error(t, err, "resource %q should be invalid", resource)
		assert.Contains(t, err.Error(), "invalid GCP CMEK resource format")
	}
}

func TestCmek_ValidatePattern_NilCmek(t *testing.T) {
	var cmek *Cmek
	err := cmek.ValidatePattern()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cmek is nil")
}

func TestCmek_ValidatePattern_UnsupportedProvider(t *testing.T) {
	cmek := &Cmek{
		Provider: CmekProvider("aws"),
		Resource: "arn:aws:kms:us-east-1:123456789:key/12345678-1234-1234-1234-123456789012",
	}
	err := cmek.ValidatePattern()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported CMEK provider")
}

func TestCmek_MarshalJSON(t *testing.T) {
	resource := "projects/my-project/locations/us-central1/keyRings/my-keyring/cryptoKeys/my-key"
	cmek := NewGCPCmek(resource)

	data, err := json.Marshal(cmek)
	require.NoError(t, err)

	expected := `{"gcp":"projects/my-project/locations/us-central1/keyRings/my-keyring/cryptoKeys/my-key"}`
	assert.JSONEq(t, expected, string(data))
}

func TestCmek_UnmarshalJSON(t *testing.T) {
	data := `{"gcp":"projects/my-project/locations/us-central1/keyRings/my-keyring/cryptoKeys/my-key"}`

	var cmek Cmek
	err := json.Unmarshal([]byte(data), &cmek)
	require.NoError(t, err)

	assert.Equal(t, CmekProviderGCP, cmek.Provider)
	assert.Equal(t, "projects/my-project/locations/us-central1/keyRings/my-keyring/cryptoKeys/my-key", cmek.Resource)
}

func TestCmek_UnmarshalJSON_PythonDocstringExample(t *testing.T) {
	// Exact example from Python's Cmek.from_dict docstring
	data := `{"gcp": "projects/p/locations/l/keyRings/r/cryptoKeys/k"}`

	var cmek Cmek
	err := json.Unmarshal([]byte(data), &cmek)
	require.NoError(t, err)

	assert.Equal(t, CmekProviderGCP, cmek.Provider)
	assert.Equal(t, "projects/p/locations/l/keyRings/r/cryptoKeys/k", cmek.Resource)
}

func TestCmek_MarshalUnmarshal_Roundtrip(t *testing.T) {
	original := NewGCPCmek("projects/test/locations/global/keyRings/ring/cryptoKeys/key")

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored Cmek
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	assert.Equal(t, original.Provider, restored.Provider)
	assert.Equal(t, original.Resource, restored.Resource)
}

func TestCmek_UnmarshalJSON_EmptyObject(t *testing.T) {
	var cmek Cmek
	err := json.Unmarshal([]byte(`{}`), &cmek)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported or missing CMEK provider")
}

func TestCmek_UnmarshalJSON_InvalidResource(t *testing.T) {
	var cmek Cmek
	err := json.Unmarshal([]byte(`{"gcp":"invalid-resource"}`), &cmek)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid GCP CMEK resource format")
}

func TestCmek_UnmarshalJSON_UnsupportedProvider(t *testing.T) {
	var cmek Cmek
	err := json.Unmarshal([]byte(`{"aws": "arn:aws:kms:..."}`), &cmek)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported or missing CMEK provider")
}

func TestCmek_MarshalJSON_UnknownProvider(t *testing.T) {
	cmek := &Cmek{
		Provider: CmekProvider("unknown"),
		Resource: "some-resource",
	}
	_, err := json.Marshal(cmek)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown CMEK provider")
}

func TestCmek_UnmarshalJSON_InvalidJSON(t *testing.T) {
	var cmek Cmek
	err := json.Unmarshal([]byte(`not json`), &cmek)
	assert.Error(t, err)
}

func TestSchema_WithCmek(t *testing.T) {
	cmek := NewGCPCmek("projects/my-project/locations/us-central1/keyRings/my-keyring/cryptoKeys/my-key")
	schema, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
		WithCmek(cmek),
	)
	require.NoError(t, err)
	assert.NotNil(t, schema.Cmek())
	assert.Equal(t, CmekProviderGCP, schema.Cmek().Provider)
}

func TestSchema_WithCmek_InvalidResource(t *testing.T) {
	cmek := NewGCPCmek("invalid-resource")
	_, err := NewSchema(WithCmek(cmek))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid GCP CMEK resource format")
}

func TestSchema_WithCmek_NilError(t *testing.T) {
	_, err := NewSchema(WithCmek(nil))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cmek cannot be nil")
}

func TestSchema_WithoutCmek(t *testing.T) {
	schema, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
	)
	require.NoError(t, err)
	assert.Nil(t, schema.Cmek())
}

func TestSchema_MarshalJSON_WithCmek(t *testing.T) {
	cmek := NewGCPCmek("projects/my-project/locations/us-central1/keyRings/my-keyring/cryptoKeys/my-key")
	schema, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
		WithCmek(cmek),
	)
	require.NoError(t, err)

	data, err := json.Marshal(schema)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	assert.Contains(t, result, "cmek")
	cmekData, ok := result["cmek"].(map[string]interface{})
	assert.True(t, ok)
	assert.Contains(t, cmekData, "gcp")
}

func TestSchema_MarshalJSON_WithoutCmek(t *testing.T) {
	schema, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
	)
	require.NoError(t, err)

	data, err := json.Marshal(schema)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	assert.NotContains(t, result, "cmek")
}

func TestSchema_UnmarshalJSON_WithCmek(t *testing.T) {
	cmek := NewGCPCmek("projects/my-project/locations/us-central1/keyRings/my-keyring/cryptoKeys/my-key")
	original, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
		WithCmek(cmek),
	)
	require.NoError(t, err)

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored Schema
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	require.NotNil(t, restored.Cmek())
	assert.Equal(t, original.Cmek().Provider, restored.Cmek().Provider)
	assert.Equal(t, original.Cmek().Resource, restored.Cmek().Resource)
}

func TestSchema_UnmarshalJSON_WithoutCmek(t *testing.T) {
	original, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceL2))),
	)
	require.NoError(t, err)

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored Schema
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	assert.Nil(t, restored.Cmek())
}

func TestSchema_UnmarshalJSON_InvalidCmekResource(t *testing.T) {
	data := `{"keys":{},"cmek":{"gcp":"invalid-resource"}}`

	var schema Schema
	err := json.Unmarshal([]byte(data), &schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid GCP CMEK resource format")
}

func TestCmekProviderConstants(t *testing.T) {
	assert.Equal(t, CmekProvider("gcp"), CmekProviderGCP)
}

func TestWithFtsIndexCreate_MergesWithExistingSchema(t *testing.T) {
	op, err := NewCreateCollectionOp("test",
		WithFtsIndexCreate(&FtsIndexConfig{}),
		WithVectorIndexCreate(NewVectorIndexConfig(WithSpace(SpaceCosine))),
	)
	require.NoError(t, err)
	require.NotNil(t, op.Schema)

	embeddingVT, ok := op.Schema.GetKey(EmbeddingKey)
	require.True(t, ok, "should have #embedding key")
	require.NotNil(t, embeddingVT.FloatList)
	require.NotNil(t, embeddingVT.FloatList.VectorIndex)
	assert.True(t, embeddingVT.FloatList.VectorIndex.Enabled)
	assert.Equal(t, SpaceCosine, embeddingVT.FloatList.VectorIndex.Config.Space)

	documentVT, ok := op.Schema.GetKey(DocumentKey)
	require.True(t, ok, "should have #document key")
	require.NotNil(t, documentVT.String)
	require.NotNil(t, documentVT.String.FtsIndex)
	assert.True(t, documentVT.String.FtsIndex.Enabled)
}

// Embedding function serialization tests
// Uses mockEmbeddingFunction from configuration_test.go

func TestVectorIndexConfig_MarshalJSON_WithEmbeddingFunction(t *testing.T) {
	ef := &mockEmbeddingFunction{
		name:   "test-ef",
		config: map[string]interface{}{"api_key_env_var": "TEST_API_KEY"},
	}

	config := NewVectorIndexConfig(
		WithSpace(SpaceCosine),
		WithVectorEmbeddingFunction(ef),
	)

	data, err := json.Marshal(config)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify EF info is present in JSON
	assert.Contains(t, result, "embedding_function")
	efInfo, ok := result["embedding_function"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "known", efInfo["type"])
	assert.Equal(t, "test-ef", efInfo["name"])
	assert.Contains(t, efInfo, "config")
}

func TestVectorIndexConfig_MarshalJSON_WithoutEmbeddingFunction(t *testing.T) {
	config := NewVectorIndexConfig(WithSpace(SpaceCosine))

	data, err := json.Marshal(config)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// EF info should not be present
	assert.NotContains(t, result, "embedding_function")
}

func TestVectorIndexConfig_UnmarshalJSON_PreservesConfig(t *testing.T) {
	data := `{
		"space": "cosine",
		"source_key": "my_embedding",
		"hnsw": {"ef_construction": 100, "max_neighbors": 16}
	}`

	var config VectorIndexConfig
	err := json.Unmarshal([]byte(data), &config)
	require.NoError(t, err)

	assert.Equal(t, SpaceCosine, config.Space)
	assert.Equal(t, "my_embedding", config.SourceKey)
	assert.NotNil(t, config.Hnsw)
	assert.Equal(t, uint(100), config.Hnsw.EfConstruction)
	assert.Equal(t, uint(16), config.Hnsw.MaxNeighbors)
	assert.Nil(t, config.EmbeddingFunction) // No registered EF
}

func TestSchema_SetEmbeddingFunction(t *testing.T) {
	schema, err := NewSchema()
	require.NoError(t, err)

	ef := &mockEmbeddingFunction{
		name:   "test-ef",
		config: map[string]interface{}{"model": "test-model"},
	}

	schema.SetEmbeddingFunction(ef)

	// Verify EF was set
	retrieved := schema.GetEmbeddingFunction()
	assert.NotNil(t, retrieved)
	assert.Equal(t, "test-ef", retrieved.Name())
}

func TestSchema_SetEmbeddingFunction_NilSchema(t *testing.T) {
	var schema *Schema
	ef := &mockEmbeddingFunction{name: "test-ef"}

	// Should not panic
	schema.SetEmbeddingFunction(ef)
}

func TestSchema_SetEmbeddingFunction_NilEF(t *testing.T) {
	schema, err := NewSchema()
	require.NoError(t, err)

	// Should not panic
	schema.SetEmbeddingFunction(nil)

	// Should return nil
	assert.Nil(t, schema.GetEmbeddingFunction())
}

func TestSchema_GetEmbeddingFunction_NoVectorIndex(t *testing.T) {
	schema, err := NewSchema()
	require.NoError(t, err)

	// No vector index configured
	ef := schema.GetEmbeddingFunction()
	assert.Nil(t, ef)
}

func TestSchema_GetEmbeddingFunction_FromExistingSchema(t *testing.T) {
	ef := &mockEmbeddingFunction{
		name:   "my-ef",
		config: map[string]interface{}{"key": "value"},
	}

	schema, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(
			WithSpace(SpaceL2),
			WithVectorEmbeddingFunction(ef),
		)),
	)
	require.NoError(t, err)

	retrieved := schema.GetEmbeddingFunction()
	assert.NotNil(t, retrieved)
	assert.Equal(t, "my-ef", retrieved.Name())
}

func TestSchema_MarshalJSON_WithEmbeddingFunction(t *testing.T) {
	ef := &mockEmbeddingFunction{
		name:   "test-ef",
		config: map[string]interface{}{"api_key_env_var": "MY_API_KEY"},
	}

	schema, err := NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(
			WithSpace(SpaceCosine),
			WithVectorEmbeddingFunction(ef),
		)),
	)
	require.NoError(t, err)

	data, err := json.Marshal(schema)
	require.NoError(t, err)

	// Verify the JSON contains EF info
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	keys, ok := result["keys"].(map[string]interface{})
	require.True(t, ok)
	embeddingKey, ok := keys[EmbeddingKey].(map[string]interface{})
	require.True(t, ok)
	floatList, ok := embeddingKey["float_list"].(map[string]interface{})
	require.True(t, ok)
	vectorIndex, ok := floatList["vector_index"].(map[string]interface{})
	require.True(t, ok)
	config, ok := vectorIndex["config"].(map[string]interface{})
	require.True(t, ok)
	efInfo, ok := config["embedding_function"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "known", efInfo["type"])
	assert.Equal(t, "test-ef", efInfo["name"])
}

// mockSparseEmbeddingFunction is a minimal mock for testing sparse EF serialization
type mockSparseEmbeddingFunction struct {
	name   string
	config map[string]interface{}
}

func (m *mockSparseEmbeddingFunction) EmbedDocumentsSparse(_ context.Context, _ []string) ([]*embeddings.SparseVector, error) {
	return nil, nil
}

func (m *mockSparseEmbeddingFunction) EmbedQuerySparse(_ context.Context, _ string) (*embeddings.SparseVector, error) {
	return nil, nil
}

func (m *mockSparseEmbeddingFunction) Name() string {
	return m.name
}

func (m *mockSparseEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	return m.config
}

func TestSparseVectorIndexConfig_MarshalJSON_WithEmbeddingFunction(t *testing.T) {
	ef := &mockSparseEmbeddingFunction{
		name:   "test-sparse-ef",
		config: map[string]interface{}{"api_key_env_var": "TEST_API_KEY", "model": "test-model"},
	}

	config := NewSparseVectorIndexConfig(
		WithSparseEmbeddingFunction(ef),
		WithSparseSourceKey("#document"),
	)

	data, err := json.Marshal(config)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify EF info is present in JSON
	assert.Contains(t, result, "embedding_function")
	efInfo, ok := result["embedding_function"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "known", efInfo["type"])
	assert.Equal(t, "test-sparse-ef", efInfo["name"])
	assert.Contains(t, efInfo, "config")
	assert.Equal(t, "#document", result["source_key"])
}

func TestSparseVectorIndexConfig_MarshalJSON_WithoutEmbeddingFunction(t *testing.T) {
	config := NewSparseVectorIndexConfig(
		WithSparseSourceKey("#document"),
		WithBM25(true),
	)

	data, err := json.Marshal(config)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// EF info should not be present
	assert.NotContains(t, result, "embedding_function")
	assert.Equal(t, "#document", result["source_key"])
	assert.Equal(t, true, result["bm25"])
}

func TestSparseVectorIndexConfig_UnmarshalJSON_PreservesConfig(t *testing.T) {
	data := `{
		"source_key": "my_sparse_embedding",
		"bm25": true
	}`

	var config SparseVectorIndexConfig
	err := json.Unmarshal([]byte(data), &config)
	require.NoError(t, err)

	assert.Equal(t, "my_sparse_embedding", config.SourceKey)
	assert.True(t, config.BM25)
	assert.Nil(t, config.EmbeddingFunction) // No registered EF
}

func TestSparseVectorIndexConfig_UnmarshalJSON_WithUnknownEF(t *testing.T) {
	// Cloud returns "type": "unknown" for unrecognized EFs
	data := `{
		"embedding_function": {"type": "unknown"},
		"source_key": "#document",
		"bm25": false
	}`

	var config SparseVectorIndexConfig
	err := json.Unmarshal([]byte(data), &config)
	require.NoError(t, err)

	assert.Equal(t, "#document", config.SourceKey)
	assert.False(t, config.BM25)
	assert.Nil(t, config.EmbeddingFunction) // Should not reconstruct unknown EF
}

func TestSchema_SetSparseEmbeddingFunction(t *testing.T) {
	schema, err := NewSchema()
	require.NoError(t, err)

	ef := &mockSparseEmbeddingFunction{
		name:   "test-sparse-ef",
		config: map[string]interface{}{"model": "test-model"},
	}

	schema.SetSparseEmbeddingFunction("sparse_embedding", ef)

	// Verify EF was set
	retrieved := schema.GetSparseEmbeddingFunction("sparse_embedding")
	assert.NotNil(t, retrieved)
	assert.Equal(t, "test-sparse-ef", retrieved.Name())
}

func TestSchema_SetSparseEmbeddingFunction_NilSchema(t *testing.T) {
	var schema *Schema
	ef := &mockSparseEmbeddingFunction{name: "test-sparse-ef"}

	// Should not panic
	schema.SetSparseEmbeddingFunction("sparse_embedding", ef)
}

func TestSchema_SetSparseEmbeddingFunction_NilEF(t *testing.T) {
	schema, err := NewSchema()
	require.NoError(t, err)

	// Should not panic
	schema.SetSparseEmbeddingFunction("sparse_embedding", nil)

	// Should return nil
	assert.Nil(t, schema.GetSparseEmbeddingFunction("sparse_embedding"))
}

func TestSchema_SetSparseEmbeddingFunction_EmptyKey(t *testing.T) {
	schema, err := NewSchema()
	require.NoError(t, err)

	ef := &mockSparseEmbeddingFunction{name: "test-sparse-ef"}

	// Should not panic or create anything with empty key
	schema.SetSparseEmbeddingFunction("", ef)
}

func TestSchema_GetSparseEmbeddingFunction_NoIndex(t *testing.T) {
	schema, err := NewSchema()
	require.NoError(t, err)

	// No sparse vector index configured
	ef := schema.GetSparseEmbeddingFunction("sparse_embedding")
	assert.Nil(t, ef)
}

func TestSchema_GetAllSparseEmbeddingFunctions(t *testing.T) {
	schema, err := NewSchema()
	require.NoError(t, err)

	ef1 := &mockSparseEmbeddingFunction{
		name:   "sparse-ef-1",
		config: map[string]interface{}{"key": "value1"},
	}
	ef2 := &mockSparseEmbeddingFunction{
		name:   "sparse-ef-2",
		config: map[string]interface{}{"key": "value2"},
	}

	schema.SetSparseEmbeddingFunction("sparse_key_1", ef1)
	schema.SetSparseEmbeddingFunction("sparse_key_2", ef2)

	all := schema.GetAllSparseEmbeddingFunctions()
	assert.Len(t, all, 2)
	assert.Equal(t, "sparse-ef-1", all["sparse_key_1"].Name())
	assert.Equal(t, "sparse-ef-2", all["sparse_key_2"].Name())
}

func TestSchema_GetAllSparseEmbeddingFunctions_NoSparse(t *testing.T) {
	schema, err := NewSchema()
	require.NoError(t, err)

	all := schema.GetAllSparseEmbeddingFunctions()
	assert.Empty(t, all)
}

func TestSchema_MarshalJSON_WithSparseEmbeddingFunction(t *testing.T) {
	ef := &mockSparseEmbeddingFunction{
		name:   "test-sparse-ef",
		config: map[string]interface{}{"api_key_env_var": "MY_API_KEY"},
	}

	schema, err := NewSchema(
		WithSparseVectorIndex("sparse_embedding", NewSparseVectorIndexConfig(
			WithSparseEmbeddingFunction(ef),
			WithSparseSourceKey("#document"),
		)),
	)
	require.NoError(t, err)

	data, err := json.Marshal(schema)
	require.NoError(t, err)

	// Verify sparse EF info is serialized correctly
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	keys, ok := result["keys"].(map[string]interface{})
	require.True(t, ok)
	sparseKey, ok := keys["sparse_embedding"].(map[string]interface{})
	require.True(t, ok)
	sparseVector, ok := sparseKey["sparse_vector"].(map[string]interface{})
	require.True(t, ok)
	sparseVectorIndex, ok := sparseVector["sparse_vector_index"].(map[string]interface{})
	require.True(t, ok)
	config, ok := sparseVectorIndex["config"].(map[string]interface{})
	require.True(t, ok)
	efInfo, ok := config["embedding_function"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "known", efInfo["type"])
	assert.Equal(t, "test-sparse-ef", efInfo["name"])
}

func TestSparseVectorIndexConfig_Roundtrip_WithRegisteredEF(t *testing.T) {
	// Use BM25 which is a registered sparse EF that doesn't require API keys
	bm25EF, err := embeddings.BuildSparse("bm25", embeddings.EmbeddingFunctionConfig{
		"k":       1.2,
		"b":       0.75,
		"avg_len": 256.0,
	})
	require.NoError(t, err)
	require.NotNil(t, bm25EF)

	// Create config with the real EF
	config := NewSparseVectorIndexConfig(
		WithSparseEmbeddingFunction(bm25EF),
		WithSparseSourceKey("#document"),
	)

	// Serialize
	data, err := json.Marshal(config)
	require.NoError(t, err)

	// Deserialize into new config
	var reconstructed SparseVectorIndexConfig
	err = json.Unmarshal(data, &reconstructed)
	require.NoError(t, err)

	// Verify the EF was reconstructed
	assert.NotNil(t, reconstructed.EmbeddingFunction, "EF should be reconstructed from registry")
	assert.Equal(t, "chroma_bm25", reconstructed.EmbeddingFunction.Name())
	assert.Equal(t, "#document", reconstructed.SourceKey)
}

func TestSchema_Roundtrip_WithSparseEmbeddingFunction(t *testing.T) {
	// Use BM25 which is a registered sparse EF
	bm25EF, err := embeddings.BuildSparse("bm25", embeddings.EmbeddingFunctionConfig{
		"k": 1.5,
		"b": 0.8,
	})
	require.NoError(t, err)

	// Create schema with sparse EF
	schema, err := NewSchema(
		WithSparseVectorIndex("sparse_embedding", NewSparseVectorIndexConfig(
			WithSparseEmbeddingFunction(bm25EF),
			WithSparseSourceKey("#document"),
		)),
	)
	require.NoError(t, err)

	// Serialize schema
	data, err := json.Marshal(schema)
	require.NoError(t, err)

	// Deserialize into new schema
	var reconstructed Schema
	err = json.Unmarshal(data, &reconstructed)
	require.NoError(t, err)

	// Verify sparse EF was reconstructed
	ef := reconstructed.GetSparseEmbeddingFunction("sparse_embedding")
	require.NotNil(t, ef, "Sparse EF should be auto-wired from registry")
	assert.Equal(t, "chroma_bm25", ef.Name())

	// Also test GetAllSparseEmbeddingFunctions
	allSparse := reconstructed.GetAllSparseEmbeddingFunctions()
	assert.Len(t, allSparse, 1)
	assert.NotNil(t, allSparse["sparse_embedding"])
	assert.Equal(t, "chroma_bm25", allSparse["sparse_embedding"].Name())
}

func TestSchema_Roundtrip_SimulatesCloudResponse(t *testing.T) {
	// Simulate the JSON that would come back from Chroma Cloud
	// This is the key test - proving auto-wiring works with real Cloud response format
	cloudJSON := `{
		"keys": {
			"sparse_embedding": {
				"sparse_vector": {
					"sparse_vector_index": {
						"enabled": true,
						"config": {
							"embedding_function": {
								"type": "known",
								"name": "chroma_bm25",
								"config": {
									"k": 1.2,
									"b": 0.75,
									"avg_len": 256.0
								}
							},
							"source_key": "#document"
						}
					}
				}
			}
		}
	}`

	var schema Schema
	err := json.Unmarshal([]byte(cloudJSON), &schema)
	require.NoError(t, err)

	// Verify sparse EF was auto-wired
	ef := schema.GetSparseEmbeddingFunction("sparse_embedding")
	require.NotNil(t, ef, "Sparse EF should be auto-wired from Cloud response")
	assert.Equal(t, "chroma_bm25", ef.Name())

	// Verify config was passed through
	config := ef.GetConfig()
	k, ok := embeddings.ConfigFloat64(config, "k")
	assert.True(t, ok)
	assert.Equal(t, 1.2, k)
}
