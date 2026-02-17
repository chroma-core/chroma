package chroma

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"

	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/cohere"
	_ "github.com/chroma-core/chroma/clients/go/pkg/embeddings/openai"
)

func TestNewCollectionConfiguration(t *testing.T) {
	config := NewCollectionConfiguration()
	assert.NotNil(t, config)
	assert.NotNil(t, config.raw)
}

func TestNewCollectionConfigurationFromMap(t *testing.T) {
	rawData := map[string]interface{}{
		"foo": "bar",
		"baz": 123,
	}

	config := NewCollectionConfigurationFromMap(rawData)
	assert.NotNil(t, config)

	val, ok := config.GetRaw("foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", val)

	val, ok = config.GetRaw("baz")
	assert.True(t, ok)
	assert.Equal(t, 123, val)
}

func TestCollectionConfiguration_GetSetRaw(t *testing.T) {
	config := NewCollectionConfiguration()

	// Test SetRaw and GetRaw
	config.SetRaw("key1", "value1")
	val, ok := config.GetRaw("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)

	// Test non-existent key
	val, ok = config.GetRaw("nonexistent")
	assert.False(t, ok)
	assert.Nil(t, val)
}

func TestCollectionConfiguration_Keys(t *testing.T) {
	config := NewCollectionConfiguration()

	// Initially empty
	keys := config.Keys()
	assert.Equal(t, 0, len(keys))

	// Add some values
	config.SetRaw("key1", "value1")
	config.SetRaw("key2", "value2")

	keys = config.Keys()
	assert.Equal(t, 2, len(keys))
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")
}

func TestCollectionConfiguration_MarshalJSON(t *testing.T) {
	config := NewCollectionConfiguration()
	config.SetRaw("custom_key", "custom_value")

	data, err := json.Marshal(config)
	require.NoError(t, err)
	assert.NotNil(t, data)

	// Verify JSON structure
	var result map[string]interface{}
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Contains(t, result, "custom_key")
	assert.Equal(t, "custom_value", result["custom_key"])
}

func TestCollectionConfiguration_UnmarshalJSON(t *testing.T) {
	jsonData := `{
		"custom_key": "custom_value",
		"another_key": 42
	}`

	config := &CollectionConfigurationImpl{}
	err := json.Unmarshal([]byte(jsonData), config)
	require.NoError(t, err)

	val, ok := config.GetRaw("custom_key")
	assert.True(t, ok)
	assert.Equal(t, "custom_value", val)

	val, ok = config.GetRaw("another_key")
	assert.True(t, ok)
	// JSON numbers are decoded as float64
	assert.Equal(t, float64(42), val)
}

// mockEmbeddingFunction is a test EF for configuration testing
type mockEmbeddingFunction struct {
	name   string
	config embeddings.EmbeddingFunctionConfig
}

func (m *mockEmbeddingFunction) EmbedDocuments(ctx context.Context, texts []string) ([]embeddings.Embedding, error) {
	return nil, nil
}

func (m *mockEmbeddingFunction) EmbedQuery(ctx context.Context, text string) (embeddings.Embedding, error) {
	return nil, nil
}

func (m *mockEmbeddingFunction) Name() string {
	return m.name
}

func (m *mockEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	return m.config
}

func (m *mockEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.L2
}

func (m *mockEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.L2, embeddings.COSINE}
}

func TestEmbeddingFunctionInfo_IsKnown(t *testing.T) {
	tests := []struct {
		name     string
		info     *EmbeddingFunctionInfo
		expected bool
	}{
		{
			name:     "nil info",
			info:     nil,
			expected: false,
		},
		{
			name:     "known type",
			info:     &EmbeddingFunctionInfo{Type: "known", Name: "openai"},
			expected: true,
		},
		{
			name:     "unknown type",
			info:     &EmbeddingFunctionInfo{Type: "unknown", Name: "custom"},
			expected: false,
		},
		{
			name:     "empty type",
			info:     &EmbeddingFunctionInfo{Type: "", Name: "test"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.info.IsKnown())
		})
	}
}

func TestCollectionConfiguration_GetEmbeddingFunctionInfo(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		config := &CollectionConfigurationImpl{raw: nil}
		info, ok := config.GetEmbeddingFunctionInfo()
		assert.False(t, ok)
		assert.Nil(t, info)
	})

	t.Run("no embedding_function key", func(t *testing.T) {
		config := NewCollectionConfiguration()
		config.SetRaw("other_key", "value")
		info, ok := config.GetEmbeddingFunctionInfo()
		assert.False(t, ok)
		assert.Nil(t, info)
	})

	t.Run("embedding_function is not a map", func(t *testing.T) {
		config := NewCollectionConfiguration()
		config.SetRaw("embedding_function", "not_a_map")
		info, ok := config.GetEmbeddingFunctionInfo()
		assert.False(t, ok)
		assert.Nil(t, info)
	})

	t.Run("valid embedding_function", func(t *testing.T) {
		config := NewCollectionConfiguration()
		config.SetRaw("embedding_function", map[string]interface{}{
			"type":   "known",
			"name":   "openai",
			"config": map[string]interface{}{"api_key_env_var": "OPENAI_API_KEY", "model_name": "text-embedding-3-small"},
		})
		info, ok := config.GetEmbeddingFunctionInfo()
		assert.True(t, ok)
		require.NotNil(t, info)
		assert.Equal(t, "known", info.Type)
		assert.Equal(t, "openai", info.Name)
		assert.Equal(t, "OPENAI_API_KEY", info.Config["api_key_env_var"])
		assert.Equal(t, "text-embedding-3-small", info.Config["model_name"])
	})

	t.Run("partial embedding_function", func(t *testing.T) {
		config := NewCollectionConfiguration()
		config.SetRaw("embedding_function", map[string]interface{}{
			"type": "known",
			"name": "default",
		})
		info, ok := config.GetEmbeddingFunctionInfo()
		assert.True(t, ok)
		require.NotNil(t, info)
		assert.Equal(t, "known", info.Type)
		assert.Equal(t, "default", info.Name)
		assert.Nil(t, info.Config)
	})
}

func TestCollectionConfiguration_SetEmbeddingFunctionInfo(t *testing.T) {
	t.Run("nil info does nothing", func(t *testing.T) {
		config := NewCollectionConfiguration()
		config.SetEmbeddingFunctionInfo(nil)
		_, ok := config.GetRaw("embedding_function")
		assert.False(t, ok)
	})

	t.Run("sets info correctly", func(t *testing.T) {
		config := NewCollectionConfiguration()
		info := &EmbeddingFunctionInfo{
			Type:   "known",
			Name:   "cohere",
			Config: map[string]interface{}{"api_key_env_var": "COHERE_API_KEY"},
		}
		config.SetEmbeddingFunctionInfo(info)

		retrieved, ok := config.GetEmbeddingFunctionInfo()
		assert.True(t, ok)
		require.NotNil(t, retrieved)
		assert.Equal(t, "known", retrieved.Type)
		assert.Equal(t, "cohere", retrieved.Name)
		assert.Equal(t, "COHERE_API_KEY", retrieved.Config["api_key_env_var"])
	})

	t.Run("initializes raw map if nil", func(t *testing.T) {
		config := &CollectionConfigurationImpl{raw: nil}
		info := &EmbeddingFunctionInfo{Type: "known", Name: "test"}
		config.SetEmbeddingFunctionInfo(info)

		retrieved, ok := config.GetEmbeddingFunctionInfo()
		assert.True(t, ok)
		assert.NotNil(t, retrieved)
	})
}

func TestCollectionConfiguration_SetEmbeddingFunction(t *testing.T) {
	t.Run("nil EF does nothing", func(t *testing.T) {
		config := NewCollectionConfiguration()
		config.SetEmbeddingFunction(nil)
		_, ok := config.GetRaw("embedding_function")
		assert.False(t, ok)
	})

	t.Run("sets EF from interface", func(t *testing.T) {
		config := NewCollectionConfiguration()
		mockEF := &mockEmbeddingFunction{
			name: "mock_provider",
			config: embeddings.EmbeddingFunctionConfig{
				"api_key_env_var": "MOCK_API_KEY",
				"model_name":      "mock-model",
			},
		}
		config.SetEmbeddingFunction(mockEF)

		info, ok := config.GetEmbeddingFunctionInfo()
		assert.True(t, ok)
		require.NotNil(t, info)
		assert.Equal(t, "known", info.Type)
		assert.Equal(t, "mock_provider", info.Name)
		assert.Equal(t, "MOCK_API_KEY", info.Config["api_key_env_var"])
		assert.Equal(t, "mock-model", info.Config["model_name"])
	})
}

func TestBuildEmbeddingFunctionFromConfig(t *testing.T) {
	t.Run("nil config returns nil", func(t *testing.T) {
		ef, err := BuildEmbeddingFunctionFromConfig(nil)
		assert.NoError(t, err)
		assert.Nil(t, ef)
	})

	t.Run("no EF info returns nil", func(t *testing.T) {
		config := NewCollectionConfiguration()
		ef, err := BuildEmbeddingFunctionFromConfig(config)
		assert.NoError(t, err)
		assert.Nil(t, ef)
	})

	t.Run("unknown type returns nil", func(t *testing.T) {
		config := NewCollectionConfiguration()
		config.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
			Type: "unknown",
			Name: "custom",
		})
		ef, err := BuildEmbeddingFunctionFromConfig(config)
		assert.NoError(t, err)
		assert.Nil(t, ef)
	})

	t.Run("unregistered name returns nil", func(t *testing.T) {
		config := NewCollectionConfiguration()
		config.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
			Type: "known",
			Name: "not_registered_provider_xyz",
		})
		ef, err := BuildEmbeddingFunctionFromConfig(config)
		assert.NoError(t, err)
		assert.Nil(t, ef)
	})
}

func TestEmbeddingFunctionInfo_JSONRoundTrip(t *testing.T) {
	original := &EmbeddingFunctionInfo{
		Type: "known",
		Name: "openai",
		Config: map[string]interface{}{
			"api_key_env_var": "OPENAI_API_KEY",
			"model_name":      "text-embedding-3-small",
			"dimensions":      float64(1536),
		},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded EmbeddingFunctionInfo
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Type, decoded.Type)
	assert.Equal(t, original.Name, decoded.Name)
	assert.Equal(t, original.Config["api_key_env_var"], decoded.Config["api_key_env_var"])
	assert.Equal(t, original.Config["model_name"], decoded.Config["model_name"])
	assert.Equal(t, original.Config["dimensions"], decoded.Config["dimensions"])
}

func TestCollectionConfiguration_EFConfigFromServerResponse(t *testing.T) {
	// Simulates parsing a server response with EF config
	serverResponse := `{
		"hnsw": {
			"space": "l2",
			"ef_construction": 100
		},
		"embedding_function": {
			"type": "known",
			"name": "default",
			"config": {}
		}
	}`

	config := &CollectionConfigurationImpl{}
	err := json.Unmarshal([]byte(serverResponse), config)
	require.NoError(t, err)

	info, ok := config.GetEmbeddingFunctionInfo()
	assert.True(t, ok)
	require.NotNil(t, info)
	assert.Equal(t, "known", info.Type)
	assert.Equal(t, "default", info.Name)
	assert.True(t, info.IsKnown())
}

// Negative tests for BuildEmbeddingFunctionFromConfig failure paths

func TestBuildEmbeddingFunctionFromConfig_MissingEnvVar(t *testing.T) {
	// Save and unset the env var
	origValue := os.Getenv("OPENAI_API_KEY")
	_ = os.Unsetenv("OPENAI_API_KEY")
	defer func() {
		if origValue != "" {
			_ = os.Setenv("OPENAI_API_KEY", origValue)
		}
	}()

	config := NewCollectionConfiguration()
	config.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
		Type: "known",
		Name: "openai",
		Config: map[string]any{
			"api_key_env_var": "OPENAI_API_KEY",
			"model_name":      "text-embedding-3-small",
		},
	})

	ef, err := BuildEmbeddingFunctionFromConfig(config)
	// BuildDense should fail when env var is not set
	assert.Error(t, err)
	assert.Nil(t, ef)
	assert.Contains(t, err.Error(), "OPENAI_API_KEY")
}

func TestBuildEmbeddingFunctionFromConfig_MissingRequiredConfig(t *testing.T) {
	// Unset the env var to ensure it fails
	origValue := os.Getenv("COHERE_API_KEY")
	_ = os.Unsetenv("COHERE_API_KEY")
	defer func() {
		if origValue != "" {
			_ = os.Setenv("COHERE_API_KEY", origValue)
		}
	}()

	config := NewCollectionConfiguration()
	config.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
		Type:   "known",
		Name:   "cohere",
		Config: map[string]any{
			// Missing api_key_env_var - should fail
		},
	})

	ef, err := BuildEmbeddingFunctionFromConfig(config)
	// BuildDense should fail when required config is missing
	assert.Error(t, err)
	assert.Nil(t, ef)
}

func TestBuildEmbeddingFunctionFromConfig_InvalidEnvVarName(t *testing.T) {
	// Ensure the nonexistent env var is definitely not set
	_ = os.Unsetenv("NONEXISTENT_ENV_VAR_12345")

	config := NewCollectionConfiguration()
	config.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
		Type: "known",
		Name: "openai",
		Config: map[string]any{
			"api_key_env_var": "NONEXISTENT_ENV_VAR_12345",
			"model_name":      "text-embedding-3-small",
		},
	})

	ef, err := BuildEmbeddingFunctionFromConfig(config)
	assert.Error(t, err)
	assert.Nil(t, ef)
	assert.Contains(t, err.Error(), "NONEXISTENT_ENV_VAR_12345")
}

func TestBuildEmbeddingFunctionFromConfig_SuccessWithValidEnvVar(t *testing.T) {
	// Test successful reconstruction when env var is set
	t.Setenv("OPENAI_API_KEY", "test-api-key-123")

	config := NewCollectionConfiguration()
	config.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
		Type: "known",
		Name: "openai",
		Config: map[string]any{
			"api_key_env_var": "OPENAI_API_KEY",
			"model_name":      "text-embedding-3-small",
		},
	})

	ef, err := BuildEmbeddingFunctionFromConfig(config)
	assert.NoError(t, err)
	assert.NotNil(t, ef)
	assert.Equal(t, "openai", ef.Name())
}

func TestBuildEmbeddingFunctionFromConfig_ConsistentHashNoEnvVar(t *testing.T) {
	// consistent_hash doesn't require env vars - should always work
	config := NewCollectionConfiguration()
	config.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
		Type: "known",
		Name: "consistent_hash",
		Config: map[string]any{
			"dim": float64(128),
		},
	})

	ef, err := BuildEmbeddingFunctionFromConfig(config)
	assert.NoError(t, err)
	assert.NotNil(t, ef)
	assert.Equal(t, "consistent_hash", ef.Name())
}

func TestBuildEmbeddingFunctionFromConfig_LegacyType(t *testing.T) {
	// "legacy" or other non-"known" types should return nil without error
	config := NewCollectionConfiguration()
	config.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
		Type:   "legacy",
		Name:   "some_old_ef",
		Config: map[string]any{},
	})

	ef, err := BuildEmbeddingFunctionFromConfig(config)
	assert.NoError(t, err)
	assert.Nil(t, ef)
}

func TestBuildEmbeddingFunctionFromConfig_EmptyConfig(t *testing.T) {
	// Empty config map should cause BuildDense to fail for providers requiring config
	// Unset the env var to ensure failure
	origValue := os.Getenv("OPENAI_API_KEY")
	_ = os.Unsetenv("OPENAI_API_KEY")
	defer func() {
		if origValue != "" {
			_ = os.Setenv("OPENAI_API_KEY", origValue)
		}
	}()

	config := NewCollectionConfiguration()
	config.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
		Type:   "known",
		Name:   "openai",
		Config: map[string]any{},
	})

	ef, err := BuildEmbeddingFunctionFromConfig(config)
	// Should fail because api_key_env_var is missing
	assert.Error(t, err)
	assert.Nil(t, ef)
}

// GetSchema tests

func TestGetSchema_NoSchema(t *testing.T) {
	config := NewCollectionConfiguration()
	schema := config.GetSchema()
	assert.Nil(t, schema)
}

func TestGetSchema_WithSchema(t *testing.T) {
	// Create a raw config map with schema data
	schemaData := map[string]any{
		"keys": map[string]any{
			EmbeddingKey: map[string]any{
				"float_list": map[string]any{
					"vector_index": map[string]any{
						"enabled": true,
						"config": map[string]any{
							"space": "cosine",
						},
					},
				},
			},
		},
	}

	rawConfig := map[string]any{
		"schema": schemaData,
	}

	config := NewCollectionConfigurationFromMap(rawConfig)
	schema := config.GetSchema()
	require.NotNil(t, schema)

	// Verify the schema was properly parsed
	vt, ok := schema.GetKey(EmbeddingKey)
	assert.True(t, ok)
	assert.NotNil(t, vt.FloatList)
	assert.NotNil(t, vt.FloatList.VectorIndex)
	assert.True(t, vt.FloatList.VectorIndex.Enabled)
	assert.Equal(t, SpaceCosine, vt.FloatList.VectorIndex.Config.Space)
}

func TestGetSchema_InvalidSchema(t *testing.T) {
	// Invalid schema data (not a map)
	rawConfig := map[string]any{
		"schema": "not a valid schema",
	}

	config := NewCollectionConfigurationFromMap(rawConfig)
	schema := config.GetSchema()
	assert.Nil(t, schema)
}

func TestBuildEmbeddingFunctionFromConfig_FromSchema(t *testing.T) {
	// Create a schema with EmbeddingFunctionInfo
	// Use consistent_hash which doesn't require API keys
	schemaData := map[string]any{
		"keys": map[string]any{
			EmbeddingKey: map[string]any{
				"float_list": map[string]any{
					"vector_index": map[string]any{
						"enabled": true,
						"config": map[string]any{
							"space": "cosine",
							"embedding_function": map[string]any{
								"type":   "known",
								"name":   "consistent_hash",
								"config": map[string]any{},
							},
						},
					},
				},
			},
		},
	}

	rawConfig := map[string]any{
		"schema": schemaData,
	}

	config := NewCollectionConfigurationFromMap(rawConfig)
	ef, err := BuildEmbeddingFunctionFromConfig(config)
	assert.NoError(t, err)
	assert.NotNil(t, ef)
	assert.Equal(t, "consistent_hash", ef.Name())
}

func TestBuildEmbeddingFunctionFromConfig_PrefersDirectConfig(t *testing.T) {
	// When both direct embedding_function and schema have EF,
	// the direct one should be preferred
	// Use consistent_hash in schema (since it doesn't require API keys)
	schemaData := map[string]any{
		"keys": map[string]any{
			EmbeddingKey: map[string]any{
				"float_list": map[string]any{
					"vector_index": map[string]any{
						"enabled": true,
						"config": map[string]any{
							"embedding_function": map[string]any{
								"type":   "known",
								"name":   "consistent_hash",
								"config": map[string]any{},
							},
						},
					},
				},
			},
		},
	}

	// Set OPENAI_API_KEY so the direct config can be built
	origValue := os.Getenv("OPENAI_API_KEY")
	if origValue == "" {
		_ = os.Setenv("OPENAI_API_KEY", "test-key-for-testing")
		defer func() { _ = os.Unsetenv("OPENAI_API_KEY") }()
	}

	config := NewCollectionConfiguration()
	config.SetRaw("schema", schemaData)
	// Also set direct embedding_function with openai (higher priority)
	config.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
		Type: "known",
		Name: "openai",
		Config: map[string]any{
			"api_key_env_var": "OPENAI_API_KEY",
		},
	})

	ef, err := BuildEmbeddingFunctionFromConfig(config)
	assert.NoError(t, err)
	assert.NotNil(t, ef)
	// Should prefer the direct config (openai) over schema (consistent_hash)
	assert.Equal(t, "openai", ef.Name())
}

func TestUpdateCollectionConfiguration_JSONMarshal(t *testing.T) {
	t.Run("only set fields appear in JSON", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWEfSearchModify(200))
		data, err := json.Marshal(cfg)
		require.NoError(t, err)

		var raw map[string]interface{}
		err = json.Unmarshal(data, &raw)
		require.NoError(t, err)
		assert.Contains(t, raw, "hnsw")
		assert.NotContains(t, raw, "spann")

		hnsw, ok := raw["hnsw"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, float64(200), hnsw["ef_search"])
		assert.NotContains(t, hnsw, "num_threads")
		assert.NotContains(t, hnsw, "batch_size")
	})

	t.Run("multiple HNSW fields", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(
			WithHNSWEfSearchModify(100),
			WithHNSWBatchSizeModify(500),
			WithHNSWResizeFactorModify(1.5),
		)
		data, err := json.Marshal(cfg)
		require.NoError(t, err)

		var raw map[string]interface{}
		err = json.Unmarshal(data, &raw)
		require.NoError(t, err)

		hnsw := raw["hnsw"].(map[string]interface{})
		assert.Equal(t, float64(100), hnsw["ef_search"])
		assert.Equal(t, float64(500), hnsw["batch_size"])
		assert.Equal(t, 1.5, hnsw["resize_factor"])
	})

	t.Run("SPANN fields", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(
			WithSpannSearchNprobeModify(32),
			WithSpannEfSearchModify(64),
		)
		data, err := json.Marshal(cfg)
		require.NoError(t, err)

		var raw map[string]interface{}
		err = json.Unmarshal(data, &raw)
		require.NoError(t, err)
		assert.NotContains(t, raw, "hnsw")
		assert.Contains(t, raw, "spann")

		spann := raw["spann"].(map[string]interface{})
		assert.Equal(t, float64(32), spann["search_nprobe"])
		assert.Equal(t, float64(64), spann["ef_search"])
	})

	t.Run("empty config marshals to empty object", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration()
		data, err := json.Marshal(cfg)
		require.NoError(t, err)
		assert.Equal(t, `{}`, string(data))
	})
}

func TestUpdateCollectionConfiguration_GetRaw(t *testing.T) {
	t.Run("returns hnsw when set", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWEfSearchModify(100))
		val, ok := cfg.GetRaw("hnsw")
		assert.True(t, ok)
		assert.NotNil(t, val)
	})

	t.Run("returns false for unset hnsw", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration()
		_, ok := cfg.GetRaw("hnsw")
		assert.False(t, ok)
	})

	t.Run("returns spann when set", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithSpannEfSearchModify(64))
		val, ok := cfg.GetRaw("spann")
		assert.True(t, ok)
		assert.NotNil(t, val)
	})

	t.Run("returns false for unknown key", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWEfSearchModify(100))
		_, ok := cfg.GetRaw("unknown")
		assert.False(t, ok)
	})
}

func TestNewUpdateCollectionConfiguration_AllOptions(t *testing.T) {
	cfg := NewUpdateCollectionConfiguration(
		WithHNSWEfSearchModify(200),
		WithHNSWNumThreadsModify(4),
		WithHNSWBatchSizeModify(500),
		WithHNSWSyncThresholdModify(1000),
		WithHNSWResizeFactorModify(1.5),
	)

	require.NotNil(t, cfg.Hnsw)
	assert.Equal(t, uint(200), *cfg.Hnsw.EfSearch)
	assert.Equal(t, uint(4), *cfg.Hnsw.NumThreads)
	assert.Equal(t, uint(500), *cfg.Hnsw.BatchSize)
	assert.Equal(t, uint(1000), *cfg.Hnsw.SyncThreshold)
	assert.Equal(t, 1.5, *cfg.Hnsw.ResizeFactor)

	spannCfg := NewUpdateCollectionConfiguration(
		WithSpannSearchNprobeModify(32),
		WithSpannEfSearchModify(64),
	)

	require.NotNil(t, spannCfg.Spann)
	assert.Equal(t, uint(32), *spannCfg.Spann.SearchNprobe)
	assert.Equal(t, uint(64), *spannCfg.Spann.EfSearch)
}

func TestUpdateCollectionConfiguration_Validate(t *testing.T) {
	t.Run("empty config returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration()
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one parameter")
	})

	t.Run("hnsw only is valid", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWEfSearchModify(200))
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("spann only is valid", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithSpannEfSearchModify(64))
		err := cfg.Validate()
		require.NoError(t, err)
	})

	t.Run("both hnsw and spann returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(
			WithHNSWEfSearchModify(200),
			WithSpannEfSearchModify(64),
		)
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot update both")
	})

	t.Run("hnsw ef_search zero returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWEfSearchModify(0))
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ef_search must be greater than 0")
	})

	t.Run("hnsw num_threads zero returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWNumThreadsModify(0))
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "num_threads must be greater than 0")
	})

	t.Run("hnsw batch_size zero returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWBatchSizeModify(0))
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "batch_size must be greater than 0")
	})

	t.Run("hnsw sync_threshold zero returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWSyncThresholdModify(0))
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sync_threshold must be greater than 0")
	})

	t.Run("hnsw resize_factor zero returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWResizeFactorModify(0))
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resize_factor must be greater than 0")
	})

	t.Run("hnsw resize_factor negative returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWResizeFactorModify(-1.5))
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resize_factor must be greater than 0")
	})

	t.Run("hnsw resize_factor NaN returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWResizeFactorModify(math.NaN()))
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resize_factor must be a finite number")
	})

	t.Run("hnsw resize_factor positive Inf returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithHNSWResizeFactorModify(math.Inf(1)))
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resize_factor must be a finite number")
	})

	t.Run("hnsw empty sub-config returns error", func(t *testing.T) {
		cfg := &UpdateCollectionConfiguration{Hnsw: &UpdateHNSWConfiguration{}}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "hnsw configuration must specify at least one parameter")
	})

	t.Run("spann empty sub-config returns error", func(t *testing.T) {
		cfg := &UpdateCollectionConfiguration{Spann: &UpdateSpannConfiguration{}}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "spann configuration must specify at least one parameter")
	})

	t.Run("spann search_nprobe zero returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithSpannSearchNprobeModify(0))
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "search_nprobe must be greater than 0")
	})

	t.Run("spann ef_search zero returns error", func(t *testing.T) {
		cfg := NewUpdateCollectionConfiguration(WithSpannEfSearchModify(0))
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ef_search must be greater than 0")
	})
}

func TestUpdateCollectionConfiguration_JSONMarshal_Combined(t *testing.T) {
	t.Run("hnsw and spann both present in JSON", func(t *testing.T) {
		cfg := &UpdateCollectionConfiguration{
			Hnsw:  &UpdateHNSWConfiguration{EfSearch: ptrUint(200)},
			Spann: &UpdateSpannConfiguration{SearchNprobe: ptrUint(32)},
		}
		data, err := json.Marshal(cfg)
		require.NoError(t, err)

		var raw map[string]interface{}
		err = json.Unmarshal(data, &raw)
		require.NoError(t, err)
		assert.Contains(t, raw, "hnsw")
		assert.Contains(t, raw, "spann")

		hnsw := raw["hnsw"].(map[string]interface{})
		assert.Equal(t, float64(200), hnsw["ef_search"])
		spann := raw["spann"].(map[string]interface{})
		assert.Equal(t, float64(32), spann["search_nprobe"])
	})
}

func ptrUint(v uint) *uint { return &v }
