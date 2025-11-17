package model

import (
	"encoding/json"
	"fmt"
)

type EmbeddingFunctionConfiguration struct {
	Type   string      `json:"type"`
	Name   string      `json:"name"`
	Config interface{} `json:"config"`
}

type VectorIndexConfiguration struct {
	Hnsw  *HnswConfiguration  `json:"hnsw,omitempty"`
	Spann *SpannConfiguration `json:"spann,omitempty"`
}

type HnswConfiguration struct {
	Space          string  `json:"space"`
	EfConstruction int     `json:"ef_construction"`
	EfSearch       int     `json:"ef_search"`
	MaxNeighbors   int     `json:"max_neighbors"`
	NumThreads     int     `json:"num_threads"`
	ResizeFactor   float64 `json:"resize_factor"`
	BatchSize      int     `json:"batch_size"`
	SyncThreshold  int     `json:"sync_threshold"`
}

// DefaultHnswConfiguration returns the default HNSW configuration
func DefaultHnswConfiguration() *HnswConfiguration {
	return &HnswConfiguration{
		Space:          "l2",
		EfConstruction: 100,
		EfSearch:       100,
		MaxNeighbors:   16,
		NumThreads:     16,
		ResizeFactor:   1.2,
		BatchSize:      100,
		SyncThreshold:  1000,
	}
}

type SpannConfiguration struct {
	SearchNprobe          int    `json:"search_nprobe"`
	WriteNprobe           int    `json:"write_nprobe"`
	Space                 string `json:"space"`
	EfConstruction        int    `json:"ef_construction"`
	EfSearch              int    `json:"ef_search"`
	MaxNeighbors          int    `json:"max_neighbors"`
	ReassignNeighborCount int    `json:"reassign_neighbor_count"`
	SplitThreshold        int    `json:"split_threshold"`
	MergeThreshold        int    `json:"merge_threshold"`
}

type InternalCollectionConfiguration struct {
	VectorIndex       *VectorIndexConfiguration       `json:"vector_index"`
	EmbeddingFunction *EmbeddingFunctionConfiguration `json:"embedding_function,omitempty"`
}

// DefaultHnswCollectionConfiguration returns a default configuration using HNSW
func DefaultHnswCollectionConfiguration() *InternalCollectionConfiguration {
	return &InternalCollectionConfiguration{
		VectorIndex: &VectorIndexConfiguration{
			Hnsw: DefaultHnswConfiguration(),
		},
	}
}

// FromLegacyMetadata creates a configuration from legacy metadata
func FromLegacyMetadata(metadata map[string]interface{}) *InternalCollectionConfiguration {
	config := DefaultHnswCollectionConfiguration()

	// Try to extract HNSW parameters from legacy metadata
	if metadata != nil {
		if efConstruction, ok := metadata["hnsw:construction_ef"].(float64); ok {
			config.VectorIndex.Hnsw.EfConstruction = int(efConstruction)
		}
		if efSearch, ok := metadata["hnsw:ef"].(float64); ok {
			config.VectorIndex.Hnsw.EfSearch = int(efSearch)
		}
		if maxNeighbors, ok := metadata["hnsw:max_elements"].(float64); ok {
			config.VectorIndex.Hnsw.MaxNeighbors = int(maxNeighbors)
		}
		if numThreads, ok := metadata["hnsw:num_threads"].(float64); ok {
			config.VectorIndex.Hnsw.NumThreads = int(numThreads)
		}
		if resizeFactor, ok := metadata["hnsw:resize_factor"].(float64); ok {
			config.VectorIndex.Hnsw.ResizeFactor = resizeFactor
		}
		if batchSize, ok := metadata["hnsw:batch_size"].(float64); ok {
			config.VectorIndex.Hnsw.BatchSize = int(batchSize)
		}
		if syncThreshold, ok := metadata["hnsw:sync_threshold"].(float64); ok {
			config.VectorIndex.Hnsw.SyncThreshold = int(syncThreshold)
		}
		if space, ok := metadata["hnsw:space"].(string); ok {
			config.VectorIndex.Hnsw.Space = space
		}
	}

	return config
}

// Update configuration types
type UpdateHnswConfiguration struct {
	EfSearch      *int     `json:"ef_search,omitempty"`
	MaxNeighbors  *int     `json:"max_neighbors,omitempty"`
	NumThreads    *int     `json:"num_threads,omitempty"`
	ResizeFactor  *float64 `json:"resize_factor,omitempty"`
	BatchSize     *int     `json:"batch_size,omitempty"`
	SyncThreshold *int     `json:"sync_threshold,omitempty"`
}

type UpdateSpannConfiguration struct {
	SearchNprobe *int `json:"search_nprobe,omitempty"`
	EfSearch     *int `json:"ef_search,omitempty"`
}

type UpdateVectorIndexConfiguration struct {
	Hnsw  *UpdateHnswConfiguration  `json:"hnsw,omitempty"`
	Spann *UpdateSpannConfiguration `json:"spann,omitempty"`
}

type InternalUpdateCollectionConfiguration struct {
	VectorIndex       *UpdateVectorIndexConfiguration `json:"vector_index,omitempty"`
	EmbeddingFunction *EmbeddingFunctionConfiguration `json:"embedding_function,omitempty"`
}

// Schema structures - simplified representation of the schema format
type VectorIndexConfig struct {
	Space             *string                         `json:"space,omitempty"`
	EmbeddingFunction *EmbeddingFunctionConfiguration `json:"embedding_function,omitempty"`
	SourceKey         *string                         `json:"source_key,omitempty"`
	Hnsw              *HnswIndexConfig                `json:"hnsw,omitempty"`
	Spann             *SpannIndexConfig               `json:"spann,omitempty"`
}

type HnswIndexConfig struct {
	EfConstruction *int     `json:"ef_construction,omitempty"`
	MaxNeighbors   *int     `json:"max_neighbors,omitempty"`
	EfSearch       *int     `json:"ef_search,omitempty"`
	NumThreads     *int     `json:"num_threads,omitempty"`
	BatchSize      *int     `json:"batch_size,omitempty"`
	SyncThreshold  *int     `json:"sync_threshold,omitempty"`
	ResizeFactor   *float64 `json:"resize_factor,omitempty"`
}

type SpannIndexConfig struct {
	SearchNprobe          *int     `json:"search_nprobe,omitempty"`
	SearchRngFactor       *float64 `json:"search_rng_factor,omitempty"`
	SearchRngEpsilon      *float64 `json:"search_rng_epsilon,omitempty"`
	NreplicaCount         *int     `json:"nreplica_count,omitempty"`
	WriteRngFactor        *float64 `json:"write_rng_factor,omitempty"`
	WriteRngEpsilon       *float64 `json:"write_rng_epsilon,omitempty"`
	SplitThreshold        *int     `json:"split_threshold,omitempty"`
	NumSamplesKmeans      *int     `json:"num_samples_kmeans,omitempty"`
	InitialLambda         *float64 `json:"initial_lambda,omitempty"`
	ReassignNeighborCount *int     `json:"reassign_neighbor_count,omitempty"`
	MergeThreshold        *int     `json:"merge_threshold,omitempty"`
	NumCentersToMergeTo   *int     `json:"num_centers_to_merge_to,omitempty"`
	WriteNprobe           *int     `json:"write_nprobe,omitempty"`
	EfConstruction        *int     `json:"ef_construction,omitempty"`
	EfSearch              *int     `json:"ef_search,omitempty"`
	MaxNeighbors          *int     `json:"max_neighbors,omitempty"`
}

type VectorIndexType struct {
	Enabled bool              `json:"enabled"`
	Config  VectorIndexConfig `json:"config"`
}

type FloatListValueType struct {
	VectorIndex *VectorIndexType `json:"vector_index,omitempty"`
}

type StringValueType struct {
	StringInvertedIndex *StringInvertedIndexType `json:"string_inverted_index,omitempty"`
	FtsIndex            *FtsIndexType            `json:"fts_index,omitempty"`
}

type IntValueType struct {
	IntInvertedIndex *IntInvertedIndexType `json:"int_inverted_index,omitempty"`
}

type FloatValueType struct {
	FloatInvertedIndex *FloatInvertedIndexType `json:"float_inverted_index,omitempty"`
}

type BoolValueType struct {
	BoolInvertedIndex *BoolInvertedIndexType `json:"bool_inverted_index,omitempty"`
}

type SparseVectorValueType struct {
	SparseVectorIndex *SparseVectorIndexType `json:"sparse_vector_index,omitempty"`
}

// Index type structs
type StringInvertedIndexType struct {
	Enabled bool                      `json:"enabled"`
	Config  StringInvertedIndexConfig `json:"config"`
}

type IntInvertedIndexType struct {
	Enabled bool                   `json:"enabled"`
	Config  IntInvertedIndexConfig `json:"config"`
}

type FloatInvertedIndexType struct {
	Enabled bool                     `json:"enabled"`
	Config  FloatInvertedIndexConfig `json:"config"`
}

type BoolInvertedIndexType struct {
	Enabled bool                    `json:"enabled"`
	Config  BoolInvertedIndexConfig `json:"config"`
}

type FtsIndexType struct {
	Enabled bool           `json:"enabled"`
	Config  FtsIndexConfig `json:"config"`
}

type SparseVectorIndexType struct {
	Enabled bool                    `json:"enabled"`
	Config  SparseVectorIndexConfig `json:"config"`
}

// Config structs for the index types
type StringInvertedIndexConfig struct{}

type IntInvertedIndexConfig struct{}

type FloatInvertedIndexConfig struct{}

type BoolInvertedIndexConfig struct{}

type FtsIndexConfig struct{}

type SparseVectorIndexConfig struct {
	EmbeddingFunction *EmbeddingFunctionConfiguration `json:"embedding_function,omitempty"`
	SourceKey         *string                         `json:"source_key,omitempty"`
	Bm25              *bool                           `json:"bm25,omitempty"`
}

type ValueTypes struct {
	String       *StringValueType       `json:"string,omitempty"`
	FloatList    *FloatListValueType    `json:"float_list,omitempty"`
	SparseVector *SparseVectorValueType `json:"sparse_vector,omitempty"`
	Int          *IntValueType          `json:"int,omitempty"`
	Float        *FloatValueType        `json:"float,omitempty"`
	Boolean      *BoolValueType         `json:"bool,omitempty"`
}

type Schema struct {
	Defaults ValueTypes            `json:"defaults"`
	Keys     map[string]ValueTypes `json:"keys"`
}

// UpdateSchemaFromConfig merges an InternalCollectionConfiguration into a Schema
// It updates the vector index configuration in the schema with values from the config
func UpdateSchemaFromConfig(config InternalUpdateCollectionConfiguration, schemaStr string) (string, error) {
	// Early return for empty or trivial schema - this is a programming error
	if schemaStr == "" || schemaStr == "{}" {
		return "", fmt.Errorf("schemaStr is empty or trivial: should not call UpdateSchemaFromConfig")
	}

	// Parse the schema
	var schema Schema
	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		return "", fmt.Errorf("failed to parse schema: %w", err)
	}

	// Vector index config exists in two places:
	// 1. defaults.float_list.vector_index - default for all float_list values
	// 2. keys["#embedding"].float_list.vector_index - specific override for #embedding
	// We need to update BOTH to keep them in sync

	embeddingKey := "#embedding"

	// Validate defaults vector index exists (should always be present in a valid schema)
	if schema.Defaults.FloatList == nil || schema.Defaults.FloatList.VectorIndex == nil {
		return "", fmt.Errorf("schema is missing defaults.float_list.vector_index - invalid schema")
	}

	// Validate #embedding key vector index exists (should always be present in a valid schema)
	embeddingValueTypes, exists := schema.Keys[embeddingKey]
	if !exists || embeddingValueTypes.FloatList == nil || embeddingValueTypes.FloatList.VectorIndex == nil {
		return "", fmt.Errorf("schema is missing keys[%s].float_list.vector_index - invalid schema", embeddingKey)
	}

	schemaHnswDefault := schema.Defaults.FloatList.VectorIndex.Config.Hnsw != nil
	schemaSpannDefault := schema.Defaults.FloatList.VectorIndex.Config.Spann != nil
	// Exactly one of schemaHnsw or schemaSpann should be non-nil
	if schemaHnswDefault == schemaSpannDefault {
		return "", fmt.Errorf("schema must have exactly one of HNSW or SPANN")
	}

	schemaHnswEmbedding := embeddingValueTypes.FloatList.VectorIndex.Config.Hnsw != nil
	schemaSpannEmbedding := embeddingValueTypes.FloatList.VectorIndex.Config.Spann != nil
	if schemaHnswEmbedding == schemaSpannEmbedding {
		return "", fmt.Errorf("schema must have exactly one of HNSW or SPANN")
	}

	if schemaHnswDefault != schemaHnswEmbedding || schemaSpannDefault != schemaSpannEmbedding {
		return "", fmt.Errorf("schema and embedding key must have the same index type")
	}

	// Helper function to update a vector index config
	updateVectorIndexConfig := func(vectorIndexType *VectorIndexType) error {
		if config.VectorIndex != nil {
			if config.VectorIndex.Hnsw != nil {
				// Update HNSW config - only update fields that are not nil
				updateHnsw := config.VectorIndex.Hnsw

				// Ensure HNSW config exists in schema
				if vectorIndexType.Config.Hnsw == nil {
					return fmt.Errorf("trying to update hnsw config but schema has spann")
				}

				// Only update fields that are provided in the update
				if updateHnsw.EfSearch != nil {
					vectorIndexType.Config.Hnsw.EfSearch = updateHnsw.EfSearch
				}
				if updateHnsw.MaxNeighbors != nil {
					vectorIndexType.Config.Hnsw.MaxNeighbors = updateHnsw.MaxNeighbors
				}
				if updateHnsw.NumThreads != nil {
					vectorIndexType.Config.Hnsw.NumThreads = updateHnsw.NumThreads
				}
				if updateHnsw.BatchSize != nil {
					vectorIndexType.Config.Hnsw.BatchSize = updateHnsw.BatchSize
				}
				if updateHnsw.SyncThreshold != nil {
					vectorIndexType.Config.Hnsw.SyncThreshold = updateHnsw.SyncThreshold
				}
				if updateHnsw.ResizeFactor != nil {
					vectorIndexType.Config.Hnsw.ResizeFactor = updateHnsw.ResizeFactor
				}

			} else if config.VectorIndex.Spann != nil {
				// Update SPANN config - only update fields that are not nil
				updateSpann := config.VectorIndex.Spann

				// Ensure SPANN config exists in schema
				if vectorIndexType.Config.Spann == nil {
					return fmt.Errorf("trying to update spann config but schema has hnsw")
				}

				// Only update fields that are provided in the update
				if updateSpann.SearchNprobe != nil {
					vectorIndexType.Config.Spann.SearchNprobe = updateSpann.SearchNprobe
				}
				if updateSpann.EfSearch != nil {
					vectorIndexType.Config.Spann.EfSearch = updateSpann.EfSearch
				}
			}
		}

		// Update embedding function if present
		if config.EmbeddingFunction != nil {
			vectorIndexType.Config.EmbeddingFunction = config.EmbeddingFunction
		}
		return nil
	}

	// Update vector index in BOTH locations
	if err := updateVectorIndexConfig(schema.Defaults.FloatList.VectorIndex); err != nil {
		return "", err
	}
	if err := updateVectorIndexConfig(schema.Keys[embeddingKey].FloatList.VectorIndex); err != nil {
		return "", err
	}

	// Serialize the updated schema back to JSON
	updatedSchemaBytes, err := json.Marshal(schema)
	if err != nil {
		return "", fmt.Errorf("failed to serialize updated schema: %w", err)
	}

	return string(updatedSchemaBytes), nil
}
