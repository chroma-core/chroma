package model

type EmbeddingFunctionConfiguration struct {
	Type   string                             `json:"type"`
	Config *EmbeddingFunctionNewConfiguration `json:"config,omitempty"`
}

type EmbeddingFunctionNewConfiguration struct {
	Name   string      `json:"name"`
	Config interface{} `json:"config"`
}

type VectorIndexConfiguration struct {
	Type  string              `json:"type"`
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
			Type: "hnsw",
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
	Type  string                    `json:"type"`
	Hnsw  *UpdateHnswConfiguration  `json:"hnsw,omitempty"`
	Spann *UpdateSpannConfiguration `json:"spann,omitempty"`
}

type InternalUpdateCollectionConfiguration struct {
	VectorIndex       *UpdateVectorIndexConfiguration `json:"vector_index,omitempty"`
	EmbeddingFunction *EmbeddingFunctionConfiguration `json:"embedding_function,omitempty"`
}
