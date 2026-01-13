package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateSchemaFromConfig_EmptySchema(t *testing.T) {
	efSearch := 50
	config := InternalUpdateCollectionConfiguration{
		VectorIndex: &UpdateVectorIndexConfiguration{
			Hnsw: &UpdateHnswConfiguration{
				EfSearch: &efSearch,
			},
		},
	}

	// Test empty string
	_, err := UpdateSchemaFromConfig(config, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty or trivial")

	// Test empty object
	_, err = UpdateSchemaFromConfig(config, "{}")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty or trivial")
}

func TestUpdateSchemaFromConfig_InvalidSchema(t *testing.T) {
	efSearch := 50
	config := InternalUpdateCollectionConfiguration{
		VectorIndex: &UpdateVectorIndexConfiguration{
			Hnsw: &UpdateHnswConfiguration{
				EfSearch: &efSearch,
			},
		},
	}

	// Schema missing defaults.float_list.vector_index
	invalidSchema := `{"defaults": {}, "keys": {}}`
	_, err := UpdateSchemaFromConfig(config, invalidSchema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing defaults.float_list.vector_index")

	// Schema missing keys["#embedding"].float_list.vector_index
	invalidSchema2 := `{
		"defaults": {
			"float_list": {
				"vector_index": {
					"enabled": false,
					"config": {"hnsw": {}}
				}
			}
		},
		"keys": {}
	}`
	_, err = UpdateSchemaFromConfig(config, invalidSchema2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing keys[#embedding].float_list.vector_index")
}

func TestUpdateSchemaFromConfig_IndexTypeMismatch(t *testing.T) {
	// Config has HNSW update
	efSearch := 50
	hnswConfig := InternalUpdateCollectionConfiguration{
		VectorIndex: &UpdateVectorIndexConfiguration{
			Hnsw: &UpdateHnswConfiguration{
				EfSearch: &efSearch,
			},
		},
	}

	// Schema has SPANN
	spannSchema := `{
		"defaults": {
			"float_list": {
				"vector_index": {
					"enabled": false,
					"config": {
						"space": "l2",
						"spann": {
							"search_nprobe": 10,
							"ef_search": 50
						}
					}
				}
			}
		},
		"keys": {
			"#embedding": {
				"float_list": {
					"vector_index": {
						"enabled": true,
						"config": {
							"space": "l2",
							"spann": {
								"search_nprobe": 10,
								"ef_search": 50
							}
						}
					}
				}
			}
		}
	}`

	_, err := UpdateSchemaFromConfig(hnswConfig, spannSchema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "trying to update hnsw config but schema has spann")
}

func TestUpdateSchemaFromConfig_HnswSuccess(t *testing.T) {
	// Only updating ef_search - all other fields should be preserved
	efSearch := 75
	config := InternalUpdateCollectionConfiguration{
		VectorIndex: &UpdateVectorIndexConfiguration{
			Hnsw: &UpdateHnswConfiguration{
				EfSearch: &efSearch,
			},
		},
	}

	initialSchema := `{
		"defaults": {
			"string": {
				"string_inverted_index": {
					"enabled": true,
					"config": {}
				},
				"fts_index": {
					"enabled": false,
					"config": {}
				}
			},
			"int": {
				"int_inverted_index": {
					"enabled": true,
					"config": {}
				}
			},
			"float": {
				"float_inverted_index": {
					"enabled": true,
					"config": {}
				}
			},
			"bool": {
				"bool_inverted_index": {
					"enabled": true,
					"config": {}
				}
			},
			"float_list": {
				"vector_index": {
					"enabled": false,
					"config": {
						"space": "l2",
						"hnsw": {
							"ef_construction": 100,
							"ef_search": 50,
							"max_neighbors": 16,
							"num_threads": 8,
							"batch_size": 100,
							"sync_threshold": 1000,
							"resize_factor": 1.2
						}
					}
				}
			},
			"sparse_vector": {
				"sparse_vector_index": {
					"enabled": false,
					"config": {
						"bm25": false
					}
				}
			}
		},
		"keys": {
			"#embedding": {
				"float_list": {
					"vector_index": {
						"enabled": true,
						"config": {
							"space": "l2",
							"source_key": "#document",
							"hnsw": {
								"ef_construction": 100,
								"ef_search": 50,
								"max_neighbors": 16,
								"num_threads": 8,
								"batch_size": 100,
								"sync_threshold": 1000,
								"resize_factor": 1.2
							}
						}
					}
				}
			},
			"#document": {
				"string": {
					"fts_index": {
						"enabled": true,
						"config": {}
					},
					"string_inverted_index": {
						"enabled": false,
						"config": {}
					}
				}
			}
		}
	}`

	result, err := UpdateSchemaFromConfig(config, initialSchema)
	require.NoError(t, err)

	// Parse result and verify updates
	var schema Schema
	err = json.Unmarshal([]byte(result), &schema)
	require.NoError(t, err)

	// Check ef_search was updated in both locations
	assert.NotNil(t, schema.Defaults.FloatList.VectorIndex.Config.Hnsw)
	assert.Equal(t, 75, *schema.Defaults.FloatList.VectorIndex.Config.Hnsw.EfSearch)

	embeddingConfig := schema.Keys["#embedding"].FloatList.VectorIndex.Config
	assert.Equal(t, 75, *embeddingConfig.Hnsw.EfSearch)

	// Verify other HNSW fields were preserved (not updated)
	assert.Equal(t, "l2", *schema.Defaults.FloatList.VectorIndex.Config.Space)
	assert.Equal(t, 100, *schema.Defaults.FloatList.VectorIndex.Config.Hnsw.EfConstruction)
	assert.Equal(t, 16, *schema.Defaults.FloatList.VectorIndex.Config.Hnsw.MaxNeighbors)
	assert.Equal(t, 8, *schema.Defaults.FloatList.VectorIndex.Config.Hnsw.NumThreads)
	assert.Equal(t, 1.2, *schema.Defaults.FloatList.VectorIndex.Config.Hnsw.ResizeFactor)
	assert.Equal(t, 100, *schema.Defaults.FloatList.VectorIndex.Config.Hnsw.BatchSize)
	assert.Equal(t, 1000, *schema.Defaults.FloatList.VectorIndex.Config.Hnsw.SyncThreshold)
	assert.Nil(t, schema.Defaults.FloatList.VectorIndex.Config.Spann)

	// Verify source_key was preserved
	assert.Equal(t, "#document", *embeddingConfig.SourceKey)

	// Verify other value types in defaults were not modified
	assert.NotNil(t, schema.Defaults.String)
	assert.NotNil(t, schema.Defaults.String.StringInvertedIndex)
	assert.True(t, schema.Defaults.String.StringInvertedIndex.Enabled)
	assert.NotNil(t, schema.Defaults.String.FtsIndex)
	assert.False(t, schema.Defaults.String.FtsIndex.Enabled)

	assert.NotNil(t, schema.Defaults.Int)
	assert.NotNil(t, schema.Defaults.Int.IntInvertedIndex)
	assert.True(t, schema.Defaults.Int.IntInvertedIndex.Enabled)

	assert.NotNil(t, schema.Defaults.Float)
	assert.NotNil(t, schema.Defaults.Float.FloatInvertedIndex)
	assert.True(t, schema.Defaults.Float.FloatInvertedIndex.Enabled)

	assert.NotNil(t, schema.Defaults.Boolean)
	assert.NotNil(t, schema.Defaults.Boolean.BoolInvertedIndex)
	assert.True(t, schema.Defaults.Boolean.BoolInvertedIndex.Enabled)

	assert.NotNil(t, schema.Defaults.SparseVector)
	assert.NotNil(t, schema.Defaults.SparseVector.SparseVectorIndex)
	assert.False(t, schema.Defaults.SparseVector.SparseVectorIndex.Enabled)

	// Verify #document key was preserved
	assert.NotNil(t, schema.Keys["#document"])
	assert.NotNil(t, schema.Keys["#document"].String)
	assert.NotNil(t, schema.Keys["#document"].String.FtsIndex)
	assert.True(t, schema.Keys["#document"].String.FtsIndex.Enabled)
	assert.NotNil(t, schema.Keys["#document"].String.StringInvertedIndex)
	assert.False(t, schema.Keys["#document"].String.StringInvertedIndex.Enabled)
}

func TestUpdateSchemaFromConfig_SpannSuccess(t *testing.T) {
	// Only updating search_nprobe and ef_search
	searchNprobe := 20
	efSearch := 80
	config := InternalUpdateCollectionConfiguration{
		VectorIndex: &UpdateVectorIndexConfiguration{
			Spann: &UpdateSpannConfiguration{
				SearchNprobe: &searchNprobe,
				EfSearch:     &efSearch,
			},
		},
	}

	initialSchema := `{
		"defaults": {
			"string": {
				"string_inverted_index": {
					"enabled": true,
					"config": {}
				},
				"fts_index": {
					"enabled": false,
					"config": {}
				}
			},
			"int": {
				"int_inverted_index": {
					"enabled": true,
					"config": {}
				}
			},
			"float": {
				"float_inverted_index": {
					"enabled": true,
					"config": {}
				}
			},
			"bool": {
				"bool_inverted_index": {
					"enabled": true,
					"config": {}
				}
			},
			"float_list": {
				"vector_index": {
					"enabled": false,
					"config": {
						"space": "l2",
						"spann": {
							"search_nprobe": 10,
							"write_nprobe": 5,
							"ef_construction": 100,
							"ef_search": 50,
							"max_neighbors": 16,
							"reassign_neighbor_count": 32,
							"split_threshold": 80,
							"merge_threshold": 40,
							"search_rng_factor": 1.5,
							"num_samples_kmeans": 1000
						}
					}
				}
			},
			"sparse_vector": {
				"sparse_vector_index": {
					"enabled": false,
					"config": {
						"bm25": false
					}
				}
			}
		},
		"keys": {
			"#embedding": {
				"float_list": {
					"vector_index": {
						"enabled": true,
						"config": {
							"space": "l2",
							"spann": {
								"search_nprobe": 10,
								"write_nprobe": 5,
								"ef_construction": 100,
								"ef_search": 50,
								"max_neighbors": 16,
								"reassign_neighbor_count": 32,
								"split_threshold": 80,
								"merge_threshold": 40,
								"search_rng_factor": 1.5,
								"num_samples_kmeans": 1000
							}
						}
					}
				}
			},
			"#document": {
				"string": {
					"fts_index": {
						"enabled": true,
						"config": {}
					},
					"string_inverted_index": {
						"enabled": false,
						"config": {}
					}
				}
			}
		}
	}`

	result, err := UpdateSchemaFromConfig(config, initialSchema)
	require.NoError(t, err)

	// Parse result and verify updates
	var schema Schema
	err = json.Unmarshal([]byte(result), &schema)
	require.NoError(t, err)

	// Check the two fields were updated in both locations
	assert.NotNil(t, schema.Defaults.FloatList.VectorIndex.Config.Spann)
	assert.Equal(t, 20, *schema.Defaults.FloatList.VectorIndex.Config.Spann.SearchNprobe)
	assert.Equal(t, 80, *schema.Defaults.FloatList.VectorIndex.Config.Spann.EfSearch)

	embeddingConfig := schema.Keys["#embedding"].FloatList.VectorIndex.Config
	assert.Equal(t, 20, *embeddingConfig.Spann.SearchNprobe)
	assert.Equal(t, 80, *embeddingConfig.Spann.EfSearch)

	// Verify other SPANN fields were preserved (not updated)
	assert.Equal(t, "l2", *schema.Defaults.FloatList.VectorIndex.Config.Space)
	assert.Equal(t, 5, *schema.Defaults.FloatList.VectorIndex.Config.Spann.WriteNprobe)
	assert.Equal(t, 100, *schema.Defaults.FloatList.VectorIndex.Config.Spann.EfConstruction)
	assert.Equal(t, 16, *schema.Defaults.FloatList.VectorIndex.Config.Spann.MaxNeighbors)
	assert.Equal(t, 32, *schema.Defaults.FloatList.VectorIndex.Config.Spann.ReassignNeighborCount)
	assert.Equal(t, 80, *schema.Defaults.FloatList.VectorIndex.Config.Spann.SplitThreshold)
	assert.Equal(t, 40, *schema.Defaults.FloatList.VectorIndex.Config.Spann.MergeThreshold)
	assert.Nil(t, schema.Defaults.FloatList.VectorIndex.Config.Hnsw)

	// Verify fields not in UpdateSpannConfiguration were preserved
	assert.Equal(t, 1.5, *schema.Defaults.FloatList.VectorIndex.Config.Spann.SearchRngFactor)
	assert.Equal(t, 1000, *schema.Defaults.FloatList.VectorIndex.Config.Spann.NumSamplesKmeans)

	// Verify other value types in defaults were not modified
	assert.NotNil(t, schema.Defaults.String)
	assert.NotNil(t, schema.Defaults.String.StringInvertedIndex)
	assert.True(t, schema.Defaults.String.StringInvertedIndex.Enabled)
	assert.NotNil(t, schema.Defaults.String.FtsIndex)
	assert.False(t, schema.Defaults.String.FtsIndex.Enabled)

	assert.NotNil(t, schema.Defaults.Int)
	assert.NotNil(t, schema.Defaults.Int.IntInvertedIndex)
	assert.True(t, schema.Defaults.Int.IntInvertedIndex.Enabled)

	assert.NotNil(t, schema.Defaults.Float)
	assert.NotNil(t, schema.Defaults.Float.FloatInvertedIndex)
	assert.True(t, schema.Defaults.Float.FloatInvertedIndex.Enabled)

	assert.NotNil(t, schema.Defaults.Boolean)
	assert.NotNil(t, schema.Defaults.Boolean.BoolInvertedIndex)
	assert.True(t, schema.Defaults.Boolean.BoolInvertedIndex.Enabled)

	assert.NotNil(t, schema.Defaults.SparseVector)
	assert.NotNil(t, schema.Defaults.SparseVector.SparseVectorIndex)
	assert.False(t, schema.Defaults.SparseVector.SparseVectorIndex.Enabled)

	// Verify #document key was preserved
	assert.NotNil(t, schema.Keys["#document"])
	assert.NotNil(t, schema.Keys["#document"].String)
	assert.NotNil(t, schema.Keys["#document"].String.FtsIndex)
	assert.True(t, schema.Keys["#document"].String.FtsIndex.Enabled)
	assert.NotNil(t, schema.Keys["#document"].String.StringInvertedIndex)
	assert.False(t, schema.Keys["#document"].String.StringInvertedIndex.Enabled)
}

func TestUpdateSchemaFromConfig_EmbeddingFunction(t *testing.T) {
	config := InternalUpdateCollectionConfiguration{
		EmbeddingFunction: &EmbeddingFunctionConfiguration{
			Type:   "known",
			Name:   "custom-embedder",
			Config: map[string]interface{}{"model": "text-embedding-3-small"},
		},
	}

	initialSchema := `{
		"defaults": {
			"string": {
				"string_inverted_index": {
					"enabled": true,
					"config": {}
				},
				"fts_index": {
					"enabled": false,
					"config": {}
				}
			},
			"int": {
				"int_inverted_index": {
					"enabled": true,
					"config": {}
				}
			},
			"float": {
				"float_inverted_index": {
					"enabled": true,
					"config": {}
				}
			},
			"bool": {
				"bool_inverted_index": {
					"enabled": true,
					"config": {}
				}
			},
			"float_list": {
				"vector_index": {
					"enabled": false,
					"config": {
						"hnsw": {}
					}
				}
			},
			"sparse_vector": {
				"sparse_vector_index": {
					"enabled": false,
					"config": {
						"bm25": false
					}
				}
			}
		},
		"keys": {
			"#embedding": {
				"float_list": {
					"vector_index": {
						"enabled": true,
						"config": {
							"hnsw": {},
							"embedding_function": {
								"type": "known",
								"name": "old-embedder",
								"config": {}
							}
						}
					}
				}
			},
			"#document": {
				"string": {
					"fts_index": {
						"enabled": true,
						"config": {}
					},
					"string_inverted_index": {
						"enabled": false,
						"config": {}
					}
				}
			}
		}
	}`

	result, err := UpdateSchemaFromConfig(config, initialSchema)
	require.NoError(t, err)

	var schema Schema
	err = json.Unmarshal([]byte(result), &schema)
	require.NoError(t, err)

	// Verify embedding function was updated in both locations
	assert.Equal(t, "custom-embedder", schema.Defaults.FloatList.VectorIndex.Config.EmbeddingFunction.Name)
	assert.Equal(t, "custom-embedder", schema.Keys["#embedding"].FloatList.VectorIndex.Config.EmbeddingFunction.Name)

	// Verify other value types in defaults were not modified
	assert.NotNil(t, schema.Defaults.String)
	assert.NotNil(t, schema.Defaults.String.StringInvertedIndex)
	assert.True(t, schema.Defaults.String.StringInvertedIndex.Enabled)

	assert.NotNil(t, schema.Defaults.Int)
	assert.NotNil(t, schema.Defaults.Int.IntInvertedIndex)
	assert.True(t, schema.Defaults.Int.IntInvertedIndex.Enabled)

	assert.NotNil(t, schema.Defaults.Float)
	assert.NotNil(t, schema.Defaults.Float.FloatInvertedIndex)
	assert.True(t, schema.Defaults.Float.FloatInvertedIndex.Enabled)

	assert.NotNil(t, schema.Defaults.Boolean)
	assert.NotNil(t, schema.Defaults.Boolean.BoolInvertedIndex)
	assert.True(t, schema.Defaults.Boolean.BoolInvertedIndex.Enabled)

	assert.NotNil(t, schema.Defaults.SparseVector)
	assert.NotNil(t, schema.Defaults.SparseVector.SparseVectorIndex)
	assert.False(t, schema.Defaults.SparseVector.SparseVectorIndex.Enabled)

	// Verify #document key was preserved
	assert.NotNil(t, schema.Keys["#document"])
	assert.NotNil(t, schema.Keys["#document"].String)
	assert.NotNil(t, schema.Keys["#document"].String.FtsIndex)
	assert.True(t, schema.Keys["#document"].String.FtsIndex.Enabled)
}
