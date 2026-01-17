package chroma

import (
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

const (
	embeddingFunctionKey     = "embedding_function"
	embeddingFunctionTypeKey = "type"
	embeddingFunctionNameKey = "name"
	embeddingFunctionCfgKey  = "config"
	efTypeKnown              = "known"
)

// CollectionConfigurationImpl is the concrete implementation of CollectionConfiguration
type CollectionConfigurationImpl struct {
	raw map[string]interface{}
}

// NewCollectionConfiguration creates a new CollectionConfigurationImpl with the given schema
func NewCollectionConfiguration() *CollectionConfigurationImpl {
	return &CollectionConfigurationImpl{
		raw: make(map[string]interface{}),
	}
}

// NewCollectionConfigurationFromMap creates a CollectionConfigurationImpl from a raw map
// This is useful when deserializing from API responses
func NewCollectionConfigurationFromMap(raw map[string]interface{}) *CollectionConfigurationImpl {
	config := &CollectionConfigurationImpl{
		raw: raw,
	}

	// Try to extract schema if present
	return config
}

// GetRaw returns the raw value for a given key
func (c *CollectionConfigurationImpl) GetRaw(key string) (interface{}, bool) {
	if c.raw == nil {
		return nil, false
	}
	val, ok := c.raw[key]
	return val, ok
}

// SetRaw sets a raw value for a given key
func (c *CollectionConfigurationImpl) SetRaw(key string, value interface{}) {
	if c.raw == nil {
		c.raw = make(map[string]interface{})
	}
	c.raw[key] = value
}

// Keys returns all keys in the configuration
func (c *CollectionConfigurationImpl) Keys() []string {
	if c.raw == nil {
		return []string{}
	}
	keys := make([]string, 0, len(c.raw))
	for k := range c.raw {
		keys = append(keys, k)
	}
	return keys
}

// MarshalJSON serializes the configuration to JSON
func (c *CollectionConfigurationImpl) MarshalJSON() ([]byte, error) {
	if c.raw == nil {
		c.raw = make(map[string]interface{})
	}

	return json.Marshal(c.raw)
}

// UnmarshalJSON deserializes the configuration from JSON
func (c *CollectionConfigurationImpl) UnmarshalJSON(data []byte) error {
	if c.raw == nil {
		c.raw = make(map[string]interface{})
	}

	if err := json.Unmarshal(data, &c.raw); err != nil {
		return errors.Wrap(err, "failed to unmarshal configuration")
	}

	return nil
}

// EmbeddingFunctionInfo represents the embedding function configuration stored in collection configuration
type EmbeddingFunctionInfo struct {
	Type   string                 `json:"type"`
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
}

// IsKnown returns true if the embedding function type is "known" and can be reconstructed
func (e *EmbeddingFunctionInfo) IsKnown() bool {
	return e != nil && e.Type == efTypeKnown
}

// GetEmbeddingFunctionInfo extracts the embedding function configuration from the collection configuration
func (c *CollectionConfigurationImpl) GetEmbeddingFunctionInfo() (*EmbeddingFunctionInfo, bool) {
	if c.raw == nil {
		return nil, false
	}
	efRaw, ok := c.raw[embeddingFunctionKey]
	if !ok {
		return nil, false
	}
	efMap, ok := efRaw.(map[string]interface{})
	if !ok {
		return nil, false
	}

	info := &EmbeddingFunctionInfo{}
	if t, ok := efMap[embeddingFunctionTypeKey].(string); ok {
		info.Type = t
	}
	if n, ok := efMap[embeddingFunctionNameKey].(string); ok {
		info.Name = n
	}
	if cfg, ok := efMap[embeddingFunctionCfgKey].(map[string]interface{}); ok {
		info.Config = cfg
	}

	return info, true
}

// SetEmbeddingFunctionInfo sets the embedding function configuration in the collection configuration
func (c *CollectionConfigurationImpl) SetEmbeddingFunctionInfo(info *EmbeddingFunctionInfo) {
	if c.raw == nil {
		c.raw = make(map[string]interface{})
	}
	if info == nil {
		return
	}
	c.raw[embeddingFunctionKey] = map[string]interface{}{
		embeddingFunctionTypeKey: info.Type,
		embeddingFunctionNameKey: info.Name,
		embeddingFunctionCfgKey:  info.Config,
	}
}

// SetEmbeddingFunction creates an EmbeddingFunctionInfo from an EmbeddingFunction and stores it
func (c *CollectionConfigurationImpl) SetEmbeddingFunction(ef embeddings.EmbeddingFunction) {
	if ef == nil {
		return
	}
	c.SetEmbeddingFunctionInfo(&EmbeddingFunctionInfo{
		Type:   efTypeKnown,
		Name:   ef.Name(),
		Config: ef.GetConfig(),
	})
}

// GetSchema extracts the Schema from the configuration if present
// Returns nil if no schema is found or if unmarshaling fails
func (c *CollectionConfigurationImpl) GetSchema() *Schema {
	if c.raw == nil {
		return nil
	}

	schemaRaw, ok := c.raw["schema"]
	if !ok {
		return nil
	}

	// Re-marshal and unmarshal to convert map to Schema
	schemaBytes, err := json.Marshal(schemaRaw)
	if err != nil {
		return nil
	}

	var schema Schema
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		return nil
	}

	return &schema
}

// BuildEmbeddingFunctionFromConfig attempts to reconstruct an embedding function from the configuration.
// First tries to get EF from configuration.embedding_function, then from schema if present.
// Returns nil without error if:
// - Configuration is nil
// - No embedding_function in config and no schema with EF
// - Type is not "known"
// - Name not registered in the dense registry
// Returns error if the factory fails to build the embedding function.
func BuildEmbeddingFunctionFromConfig(cfg *CollectionConfigurationImpl) (embeddings.EmbeddingFunction, error) {
	if cfg == nil {
		return nil, nil
	}

	// First try to get EF from direct embedding_function config
	efInfo, ok := cfg.GetEmbeddingFunctionInfo()
	if ok && efInfo != nil && efInfo.IsKnown() && embeddings.HasDense(efInfo.Name) {
		return embeddings.BuildDense(efInfo.Name, efInfo.Config)
	}

	// Try to get EF from schema if present
	schema := cfg.GetSchema()
	if schema != nil {
		ef := schema.GetEmbeddingFunction()
		if ef != nil {
			return ef, nil
		}
	}

	return nil, nil
}
