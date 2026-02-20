package chroma

import (
	"encoding/json"
	"regexp"

	"github.com/creasty/defaults"
	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// Space represents the distance metric for vector similarity search
type Space string

const (
	SpaceL2     Space = "l2"
	SpaceCosine Space = "cosine"
	SpaceIP     Space = "ip"
)

// Reserved keys for system-managed fields
const (
	DocumentKey  = "#document"
	EmbeddingKey = "#embedding"
)

// CmekProvider represents supported cloud providers for customer-managed encryption keys
type CmekProvider string

const (
	CmekProviderGCP CmekProvider = "gcp"
)

// cmekGCPPattern validates GCP KMS resource format.
// MustCompile is safe here: the pattern is a compile-time constant that is known valid.
var cmekGCPPattern = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+$`)

// Cmek represents a customer-managed encryption key configuration
type Cmek struct {
	Provider CmekProvider
	Resource string
}

// NewGCPCmek creates a CMEK configuration for Google Cloud Platform KMS.
// The resource should be in the format:
// projects/{project-id}/locations/{location}/keyRings/{key-ring}/cryptoKeys/{key}
//
// Validation occurs when the CMEK is added to a schema via WithCmek.
// For early validation, call ValidatePattern() on the returned Cmek.
func NewGCPCmek(resource string) *Cmek {
	return &Cmek{
		Provider: CmekProviderGCP,
		Resource: resource,
	}
}

// ValidatePattern validates the CMEK resource format for the provider.
// This validates format only; it does not verify key accessibility.
func (c *Cmek) ValidatePattern() error {
	if c == nil {
		return errors.New("cmek is nil")
	}
	switch c.Provider {
	case CmekProviderGCP:
		if !cmekGCPPattern.MatchString(c.Resource) {
			return errors.New("invalid GCP CMEK resource format: expected projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}")
		}
	default:
		return errors.Errorf("unsupported CMEK provider: %s", c.Provider)
	}
	return nil
}

// MarshalJSON serializes CMEK to the variant format {"provider": "resource"}
func (c *Cmek) MarshalJSON() ([]byte, error) {
	switch c.Provider {
	case CmekProviderGCP:
		return json.Marshal(map[string]string{"gcp": c.Resource})
	default:
		return nil, errors.Errorf("unknown CMEK provider: %s", c.Provider)
	}
}

// UnmarshalJSON deserializes CMEK from the variant format {"provider": "resource"}
// and validates the resource format.
func (c *Cmek) UnmarshalJSON(data []byte) error {
	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return errors.Wrap(err, "failed to unmarshal CMEK")
	}
	if resource, ok := raw["gcp"]; ok {
		c.Provider = CmekProviderGCP
		c.Resource = resource
		return c.ValidatePattern()
	}
	return errors.Errorf("unsupported or missing CMEK provider in data: %v", raw)
}

// HnswIndexConfig represents HNSW algorithm parameters
type HnswIndexConfig struct {
	EfConstruction uint    `json:"ef_construction,omitempty" default:"100"`
	MaxNeighbors   uint    `json:"max_neighbors,omitempty" default:"16"`
	EfSearch       uint    `json:"ef_search,omitempty" default:"100"`
	NumThreads     uint    `json:"num_threads,omitempty" default:"1"`
	BatchSize      uint    `json:"batch_size,omitempty" default:"100" validate:"min=2"`
	SyncThreshold  uint    `json:"sync_threshold,omitempty" default:"1000" validate:"min=2"`
	ResizeFactor   float64 `json:"resize_factor,omitempty" default:"1.2"`
}

// HnswOption configures an HnswIndexConfig
type HnswOption func(*HnswIndexConfig)

// NewHnswConfig creates a new HnswIndexConfig with the given options
func NewHnswConfig(opts ...HnswOption) *HnswIndexConfig {
	cfg := &HnswIndexConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// NewHnswConfigWithDefaults creates a new HnswIndexConfig with defaults applied and validation
func NewHnswConfigWithDefaults(opts ...HnswOption) (*HnswIndexConfig, error) {
	cfg := &HnswIndexConfig{}
	if err := defaults.Set(cfg); err != nil {
		return nil, errors.Wrap(err, "failed to set defaults")
	}
	for _, opt := range opts {
		opt(cfg)
	}
	validate := validator.New()
	if err := validate.Struct(cfg); err != nil {
		return nil, errors.Wrap(err, "validation failed")
	}
	return cfg, nil
}

func WithEfConstruction(ef uint) HnswOption {
	return func(c *HnswIndexConfig) {
		c.EfConstruction = ef
	}
}

func WithMaxNeighbors(m uint) HnswOption {
	return func(c *HnswIndexConfig) {
		c.MaxNeighbors = m
	}
}

func WithEfSearch(ef uint) HnswOption {
	return func(c *HnswIndexConfig) {
		c.EfSearch = ef
	}
}

func WithNumThreads(n uint) HnswOption {
	return func(c *HnswIndexConfig) {
		c.NumThreads = n
	}
}

func WithBatchSize(size uint) HnswOption {
	return func(c *HnswIndexConfig) {
		c.BatchSize = size
	}
}

func WithSyncThreshold(threshold uint) HnswOption {
	return func(c *HnswIndexConfig) {
		c.SyncThreshold = threshold
	}
}

func WithResizeFactor(factor float64) HnswOption {
	return func(c *HnswIndexConfig) {
		c.ResizeFactor = factor
	}
}

// SpannQuantization represents supported quantization implementations for SPANN.
type SpannQuantization string

const (
	SpannQuantizationNone SpannQuantization = "none"
	// RaBitQ naming follows upstream Chroma and the wire value.
	SpannQuantizationFourBitRabitQWithUSearch SpannQuantization = "four_bit_rabit_q_with_u_search"
	// Deprecated: Use [SpannQuantizationFourBitRabitQWithUSearch] instead.
	SpannQuantizationFourBitRabbitQWithUSearch = SpannQuantizationFourBitRabitQWithUSearch
)

// SpannIndexConfig represents SPANN algorithm configuration for Chroma Cloud
type SpannIndexConfig struct {
	SearchNprobe          uint              `json:"search_nprobe,omitempty" default:"64" validate:"omitempty,min=1,max=128"`
	SearchRngFactor       float64           `json:"search_rng_factor,omitempty" default:"1.0"`
	SearchRngEpsilon      float64           `json:"search_rng_epsilon,omitempty" default:"10.0" validate:"omitempty,min=5.0,max=10.0"`
	NReplicaCount         uint              `json:"nreplica_count,omitempty" default:"8" validate:"omitempty,min=1,max=8"`
	WriteRngFactor        float64           `json:"write_rng_factor,omitempty" default:"1.0"`
	WriteRngEpsilon       float64           `json:"write_rng_epsilon,omitempty" default:"5.0" validate:"omitempty,min=5.0,max=10.0"`
	SplitThreshold        uint              `json:"split_threshold,omitempty" default:"50" validate:"omitempty,min=50,max=200"`
	NumSamplesKmeans      uint              `json:"num_samples_kmeans,omitempty" default:"1000" validate:"omitempty,min=1,max=1000"`
	InitialLambda         float64           `json:"initial_lambda,omitempty" default:"100.0"`
	ReassignNeighborCount uint              `json:"reassign_neighbor_count,omitempty" default:"64" validate:"omitempty,min=1,max=64"`
	MergeThreshold        uint              `json:"merge_threshold,omitempty" default:"25" validate:"omitempty,min=25,max=100"`
	NumCentersToMergeTo   uint              `json:"num_centers_to_merge_to,omitempty" default:"8" validate:"omitempty,min=1,max=8"`
	WriteNprobe           uint              `json:"write_nprobe,omitempty" default:"32" validate:"omitempty,min=1,max=64"`
	EfConstruction        uint              `json:"ef_construction,omitempty" default:"200" validate:"omitempty,min=1,max=200"`
	EfSearch              uint              `json:"ef_search,omitempty" default:"200" validate:"omitempty,min=1,max=200"`
	MaxNeighbors          uint              `json:"max_neighbors,omitempty" default:"64" validate:"omitempty,min=1,max=64"`
	Quantize              SpannQuantization `json:"quantize,omitempty" validate:"omitempty,oneof=none four_bit_rabit_q_with_u_search"`
}

// SpannOption configures a SpannIndexConfig
type SpannOption func(*SpannIndexConfig)

// NewSpannConfig creates a new SpannIndexConfig with the given options
func NewSpannConfig(opts ...SpannOption) *SpannIndexConfig {
	cfg := &SpannIndexConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// NewSpannConfigWithDefaults creates a new SpannIndexConfig with defaults applied and validation
func NewSpannConfigWithDefaults(opts ...SpannOption) (*SpannIndexConfig, error) {
	cfg := &SpannIndexConfig{}
	if err := defaults.Set(cfg); err != nil {
		return nil, errors.Wrap(err, "failed to set defaults")
	}
	for _, opt := range opts {
		opt(cfg)
	}
	validate := validator.New()
	if err := validate.Struct(cfg); err != nil {
		return nil, errors.Wrap(err, "validation failed")
	}
	return cfg, nil
}

func WithSpannSearchNprobe(n uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.SearchNprobe = n
	}
}

func WithSpannSearchRngFactor(f float64) SpannOption {
	return func(c *SpannIndexConfig) {
		c.SearchRngFactor = f
	}
}

func WithSpannSearchRngEpsilon(e float64) SpannOption {
	return func(c *SpannIndexConfig) {
		c.SearchRngEpsilon = e
	}
}

func WithSpannNReplicaCount(n uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.NReplicaCount = n
	}
}

func WithSpannWriteRngFactor(f float64) SpannOption {
	return func(c *SpannIndexConfig) {
		c.WriteRngFactor = f
	}
}

func WithSpannWriteRngEpsilon(e float64) SpannOption {
	return func(c *SpannIndexConfig) {
		c.WriteRngEpsilon = e
	}
}

func WithSpannSplitThreshold(t uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.SplitThreshold = t
	}
}

func WithSpannNumSamplesKmeans(n uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.NumSamplesKmeans = n
	}
}

func WithSpannInitialLambda(l float64) SpannOption {
	return func(c *SpannIndexConfig) {
		c.InitialLambda = l
	}
}

func WithSpannReassignNeighborCount(n uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.ReassignNeighborCount = n
	}
}

func WithSpannMergeThreshold(t uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.MergeThreshold = t
	}
}

func WithSpannNumCentersToMergeTo(n uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.NumCentersToMergeTo = n
	}
}

func WithSpannWriteNprobe(n uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.WriteNprobe = n
	}
}

func WithSpannEfConstruction(ef uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.EfConstruction = ef
	}
}

func WithSpannEfSearch(ef uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.EfSearch = ef
	}
}

func WithSpannMaxNeighbors(m uint) SpannOption {
	return func(c *SpannIndexConfig) {
		c.MaxNeighbors = m
	}
}

func WithSpannQuantize(q SpannQuantization) SpannOption {
	return func(c *SpannIndexConfig) {
		c.Quantize = q
	}
}

// VectorIndexConfig represents configuration for dense vector indexing
type VectorIndexConfig struct {
	Space             Space                        `json:"space,omitempty"`
	EmbeddingFunction embeddings.EmbeddingFunction `json:"-"`
	SourceKey         string                       `json:"source_key,omitempty"`
	Hnsw              *HnswIndexConfig             `json:"hnsw,omitempty"`
	Spann             *SpannIndexConfig            `json:"spann,omitempty"`
}

// vectorIndexConfigJSON is the JSON representation of VectorIndexConfig
type vectorIndexConfigJSON struct {
	Space             Space                  `json:"space,omitempty"`
	EmbeddingFunction *EmbeddingFunctionInfo `json:"embedding_function,omitempty"`
	SourceKey         string                 `json:"source_key,omitempty"`
	Hnsw              *HnswIndexConfig       `json:"hnsw,omitempty"`
	Spann             *SpannIndexConfig      `json:"spann,omitempty"`
}

// MarshalJSON serializes VectorIndexConfig to JSON, including EmbeddingFunction as EmbeddingFunctionInfo
func (v *VectorIndexConfig) MarshalJSON() ([]byte, error) {
	j := vectorIndexConfigJSON{
		Space:     v.Space,
		SourceKey: v.SourceKey,
		Hnsw:      v.Hnsw,
		Spann:     v.Spann,
	}

	// Convert EmbeddingFunction to EmbeddingFunctionInfo if present
	if v.EmbeddingFunction != nil {
		j.EmbeddingFunction = &EmbeddingFunctionInfo{
			Type:   efTypeKnown,
			Name:   v.EmbeddingFunction.Name(),
			Config: v.EmbeddingFunction.GetConfig(),
		}
	}

	return json.Marshal(j)
}

// UnmarshalJSON deserializes VectorIndexConfig from JSON, optionally reconstructing EmbeddingFunction
func (v *VectorIndexConfig) UnmarshalJSON(data []byte) error {
	var j vectorIndexConfigJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return errors.Wrap(err, "failed to unmarshal vector index config")
	}

	v.Space = j.Space
	v.SourceKey = j.SourceKey
	v.Hnsw = j.Hnsw
	v.Spann = j.Spann

	// Try to reconstruct EmbeddingFunction from EmbeddingFunctionInfo
	if j.EmbeddingFunction != nil && j.EmbeddingFunction.IsKnown() {
		if embeddings.HasDense(j.EmbeddingFunction.Name) {
			ef, err := embeddings.BuildDense(j.EmbeddingFunction.Name, j.EmbeddingFunction.Config)
			if err == nil {
				v.EmbeddingFunction = ef
			}
			// Silently ignore build errors - EF will be nil
		}
	}

	return nil
}

// VectorIndexOption configures a VectorIndexConfig
type VectorIndexOption func(*VectorIndexConfig)

// NewVectorIndexConfig creates a new VectorIndexConfig with the given options
func NewVectorIndexConfig(opts ...VectorIndexOption) *VectorIndexConfig {
	cfg := &VectorIndexConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func WithSpace(space Space) VectorIndexOption {
	return func(c *VectorIndexConfig) {
		c.Space = space
	}
}

func WithVectorEmbeddingFunction(ef embeddings.EmbeddingFunction) VectorIndexOption {
	return func(c *VectorIndexConfig) {
		c.EmbeddingFunction = ef
	}
}

func WithSourceKey(key string) VectorIndexOption {
	return func(c *VectorIndexConfig) {
		c.SourceKey = key
	}
}

func WithHnsw(cfg *HnswIndexConfig) VectorIndexOption {
	return func(c *VectorIndexConfig) {
		c.Hnsw = cfg
	}
}

func WithSpann(cfg *SpannIndexConfig) VectorIndexOption {
	return func(c *VectorIndexConfig) {
		c.Spann = cfg
	}
}

// FtsIndexConfig represents Full-Text Search index configuration
type FtsIndexConfig struct{}

// SparseVectorIndexConfig represents configuration for sparse vector indexing
type SparseVectorIndexConfig struct {
	EmbeddingFunction embeddings.SparseEmbeddingFunction `json:"-"`
	SourceKey         string                             `json:"source_key,omitempty"`
	BM25              bool                               `json:"bm25,omitempty"`
}

// sparseVectorIndexConfigJSON is the JSON representation of SparseVectorIndexConfig
type sparseVectorIndexConfigJSON struct {
	EmbeddingFunction *EmbeddingFunctionInfo `json:"embedding_function,omitempty"`
	SourceKey         string                 `json:"source_key,omitempty"`
	BM25              bool                   `json:"bm25,omitempty"`
}

// MarshalJSON serializes SparseVectorIndexConfig to JSON, including EmbeddingFunction as EmbeddingFunctionInfo
func (s *SparseVectorIndexConfig) MarshalJSON() ([]byte, error) {
	j := sparseVectorIndexConfigJSON{
		SourceKey: s.SourceKey,
		BM25:      s.BM25,
	}

	if s.EmbeddingFunction != nil {
		j.EmbeddingFunction = &EmbeddingFunctionInfo{
			Type:   efTypeKnown,
			Name:   s.EmbeddingFunction.Name(),
			Config: s.EmbeddingFunction.GetConfig(),
		}
	}

	return json.Marshal(j)
}

// UnmarshalJSON deserializes SparseVectorIndexConfig from JSON, optionally reconstructing EmbeddingFunction
func (s *SparseVectorIndexConfig) UnmarshalJSON(data []byte) error {
	var j sparseVectorIndexConfigJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return errors.Wrap(err, "failed to unmarshal sparse vector index config")
	}

	s.SourceKey = j.SourceKey
	s.BM25 = j.BM25

	// Try to reconstruct SparseEmbeddingFunction from EmbeddingFunctionInfo
	if j.EmbeddingFunction != nil && j.EmbeddingFunction.IsKnown() {
		if embeddings.HasSparse(j.EmbeddingFunction.Name) {
			ef, err := embeddings.BuildSparse(j.EmbeddingFunction.Name, j.EmbeddingFunction.Config)
			if err == nil {
				s.EmbeddingFunction = ef
			}
			// Silently ignore build errors - EF will be nil
		}
	}

	return nil
}

// SparseVectorIndexOption configures a SparseVectorIndexConfig
type SparseVectorIndexOption func(*SparseVectorIndexConfig)

// NewSparseVectorIndexConfig creates a new SparseVectorIndexConfig with the given options
func NewSparseVectorIndexConfig(opts ...SparseVectorIndexOption) *SparseVectorIndexConfig {
	cfg := &SparseVectorIndexConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func WithSparseEmbeddingFunction(ef embeddings.SparseEmbeddingFunction) SparseVectorIndexOption {
	return func(c *SparseVectorIndexConfig) {
		c.EmbeddingFunction = ef
	}
}

func WithSparseSourceKey(key string) SparseVectorIndexOption {
	return func(c *SparseVectorIndexConfig) {
		c.SourceKey = key
	}
}

func WithBM25(enabled bool) SparseVectorIndexOption {
	return func(c *SparseVectorIndexConfig) {
		c.BM25 = enabled
	}
}

// Inverted index configs for metadata fields
type StringInvertedIndexConfig struct{}
type IntInvertedIndexConfig struct{}
type FloatInvertedIndexConfig struct{}
type BoolInvertedIndexConfig struct{}

// Index type wrappers - pair enabled state with configuration

// VectorIndexType wraps VectorIndexConfig with enabled state
type VectorIndexType struct {
	Enabled bool               `json:"enabled"`
	Config  *VectorIndexConfig `json:"config,omitempty"`
}

// FtsIndexType wraps FtsIndexConfig with enabled state
type FtsIndexType struct {
	Enabled bool            `json:"enabled"`
	Config  *FtsIndexConfig `json:"config,omitempty"`
}

// SparseVectorIndexType wraps SparseVectorIndexConfig with enabled state
type SparseVectorIndexType struct {
	Enabled bool                     `json:"enabled"`
	Config  *SparseVectorIndexConfig `json:"config,omitempty"`
}

// StringInvertedIndexType wraps StringInvertedIndexConfig with enabled state
type StringInvertedIndexType struct {
	Enabled bool                       `json:"enabled"`
	Config  *StringInvertedIndexConfig `json:"config,omitempty"`
}

// IntInvertedIndexType wraps IntInvertedIndexConfig with enabled state
type IntInvertedIndexType struct {
	Enabled bool                    `json:"enabled"`
	Config  *IntInvertedIndexConfig `json:"config,omitempty"`
}

// FloatInvertedIndexType wraps FloatInvertedIndexConfig with enabled state
type FloatInvertedIndexType struct {
	Enabled bool                      `json:"enabled"`
	Config  *FloatInvertedIndexConfig `json:"config,omitempty"`
}

// BoolInvertedIndexType wraps BoolInvertedIndexConfig with enabled state
type BoolInvertedIndexType struct {
	Enabled bool                     `json:"enabled"`
	Config  *BoolInvertedIndexConfig `json:"config,omitempty"`
}

// Value type structures - map data types to applicable indexes

// StringValueType defines indexes applicable to string fields
type StringValueType struct {
	FtsIndex            *FtsIndexType            `json:"fts_index,omitempty"`
	StringInvertedIndex *StringInvertedIndexType `json:"string_inverted_index,omitempty"`
}

// FloatListValueType defines indexes for dense vectors
type FloatListValueType struct {
	VectorIndex *VectorIndexType `json:"vector_index,omitempty"`
}

// SparseVectorValueType defines indexes for sparse vectors
type SparseVectorValueType struct {
	SparseVectorIndex *SparseVectorIndexType `json:"sparse_vector_index,omitempty"`
}

// IntValueType defines indexes for integer metadata
type IntValueType struct {
	IntInvertedIndex *IntInvertedIndexType `json:"int_inverted_index,omitempty"`
}

// FloatValueType defines indexes for float metadata
type FloatValueType struct {
	FloatInvertedIndex *FloatInvertedIndexType `json:"float_inverted_index,omitempty"`
}

// BoolValueType defines indexes for boolean metadata
type BoolValueType struct {
	BoolInvertedIndex *BoolInvertedIndexType `json:"bool_inverted_index,omitempty"`
}

// ValueTypes contains all value type configurations
type ValueTypes struct {
	String       *StringValueType       `json:"string,omitempty"`
	FloatList    *FloatListValueType    `json:"float_list,omitempty"`
	SparseVector *SparseVectorValueType `json:"sparse_vector,omitempty"`
	Int          *IntValueType          `json:"int,omitempty"`
	Float        *FloatValueType        `json:"float,omitempty"`
	Bool         *BoolValueType         `json:"bool,omitempty"`
}

// Schema manages index configurations for a collection
type Schema struct {
	defaults *ValueTypes
	keys     map[string]*ValueTypes
	cmek     *Cmek
}

// SchemaOption configures a Schema
type SchemaOption func(*Schema) error

// NewSchema creates a new Schema with the given options.
//
// Example:
//
//	schema, err := NewSchema(
//		WithDefaultVectorIndex(NewVectorIndexConfig(WithSpace(SpaceCosine))),
//		WithStringIndex("category"),
//		WithIntIndex("year"),
//	)
//	if err != nil {
//		// handle error
//	}
//
// For a schema with default L2 vector index, use [NewSchemaWithDefaults].
func NewSchema(opts ...SchemaOption) (*Schema, error) {
	s := &Schema{
		defaults: &ValueTypes{},
		keys:     make(map[string]*ValueTypes),
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// NewSchemaWithDefaults creates a Schema with L2 vector index configuration.
// All other indexes (FTS, string, int, float, bool) are enabled by default
// in Chroma, so they don't need to be explicitly set.
func NewSchemaWithDefaults() (*Schema, error) {
	return NewSchema(
		WithDefaultVectorIndex(NewVectorIndexConfig(
			WithSpace(SpaceL2),
			WithHnsw(NewHnswConfig(
				WithEfConstruction(100),
				WithMaxNeighbors(16),
				WithEfSearch(10),
			)),
		)),
	)
}

// Default configuration options

func WithDefaultVectorIndex(cfg *VectorIndexConfig) SchemaOption {
	return func(s *Schema) error {
		if cfg == nil {
			return errors.New("vector index config cannot be nil")
		}
		// Vector index must be on #embedding key, not in defaults (Chroma Cloud requirement)
		if s.keys[EmbeddingKey] == nil {
			s.keys[EmbeddingKey] = &ValueTypes{}
		}
		if s.keys[EmbeddingKey].FloatList == nil {
			s.keys[EmbeddingKey].FloatList = &FloatListValueType{}
		}
		s.keys[EmbeddingKey].FloatList.VectorIndex = &VectorIndexType{
			Enabled: true,
			Config:  cfg,
		}
		return nil
	}
}

func WithDefaultSparseVectorIndex(cfg *SparseVectorIndexConfig) SchemaOption {
	return func(s *Schema) error {
		if cfg == nil {
			return errors.New("sparse vector index config cannot be nil")
		}
		if s.defaults.SparseVector == nil {
			s.defaults.SparseVector = &SparseVectorValueType{}
		}
		s.defaults.SparseVector.SparseVectorIndex = &SparseVectorIndexType{
			Enabled: true,
			Config:  cfg,
		}
		return nil
	}
}

func WithDefaultFtsIndex(cfg *FtsIndexConfig) SchemaOption {
	return func(s *Schema) error {
		if cfg == nil {
			return errors.New("FTS index config cannot be nil")
		}
		// FTS index must be on #document key, not in defaults (Chroma Cloud requirement)
		if s.keys[DocumentKey] == nil {
			s.keys[DocumentKey] = &ValueTypes{}
		}
		if s.keys[DocumentKey].String == nil {
			s.keys[DocumentKey].String = &StringValueType{}
		}
		s.keys[DocumentKey].String.FtsIndex = &FtsIndexType{
			Enabled: true,
			Config:  cfg,
		}
		return nil
	}
}

// Per-key configuration options

func WithVectorIndex(key string, cfg *VectorIndexConfig) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if cfg == nil {
			return errors.New("vector index config cannot be nil")
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].FloatList == nil {
			s.keys[key].FloatList = &FloatListValueType{}
		}
		s.keys[key].FloatList.VectorIndex = &VectorIndexType{
			Enabled: true,
			Config:  cfg,
		}
		return nil
	}
}

func WithFtsIndex(key string) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].String == nil {
			s.keys[key].String = &StringValueType{}
		}
		s.keys[key].String.FtsIndex = &FtsIndexType{
			Enabled: true,
			Config:  &FtsIndexConfig{},
		}
		return nil
	}
}

func WithSparseVectorIndex(key string, cfg *SparseVectorIndexConfig) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if cfg == nil {
			return errors.New("sparse vector index config cannot be nil")
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].SparseVector == nil {
			s.keys[key].SparseVector = &SparseVectorValueType{}
		}
		s.keys[key].SparseVector.SparseVectorIndex = &SparseVectorIndexType{
			Enabled: true,
			Config:  cfg,
		}
		return nil
	}
}

func WithStringIndex(key string) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].String == nil {
			s.keys[key].String = &StringValueType{}
		}
		s.keys[key].String.StringInvertedIndex = &StringInvertedIndexType{
			Enabled: true,
			Config:  &StringInvertedIndexConfig{},
		}
		return nil
	}
}

func WithIntIndex(key string) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].Int == nil {
			s.keys[key].Int = &IntValueType{}
		}
		s.keys[key].Int.IntInvertedIndex = &IntInvertedIndexType{
			Enabled: true,
			Config:  &IntInvertedIndexConfig{},
		}
		return nil
	}
}

func WithFloatIndex(key string) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].Float == nil {
			s.keys[key].Float = &FloatValueType{}
		}
		s.keys[key].Float.FloatInvertedIndex = &FloatInvertedIndexType{
			Enabled: true,
			Config:  &FloatInvertedIndexConfig{},
		}
		return nil
	}
}

func WithBoolIndex(key string) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].Bool == nil {
			s.keys[key].Bool = &BoolValueType{}
		}
		s.keys[key].Bool.BoolInvertedIndex = &BoolInvertedIndexType{
			Enabled: true,
			Config:  &BoolInvertedIndexConfig{},
		}
		return nil
	}
}

// Disable options - disable indexes on specific keys

func DisableStringIndex(key string) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if key == DocumentKey || key == EmbeddingKey {
			return errors.Errorf("cannot disable string index on reserved key: %s", key)
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].String == nil {
			s.keys[key].String = &StringValueType{}
		}
		s.keys[key].String.StringInvertedIndex = &StringInvertedIndexType{
			Enabled: false,
			Config:  &StringInvertedIndexConfig{},
		}
		return nil
	}
}

func DisableIntIndex(key string) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if key == DocumentKey || key == EmbeddingKey {
			return errors.Errorf("cannot disable int index on reserved key: %s", key)
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].Int == nil {
			s.keys[key].Int = &IntValueType{}
		}
		s.keys[key].Int.IntInvertedIndex = &IntInvertedIndexType{
			Enabled: false,
			Config:  &IntInvertedIndexConfig{},
		}
		return nil
	}
}

func DisableFloatIndex(key string) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if key == DocumentKey || key == EmbeddingKey {
			return errors.Errorf("cannot disable float index on reserved key: %s", key)
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].Float == nil {
			s.keys[key].Float = &FloatValueType{}
		}
		s.keys[key].Float.FloatInvertedIndex = &FloatInvertedIndexType{
			Enabled: false,
			Config:  &FloatInvertedIndexConfig{},
		}
		return nil
	}
}

func DisableBoolIndex(key string) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if key == DocumentKey || key == EmbeddingKey {
			return errors.Errorf("cannot disable bool index on reserved key: %s", key)
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].Bool == nil {
			s.keys[key].Bool = &BoolValueType{}
		}
		s.keys[key].Bool.BoolInvertedIndex = &BoolInvertedIndexType{
			Enabled: false,
			Config:  &BoolInvertedIndexConfig{},
		}
		return nil
	}
}

func DisableFtsIndex(key string) SchemaOption {
	return func(s *Schema) error {
		if key == "" {
			return errors.New("key cannot be empty")
		}
		if key == EmbeddingKey {
			return errors.Errorf("cannot disable FTS index on reserved key: %s", key)
		}
		if s.keys[key] == nil {
			s.keys[key] = &ValueTypes{}
		}
		if s.keys[key].String == nil {
			s.keys[key].String = &StringValueType{}
		}
		s.keys[key].String.FtsIndex = &FtsIndexType{
			Enabled: false,
			Config:  &FtsIndexConfig{},
		}
		return nil
	}
}

// DisableDocumentFtsIndex disables full-text search for the reserved [DocumentKey].
// Chroma applies FTS on [DocumentKey], not on schema defaults.
func DisableDocumentFtsIndex() SchemaOption {
	return DisableFtsIndex(DocumentKey)
}

// Disable default options - disable indexes globally.
// Note: FTS is controlled on [DocumentKey] as required by Chroma.

func DisableDefaultStringIndex() SchemaOption {
	return func(s *Schema) error {
		if s.defaults.String == nil {
			s.defaults.String = &StringValueType{}
		}
		s.defaults.String.StringInvertedIndex = &StringInvertedIndexType{
			Enabled: false,
			Config:  &StringInvertedIndexConfig{},
		}
		return nil
	}
}

func DisableDefaultIntIndex() SchemaOption {
	return func(s *Schema) error {
		if s.defaults.Int == nil {
			s.defaults.Int = &IntValueType{}
		}
		s.defaults.Int.IntInvertedIndex = &IntInvertedIndexType{
			Enabled: false,
			Config:  &IntInvertedIndexConfig{},
		}
		return nil
	}
}

func DisableDefaultFloatIndex() SchemaOption {
	return func(s *Schema) error {
		if s.defaults.Float == nil {
			s.defaults.Float = &FloatValueType{}
		}
		s.defaults.Float.FloatInvertedIndex = &FloatInvertedIndexType{
			Enabled: false,
			Config:  &FloatInvertedIndexConfig{},
		}
		return nil
	}
}

func DisableDefaultBoolIndex() SchemaOption {
	return func(s *Schema) error {
		if s.defaults.Bool == nil {
			s.defaults.Bool = &BoolValueType{}
		}
		s.defaults.Bool.BoolInvertedIndex = &BoolInvertedIndexType{
			Enabled: false,
			Config:  &BoolInvertedIndexConfig{},
		}
		return nil
	}
}

// DisableDefaultFtsIndex disables FTS on defaults and [DocumentKey].
// This keeps backward compatibility for callers inspecting schema defaults.
// Note: this also adds/updates a [DocumentKey] override and may change serialized schema keys.
//
// Deprecated: Use [DisableDocumentFtsIndex] or [DisableFtsIndex] with [DocumentKey] instead.
func DisableDefaultFtsIndex() SchemaOption {
	return func(s *Schema) error {
		if s.defaults.String == nil {
			s.defaults.String = &StringValueType{}
		}
		s.defaults.String.FtsIndex = &FtsIndexType{
			Enabled: false,
			Config:  &FtsIndexConfig{},
		}
		return DisableDocumentFtsIndex()(s)
	}
}

// WithCmek sets a customer-managed encryption key for the schema.
// Returns an error if the CMEK is nil or has an invalid resource format.
func WithCmek(cmek *Cmek) SchemaOption {
	return func(s *Schema) error {
		if cmek == nil {
			return errors.New("cmek cannot be nil")
		}
		if err := cmek.ValidatePattern(); err != nil {
			return err
		}
		s.cmek = cmek
		return nil
	}
}

// Accessor methods

// Defaults returns the default value types configuration
func (s *Schema) Defaults() *ValueTypes {
	return s.defaults
}

// IsFtsEnabled reports whether FTS is enabled, preferring [DocumentKey] overrides
// and falling back to defaults for backward compatibility with legacy schemas.
func (s *Schema) IsFtsEnabled() bool {
	if s == nil {
		return true
	}
	if s.keys != nil {
		if vt, ok := s.keys[DocumentKey]; ok && vt != nil && vt.String != nil && vt.String.FtsIndex != nil {
			return vt.String.FtsIndex.Enabled
		}
	}
	if s.defaults != nil && s.defaults.String != nil && s.defaults.String.FtsIndex != nil {
		return s.defaults.String.FtsIndex.Enabled
	}
	return true
}

// Keys returns all keys with overrides
func (s *Schema) Keys() []string {
	keys := make([]string, 0, len(s.keys))
	for k := range s.keys {
		keys = append(keys, k)
	}
	return keys
}

// GetKey returns the value types for a specific key
func (s *Schema) GetKey(key string) (*ValueTypes, bool) {
	vt, ok := s.keys[key]
	return vt, ok
}

// Cmek returns the customer-managed encryption key configuration, if set
func (s *Schema) Cmek() *Cmek {
	return s.cmek
}

// JSON serialization

type schemaJSON struct {
	Defaults *ValueTypes            `json:"defaults,omitempty"`
	Keys     map[string]*ValueTypes `json:"keys"`
	Cmek     *Cmek                  `json:"cmek,omitempty"`
}

// MarshalJSON serializes the Schema to JSON
func (s *Schema) MarshalJSON() ([]byte, error) {
	return json.Marshal(schemaJSON{
		Defaults: s.defaults,
		Keys:     s.keys,
		Cmek:     s.cmek,
	})
}

// UnmarshalJSON deserializes the Schema from JSON
func (s *Schema) UnmarshalJSON(data []byte) error {
	var raw schemaJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return errors.Wrap(err, "failed to unmarshal schema")
	}
	s.defaults = raw.Defaults
	s.keys = raw.Keys
	s.cmek = raw.Cmek
	if s.defaults == nil {
		s.defaults = &ValueTypes{}
	}
	if s.keys == nil {
		s.keys = make(map[string]*ValueTypes)
	}
	return nil
}

// GetEmbeddingFunction returns the EmbeddingFunction from the default vector index (#embedding key)
// Returns nil if no vector index is configured or if the EmbeddingFunction couldn't be reconstructed
func (s *Schema) GetEmbeddingFunction() embeddings.EmbeddingFunction {
	if s == nil || s.keys == nil {
		return nil
	}

	// Check #embedding key first (default vector index location)
	if vt, ok := s.keys[EmbeddingKey]; ok && vt != nil {
		if vt.FloatList != nil && vt.FloatList.VectorIndex != nil && vt.FloatList.VectorIndex.Config != nil {
			return vt.FloatList.VectorIndex.Config.EmbeddingFunction
		}
	}

	return nil
}

// SetEmbeddingFunction sets the EmbeddingFunction on the default vector index (#embedding key)
// Creates the necessary structure if it doesn't exist
func (s *Schema) SetEmbeddingFunction(ef embeddings.EmbeddingFunction) {
	if s == nil || ef == nil {
		return
	}

	if s.keys == nil {
		s.keys = make(map[string]*ValueTypes)
	}

	// Ensure #embedding key exists
	if s.keys[EmbeddingKey] == nil {
		s.keys[EmbeddingKey] = &ValueTypes{}
	}

	// Ensure FloatList exists
	if s.keys[EmbeddingKey].FloatList == nil {
		s.keys[EmbeddingKey].FloatList = &FloatListValueType{}
	}

	// Ensure VectorIndex exists
	if s.keys[EmbeddingKey].FloatList.VectorIndex == nil {
		s.keys[EmbeddingKey].FloatList.VectorIndex = &VectorIndexType{
			Enabled: true,
			Config:  &VectorIndexConfig{},
		}
	}

	// Ensure Config exists
	if s.keys[EmbeddingKey].FloatList.VectorIndex.Config == nil {
		s.keys[EmbeddingKey].FloatList.VectorIndex.Config = &VectorIndexConfig{}
	}

	// Set the EmbeddingFunction
	s.keys[EmbeddingKey].FloatList.VectorIndex.Config.EmbeddingFunction = ef
}

// GetSparseEmbeddingFunction returns the SparseEmbeddingFunction from a specific key
// Returns nil if no sparse vector index is configured at that key or if the EmbeddingFunction couldn't be reconstructed
func (s *Schema) GetSparseEmbeddingFunction(key string) embeddings.SparseEmbeddingFunction {
	if s == nil || s.keys == nil {
		return nil
	}

	if vt, ok := s.keys[key]; ok && vt != nil {
		if vt.SparseVector != nil && vt.SparseVector.SparseVectorIndex != nil && vt.SparseVector.SparseVectorIndex.Config != nil {
			return vt.SparseVector.SparseVectorIndex.Config.EmbeddingFunction
		}
	}

	return nil
}

// GetAllSparseEmbeddingFunctions returns all sparse embedding functions with their keys
// Returns a map of key name to embedding function, or nil if schema is nil
func (s *Schema) GetAllSparseEmbeddingFunctions() map[string]embeddings.SparseEmbeddingFunction {
	if s == nil || s.keys == nil {
		return nil
	}

	result := make(map[string]embeddings.SparseEmbeddingFunction)
	for key, vt := range s.keys {
		if vt != nil && vt.SparseVector != nil && vt.SparseVector.SparseVectorIndex != nil && vt.SparseVector.SparseVectorIndex.Config != nil {
			if ef := vt.SparseVector.SparseVectorIndex.Config.EmbeddingFunction; ef != nil {
				result[key] = ef
			}
		}
	}

	return result
}

// SetSparseEmbeddingFunction sets the SparseEmbeddingFunction on a specific key
// Creates the necessary structure if it doesn't exist
func (s *Schema) SetSparseEmbeddingFunction(key string, ef embeddings.SparseEmbeddingFunction) {
	if s == nil || ef == nil || key == "" {
		return
	}

	if s.keys == nil {
		s.keys = make(map[string]*ValueTypes)
	}

	if s.keys[key] == nil {
		s.keys[key] = &ValueTypes{}
	}

	if s.keys[key].SparseVector == nil {
		s.keys[key].SparseVector = &SparseVectorValueType{}
	}

	if s.keys[key].SparseVector.SparseVectorIndex == nil {
		s.keys[key].SparseVector.SparseVectorIndex = &SparseVectorIndexType{
			Enabled: true,
			Config:  &SparseVectorIndexConfig{},
		}
	}

	if s.keys[key].SparseVector.SparseVectorIndex.Config == nil {
		s.keys[key].SparseVector.SparseVectorIndex.Config = &SparseVectorIndexConfig{}
	}

	s.keys[key].SparseVector.SparseVectorIndex.Config.EmbeddingFunction = ef
}
