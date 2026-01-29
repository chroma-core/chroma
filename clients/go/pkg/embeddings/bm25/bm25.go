package bm25

import (
	"context"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/twmb/murmur3"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// Client holds the BM25 configuration
type Client struct {
	K              float64
	B              float64
	AvgDocLength   float64
	TokenMaxLength int
	Stopwords      []string
	IncludeTokens  bool
	tokenizer      *Tokenizer
	kSet           bool // tracks if K was explicitly set
	bSet           bool // tracks if B was explicitly set
}

// NewClient creates a new BM25 client with the given options
func NewClient(opts ...Option) (*Client, error) {
	c := &Client{}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, errors.Wrap(err, "failed to apply option")
		}
	}
	applyDefaults(c)
	c.tokenizer = NewTokenizer(c.Stopwords, c.TokenMaxLength)
	return c, nil
}

// EmbeddingFunction wraps Client to implement SparseEmbeddingFunction
type EmbeddingFunction struct {
	client *Client
}

// NewEmbeddingFunction creates a new BM25 embedding function
func NewEmbeddingFunction(opts ...Option) (*EmbeddingFunction, error) {
	client, err := NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &EmbeddingFunction{client: client}, nil
}

// embed computes BM25 sparse embeddings for the given texts
func (c *Client) embed(texts []string) ([]*embeddings.SparseVector, error) {
	if len(texts) == 0 {
		return []*embeddings.SparseVector{}, nil
	}

	result := make([]*embeddings.SparseVector, len(texts))
	for i, text := range texts {
		sv, err := c.embedSingle(text)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to embed text at index %d", i)
		}
		result[i] = sv
	}
	return result, nil
}

// embedSingle computes BM25 sparse embedding for a single text
func (c *Client) embedSingle(text string) (*embeddings.SparseVector, error) {
	if text == "" {
		return &embeddings.SparseVector{
			Indices: []int{},
			Values:  []float32{},
		}, nil
	}

	tokens := c.tokenizer.Tokenize(text)
	if len(tokens) == 0 {
		return &embeddings.SparseVector{
			Indices: []int{},
			Values:  []float32{},
		}, nil
	}

	// Count term frequencies
	tf := make(map[string]int)
	for _, token := range tokens {
		tf[token]++
	}

	docLen := float64(len(tokens))

	// Sort tokens for deterministic output
	uniqueTokens := make([]string, 0, len(tf))
	for token := range tf {
		uniqueTokens = append(uniqueTokens, token)
	}
	sort.Strings(uniqueTokens)

	// Use map to handle hash collisions by summing scores
	indexScores := make(map[int]float32, len(tf))
	indexLabels := make(map[int][]string)

	for _, token := range uniqueTokens {
		freq := tf[token]

		// Compute BM25 score
		tfFloat := float64(freq)
		denominator := tfFloat + c.K*(1-c.B+c.B*docLen/c.AvgDocLength)
		score := tfFloat * (c.K + 1) / denominator

		// Hash token to index using murmur3, matching Python mmh3 behavior.
		// Python's mmh3.hash() returns signed 32-bit, then abs() is applied.
		// Go's murmur3.Sum32() returns unsigned 32-bit. To match Python:
		// - If hash >= 2^31, interpret as signed negative, then take abs
		// - abs(signed) = 2^32 - unsigned for values >= 2^31
		hash := murmur3.Sum32([]byte(token))
		var index int
		if hash >= 0x80000000 {
			// Interpret as signed negative and take absolute value
			index = int(0x100000000 - uint64(hash))
		} else {
			index = int(hash)
		}

		// Handle hash collisions by summing scores
		indexScores[index] += float32(score)
		if c.IncludeTokens {
			indexLabels[index] = append(indexLabels[index], token)
		}
	}

	// Extract sorted indices for deterministic output
	indices := make([]int, 0, len(indexScores))
	for idx := range indexScores {
		indices = append(indices, idx)
	}
	sort.Ints(indices)

	values := make([]float32, len(indices))
	var labels []string
	if c.IncludeTokens {
		labels = make([]string, len(indices))
	}
	for i, idx := range indices {
		values[i] = indexScores[idx]
		if c.IncludeTokens {
			labels[i] = strings.Join(indexLabels[idx], "+")
		}
	}

	sv := &embeddings.SparseVector{
		Indices: indices,
		Values:  values,
	}
	if c.IncludeTokens {
		sv.Labels = labels
	}

	if err := sv.Validate(); err != nil {
		return nil, errors.Wrap(err, "generated invalid sparse vector")
	}
	return sv, nil
}

// EmbedDocumentsSparse returns a sparse vector for each text
func (e *EmbeddingFunction) EmbedDocumentsSparse(_ context.Context, texts []string) ([]*embeddings.SparseVector, error) {
	return e.client.embed(texts)
}

// EmbedQuerySparse embeds a single text as a sparse vector
func (e *EmbeddingFunction) EmbedQuerySparse(_ context.Context, text string) (*embeddings.SparseVector, error) {
	results, err := e.client.embed([]string{text})
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("no embedding returned")
	}
	return results[0], nil
}

// Ensure EmbeddingFunction implements SparseEmbeddingFunction
var _ embeddings.SparseEmbeddingFunction = (*EmbeddingFunction)(nil)

func (e *EmbeddingFunction) Name() string {
	return "chroma_bm25"
}

func (e *EmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	cfg := embeddings.EmbeddingFunctionConfig{
		"k":                e.client.K,
		"b":                e.client.B,
		"avg_len":          e.client.AvgDocLength,
		"token_max_length": e.client.TokenMaxLength,
		"include_tokens":   e.client.IncludeTokens,
	}
	if len(e.client.Stopwords) > 0 {
		cfg["stopwords"] = e.client.Stopwords
	}
	return cfg
}

// NewEmbeddingFunctionFromConfig creates a BM25 embedding function from a config map.
// Uses schema-compliant field names: k, b, avg_len, token_max_length, include_tokens, stopwords.
func NewEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*EmbeddingFunction, error) {
	opts := make([]Option, 0)
	if k, ok := embeddings.ConfigFloat64(cfg, "k"); ok {
		opts = append(opts, WithK(k))
	}
	if b, ok := embeddings.ConfigFloat64(cfg, "b"); ok {
		opts = append(opts, WithB(b))
	}
	if avgLen, ok := embeddings.ConfigFloat64(cfg, "avg_len"); ok {
		opts = append(opts, WithAvgDocLength(avgLen))
	}
	if tokenMaxLength, ok := embeddings.ConfigInt(cfg, "token_max_length"); ok {
		opts = append(opts, WithTokenMaxLength(tokenMaxLength))
	}
	if includeTokens, ok := cfg["include_tokens"].(bool); ok {
		opts = append(opts, WithIncludeTokens(includeTokens))
	}
	if stopwords, ok := embeddings.ConfigStringSlice(cfg, "stopwords"); ok {
		opts = append(opts, WithStopwords(stopwords))
	}
	return NewEmbeddingFunction(opts...)
}

func init() {
	factory := func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.SparseEmbeddingFunction, error) {
		return NewEmbeddingFunctionFromConfig(cfg)
	}
	// Register primary name (matches Python client)
	if err := embeddings.RegisterSparse("chroma_bm25", factory); err != nil {
		panic(err)
	}
	// Register alias for backward compatibility
	if err := embeddings.RegisterSparse("bm25", factory); err != nil {
		panic(err)
	}
}
