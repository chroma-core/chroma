package chromacloudsplade

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

const (
	defaultBaseURL = "https://embed.trychroma.com/embed_sparse"
	defaultModel   = "prithivida/Splade_PP_en_v1"
	defaultTimeout = 60 * time.Second
	APIKeyEnvVar   = "CHROMA_API_KEY"
)

type Client struct {
	BaseURL    string
	APIKey     embeddings.Secret `json:"-"`
	Model      embeddings.EmbeddingModel
	HTTPClient *http.Client
	Insecure   bool
}

type embeddingRequest struct {
	Texts  []string `json:"texts"`
	Task   string   `json:"task"`
	Target string   `json:"target"`
}

type sparseEmbedding struct {
	Indices []int     `json:"indices"`
	Values  []float32 `json:"values"`
}

type embeddingResponse struct {
	Embeddings []sparseEmbedding `json:"embeddings,omitempty"`
	Error      string            `json:"error,omitempty"`
}

func applyDefaults(c *Client) {
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: defaultTimeout}
	}
	if c.BaseURL == "" {
		c.BaseURL = defaultBaseURL
	}
	if c.Model == "" {
		c.Model = defaultModel
	}
}

func validate(c *Client) error {
	if c.APIKey.IsEmpty() {
		return errors.New("API key is required")
	}
	parsed, err := url.Parse(c.BaseURL)
	if err != nil {
		return errors.Wrap(err, "invalid base URL")
	}
	if !c.Insecure && !strings.EqualFold(parsed.Scheme, "https") {
		return errors.New("base URL must use HTTPS scheme for secure API key transmission; use WithInsecure() to override")
	}
	return nil
}

func NewClient(opts ...Option) (*Client, error) {
	client := &Client{}
	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, errors.Wrap(err, "failed to apply option")
		}
	}
	applyDefaults(client)
	if err := validate(client); err != nil {
		return nil, errors.Wrap(err, "failed to validate client")
	}
	return client, nil
}

func (c *Client) embed(ctx context.Context, texts []string) ([]*embeddings.SparseVector, error) {
	if len(texts) == 0 {
		return make([]*embeddings.SparseVector, 0), nil
	}

	reqBody := embeddingRequest{
		Texts:  texts,
		Task:   "",
		Target: "",
	}
	reqData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL, bytes.NewReader(reqData))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)
	req.Header.Set("Cache-Control", "no-store")
	req.Header.Set("x-chroma-token", c.APIKey.Value())
	req.Header.Set("x-chroma-embedding-model", string(c.Model))

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send request")
	}
	defer resp.Body.Close()

	body, err := chttp.ReadLimitedBody(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var embResp embeddingResponse
	if err := json.Unmarshal(body, &embResp); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response")
	}

	if embResp.Error != "" {
		return nil, errors.Errorf("API error [status %d]: %s", resp.StatusCode, embResp.Error)
	}

	result := make([]*embeddings.SparseVector, len(embResp.Embeddings))
	for i, emb := range embResp.Embeddings {
		sv, err := embeddings.NewSparseVector(emb.Indices, emb.Values)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create sparse vector at index %d", i)
		}
		result[i] = sv
	}

	return result, nil
}

var _ embeddings.SparseEmbeddingFunction = (*EmbeddingFunction)(nil)

type EmbeddingFunction struct {
	client *Client
}

func NewEmbeddingFunction(opts ...Option) (*EmbeddingFunction, error) {
	client, err := NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create client")
	}
	return &EmbeddingFunction{client: client}, nil
}

func (e *EmbeddingFunction) EmbedDocumentsSparse(ctx context.Context, documents []string) ([]*embeddings.SparseVector, error) {
	if len(documents) == 0 {
		return make([]*embeddings.SparseVector, 0), nil
	}

	vectors, err := e.client.embed(ctx, documents)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}

	return vectors, nil
}

func (e *EmbeddingFunction) EmbedQuerySparse(ctx context.Context, query string) (*embeddings.SparseVector, error) {
	vectors, err := e.client.embed(ctx, []string{query})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	if len(vectors) == 0 {
		return nil, errors.New("no embedding returned")
	}
	return vectors[0], nil
}

func (e *EmbeddingFunction) Name() string {
	return "chroma-cloud-splade"
}

func (e *EmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	cfg := embeddings.EmbeddingFunctionConfig{
		"model":           string(e.client.Model),
		"api_key_env_var": APIKeyEnvVar,
	}
	if e.client.Insecure {
		cfg["insecure"] = true
	}
	if e.client.BaseURL != "" {
		cfg["base_url"] = e.client.BaseURL
	}
	return cfg
}

// NewEmbeddingFunctionFromConfig creates a ChromaCloud Splade embedding function from a config map.
// Uses schema-compliant field names: api_key_env_var, model, base_url, insecure.
func NewEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*EmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	opts := []Option{WithAPIKeyFromEnvVar(envVar)}
	if model, ok := cfg["model"].(string); ok && model != "" {
		opts = append(opts, WithModel(embeddings.EmbeddingModel(model)))
	}
	if baseURL, ok := cfg["base_url"].(string); ok && baseURL != "" {
		opts = append(opts, WithBaseURL(baseURL))
	}
	if insecure, ok := cfg["insecure"].(bool); ok && insecure {
		opts = append(opts, WithInsecure())
	} else if embeddings.AllowInsecureFromEnv() {
		embeddings.LogInsecureEnvVarWarning("ChromaCloudSplade")
		opts = append(opts, WithInsecure())
	}
	return NewEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterSparse("chroma-cloud-splade", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.SparseEmbeddingFunction, error) {
		return NewEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
