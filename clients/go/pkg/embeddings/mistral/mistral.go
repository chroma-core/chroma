package mistral

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

const (
	DefaultEmbeddingModel = "mistral-embed"
	ModelContextVar       = "model"
	APIKeyEnvVar          = "MISTRAL_API_KEY"
	DefaultBaseURL        = "https://api.mistral.ai"
	EmbeddingsEndpoint    = "/v1/embeddings"
	DefaultMaxBatchSize   = 100
)

type Client struct {
	APIKey            embeddings.Secret `json:"-" validate:"required"`
	APIKeyEnvVar      string
	DefaultModel      string
	Client            *http.Client
	DefaultContext    *context.Context
	MaxBatchSize      int
	EmbeddingEndpoint string
	DefaultHeaders    map[string]string
}

func applyDefaults(c *Client) (err error) {
	if c.DefaultModel == "" {
		c.DefaultModel = DefaultEmbeddingModel
	}

	if c.DefaultContext == nil {
		ctx := context.Background()
		c.DefaultContext = &ctx
	}

	if c.Client == nil {
		c.Client = http.DefaultClient
	}
	if c.MaxBatchSize == 0 {
		c.MaxBatchSize = DefaultMaxBatchSize
	}
	var s = DefaultBaseURL + EmbeddingsEndpoint
	c.EmbeddingEndpoint = s
	return nil
}

func validate(c *Client) error {
	return embeddings.NewValidator().Struct(c)
}

func NewMistralClient(opts ...Option) (*Client, error) {
	client := &Client{}
	err := applyDefaults(client)
	if err != nil {
		return nil, errors.Wrap(err, "failed to apply Mistral default options")
	}
	for _, opt := range opts {
		err := opt(client)
		if err != nil {
			return nil, errors.Wrap(err, "failed to apply Mistral option")
		}
	}
	if err := validate(client); err != nil {
		return nil, errors.Wrap(err, "failed to validate Mistral client options")
	}
	return client, nil
}

type CreateEmbeddingRequest struct {
	Model          string   `json:"model"`
	Input          []string `json:"input"`
	EncodingFormat string   `json:"encoding_format,omitempty"`
}

type Embedding struct {
	Object    string    `json:"object"`
	Embedding []float32 `json:"embedding"` // TODO this can be also ints depending on encoding format
	Index     int       `json:"index"`
}

type CreateEmbeddingResponse struct {
	ID     string         `json:"id"`
	Object string         `json:"object"`
	Model  string         `json:"model"`
	Usage  map[string]any `json:"usage"`
	Data   []Embedding    `json:"data"`
}

func (c *CreateEmbeddingRequest) JSON() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal embedding request")
	}
	return string(data), nil
}

func (c *Client) CreateEmbedding(ctx context.Context, req CreateEmbeddingRequest) ([]embeddings.Embedding, error) {
	reqJSON, err := req.JSON()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal request to JSON")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.EmbeddingEndpoint, bytes.NewBufferString(reqJSON))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HTTP request")
	}
	for k, v := range c.DefaultHeaders {
		httpReq.Header.Set(k, v)
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey.Value())

	resp, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send request to Mistral API")
	}
	defer resp.Body.Close()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected code [%v] while making a request to %v: %v", resp.Status, c.EmbeddingEndpoint, string(respData))
	}

	var embeddingResponse CreateEmbeddingResponse
	if err := json.Unmarshal(respData, &embeddingResponse); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response body")
	}
	embs := make([]embeddings.Embedding, len(embeddingResponse.Data))
	for i, e := range embeddingResponse.Data {
		embs[i] = embeddings.NewEmbeddingFromFloat32(e.Embedding)
	}
	return embs, nil
}

var _ embeddings.EmbeddingFunction = (*MistralEmbeddingFunction)(nil)

type MistralEmbeddingFunction struct {
	apiClient *Client
}

func NewMistralEmbeddingFunction(opts ...Option) (*MistralEmbeddingFunction, error) {
	client, err := NewMistralClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize MistralClient")
	}

	return &MistralEmbeddingFunction{apiClient: client}, nil
}

func (e *MistralEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	if len(documents) > e.apiClient.MaxBatchSize {
		return nil, errors.Errorf("number of documents exceeds the maximum batch size %v", e.apiClient.MaxBatchSize)
	}
	if e.apiClient.MaxBatchSize > 0 && len(documents) > e.apiClient.MaxBatchSize {
		return nil, errors.Errorf("number of documents exceeds the maximum batch size %v", e.apiClient.MaxBatchSize)
	}
	if len(documents) == 0 {
		return embeddings.NewEmptyEmbeddings(), nil
	}
	model := e.apiClient.DefaultModel
	if m, ok := ctx.Value(ModelContextVar).(string); ok {
		model = m
	}
	req := CreateEmbeddingRequest{
		Model: model,
		Input: documents,
	}
	response, err := e.apiClient.CreateEmbedding(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}
	return response, nil
}

func (e *MistralEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	model := e.apiClient.DefaultModel
	if m, ok := ctx.Value(ModelContextVar).(string); ok {
		model = m
	}
	req := CreateEmbeddingRequest{
		Model: model,
		Input: []string{document},
	}
	response, err := e.apiClient.CreateEmbedding(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	if len(response) == 0 {
		return nil, errors.New("no embedding returned from Mistral API")
	}
	return response[0], nil
}

func (e *MistralEmbeddingFunction) Name() string {
	return "mistral"
}

func (e *MistralEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.apiClient.APIKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	return embeddings.EmbeddingFunctionConfig{
		"model_name":      e.apiClient.DefaultModel,
		"api_key_env_var": envVar,
	}
}

func (e *MistralEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *MistralEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewMistralEmbeddingFunctionFromConfig creates a Mistral embedding function from a config map.
// Uses schema-compliant field names: api_key_env_var, model_name.
func NewMistralEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*MistralEmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	opts := []Option{WithAPIKeyFromEnvVar(envVar)}
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithDefaultModel(model))
	}
	return NewMistralEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("mistral", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewMistralEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
