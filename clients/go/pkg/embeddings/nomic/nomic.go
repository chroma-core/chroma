package nomic

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// Docs:  https://docs.nomic.ai/reference/endpoints/nomic-embed-text

type TaskType string

type contextKey struct{ name string }

var (
	modelContextKey          = contextKey{"model"}
	dimensionalityContextKey = contextKey{"dimensionality"}
	taskTypeContextKey       = contextKey{"task_type"}
)

func ContextWithModel(ctx context.Context, model string) context.Context {
	return context.WithValue(ctx, modelContextKey, model)
}

func ContextWithDimensionality(ctx context.Context, dimensionality *int) context.Context {
	return context.WithValue(ctx, dimensionalityContextKey, dimensionality)
}

func ContextWithTaskType(ctx context.Context, taskType TaskType) context.Context {
	return context.WithValue(ctx, taskTypeContextKey, taskType)
}

const (
	DefaultEmbeddingModel           = NomicEmbedTextV1
	APIKeyEnvVar                    = "NOMIC_API_KEY"
	DefaultBaseURL                  = "https://api-atlas.nomic.ai/v1/embedding"
	TextEmbeddingsEndpoint          = "/text"
	DefaultMaxBatchSize             = 100
	TaskTypeSearchQuery    TaskType = "search_query"
	TaskTypeSearchDocument TaskType = "search_document"
	TaskTypeClustering     TaskType = "clustering"
	TaskTypeClassification TaskType = "classification"
	NomicEmbedTextV1                = "nomic-embed-text-v1"
	NomicEmbedTextV15               = "nomic-embed-text-v1.5"
)

type Client struct {
	APIKey                   embeddings.Secret `json:"-" validate:"required"`
	APIKeyEnvVar             string
	DefaultModel             embeddings.EmbeddingModel
	Client                   *http.Client
	DefaultContext           *context.Context
	MaxBatchSize             int
	EmbeddingEndpoint        string
	DefaultHeaders           map[string]string
	DefaultTaskType          *TaskType
	DefaultDimensionality    *int
	BaseURL                  string
	EmbeddingsEndpointSuffix string
	Insecure                 bool
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
	if c.BaseURL == "" {
		c.BaseURL = DefaultBaseURL
	}
	if c.EmbeddingsEndpointSuffix == "" {
		c.EmbeddingsEndpointSuffix = TextEmbeddingsEndpoint
	}
	c.EmbeddingEndpoint, err = url.JoinPath(c.BaseURL, c.EmbeddingsEndpointSuffix)
	if err != nil {
		return errors.Wrap(err, "failed parse embedding endpoint")
	}
	return nil
}

func validate(c *Client) error {
	if err := embeddings.NewValidator().Struct(c); err != nil {
		return err
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

func NewNomicClient(opts ...Option) (*Client, error) {
	client := &Client{}
	err := applyDefaults(client)
	if err != nil {
		return nil, errors.Wrap(err, "failed to apply Nomic default options")
	}
	for _, opt := range opts {
		err := opt(client)
		if err != nil {
			return nil, errors.Wrap(err, "failed to apply Nomic options")
		}
	}

	if err := validate(client); err != nil {
		return nil, errors.Wrap(err, "failed to validate Nomic client options")
	}
	return client, nil
}

type CreateEmbeddingRequest struct {
	Model          embeddings.EmbeddingModel `json:"model"`
	Texts          []string                  `json:"texts"`
	TaskType       *TaskType                 `json:"task_type,omitempty"`
	Dimensionality *int                      `json:"dimensionality,omitempty"`
}

type CreateEmbeddingResponse struct {
	Usage      map[string]any `json:"usage,omitempty"`
	Embeddings [][]float32    `json:"embeddings"`
}

func (c *CreateEmbeddingRequest) JSON() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal embedding request JSON")
	}
	return string(data), nil
}

func (c *Client) CreateEmbedding(ctx context.Context, req CreateEmbeddingRequest) ([]embeddings.Embedding, error) {
	reqJSON, err := req.JSON()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal embedding request JSON")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.EmbeddingEndpoint, bytes.NewBufferString(reqJSON))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create http request")
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
		return nil, errors.Wrap(err, "failed to send request to Nomic API")
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
		return nil, errors.Wrap(err, "failed to unmarshal embedding response")
	}
	embs := make([]embeddings.Embedding, len(embeddingResponse.Embeddings))
	for i, e := range embeddingResponse.Embeddings {
		embs[i] = embeddings.NewEmbeddingFromFloat32(e)
	}
	return embs, nil
}

var _ embeddings.EmbeddingFunction = (*NomicEmbeddingFunction)(nil)

type NomicEmbeddingFunction struct {
	apiClient *Client
}

func NewNomicEmbeddingFunction(opts ...Option) (*NomicEmbeddingFunction, error) {
	client, err := NewNomicClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize Nomic client")
	}

	return &NomicEmbeddingFunction{apiClient: client}, nil
}

func (e *NomicEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	if len(documents) > e.apiClient.MaxBatchSize {
		return nil, errors.Errorf("number of documents exceeds the maximum batch size %v", e.apiClient.MaxBatchSize)
	}
	if len(documents) == 0 {
		return embeddings.NewEmptyEmbeddings(), nil
	}
	model := e.apiClient.DefaultModel
	if m, ok := ctx.Value(modelContextKey).(string); ok {
		model = embeddings.EmbeddingModel(m)
	}
	dimensionality := e.apiClient.DefaultDimensionality
	if d, ok := ctx.Value(dimensionalityContextKey).(*int); ok {
		dimensionality = d
	}
	taskType := TaskTypeSearchDocument
	if t, ok := ctx.Value(taskTypeContextKey).(TaskType); ok {
		taskType = t
	}
	req := CreateEmbeddingRequest{
		Model:          model,
		Texts:          documents,
		Dimensionality: dimensionality,
		TaskType:       &taskType,
	}
	response, err := e.apiClient.CreateEmbedding(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}
	return response, nil
}

func (e *NomicEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	model := e.apiClient.DefaultModel
	if m, ok := ctx.Value(modelContextKey).(string); ok {
		model = embeddings.EmbeddingModel(m)
	}
	dimensionality := e.apiClient.DefaultDimensionality
	if d, ok := ctx.Value(dimensionalityContextKey).(*int); ok {
		dimensionality = d
	}
	taskType := TaskTypeSearchQuery
	if t, ok := ctx.Value(taskTypeContextKey).(TaskType); ok {
		taskType = t
	}
	req := CreateEmbeddingRequest{
		Model:          model,
		Texts:          []string{document},
		Dimensionality: dimensionality,
		TaskType:       &taskType,
	}
	response, err := e.apiClient.CreateEmbedding(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	if len(response) == 0 {
		return nil, errors.New("no embedding returned from Nomic API")
	}
	return response[0], nil
}

func (e *NomicEmbeddingFunction) Name() string {
	return "nomic"
}

func (e *NomicEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.apiClient.APIKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	cfg := embeddings.EmbeddingFunctionConfig{
		"model_name":      string(e.apiClient.DefaultModel),
		"api_key_env_var": envVar,
	}
	if e.apiClient.Insecure {
		cfg["insecure"] = true
	}
	if e.apiClient.BaseURL != "" {
		cfg["base_url"] = e.apiClient.BaseURL
	}
	return cfg
}

func (e *NomicEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *NomicEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewNomicEmbeddingFunctionFromConfig creates a Nomic embedding function from a config map.
// Uses schema-compliant field names: api_key_env_var, model_name, base_url, insecure.
func NewNomicEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*NomicEmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	opts := []Option{WithAPIKeyFromEnvVar(envVar)}
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithDefaultModel(embeddings.EmbeddingModel(model)))
	}
	if baseURL, ok := cfg["base_url"].(string); ok && baseURL != "" {
		opts = append(opts, WithBaseURL(baseURL))
	}
	if insecure, ok := cfg["insecure"].(bool); ok && insecure {
		opts = append(opts, WithInsecure())
	} else if embeddings.AllowInsecureFromEnv() {
		embeddings.LogInsecureEnvVarWarning("Nomic")
		opts = append(opts, WithInsecure())
	}
	return NewNomicEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("nomic", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewNomicEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
