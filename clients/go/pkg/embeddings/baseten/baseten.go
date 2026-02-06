package baseten

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

const (
	APIKeyEnvVar    = "BASETEN_API_KEY"
	ModelContextVar = "model"
)

// Input represents the input for an embedding request.
type Input struct {
	Text  string   `json:"-"`
	Texts []string `json:"-"`
}

func (i *Input) MarshalJSON() ([]byte, error) {
	switch {
	case i.Text != "":
		return json.Marshal(i.Text)
	case i.Texts != nil:
		return json.Marshal(i.Texts)
	default:
		return nil, errors.New("invalid input")
	}
}

// CreateEmbeddingRequest represents a request to create embeddings.
type CreateEmbeddingRequest struct {
	Model string `json:"model,omitempty"`
	Input *Input `json:"input"`
}

func (c *CreateEmbeddingRequest) JSON() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal embedding request JSON")
	}
	return string(data), nil
}

// EmbeddingData represents a single embedding in the response.
type EmbeddingData struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
}

// Usage represents token usage information.
type Usage struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

// CreateEmbeddingResponse represents the response from the embedding API.
type CreateEmbeddingResponse struct {
	Object string          `json:"object"`
	Data   []EmbeddingData `json:"data"`
	Model  string          `json:"model"`
	Usage  Usage           `json:"usage"`
}

// BasetenClient is the HTTP client for Baseten embedding API.
type BasetenClient struct {
	BaseURL      string            `json:"base_url,omitempty"`
	APIKey       embeddings.Secret `json:"-" validate:"required"`
	APIKeyEnvVar string            `json:"-"`
	Model        string            `json:"model,omitempty"`
	Client       *http.Client      `json:"-"`
	Insecure     bool              `json:"insecure,omitempty"`
}

func validate(c *BasetenClient) error {
	if c.BaseURL == "" {
		return errors.New("base URL is required for Baseten; use WithBaseURL() to set your deployment URL")
	}
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

// NewBasetenClient creates a new Baseten API client.
func NewBasetenClient(opts ...Option) (*BasetenClient, error) {
	client := &BasetenClient{}
	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, errors.Wrap(err, "failed to apply Baseten option")
		}
	}
	if client.Client == nil {
		client.Client = &http.Client{}
	}
	if err := validate(client); err != nil {
		return nil, errors.Wrap(err, "failed to validate Baseten client options")
	}
	return client, nil
}

// CreateEmbedding sends a request to the Baseten embedding API.
func (c *BasetenClient) CreateEmbedding(ctx context.Context, req *CreateEmbeddingRequest) (*CreateEmbeddingResponse, error) {
	if req.Model == "" && c.Model != "" {
		req.Model = c.Model
	}
	reqJSON, err := req.JSON()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal request JSON")
	}
	endpoint, err := url.JoinPath(c.BaseURL, "v1/embeddings")
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse URL")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(reqJSON))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create http request")
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey.Value())

	resp, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send request to Baseten API")
	}
	defer resp.Body.Close()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected response %v, %v", resp.Status, string(respData))
	}

	var createEmbeddingResponse CreateEmbeddingResponse
	if err := json.Unmarshal(respData, &createEmbeddingResponse); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response body")
	}

	return &createEmbeddingResponse, nil
}

var _ embeddings.EmbeddingFunction = (*BasetenEmbeddingFunction)(nil)

// BasetenEmbeddingFunction implements the EmbeddingFunction interface for Baseten.
type BasetenEmbeddingFunction struct {
	apiClient *BasetenClient
}

// NewBasetenEmbeddingFunction creates a new Baseten embedding function.
func NewBasetenEmbeddingFunction(opts ...Option) (*BasetenEmbeddingFunction, error) {
	apiClient, err := NewBasetenClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize Baseten client")
	}
	return &BasetenEmbeddingFunction{
		apiClient: apiClient,
	}, nil
}

func convertToMatrix(response *CreateEmbeddingResponse) [][]float32 {
	matrix := make([][]float32, 0, len(response.Data))
	for _, embeddingData := range response.Data {
		matrix = append(matrix, embeddingData.Embedding)
	}
	return matrix
}

func (e *BasetenEmbeddingFunction) getModel(ctx context.Context) string {
	model := e.apiClient.Model
	if m, ok := ctx.Value(ModelContextVar).(string); ok {
		model = m
	}
	return model
}

// EmbedDocuments returns embeddings for a batch of documents.
func (e *BasetenEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	if len(documents) == 0 {
		return embeddings.NewEmptyEmbeddings(), nil
	}
	response, err := e.apiClient.CreateEmbedding(ctx, &CreateEmbeddingRequest{
		Model: e.getModel(ctx),
		Input: &Input{
			Texts: documents,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}
	return embeddings.NewEmbeddingsFromFloat32(convertToMatrix(response))
}

// EmbedQuery returns an embedding for a single query.
func (e *BasetenEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	response, err := e.apiClient.CreateEmbedding(ctx, &CreateEmbeddingRequest{
		Model: e.getModel(ctx),
		Input: &Input{
			Texts: []string{document},
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	matrix := convertToMatrix(response)
	if len(matrix) == 0 {
		return nil, errors.New("no embedding returned from Baseten API")
	}
	return embeddings.NewEmbeddingFromFloat32(matrix[0]), nil
}

func (e *BasetenEmbeddingFunction) Name() string {
	return "baseten"
}

func (e *BasetenEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.apiClient.APIKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	cfg := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": envVar,
		"api_base":        e.apiClient.BaseURL,
	}
	if e.apiClient.Model != "" {
		cfg["model_name"] = e.apiClient.Model
	}
	if e.apiClient.Insecure {
		cfg["insecure"] = true
	}
	return cfg
}

func (e *BasetenEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *BasetenEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewBasetenEmbeddingFunctionFromConfig creates a Baseten embedding function from a config map.
func NewBasetenEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*BasetenEmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	baseURL, ok := cfg["api_base"].(string)
	if !ok || baseURL == "" {
		return nil, errors.New("api_base is required in config for Baseten")
	}
	opts := []Option{
		WithAPIKeyFromEnvVar(envVar),
		WithBaseURL(baseURL),
	}
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithModelID(model))
	}
	if insecure, ok := cfg["insecure"].(bool); ok && insecure {
		opts = append(opts, WithInsecure())
	} else if embeddings.AllowInsecureFromEnv() {
		embeddings.LogInsecureEnvVarWarning("Baseten")
		opts = append(opts, WithInsecure())
	}
	return NewBasetenEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("baseten", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewBasetenEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
