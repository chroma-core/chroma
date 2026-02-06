package morph

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/creasty/defaults"
	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

const (
	APIKeyEnvVar = "MORPH_API_KEY"
)

type CreateEmbeddingRequest struct {
	Model          string   `json:"model"`
	Input          []string `json:"input"`
	EncodingFormat string   `json:"encoding_format,omitempty"`
}

type EmbeddingData struct {
	Object    string    `json:"object"`
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
}

type Usage struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type CreateEmbeddingResponse struct {
	Object string          `json:"object"`
	Data   []EmbeddingData `json:"data"`
	Model  string          `json:"model"`
	Usage  Usage           `json:"usage"`
}

type MorphClient struct {
	BaseURL      string            `default:"https://api.morphllm.com/v1/"`
	APIKey       embeddings.Secret `json:"-" validate:"required"`
	APIKeyEnvVar string            `json:"-"`
	Client       *http.Client      `json:"-"`
	Model        string            `default:"morph-embedding-v2"`
	Insecure     bool
}

func validate(c *MorphClient) error {
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

func NewMorphClient(apiKey string, opts ...Option) (*MorphClient, error) {
	client := &MorphClient{
		APIKey: embeddings.NewSecret(apiKey),
	}
	if err := defaults.Set(client); err != nil {
		return nil, errors.Wrap(err, "failed to set defaults")
	}
	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, errors.Wrap(err, "failed to apply Morph option")
		}
	}
	if client.Client == nil {
		client.Client = &http.Client{}
	}
	if err := validate(client); err != nil {
		return nil, errors.Wrap(err, "failed to validate Morph client options")
	}
	return client, nil
}

func (c *MorphClient) CreateEmbedding(ctx context.Context, req *CreateEmbeddingRequest) (*CreateEmbeddingResponse, error) {
	if req.Model == "" {
		req.Model = c.Model
	}
	if req.EncodingFormat == "" {
		req.EncodingFormat = "float"
	}

	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal request JSON")
	}

	endpoint, err := url.JoinPath(c.BaseURL, "embeddings")
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse URL")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(reqJSON))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create http request")
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey.Value())

	resp, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send request to Morph API")
	}
	defer resp.Body.Close()

	respData, err := chttp.ReadLimitedBody(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected response %v: %v", resp.Status, string(respData))
	}

	var createEmbeddingResponse CreateEmbeddingResponse
	if err := json.Unmarshal(respData, &createEmbeddingResponse); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response body")
	}

	return &createEmbeddingResponse, nil
}

var _ embeddings.EmbeddingFunction = (*MorphEmbeddingFunction)(nil)

type MorphEmbeddingFunction struct {
	apiClient *MorphClient
}

func NewMorphEmbeddingFunction(opts ...Option) (*MorphEmbeddingFunction, error) {
	apiClient, err := NewMorphClient("", opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize Morph client")
	}
	return &MorphEmbeddingFunction{apiClient: apiClient}, nil
}

func (e *MorphEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	if len(documents) == 0 {
		return embeddings.NewEmptyEmbeddings(), nil
	}
	response, err := e.apiClient.CreateEmbedding(ctx, &CreateEmbeddingRequest{
		Input: documents,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}
	if len(response.Data) != len(documents) {
		return nil, errors.Errorf("embedding count mismatch: got %d, expected %d", len(response.Data), len(documents))
	}
	embs := make([]embeddings.Embedding, len(documents))
	for _, data := range response.Data {
		if data.Index < 0 || data.Index >= len(documents) {
			return nil, errors.Errorf("invalid embedding index %d for %d documents", data.Index, len(documents))
		}
		embs[data.Index] = embeddings.NewEmbeddingFromFloat32(data.Embedding)
	}
	for i, emb := range embs {
		if emb == nil {
			return nil, errors.Errorf("missing embedding at index %d (duplicate or missing index in response)", i)
		}
	}
	return embs, nil
}

func (e *MorphEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	response, err := e.apiClient.CreateEmbedding(ctx, &CreateEmbeddingRequest{
		Input: []string{document},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	if len(response.Data) == 0 {
		return nil, errors.New("no embedding returned from Morph API")
	}
	return embeddings.NewEmbeddingFromFloat32(response.Data[0].Embedding), nil
}

func (e *MorphEmbeddingFunction) Name() string {
	return "morph"
}

func (e *MorphEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.apiClient.APIKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	cfg := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": envVar,
		"model_name":      e.apiClient.Model,
	}
	if e.apiClient.BaseURL != "" {
		cfg["api_base"] = e.apiClient.BaseURL
	}
	return cfg
}

func (e *MorphEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *MorphEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

func NewMorphEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*MorphEmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	opts := []Option{WithAPIKeyFromEnvVar(envVar)}
	if baseURL, ok := cfg["api_base"].(string); ok && baseURL != "" {
		opts = append(opts, WithBaseURL(baseURL))
	}
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithModel(model))
	}
	if insecure, ok := cfg["insecure"].(bool); ok && insecure {
		opts = append(opts, WithInsecure())
	} else if embeddings.AllowInsecureFromEnv() {
		embeddings.LogInsecureEnvVarWarning("Morph")
		opts = append(opts, WithInsecure())
	}
	return NewMorphEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("morph", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewMorphEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
