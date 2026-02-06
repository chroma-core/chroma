package hf

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

const (
	APIKeyEnvVar = "HF_API_KEY"
)

type HuggingFaceClient struct {
	BaseURL        string
	APIKey         embeddings.Secret `json:"-"`
	APIKeyEnvVar   string
	Model          string
	Client         *http.Client
	DefaultHeaders map[string]string
	IsHFEIEndpoint bool
	Insecure       bool
}

func NewHuggingFaceClient(apiKey string, model string) *HuggingFaceClient {
	return &HuggingFaceClient{
		BaseURL: "https://router.huggingface.co/hf-inference/models/",
		Client:  &http.Client{},
		APIKey:  embeddings.NewSecret(apiKey),
		Model:   model,
	}
}

func NewHuggingFaceClientFromOptions(opts ...Option) (*HuggingFaceClient, error) {
	c := &HuggingFaceClient{
		BaseURL: "https://router.huggingface.co/hf-inference/models/",
		Client:  &http.Client{},
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, errors.Wrap(err, "failed to apply HuggingFace option")
		}
	}
	// API key is only required for the public HuggingFace Inference API,
	// not for self-hosted Text Embedding Inference (TEI) endpoints
	if !c.IsHFEIEndpoint && c.APIKey.IsEmpty() {
		return nil, errors.New("API key is required")
	}
	// HTTPS validation: only enforce when API key is being transmitted
	parsed, err := url.Parse(c.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "invalid base URL")
	}
	if !c.Insecure && !c.APIKey.IsEmpty() && !strings.EqualFold(parsed.Scheme, "https") {
		return nil, errors.New("base URL must use HTTPS scheme for secure API key transmission; use WithInsecure() to override")
	}
	return c, nil
}

type CreateEmbeddingRequest struct {
	Inputs  []string               `json:"inputs"`
	Options map[string]interface{} `json:"options"`
}

type CreateEmbeddingResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

func (c *CreateEmbeddingRequest) JSON() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal embedding request")
	}
	return string(data), nil
}

func (c *HuggingFaceClient) CreateEmbedding(ctx context.Context, req *CreateEmbeddingRequest) (*CreateEmbeddingResponse, error) {
	reqJSON, err := req.JSON()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal request to JSON")
	}
	var reqURL string
	if c.IsHFEIEndpoint {
		reqURL = c.BaseURL
	} else {
		reqURL, err = url.JoinPath(c.BaseURL, c.Model, "pipeline", "feature-extraction")
		if err != nil {
			return nil, errors.Wrap(err, "failed to build HF request URL")
		}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewBufferString(reqJSON))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HTTP request")
	}
	for k, v := range c.DefaultHeaders {
		httpReq.Header.Set(k, v)
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)
	httpReq.Header.Set("Content-Type", "application/json")
	if !c.APIKey.IsEmpty() {
		httpReq.Header.Set("Authorization", "Bearer "+c.APIKey.Value())
	}

	resp, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send request to Hugging Face API")
	}
	defer resp.Body.Close()

	respData, err := chttp.ReadLimitedBody(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected code [%v] while making a request to %v: %v", resp.Status, reqURL, string(respData))
	}

	var embds [][]float32
	if err := json.Unmarshal(respData, &embds); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response body")
	}
	var createEmbeddingResponse = CreateEmbeddingResponse{
		Embeddings: embds,
	}

	return &createEmbeddingResponse, nil
}

var _ embeddings.EmbeddingFunction = (*HuggingFaceEmbeddingFunction)(nil)

type HuggingFaceEmbeddingFunction struct {
	apiClient *HuggingFaceClient
}

func NewHuggingFaceEmbeddingFunction(apiKey string, model string) *HuggingFaceEmbeddingFunction {
	cli := &HuggingFaceEmbeddingFunction{
		apiClient: NewHuggingFaceClient(apiKey, model),
	}

	return cli
}

func NewHuggingFaceEmbeddingFunctionFromOptions(opts ...Option) (*HuggingFaceEmbeddingFunction, error) {
	cli, err := NewHuggingFaceClientFromOptions(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HuggingFace client")
	}

	return &HuggingFaceEmbeddingFunction{
		apiClient: cli,
	}, nil
}

func NewHuggingFaceEmbeddingInferenceFunction(baseURL string, opts ...Option) (*HuggingFaceEmbeddingFunction, error) {
	opts = append(opts, WithBaseURL(baseURL), WithIsHFEIEndpoint())
	cli, err := NewHuggingFaceClientFromOptions(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HFEI client")
	}

	return &HuggingFaceEmbeddingFunction{
		apiClient: cli,
	}, nil
}

func (e *HuggingFaceEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	response, err := e.apiClient.CreateEmbedding(ctx, &CreateEmbeddingRequest{
		Inputs: documents,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}
	return embeddings.NewEmbeddingsFromFloat32(response.Embeddings)
}

func (e *HuggingFaceEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	response, err := e.apiClient.CreateEmbedding(ctx, &CreateEmbeddingRequest{
		Inputs: []string{document},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	if len(response.Embeddings) == 0 {
		return nil, errors.New("no embedding returned from HuggingFace API")
	}
	return embeddings.NewEmbeddingFromFloat32(response.Embeddings[0]), nil
}

func (e *HuggingFaceEmbeddingFunction) Name() string {
	return "huggingface"
}

func (e *HuggingFaceEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.apiClient.APIKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	cfg := embeddings.EmbeddingFunctionConfig{
		"model_name":      e.apiClient.Model,
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

func (e *HuggingFaceEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *HuggingFaceEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewHuggingFaceEmbeddingFunctionFromConfig creates a HuggingFace embedding function from a config map.
// Uses schema-compliant field names: model_name, api_key_env_var, base_url, insecure.
func NewHuggingFaceEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*HuggingFaceEmbeddingFunction, error) {
	opts := make([]Option, 0)
	if envVar, ok := cfg["api_key_env_var"].(string); ok && envVar != "" {
		opts = append(opts, WithAPIKeyFromEnvVar(envVar))
	}
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithModel(model))
	}
	if baseURL, ok := cfg["base_url"].(string); ok && baseURL != "" {
		opts = append(opts, WithBaseURL(baseURL))
	}
	if insecure, ok := cfg["insecure"].(bool); ok && insecure {
		opts = append(opts, WithInsecure())
	} else if embeddings.AllowInsecureFromEnv() {
		embeddings.LogInsecureEnvVarWarning("HuggingFace")
		opts = append(opts, WithInsecure())
	}
	return NewHuggingFaceEmbeddingFunctionFromOptions(opts...)
}

func init() {
	if err := embeddings.RegisterDense("huggingface", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewHuggingFaceEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
