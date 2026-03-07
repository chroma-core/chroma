package perplexity

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type contextKey struct{ name string }

var modelContextKey = contextKey{"model"}

func ContextWithModel(ctx context.Context, model string) context.Context {
	return context.WithValue(ctx, modelContextKey, model)
}

const (
	defaultBaseAPI = "https://api.perplexity.ai/v1/embeddings"
	defaultModel   = "pplx-embed-v1-0.6b"
	APIKeyEnvVar   = "PERPLEXITY_API_KEY"

	EncodingFormatBase64Int8 = "base64_int8"
	maxErrorBodyChars        = 512
)

type PerplexityClient struct {
	baseAPI       string
	customBaseURL bool
	APIKey        embeddings.Secret `json:"-" validate:"required"`
	apiKeyEnvVar  string
	defaultModel  embeddings.EmbeddingModel
	dimensions    *int
	client        *http.Client
	insecure      bool
}

func applyDefaults(c *PerplexityClient) {
	if c.client == nil {
		c.client = http.DefaultClient
	}
	if c.baseAPI == "" {
		c.baseAPI = defaultBaseAPI
	}
	if c.defaultModel == "" {
		c.defaultModel = defaultModel
	}
}

func validate(c *PerplexityClient) error {
	if err := embeddings.NewValidator().Struct(c); err != nil {
		return err
	}
	parsed, err := url.Parse(c.baseAPI)
	if err != nil {
		return errors.Wrap(err, "invalid base URL")
	}
	if !c.insecure && !strings.EqualFold(parsed.Scheme, "https") {
		return errors.New("base URL must use HTTPS scheme for secure API key transmission; use WithInsecure() to override")
	}
	if c.dimensions != nil && *c.dimensions <= 0 {
		return errors.New("dimensions must be greater than 0")
	}
	return nil
}

func NewPerplexityClient(opts ...Option) (*PerplexityClient, error) {
	client := &PerplexityClient{}

	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, errors.Wrap(err, "failed to apply Perplexity option")
		}
	}
	applyDefaults(client)
	if err := validate(client); err != nil {
		return nil, errors.Wrap(err, "failed to validate Perplexity client options")
	}
	return client, nil
}

type EmbeddingInputs struct {
	Input  string
	Inputs []string
}

func (e *EmbeddingInputs) MarshalJSON() ([]byte, error) {
	if e.Input != "" {
		return json.Marshal(e.Input)
	}
	if e.Inputs != nil {
		return json.Marshal(e.Inputs)
	}
	return nil, errors.New("EmbeddingInput has no data")
}

type CreateEmbeddingRequest struct {
	Model          string           `json:"model"`
	Input          *EmbeddingInputs `json:"input"`
	Dimensions     *int             `json:"dimensions,omitempty"`
	EncodingFormat string           `json:"encoding_format"`
}

func (c *CreateEmbeddingRequest) JSON() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal embedding request JSON")
	}
	return string(data), nil
}

type EmbeddingTypeResult struct {
	Floats []float32
}

func decodeBase64Int8Embedding(encoded string) ([]float32, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode base64 embedding")
	}

	emb := make([]float32, len(decoded))
	for i, b := range decoded {
		emb[i] = float32(int8(b))
	}
	return emb, nil
}

func cloneIntPtr(v *int) *int {
	if v == nil {
		return nil
	}
	n := *v
	return &n
}

func sanitizeErrorBody(body []byte) string {
	trimmed := strings.TrimSpace(string(body))
	runes := []rune(trimmed)
	if len(runes) <= maxErrorBodyChars {
		return trimmed
	}
	return string(runes[:maxErrorBodyChars]) + "...(truncated)"
}

func (e *EmbeddingTypeResult) UnmarshalJSON(data []byte) error {
	var encoded string
	if err := json.Unmarshal(data, &encoded); err == nil {
		floats, decodeErr := decodeBase64Int8Embedding(encoded)
		if decodeErr != nil {
			return decodeErr
		}
		e.Floats = floats
		return nil
	}

	var floats []float32
	if err := json.Unmarshal(data, &floats); err == nil {
		e.Floats = floats
		return nil
	}

	return errors.Errorf("unexpected embedding payload %s", string(data))
}

type EmbeddingResult struct {
	Object    string               `json:"object"`
	Embedding *EmbeddingTypeResult `json:"embedding"`
	Index     int                  `json:"index"`
}

type UsageResult struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type CreateEmbeddingResponse struct {
	Object string            `json:"object"`
	Data   []EmbeddingResult `json:"data"`
	Model  string            `json:"model"`
	Usage  *UsageResult      `json:"usage,omitempty"`
}

func (c *PerplexityClient) CreateEmbedding(ctx context.Context, req *CreateEmbeddingRequest) (*CreateEmbeddingResponse, error) {
	if req == nil {
		return nil, errors.New("request cannot be nil")
	}
	if req.Input == nil {
		return nil, errors.New("input is required")
	}
	if req.Model == "" {
		req.Model = string(c.defaultModel)
	}
	if req.EncodingFormat == "" {
		req.EncodingFormat = EncodingFormatBase64Int8
	}
	if req.Dimensions == nil {
		req.Dimensions = cloneIntPtr(c.dimensions)
	} else {
		req.Dimensions = cloneIntPtr(req.Dimensions)
	}

	reqJSON, err := req.JSON()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal embedding request JSON")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseAPI, bytes.NewBufferString(reqJSON))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HTTP request")
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey.Value())

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send request to Perplexity API")
	}
	defer resp.Body.Close()

	respData, err := chttp.ReadLimitedBody(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected code [%v] while making a request to %v. errors: %v", resp.Status, c.baseAPI, sanitizeErrorBody(respData))
	}
	var embeddingResponse CreateEmbeddingResponse
	if err := json.Unmarshal(respData, &embeddingResponse); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response body")
	}
	return &embeddingResponse, nil
}

var _ embeddings.EmbeddingFunction = (*PerplexityEmbeddingFunction)(nil)

type PerplexityEmbeddingFunction struct {
	apiClient *PerplexityClient
}

func NewPerplexityEmbeddingFunction(opts ...Option) (*PerplexityEmbeddingFunction, error) {
	client, err := NewPerplexityClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize Perplexity client")
	}
	return &PerplexityEmbeddingFunction{apiClient: client}, nil
}

func (e *PerplexityEmbeddingFunction) getModel(ctx context.Context) embeddings.EmbeddingModel {
	model := e.apiClient.defaultModel
	if m, ok := ctx.Value(modelContextKey).(string); ok && m != "" {
		model = embeddings.EmbeddingModel(m)
	}
	return model
}

func (e *PerplexityEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	if len(documents) == 0 {
		return embeddings.NewEmptyEmbeddings(), nil
	}

	req := &CreateEmbeddingRequest{
		Model:          string(e.getModel(ctx)),
		Input:          &EmbeddingInputs{Inputs: documents},
		Dimensions:     cloneIntPtr(e.apiClient.dimensions),
		EncodingFormat: EncodingFormatBase64Int8,
	}
	response, err := e.apiClient.CreateEmbedding(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}
	if len(response.Data) != len(documents) {
		return nil, errors.Errorf("embedding count mismatch: got %d, expected %d", len(response.Data), len(documents))
	}

	embs := make([]embeddings.Embedding, len(documents))
	for _, result := range response.Data {
		if result.Index < 0 || result.Index >= len(documents) {
			return nil, errors.Errorf("invalid embedding index %d for %d documents", result.Index, len(documents))
		}
		if result.Embedding == nil {
			return nil, errors.Errorf("nil embedding at index %d", result.Index)
		}
		embs[result.Index] = embeddings.NewEmbeddingFromFloat32(result.Embedding.Floats)
	}
	for i, emb := range embs {
		if emb == nil {
			return nil, errors.Errorf("missing embedding at index %d (duplicate or missing index in response)", i)
		}
	}
	return embs, nil
}

func (e *PerplexityEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	if document == "" {
		return nil, errors.New("query document must not be empty")
	}
	req := &CreateEmbeddingRequest{
		Model:          string(e.getModel(ctx)),
		Input:          &EmbeddingInputs{Input: document},
		Dimensions:     cloneIntPtr(e.apiClient.dimensions),
		EncodingFormat: EncodingFormatBase64Int8,
	}
	response, err := e.apiClient.CreateEmbedding(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	if len(response.Data) == 0 {
		return nil, errors.New("no embedding returned from Perplexity API")
	}
	if response.Data[0].Embedding == nil {
		return nil, errors.New("nil embedding in Perplexity API response")
	}
	return embeddings.NewEmbeddingFromFloat32(response.Data[0].Embedding.Floats), nil
}

func (e *PerplexityEmbeddingFunction) Name() string {
	return "perplexity"
}

func (e *PerplexityEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.apiClient.apiKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	cfg := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": envVar,
		"model_name":      string(e.apiClient.defaultModel),
	}
	if e.apiClient.dimensions != nil {
		cfg["dimensions"] = *e.apiClient.dimensions
	}
	if e.apiClient.customBaseURL {
		cfg["base_url"] = e.apiClient.baseAPI
	}
	if e.apiClient.insecure {
		cfg["insecure"] = true
	}
	return cfg
}

func (e *PerplexityEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *PerplexityEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

func NewPerplexityEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*PerplexityEmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	model, ok := cfg["model_name"].(string)
	if !ok || model == "" {
		return nil, errors.New("model_name is required in config")
	}

	opts := []Option{
		WithAPIKeyFromEnvVar(envVar),
		WithModel(embeddings.EmbeddingModel(model)),
	}
	if baseURL, ok := cfg["base_url"].(string); ok && baseURL != "" {
		opts = append(opts, WithBaseURL(baseURL))
	}
	if dims, ok := embeddings.ConfigInt(cfg, "dimensions"); ok {
		if dims <= 0 {
			return nil, errors.New("dimensions must be greater than 0")
		}
		opts = append(opts, WithDimensions(dims))
	}
	if insecure, ok := cfg["insecure"].(bool); ok && insecure {
		opts = append(opts, WithInsecure())
	} else if embeddings.AllowInsecureFromEnv() {
		embeddings.LogInsecureEnvVarWarning("Perplexity")
		opts = append(opts, WithInsecure())
	}
	return NewPerplexityEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("perplexity", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewPerplexityEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
