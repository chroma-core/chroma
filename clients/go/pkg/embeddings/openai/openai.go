package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/creasty/defaults"
	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type EmbeddingModel string

type contextKey struct{ name string }

var (
	modelContextKey      = contextKey{"model"}
	dimensionsContextKey = contextKey{"dimensions"}
)

func ContextWithModel(ctx context.Context, model string) context.Context {
	return context.WithValue(ctx, modelContextKey, model)
}

func ContextWithDimensions(ctx context.Context, dimensions *int) context.Context {
	return context.WithValue(ctx, dimensionsContextKey, dimensions)
}

const (
	TextEmbeddingAda002 EmbeddingModel = "text-embedding-ada-002"
	TextEmbedding3Small EmbeddingModel = "text-embedding-3-small"
	TextEmbedding3Large EmbeddingModel = "text-embedding-3-large"
	APIKeyEnvVar                       = "OPENAI_API_KEY"
)

type Input struct {
	Text                 string   `json:"-"`
	Texts                []string `json:"-"`
	Integers             []int    `json:"-"`
	ListOfListOfIntegers [][]int  `json:"-"`
}

func (i *Input) MarshalJSON() ([]byte, error) {
	switch {
	case i.Text != "":
		return json.Marshal(i.Text)
	case i.Texts != nil:
		return json.Marshal(i.Texts)
	case i.Integers != nil:
		return json.Marshal(i.Integers)
	case i.ListOfListOfIntegers != nil:
		return json.Marshal(i.ListOfListOfIntegers)
	default:
		return nil, fmt.Errorf("invalid input")
	}
}

type CreateEmbeddingRequest struct {
	Model      string `json:"model"`
	User       string `json:"user"`
	Input      *Input `json:"input"`
	Dimensions *int   `json:"dimensions,omitempty"`
}

func (c *CreateEmbeddingRequest) JSON() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal embedding request JSON")
	}
	return string(data), nil
}

func (c *CreateEmbeddingRequest) String() string {
	data, _ := json.Marshal(c)
	return string(data)
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

func (c *CreateEmbeddingResponse) String() string {
	data, _ := json.Marshal(c)
	return string(data)
}

type OpenAIClient struct {
	BaseURL      string            `default:"https://api.openai.com/v1/" json:"base_url,omitempty"`
	APIKey       embeddings.Secret `json:"-" validate:"required"`
	APIKeyEnvVar string            `json:"-"`
	OrgID        string            `json:"org_id,omitempty"`
	Client       *http.Client      `json:"-"`
	Model        string            `default:"text-embedding-ada-002" json:"model,omitempty"`
	Dimensions   *int              `json:"dimensions,omitempty"`
	User         string            `json:"user,omitempty"`
	Insecure     bool              `json:"insecure,omitempty"`
}

func validate(c *OpenAIClient) error {
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

func NewOpenAIClient(apiKey string, opts ...Option) (*OpenAIClient, error) {
	client := &OpenAIClient{
		APIKey: embeddings.NewSecret(apiKey),
	}
	if err := defaults.Set(client); err != nil {
		return nil, errors.Wrap(err, "failed to set defaults")
	}
	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, errors.Wrap(err, "failed to apply OpenAI option")
		}
	}
	if client.Client == nil {
		client.Client = &http.Client{}
	}
	if client.User == "" {
		client.User = chttp.ChromaGoClientUserAgent
	}
	if err := validate(client); err != nil {
		return nil, errors.Wrap(err, "failed to validate OpenAI client options")
	}
	return client, nil
}

func (c *OpenAIClient) CreateEmbedding(ctx context.Context, req *CreateEmbeddingRequest) (*CreateEmbeddingResponse, error) {
	if req.Model == "" {
		req.Model = c.Model
	}
	reqJSON, err := req.JSON()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal request JSON")
	}
	endpoint, err := url.JoinPath(c.BaseURL, "embeddings")
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

	// OpenAI Organization ID (Optional)
	if c.OrgID != "" {
		httpReq.Header.Set("OpenAI-Organization", c.OrgID)
	}

	resp, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send request to OpenAI API")
	}
	defer resp.Body.Close()

	respData, err := chttp.ReadLimitedBody(resp.Body)
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

var _ embeddings.EmbeddingFunction = (*OpenAIEmbeddingFunction)(nil)

type OpenAIEmbeddingFunction struct {
	apiClient *OpenAIClient
}

func NewOpenAIEmbeddingFunction(apiKey string, opts ...Option) (*OpenAIEmbeddingFunction, error) {
	apiClient, err := NewOpenAIClient(apiKey, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize OpenAI client")
	}
	cli := &OpenAIEmbeddingFunction{
		apiClient: apiClient,
	}

	return cli, nil
}

func ConvertToMatrix(response *CreateEmbeddingResponse) [][]float32 {
	var matrix [][]float32

	for _, embeddingData := range response.Data {
		matrix = append(matrix, embeddingData.Embedding)
	}

	return matrix
}

// getModel returns the model from the context if it exists, otherwise it returns the default model
func (e *OpenAIEmbeddingFunction) getModel(ctx context.Context) string {
	model := e.apiClient.Model
	if m, ok := ctx.Value(modelContextKey).(string); ok {
		model = m
	}
	return model
}

// getDimensions returns the dimensions from the context if it exists, otherwise it returns the default dimensions
func (e *OpenAIEmbeddingFunction) getDimensions(ctx context.Context) *int {
	dimensions := e.apiClient.Dimensions
	if dims, ok := ctx.Value(dimensionsContextKey).(*int); ok {
		dimensions = dims
	}
	return dimensions
}

func (e *OpenAIEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	response, err := e.apiClient.CreateEmbedding(ctx, &CreateEmbeddingRequest{
		User:  e.apiClient.User,
		Model: e.getModel(ctx),
		Input: &Input{
			Texts: documents,
		},
		Dimensions: e.getDimensions(ctx),
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}
	return embeddings.NewEmbeddingsFromFloat32(ConvertToMatrix(response))
}

func (e *OpenAIEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	response, err := e.apiClient.CreateEmbedding(ctx, &CreateEmbeddingRequest{
		Model: e.getModel(ctx),
		User:  e.apiClient.User,
		Input: &Input{
			Texts: []string{document},
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	matrix := ConvertToMatrix(response)
	if len(matrix) == 0 {
		return nil, errors.New("no embedding returned from OpenAI API")
	}
	return embeddings.NewEmbeddingFromFloat32(matrix[0]), nil
}

func (e *OpenAIEmbeddingFunction) Name() string {
	return "openai"
}

func (e *OpenAIEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.apiClient.APIKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	cfg := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": envVar,
		"model_name":      e.apiClient.Model,
	}
	if e.apiClient.Insecure {
		cfg["insecure"] = true
	}
	if e.apiClient.BaseURL != "" {
		cfg["api_base"] = e.apiClient.BaseURL
	}
	if e.apiClient.Dimensions != nil {
		cfg["dimensions"] = *e.apiClient.Dimensions
	}
	if e.apiClient.OrgID != "" {
		cfg["organization_id"] = e.apiClient.OrgID
	}
	return cfg
}

func (e *OpenAIEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *OpenAIEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewOpenAIEmbeddingFunctionFromConfig creates an OpenAI embedding function from a config map.
// Uses schema-compliant field names: api_key_env_var, model_name, api_base, organization_id, dimensions, insecure.
func NewOpenAIEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*OpenAIEmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	opts := []Option{WithAPIKeyFromEnvVar(envVar)}
	if baseURL, ok := cfg["api_base"].(string); ok && baseURL != "" {
		opts = append(opts, WithBaseURL(baseURL))
	}
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithModel(EmbeddingModel(model)))
	}
	if dims, ok := embeddings.ConfigInt(cfg, "dimensions"); ok && dims > 0 {
		opts = append(opts, WithDimensions(dims))
	}
	if orgID, ok := cfg["organization_id"].(string); ok && orgID != "" {
		opts = append(opts, WithOpenAIOrganizationID(orgID))
	}
	if insecure, ok := cfg["insecure"].(bool); ok && insecure {
		opts = append(opts, WithInsecure())
	} else if embeddings.AllowInsecureFromEnv() {
		embeddings.LogInsecureEnvVarWarning("OpenAI")
		opts = append(opts, WithInsecure())
	}
	return NewOpenAIEmbeddingFunction("", opts...)
}

func init() {
	if err := embeddings.RegisterDense("openai", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewOpenAIEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
