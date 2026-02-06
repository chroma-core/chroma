package gemini

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/genai"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

const (
	DefaultEmbeddingModel = "gemini-embedding-001"
	ModelContextVar       = "model"
	APIKeyEnvVar          = "GEMINI_API_KEY"
)

type Client struct {
	APIKey         embeddings.Secret `json:"-" validate:"required"`
	APIKeyEnvVar   string
	DefaultModel   embeddings.EmbeddingModel
	Client         *genai.Client
	DefaultContext *context.Context
	MaxBatchSize   int
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
		c.Client, err = genai.NewClient(*c.DefaultContext, &genai.ClientConfig{
			APIKey:  c.APIKey.Value(),
			Backend: genai.BackendGeminiAPI,
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func validate(c *Client) error {
	return embeddings.NewValidator().Struct(c)
}

func NewGeminiClient(opts ...Option) (*Client, error) {
	client := &Client{}

	for _, opt := range opts {
		err := opt(client)
		if err != nil {
			return nil, errors.Wrap(err, "failed to apply Gemini option")
		}
	}
	err := applyDefaults(client)
	if err != nil {
		return nil, err
	}
	if err := validate(client); err != nil {
		return nil, errors.Wrap(err, "failed to validate Gemini client options")
	}
	return client, nil
}

func (c *Client) CreateEmbedding(ctx context.Context, req []string) ([]embeddings.Embedding, error) {
	model := string(c.DefaultModel)
	if m, ok := ctx.Value(ModelContextVar).(string); ok {
		model = m
	}
	contents := make([]*genai.Content, len(req))
	for i, t := range req {
		contents[i] = genai.NewContentFromText(t, genai.RoleUser)
	}
	res, err := c.Client.Models.EmbedContent(ctx, model, contents, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed contents")
	}
	if res == nil || len(res.Embeddings) == 0 {
		return nil, errors.New("no embeddings returned from Gemini API")
	}
	embs := make([][]float32, 0, len(res.Embeddings))
	for _, e := range res.Embeddings {
		embs = append(embs, e.Values)
	}

	return embeddings.NewEmbeddingsFromFloat32(embs)
}

// Close is a no-op for the new genai SDK client which doesn't require cleanup.
func (c *Client) Close() error {
	return nil
}

var _ embeddings.EmbeddingFunction = (*GeminiEmbeddingFunction)(nil)
var _ embeddings.Closeable = (*GeminiEmbeddingFunction)(nil)

type GeminiEmbeddingFunction struct {
	apiClient *Client
}

func NewGeminiEmbeddingFunction(opts ...Option) (*GeminiEmbeddingFunction, error) {
	client, err := NewGeminiClient(opts...)
	if err != nil {
		return nil, err
	}

	return &GeminiEmbeddingFunction{apiClient: client}, nil
}

// Close closes the underlying client and implements the Closeable interface.
func (e *GeminiEmbeddingFunction) Close() error {
	return e.apiClient.Close()
}

func (e *GeminiEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	if e.apiClient.MaxBatchSize > 0 && len(documents) > e.apiClient.MaxBatchSize {
		return nil, errors.Errorf("number of documents exceeds the maximum batch size %v", e.apiClient.MaxBatchSize)
	}
	if len(documents) == 0 {
		return embeddings.NewEmptyEmbeddings(), nil
	}

	response, err := e.apiClient.CreateEmbedding(ctx, documents)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}
	return response, nil
}

func (e *GeminiEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	response, err := e.apiClient.CreateEmbedding(ctx, []string{document})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	if len(response) == 0 {
		return nil, errors.New("no embedding returned from Gemini API")
	}
	return response[0], nil
}

func (e *GeminiEmbeddingFunction) Name() string {
	return "google_genai"
}

func (e *GeminiEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.apiClient.APIKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	return embeddings.EmbeddingFunctionConfig{
		"model_name":      string(e.apiClient.DefaultModel),
		"api_key_env_var": envVar,
	}
}

func (e *GeminiEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *GeminiEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewGeminiEmbeddingFunctionFromConfig creates a Gemini embedding function from a config map.
// Uses schema-compliant field names: api_key_env_var, model_name.
func NewGeminiEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*GeminiEmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	opts := []Option{WithAPIKeyFromEnvVar(envVar)}
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithDefaultModel(embeddings.EmbeddingModel(model)))
	}
	return NewGeminiEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("google_genai", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewGeminiEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
